import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# --- הגדרת תצוגה רחבה ---
st.set_page_config(layout="wide", page_title="איתור הזמנות", page_icon="🔎")

# --- הגדרות קבועות ---
SPREADSHEET_ID = '1xUABIGIhnLxO2PYrpAOXZdk48Q-hNYOHkht2vUyaVdE'
WORKSHEET_NAME = "הזמנות"

# -------------------------------------------

@st.cache_data
def load_data():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        st.error("לא נמצא מפתח חיבור (Secrets). נא להגדיר אותו בהגדרות האפליקציה.")
        st.stop()

    client = gspread.authorize(creds)
    
    try:
        sh = client.open_by_key(SPREADSHEET_ID)
        sheet = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        raise ValueError(f"לא נמצאה לשונית בשם '{WORKSHEET_NAME}' בגיליון.")
    
    data = sheet.get_all_values()
    if not data:
        raise ValueError("הגיליון ריק")

    df = pd.DataFrame(data[1:], columns=data[0])
    return df

# --- פונקציות ניקוי ---

def normalize_phone(phone_input):
    if not phone_input: return ""
    clean_digits = ''.join(filter(str.isdigit, str(phone_input)))
    if clean_digits.startswith('972'):
        clean_digits = clean_digits[3:]
    if clean_digits.startswith('0'):
        return clean_digits[1:]
    return clean_digits

def clean_input_garbage(val):
    if not isinstance(val, str): val = str(val)
    garbage_chars = ['\u200f', '\u200e', '\u202a', '\u202b', '\u202c', '\u202d', '\u202e', '\u00a0', '\t', '\n', '\r']
    cleaned_val = val
    for char in garbage_chars:
        cleaned_val = cleaned_val.replace(char, '')
    return cleaned_val.strip()

# --- עיצוב CSS ---
st.markdown("""
<style>
    .stApp { direction: rtl; }
    .stMarkdown, h1, h3, h2, p, label, .stRadio { text-align: right !important; direction: rtl !important; }
    .stTextInput input { direction: rtl; text-align: right; }
    div[data-testid="stDataFrame"] th { text-align: right !important; direction: rtl !important; }
    div[data-testid="stDataFrame"] td { text-align: right !important; direction: rtl !important; }
    div[class*="stDataFrame"] div[role="columnheader"] { justify-content: flex-end; }
    div[class*="stDataFrame"] div[role="gridcell"] { text-align: right; direction: rtl; justify-content: flex-end; }
    code { direction: rtl; white-space: pre-wrap !important; text-align: right; }
    div[role="radiogroup"] { direction: rtl; text-align: right; justify-content: flex-end; }
</style>
""", unsafe_allow_html=True)

st.title("🔎 איתור הזמנות מהיר")

try:
    with st.spinner('טוען נתונים...'):
        df = load_data()
    st.success(f"הנתונים נטענו בהצלחה! סה\"כ {len(df)} שורות.")
except Exception as e:
    st.error(f"שגיאה: {e}")
    st.stop()

# --- אזור החיפוש ---
col_search, col_radio = st.columns([3, 1])

with col_radio:
    search_type = st.radio("חפש לפי:", ("טלפון", "מספר הזמנה", "מספר משלוח"), horizontal=True)

with col_search:
    search_query = st.text_input(f"הכנס {search_type} לחיפוש:", "")

# --- לוגיקה ---
if search_query:
    filtered_df = pd.DataFrame()
    clean_query = clean_input_garbage(search_query)

    if search_type == "טלפון":
        search_val = normalize_phone(clean_query)
        if df.shape[1] > 7:
            mask = df.iloc[:, 7].astype(str).apply(normalize_phone) == search_val
            filtered_df = df[mask].copy()
            
    elif search_type == "מספר הזמנה":
        if df.shape[1] > 0:
            mask = df.iloc[:, 0].astype(str).str.strip() == clean_query
            filtered_df = df[mask].copy()

    else: # מספר משלוח
        if df.shape[1] > 8:
            mask = df.iloc[:, 8].astype(str).str.strip() == clean_query
            filtered_df = df[mask].copy()

    # --- תוצאות ---
    if not filtered_df.empty:
        st.write(f"### נמצאו {len(filtered_df)} הזמנות:")
        
        if df.shape[1] > 9:
            try:
                filtered_df['temp_date'] = pd.to_datetime(filtered_df.iloc[:, 9], dayfirst=True, errors='coerce')
                filtered_df = filtered_df.sort_values(by='temp_date', ascending=True)
            except: pass

        excel_copy_lines = []
        full_text_copy_lines = []
        display_rows = []

        for index, row in filtered_df.iterrows():
            try:
                # חילוץ נתונים
                order_num = str(row.iloc[0]).strip()
                qty = str(row.iloc[1]).strip()
                sku = str(row.iloc[2]).strip()
                full_name = str(row.iloc[3]).strip()
                
                street = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else ""
                house = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ""
                city = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""
                
                address_display = f"{street} {house} {city}".strip()
                
                phone_raw = row.iloc[7]
                phone_clean = normalize_phone(phone_raw)
                phone_display = "0" + phone_clean if phone_clean else ""
                
                tracking = row.iloc[8]
                if pd.isna(tracking) or str(tracking).strip() == "": tracking = "התקנה"
                
                date_val = str(row.iloc[9]).strip()

                first_name = full_name.split()[0] if full_name else ""

                # 1. שורה לתצוגה בטבלה
                display_rows.append({
                    "מספר הזמנה": order_num,
                    "שם לקוח": full_name,
                    "טלפון": phone_display,
                    "כתובת מלאה": address_display,
                    "מוצר": sku,
                    "כמות": qty,
                    "סטטוס משלוח": tracking,
                    "תאריך": date_val
                })

                # 2. שורה להעתקה לאקסל (טאבים)
                # סדר מעודכן: הזמנה -> כמות -> מק"ט -> שם פרטי -> רחוב -> בית -> עיר -> טלפון
                excel_line = f"{order_num}\t{qty}\t{sku}\t{first_name}\t{street}\t{house}\t{city}\t{phone_display}"
                excel_copy_lines.append(excel_line)

                # 3. שורה להעתקת טקסט מלא
                text_line = (f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, "
                             f"שם: {full_name}, כתובת: {address_display}, טלפון: {phone_display}, "
                             f"מספר משלוח: {tracking}, תאריך: {date_val}")
                full_text_copy_lines.append(text_line)

            except IndexError: continue

        # --- הצגת הטבלה ---
        st.dataframe(
            pd.DataFrame(display_rows),
            use_container_width=True,
            hide_index=True
        )

        # --- אזור העתקה לאקסל ---
        st.info("👇 העתק מכאן לאקסל (הוספנו מק\"ט אחרי הכמות)")
        excel_string_final = "\n".join(excel_copy_lines)
        st.code(excel_string_final, language="csv")

        # --- אזור העתקה טקסט מלא (פתוח תמיד) ---
        st.markdown("### 📋 העתקת פרטים מלאים")
        st.code("\n".join(full_text_copy_lines), language=None)
        
    else:
        st.warning(f"לא נמצאו הזמנות עבור {search_type}: {clean_query}")
