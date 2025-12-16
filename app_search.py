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
    
    # טעינה מתוך Secrets
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

def normalize_search_input(phone_input):
    # 1. משאיר רק ספרות (מעיף +, -, רווחים)
    # דוגמה: "+972 54-123" הופך ל- "97254123"
    clean_digits = ''.join(filter(str.isdigit, str(phone_input)))
    
    # 2. אם המספר מתחיל ב-972, נחתוך את הקידומת הזו
    if clean_digits.startswith('972'):
        clean_digits = clean_digits[3:]
        
    # 3. אם יש 0 בהתחלה (למשל הזינו 054...), נוריד אותו
    # כדי שיתאים לפורמט בשיטס (54...)
    if clean_digits.startswith('0'):
        return clean_digits[1:]
        
    return clean_digits

# --- עיצוב CSS (RTL) ---
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

# טעינה
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
    
    # 1. חיפוש לפי טלפון (עם הלוגיקה החדשה ל-972)
    if search_type == "טלפון":
        search_val = normalize_search_input(search_query)
        if df.shape[1] > 7: # עמודה H
            mask = df.iloc[:, 7].astype(str) == search_val
            filtered_df = df[mask].copy()
            
    # 2. חיפוש לפי מספר הזמנה
    elif search_type == "מספר הזמנה":
        search_val = search_query.strip()
        if df.shape[1] > 0: # עמודה A
            mask = df.iloc[:, 0].astype(str) == search_val
            filtered_df = df[mask].copy()

    # 3. חיפוש לפי מספר משלוח
    else: 
        search_val = search_query.strip()
        if df.shape[1] > 8: # עמודה I
            mask = df.iloc[:, 8].astype(str) == search_val
            filtered_df = df[mask].copy()

    # --- תוצאות ---
    if not filtered_df.empty:
        st.write(f"### נמצאו {len(filtered_df)} הזמנות:")
        if df.shape[1] > 9:
            try:
                filtered_df['temp_date'] = pd.to_datetime(filtered_df.iloc[:, 9], dayfirst=True, errors='coerce')
                filtered_df = filtered_df.sort_values(by='temp_date', ascending=True)
            except: pass

        table_rows = []
        copy_texts = []

        for index, row in filtered_df.iterrows():
            try:
                order_num = row.iloc[0]
                qty = row.iloc[1]
                sku = row.iloc[2]
                name = row.iloc[3]
                addr_parts = [str(row.iloc[i]) for i in [4, 5, 6] if pd.notna(row.iloc[i])]
                address = " ".join(addr_parts)
                phone_raw = row.iloc[7]
                phone_display = "0" + str(phone_raw) if phone_raw else ""
                tracking = row.iloc[8]
                if pd.isna(tracking) or str(tracking).strip() == "": tracking = "התקנה"
                date_val = row.iloc[9]

                table_rows.append({
                    "תאריך": date_val, "מספר הזמנה": order_num, "שם לקוח": name,
                    "טלפון": phone_display, "כתובת": address, "מוצר": sku,
                    "כמות": qty, "סטטוס": tracking
                })

                formatted_text = (f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, "
                                  f"שם: {name}, כתובת: {address}, טלפון: {phone_display}, "
                                  f"מספר משלוח: {tracking}, תאריך: {date_val}")
                copy_texts.append(formatted_text)
            except IndexError: continue

        final_df = pd.DataFrame(table_rows)
        cols_order_rtl = ["סטטוס", "כמות", "מוצר", "כתובת", "טלפון", "שם לקוח", "מספר הזמנה", "תאריך"]
        existing_cols = [c for c in cols_order_rtl if c in final_df.columns]
        
        st.dataframe(final_df[existing_cols], use_container_width=True, hide_index=True)
        
        st.markdown("### 📋 העתקה מהירה")
        st.code("\n".join(copy_texts), language=None)
    else:
        st.warning(f"לא נמצאו הזמנות עבור {search_type}: {search_query}")
