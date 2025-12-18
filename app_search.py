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

# --- עיצוב CSS נקי ---
st.markdown("""
<style>
    /* כיוון כללי לימין */
    .stApp { direction: rtl; }
    
    /* יישור טקסטים וכותרות */
    .stMarkdown, h1, h3, h2, p, label, .stRadio { 
        text-align: right !important; 
        direction: rtl !important; 
    }
    
    /* יישור קלט בתיבות טקסט */
    .stTextInput input { 
        direction: rtl; 
        text-align: right; 
    }
    
    /* עיצוב הטבלה (Data Editor) */
    div[data-testid="stDataEditor"] th { text-align: right !important; direction: rtl !important; }
    div[data-testid="stDataEditor"] td { text-align: right !important; direction: rtl !important; }
    div[class*="stDataEditor"] div[role="columnheader"] { justify-content: flex-end; }
    div[class*="stDataEditor"] div[role="gridcell"] { text-align: right; direction: rtl; justify-content: flex-end; }
    
    /* יישור תוכן תיבות קוד (העתקה) */
    code {
        text-align: right !important;
        white-space: pre-wrap !important;
        direction: rtl !important;
    }
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
# עכשיו יש רק עמודה אחת רחבה לחיפוש
search_query = st.text_input("הכנס טלפון, מספר הזמנה או מספר משלוח:", "")

# --- לוגיקה חכמה (הכל ביחד) ---
if search_query:
    filtered_df = pd.DataFrame()
    
    # 1. הכנת הערכים לחיפוש
    # ניקוי רגיל (למספרי הזמנה ומשלוח)
    clean_text_query = clean_input_garbage(search_query)
    # ניקוי לטלפון (רק ספרות, בלי 0 מוביל)
    clean_phone_query = normalize_phone(clean_text_query)

    # 2. בניית המסכות (התנאים)
    conditions = []
    
    # תנאי א': מספר הזמנה (עמודה 0) - התאמה מדויקת לטקסט הנקי
    if df.shape[1] > 0:
        mask_order = df.iloc[:, 0].astype(str).str.strip() == clean_text_query
        conditions.append(mask_order)

    # תנאי ב': מספר משלוח (עמודה 8) - התאמה מדויקת לטקסט הנקי
    if df.shape[1] > 8:
        mask_tracking = df.iloc[:, 8].astype(str).str.strip() == clean_text_query
        conditions.append(mask_tracking)

    # תנאי ג': טלפון (עמודה 7) - התאמה מנורמלת
    if df.shape[1] > 7:
        # רק אם הקלט נראה כמו מספר טלפון (יש בו ספרות), נחפש בטלפונים
        if clean_phone_query: 
            mask_phone = df.iloc[:, 7].astype(str).apply(normalize_phone) == clean_phone_query
            conditions.append(mask_phone)

    # 3. ביצוע החיפוש המשולב (OR)
    if conditions:
        # מחבר את כל התנאים עם "או" (אם נמצא בהזמנה או במשלוח או בטלפון)
        final_mask = conditions[0]
        for condition in conditions[1:]:
            final_mask = final_mask | condition
            
        filtered_df = df[final_mask].copy()

    # --- תוצאות ---
    if not filtered_df.empty:
        st.write(f"### נמצאו {len(filtered_df)} הזמנות:")
        
        if df.shape[1] > 9:
            try:
                filtered_df['temp_date'] = pd.to_datetime(filtered_df.iloc[:, 9], dayfirst=True, errors='coerce')
                filtered_df = filtered_df.sort_values(by='temp_date', ascending=True)
            except: pass

        display_rows = []
        
        for index, row in filtered_df.iterrows():
            try:
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

                display_rows.append({
                    "מספר הזמנה": order_num,
                    "שם לקוח": full_name,
                    "טלפון": phone_display,
                    "כתובת מלאה": address_display,
                    "מוצר": sku,
                    "כמות": qty,
                    "סטטוס משלוח": tracking,
                    "תאריך": date_val,
                    "בחר": False,
                    # שדות נסתרים להעתקה
                    "_excel_line": f"{order_num}\t{qty}\t{sku}\t{first_name}\t{street}\t{house}\t{city}\t{phone_display}",
                    "_text_line": f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, שם: {full_name}, כתובת: {address_display}, טלפון: {phone_display}, מספר משלוח: {tracking}, תאריך: {date_val}"
                })

            except IndexError: continue
        
        # --- בניית הטבלה ---
        display_df = pd.DataFrame(display_rows)
        cols_order = ["תאריך", "מספר הזמנה", "שם לקוח", "טלפון", "כתובת מלאה", "מוצר", "כמות", "סטטוס משלוח", "בחר"]
        visible_df = display_df[cols_order]

        st.info("💡 סמן בתיבת הבחירה (מימין) את השורות להעתקה:")
        
        edited_df = st.data_editor(
            visible_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "בחר": st.column_config.CheckboxColumn("בחר", default=False)
            },
            disabled=["תאריך", "מספר הזמנה", "שם לקוח", "טלפון", "כתובת מלאה", "מוצר", "כמות", "סטטוס משלוח"]
        )

        # --- לוגיקת בחירה ---
        selected_rows = edited_df[edited_df["בחר"] == True]
        
        if selected_rows.empty:
            final_indices = display_df.index
            msg = "מעתיק את כל השורות (לא נבחר ספציפי)"
        else:
            final_indices = selected_rows.index
            msg = f"נבחרו {len(selected_rows)} שורות להעתקה"

        final_excel_lines = display_df.loc[final_indices, "_excel_line"].tolist()
        final_text_lines = display_df.loc[final_indices, "_text_line"].tolist()

        if not selected_rows.empty:
            st.success(msg)

        # --- בלוקי העתקה ---
        st.caption("👇 העתק מכאן לאקסל (טאבים מפרידים לעמודות)")
        st.code("\n".join(final_excel_lines), language="csv")

        st.markdown("### 📋 העתקת פרטים מלאים")
        st.code("\n".join(final_text_lines), language=None)
        
    else:
        st.warning(f"לא נמצאו תוצאות עבור: {clean_text_query}")
