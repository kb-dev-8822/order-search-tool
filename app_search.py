import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import time

# --- הגדרת תצוגה רחבה ---
st.set_page_config(layout="wide", page_title="איתור הזמנות", page_icon="🔎")

# --- הגדרות קבועות ---
SPREADSHEET_ID = '1xUABIGIhnLxO2PYrpAOXZdk48Q-hNYOHkht2vUyaVdE'
WORKSHEET_NAME = "הזמנות"
LOG_COLUMN_NAME = "לוג מיילים"

# -------------------------------------------

@st.cache_data(ttl=60)
def load_data():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    if "gcp_service_account" in st.secrets:
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        st.error("לא נמצא מפתח חיבור (Secrets - GCP). נא להגדיר אותו.")
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

    headers = data[0]
    df = pd.DataFrame(data[1:], columns=headers)
    
    # שמירת אינדקס שורה מקורי
    df['original_row_idx'] = range(2, len(data) + 1)
    
    if LOG_COLUMN_NAME not in df.columns:
        df[LOG_COLUMN_NAME] = ""
        
    return df

# --- Write-Back ---
def update_log_in_sheet(row_idx, message):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    
    sh = client.open_by_key(SPREADSHEET_ID)
    sheet = sh.worksheet(WORKSHEET_NAME)
    
    headers = sheet.row_values(1)
    try:
        col_idx = headers.index(LOG_COLUMN_NAME) + 1
    except ValueError:
        col_idx = len(headers) + 1
        sheet.update_cell(1, col_idx, LOG_COLUMN_NAME)
        
    timestamp = datetime.now().strftime("%d/%m %H:%M")
    full_msg = f"📧 {message} ({timestamp})"
    
    sheet.update_cell(row_idx, col_idx, full_msg)
    load_data.clear()
    return full_msg

# --- פונקציות מייל ---

def send_custom_email(subject_line):
    if "email" not in st.secrets:
        st.error("חסרות הגדרות אימייל ב-Secrets.")
        return False

    sender = st.secrets["email"]["sender_address"]
    password = st.secrets["email"]["password"]
    recipient = st.secrets["email"]["recipient_address"]

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject_line
    msg.attach(MIMEText("", 'plain', 'utf-8'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        st.error(f"שגיאה בשליחת מייל: {e}")
        return False

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
    
    div[data-testid="stDataEditor"] th { text-align: right !important; direction: rtl !important; }
    div[data-testid="stDataEditor"] td { text-align: right !important; direction: rtl !important; }
    div[class*="stDataEditor"] div[role="columnheader"] { justify-content: flex-end; }
    div[class*="stDataEditor"] div[role="gridcell"] { text-align: right; direction: rtl; justify-content: flex-end; }
    
    code { text-align: right !important; white-space: pre-wrap !important; direction: rtl !important; }
    
    .stButton button {
        width: 100%;
        border-radius: 6px;
        height: 3em; 
    }
    
    .block-container {
        padding-top: 2rem;
        padding-bottom: 1rem;
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

# --- חיפוש ---
search_query = st.text_input("הכנס טלפון, מספר הזמנה או מספר משלוח:", "")

# --- לוגיקה ---
if search_query:
    filtered_df = pd.DataFrame()
    clean_text_query = clean_input_garbage(search_query)
    clean_phone_query = normalize_phone(clean_text_query)

    conditions = []
    
    if df.shape[1] > 0:
        col_orders = df.iloc[:, 0].astype(str).apply(clean_input_garbage)
        mask_order = col_orders.str.startswith(clean_text_query)
        conditions.append(mask_order)

    if df.shape[1] > 8:
        col_tracking = df.iloc[:, 8].astype(str).apply(clean_input_garbage)
        mask_tracking = col_tracking == clean_text_query
        conditions.append(mask_tracking)

    if df.shape[1] > 7:
        if clean_phone_query: 
            mask_phone = df.iloc[:, 7].astype(str).apply(normalize_phone) == clean_phone_query
            conditions.append(mask_phone)

    if conditions:
        final_mask = conditions[0]
        for condition in conditions[1:]:
            final_mask = final_mask | condition
        filtered_df = df[final_mask].copy()

    # --- הצגת תוצאות ---
    if not filtered_df.empty:
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
                
                log_val = str(row.get(LOG_COLUMN_NAME, ""))
                original_idx = row.get('original_row_idx', 0)

                display_rows.append({
                    "מספר הזמנה": order_num,
                    "שם לקוח": full_name,
                    "טלפון": phone_display,
                    "כתובת מלאה": address_display,
                    "מוצר": sku,
                    "כמות": qty,
                    "סטטוס משלוח": tracking,
                    "תאריך": date_val,
                    LOG_COLUMN_NAME: log_val,
                    "בחר": False,
                    "_excel_line": f"{order_num}\t{qty}\t{sku}\t{first_name}\t{street}\t{house}\t{city}\t{phone_display}",
                    "_text_line": f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, שם: {full_name}, כתובת: {address_display}, טלפון: {phone_display}, מספר משלוח: {tracking}, תאריך: {date_val}",
                    "_original_row": original_idx
                })
            except IndexError: continue
        
        display_df = pd.DataFrame(display_rows)
        cols_order = ["תאריך", "מספר הזמנה", "שם לקוח", "טלפון", "כתובת מלאה", "מוצר", "כמות", "סטטוס משלוח", LOG_COLUMN_NAME, "בחר"]
        
        edited_df = st.data_editor(
            display_df[cols_order],
            use_container_width=True,
            hide_index=True,
            column_config={
                "בחר": st.column_config.CheckboxColumn("בחר", default=False),
                LOG_COLUMN_NAME: st.column_config.TextColumn("לוג מיילים", disabled=True)
            },
            disabled=["תאריך", "מספר הזמנה", "שם לקוח", "טלפון", "כתובת מלאה", "מוצר", "כמות", "סטטוס משלוח", LOG_COLUMN_NAME]
        )

        selected_indices = edited_df[edited_df["בחר"] == True].index

        if len(display_df) == 1:
            target_rows = display_df.copy()
            allow_action = True
        else:
            if selected_indices.empty:
                target_rows = display_df
                rows_for_email = pd.DataFrame() 
            else:
                target_rows = display_df.loc[selected_indices]
                rows_for_email = target_rows
            
            allow_action = not rows_for_email.empty if not selected_indices.empty else False

        col_btn1, col_btn2, col_copy = st.columns([1, 1, 3])
        
        with col_btn1:
            if st.button("❓ מה קורה?"):
                if len(display_df) > 1 and selected_indices.empty:
                    st.toast("⚠️ למייל: נא לסמן שורות")
                else:
                    current_email_rows = target_rows if len(display_df) == 1 else rows_for_email
                    
                    tracking_nums = []
                    rows_to_update = [] 
                    
                    # בדיקת כפילות לא חוסמת - רק התראה בטוסט
                    duplicate_alert = False
                    
                    for idx, row in current_email_rows.iterrows():
                        tn = row['סטטוס משלוח']
                        log_status = row[LOG_COLUMN_NAME]
                        
                        if "נשלח בדיקה" in str(log_status):
                            duplicate_alert = True
                            
                        if tn and tn != "התקנה":
                            tracking_nums.append(tn)
                            rows_to_update.append(row['_original_row'])
                    
                    if duplicate_alert:
                        st.toast("⚠️ שים לב: לחלק מההזמנות כבר נשלח מייל בעבר. שולח שוב...")
                        time.sleep(1) # השהייה קטנה שתספיק לקרוא

                    if not tracking_nums:
                        st.toast("⚠️ לא נמצאו מספרי משלוח תקינים לשליחה")
                    else:
                        joined_nums = ", ".join(tracking_nums)
                        subject = f"{joined_nums} מה קורה עם זה בבקשה?" if len(tracking_nums)==1 else f"{joined_nums} מה קורה עם אלה בבקשה?"
                        
                        if send_custom_email(subject):
                            st.success(f"נשלח: {subject}")
                            # עדכון בשיטס (דורס את הישן עם תאריך חדש)
                            for r_idx in rows_to_update:
                                update_log_in_sheet(r_idx, "נשלח בדיקה")
                            time.sleep(1)
                            st.rerun()

        with col_btn2:
            if st.button("↩️ להחזיר"):
                if len(display_df) > 1 and selected_indices.empty:
                    st.toast("⚠️ למייל: נא לסמן שורות")
                else:
                    current_email_rows = target_rows if len(display_df) == 1 else rows_for_email
                    
                    tracking_nums = []
                    for idx, row in current_email_rows.iterrows():
                        tn = row['סטטוס משלוח']
                        if tn and tn != "התקנה":
                            tracking_nums.append(tn)
                    
                    if not tracking_nums:
                        st.toast("⚠️ לא נמצאו מספרי משלוח לשליחה")
                    else:
                        joined_nums = ", ".join(tracking_nums)
                        subject = f"{joined_nums} להחזיר אלינו בבקשה"
                        if send_custom_email(subject):
                            st.success(f"נשלח: {subject}")

        with col_copy:
            if not target_rows.empty:
                final_excel_lines = target_rows["_excel_line"].tolist()
                st.code("\n".join(final_excel_lines), language="csv")
            else:
                st.code("", language="csv")

        final_text_lines = target_rows["_text_line"].tolist() if not target_rows.empty else []
        with st.expander("📝 העתקת פרטים מלאים (טקסט)"):
            st.code("\n".join(final_text_lines), language=None)
        
    else:
        st.warning(f"לא נמצאו תוצאות עבור: {clean_text_query}")
