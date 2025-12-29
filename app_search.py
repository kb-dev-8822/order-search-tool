import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import time
import requests

# --- הגדרת תצוגה רחבה ---
st.set_page_config(layout="wide", page_title="איתור הזמנות", page_icon="🔎")

# --- הגדרות קבועות ---
SPREADSHEET_ID = '1xUABIGIhnLxO2PYrpAOXZdk48Q-hNYOHkht2vUyaVdE'
WORKSHEET_NAME = "הזמנות"
LOG_COLUMN_NAME = "לוג מיילים"

# -------------------------------------------

@st.cache_data # ללא ttl - מקסימום מהירות
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
        
    current_val = sheet.cell(row_idx, col_idx).value or ""
    
    timestamp = datetime.now().strftime("%d/%m %H:%M")
    new_entry = f"{message} ({timestamp})"
    
    if current_val:
        full_msg = f"{current_val} | {new_entry}"
    else:
        full_msg = new_entry
    
    sheet.update_cell(row_idx, col_idx, full_msg)
    load_data.clear()
    return full_msg

# --- פונקציות וואטסאפ (UltraMsg) ---

def normalize_phone_for_api(phone_input):
    if not phone_input: return None
    digits = ''.join(filter(str.isdigit, str(phone_input)))
    if not digits: return None
    if digits.startswith('972'):
        return digits 
    if digits.startswith('0'):
        return '972' + digits[1:] 
    if len(digits) == 9:
        return '972' + digits
    return digits 

def send_whatsapp_message(phone, message_body):
    if "ultramsg" not in st.secrets:
        st.error("חסרות הגדרות UltraMsg ב-Secrets.")
        return False

    instance_id = st.secrets["ultramsg"]["instance_id"]
    token = st.secrets["ultramsg"]["token"]
    
    clean_phone = normalize_phone_for_api(phone)
    if not clean_phone:
        return False
        
    url = f"https://api.ultramsg.com/{instance_id}/messages/chat"
    
    payload = {
        "token": token,
        "to": clean_phone,
        "body": message_body
    }
    
    try:
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200 and 'sent' in response.text:
            return True
        else:
            st.error(f"שגיאה בשליחת וואטסאפ: {response.text}")
            return False
    except Exception as e:
        st.error(f"תקלה בשליחה: {e}")
        return False

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
    
    def check_order_match(val, query):
        val = str(val).strip()
        if val == query:
            return True
        if '-' in val:
            parts = val.split('-')
            if parts[0].strip() == query:
                return True
        return False

    if df.shape[1] > 0:
        col_orders = df.iloc[:, 0].astype(str).apply(clean_input_garbage)
        mask_order = col_orders.apply(lambda x: check_order_match(x, clean_text_query))
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
                
                raw_phone_for_wa = str(phone_raw).strip()

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
                    "_original_row": original_idx,
                    "_raw_phone": raw_phone_for_wa
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
                LOG_COLUMN_NAME: st.column_config.TextColumn("לוג", disabled=True)
            },
            disabled=["תאריך", "מספר הזמנה", "שם לקוח", "טלפון", "כתובת מלאה", "מוצר", "כמות", "סטטוס משלוח", LOG_COLUMN_NAME]
        )

        selected_indices = edited_df[edited_df["בחר"] == True].index

        if selected_indices.empty:
            rows_for_action = display_df 
            is_implicit_select_all = True
        else:
            rows_for_action = display_df.loc[selected_indices]
            is_implicit_select_all = False
            
        if is_implicit_select_all and len(rows_for_action) > 10:
            show_bulk_warning = True
        else:
            show_bulk_warning = False

        # --- אזור הכפתורים (4 עמודות) ---
        col_wa_policy, col_wa_contact, col_mail_status, col_mail_return = st.columns([1.2, 1.2, 0.8, 0.8])
        
        # 1. וואטסאפ מדיניות
        with col_wa_policy:
            if show_bulk_warning:
                 st.warning("⚠️ סמן ידנית")
            else:
                if st.button("💬 שלח מדיניות"):
                    if rows_for_action.empty:
                        st.toast("⚠️ אין נתונים")
                    else:
                        count_sent = 0
                        rows_to_update_log = []
                        grouped = rows_for_action.groupby('_raw_phone')
                        for phone, group in grouped:
                            if not phone: continue
                            orders_str = ", ".join(group['מספר הזמנה'].unique())
                            skus_str = ", ".join(group['מוצר'].unique())
                            client_name = group.iloc[0]['שם לקוח'].split()[0] if group.iloc[0]['שם לקוח'] else "לקוח"
                            
                            msg_body = f"""שלום {client_name},
מדברים לגבי הזמנה/ות: {orders_str}.
מוצרים: {skus_str}.
הבנתי שיש בעיה במוצר/ים (פגם או חוסר בחלקים) או שאתה פשוט מעוניין להחזיר.

שים לב לאפשרויות הטיפול:
1. אם זו *החזרה רגילה* (מוצר לא פגום) - הזיכוי יהיה בניכוי דמי משלוח (99 ש"ח) על כל חבילה שחוזרת. אנא שלח לנו תמונה של המוצר כשהוא ארוז חזרה עם מסקינטייפ, כדי שנוכל לתאם שליח לאיסוף (עד 7 ימי עסקים מרגע קבלת התמונה).

2. אם זה *מוצר פגום* - אנא שלח לנו תמונות ברורות של הפגמים, ונציג מטעמנו יחזור אליך לגבי המשך הטיפול (עד 3 ימי עסקים).

3. במידה ו*חסרים חלקים* - נא לשלוח לנו את מספרי החלקים החסרים במדויק לפי דף ההוראות (מופיע בחוברת ההרכבה), ונדאג להשלים לך אותם.

תודה!"""
                            if send_whatsapp_message(phone, msg_body):
                                count_sent += 1
                                rows_to_update_log.extend(group['_original_row'].tolist())
                                st.toast(f"נשלח ל-{client_name} ✅")
                        
                        if count_sent > 0:
                            for r_idx in rows_to_update_log:
                                update_log_in_sheet(r_idx, "💬 נשלח ווצאפ מדיניות")
                            time.sleep(1)
                            st.rerun()

        # 2. וואטסאפ "חזרנו אליך" (החדש)
        with col_wa_contact:
            if show_bulk_warning:
                 st.warning("⚠️ סמן ידנית")
            else:
                if st.button("📞 חזרנו אליך"):
                    if rows_for_action.empty:
                        st.toast("⚠️ אין נתונים")
                    else:
                        count_sent = 0
                        rows_to_update_log = []
                        grouped = rows_for_action.groupby('_raw_phone')
                        for phone, group in grouped:
                            if not phone: continue
                            
                            orders_str = ", ".join(group['מספר הזמנה'].unique())
                            skus_str = ", ".join(group['מוצר'].unique())
                            tracking_str = ", ".join(group['סטטוס משלוח'].unique())
                            client_name = group.iloc[0]['שם לקוח'].split()[0] if group.iloc[0]['שם לקוח'] else "לקוח"
                            
                            msg_body = f"""היי {client_name},
חוזרים אלייך מסלימפרייס לגבי הזמנה/ות: {orders_str}
מוצרים: {skus_str}
מס משלוח/ים: {tracking_str}

קיבלנו פנייה שחיפשת אותנו, איך אפשר לעזור?"""
                            
                            if send_whatsapp_message(phone, msg_body):
                                count_sent += 1
                                rows_to_update_log.extend(group['_original_row'].tolist())
                                st.toast(f"נשלח ל-{client_name} ✅")
                        
                        if count_sent > 0:
                            for r_idx in rows_to_update_log:
                                update_log_in_sheet(r_idx, "💬 נשלח 'חזרנו אליך'")
                            time.sleep(1)
                            st.rerun()

        # 3. מייל סטטוס
        with col_mail_status:
            if show_bulk_warning:
                 st.warning("⚠️ סמן ידנית")
            else:
                if st.button("❓ מה קורה?"):
                    if rows_for_action.empty:
                        st.toast("⚠️ אין נתונים")
                    else:
                        tracking_nums = []
                        rows_to_update = []
                        duplicate_alert = False
                        for idx, row in rows_for_action.iterrows():
                            tn = row['סטטוס משלוח']
                            if "נשלח בדיקה" in str(row[LOG_COLUMN_NAME]):
                                duplicate_alert = True
                            if tn and tn != "התקנה":
                                tracking_nums.append(tn)
                                rows_to_update.append(row['_original_row'])
                        
                        if duplicate_alert:
                            st.toast("⚠️ שים לב: כבר נשלח בעבר")
                            time.sleep(1)

                        if not tracking_nums:
                            st.toast("⚠️ אין מספרי משלוח")
                        else:
                            tracking_nums = list(set(tracking_nums))
                            joined_nums = ", ".join(tracking_nums)
                            subject = f"{joined_nums} מה קורה עם זה בבקשה?" if len(tracking_nums)==1 else f"{joined_nums} מה קורה עם אלה בבקשה?"
                            if send_custom_email(subject):
                                st.success(f"נשלח: {subject}")
                                for r_idx in rows_to_update:
                                    update_log_in_sheet(r_idx, "📧 נשלח בדיקה")
                                time.sleep(1)
                                st.rerun()

        # 4. מייל החזרה
        with col_mail_return:
            if show_bulk_warning:
                 st.warning("⚠️ סמן ידנית")
            else:
                if st.button("↩️ להחזיר"):
                    if rows_for_action.empty:
                        st.toast("⚠️ אין נתונים")
                    else:
                        tracking_nums = []
                        for idx, row in rows_for_action.iterrows():
                            tn = row['סטטוס משלוח']
                            if tn and tn != "התקנה":
                                tracking_nums.append(tn)
                        if not tracking_nums:
                            st.toast("⚠️ אין מספרי משלוח")
                        else:
                            tracking_nums = list(set(tracking_nums))
                            joined_nums = ", ".join(tracking_nums)
                            subject = f"{joined_nums} להחזיר אלינו בבקשה"
                            if send_custom_email(subject):
                                st.success(f"נשלח: {subject}")

        # --- העתקה (פתוח תמיד) ---
        st.divider()
        if not rows_for_action.empty and not show_bulk_warning:
            final_excel_lines = rows_for_action["_excel_line"].tolist()
            st.caption("העתקה לאקסל (שורות נבחרות):")
            st.code("\n".join(final_excel_lines), language="csv")
            
            final_text_lines = rows_for_action["_text_line"].tolist()
            st.caption("📝 פרטים מלאים להעתקה:")
            st.code("\n".join(final_text_lines), language=None)
        
    else:
        st.warning(f"לא נמצאו תוצאות עבור: {clean_text_query}")


