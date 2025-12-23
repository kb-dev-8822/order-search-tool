import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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

    df = pd.DataFrame(data[1:], columns=data[0])
    return df

# --- פונקציות מייל (מעודכן: מקבל נושא וגוף מוכנים) ---

def send_custom_email(subject_line):
    """
    שולח מייל עם נושא מוגדר וגוף ריק
    """
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
    # גוף ריק כמו שביקשת
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
    
    /* כפתורים בגובה אחיד */
    .stButton button {
        width: 100%;
        border-radius: 6px;
        height: 3em; 
    }
    
    /* צמצום רווחים למעלה */
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
        # מיון תאריכים
        if df.shape[1] > 9:
            try:
                filtered_df['temp_date'] = pd.to_datetime(filtered_df.iloc[:, 9], dayfirst=True, errors='coerce')
                filtered_df = filtered_df.sort_values(by='temp_date', ascending=True)
            except: pass

        # הכנת הנתונים למבנה תצוגה
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
                    "_excel_line": f"{order_num}\t{qty}\t{sku}\t{first_name}\t{street}\t{house}\t{city}\t{phone_display}",
                    "_text_line": f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, שם: {full_name}, כתובת: {address_display}, טלפון: {phone_display}, מספר משלוח: {tracking}, תאריך: {date_val}"
                })
            except IndexError: continue
        
        display_df = pd.DataFrame(display_rows)
        cols_order = ["תאריך", "מספר הזמנה", "שם לקוח", "טלפון", "כתובת מלאה", "מוצר", "כמות", "סטטוס משלוח", "בחר"]
        
        # יוצרים תצוגה רק עם העמודות הרלוונטיות
        visible_df = display_df[cols_order]

        # --- טבלה עריכה ---
        edited_df = st.data_editor(
            visible_df,
            use_container_width=True,
            hide_index=True,
            column_config={"בחר": st.column_config.CheckboxColumn("בחר", default=False)},
            disabled=["תאריך", "מספר הזמנה", "שם לקוח", "טלפון", "כתובת מלאה", "מוצר", "כמות", "סטטוס משלוח"]
        )

        # --- לוגיקה חכמה לבחירה ---
        
        is_single_result = (len(display_df) == 1)
        
        if is_single_result:
            # במקרה של שורה בודדת - לוקחים את כולה מה-Dataframe המקורי (שיש בו את השדות הנסתרים)
            target_rows = display_df.copy()
            allow_action = True
        else:
            # במקרה של ריבוי שורות - בודקים מה סומן ב-edited_df
            # ואז שולפים את השורות המלאות מ-display_df לפי האינדקס
            # (זה התיקון ל-KeyError)
            selected_indices = edited_df[edited_df["בחר"] == True].index
            target_rows = display_df.loc[selected_indices]
            
            if target_rows.empty:
                allow_action = False
            else:
                allow_action = True

        # --- אזור פעולות קומפקטי ---
        col_btn1, col_btn2, col_copy = st.columns([1, 1, 3])
        
        with col_btn1:
            if st.button("❓ מה קורה?"):
                if not allow_action:
                    st.toast("⚠️ יש לסמן שורה (כשיש מספר תוצאות)")
                else:
                    # איסוף מספרי משלוח
                    tracking_nums = []
                    for idx, row in target_rows.iterrows():
                        tn = row['סטטוס משלוח']
                        if tn and tn != "התקנה":
                            tracking_nums.append(tn)
                    
                    if not tracking_nums:
                        st.toast("⚠️ לא נמצאו מספרי משלוח בשורות שנבחרו")
                    else:
                        # יצירת המחרוזת: "123, 456"
                        joined_nums = ", ".join(tracking_nums)
                        
                        # בדיקה אם יחיד או רבים
                        if len(tracking_nums) > 1:
                            subject = f"{joined_nums} מה קורה עם אלה בבקשה?"
                        else:
                            subject = f"{joined_nums} מה קורה עם זה בבקשה?"
                        
                        if send_custom_email(subject):
                            st.success(f"נשלח מייל בנושא: {subject}")

        with col_btn2:
            if st.button("↩️ להחזיר"):
                if not allow_action:
                    st.toast("⚠️ יש לסמן שורה (כשיש מספר תוצאות)")
                else:
                    tracking_nums = []
                    for idx, row in target_rows.iterrows():
                        tn = row['סטטוס משלוח']
                        if tn and tn != "התקנה":
                            tracking_nums.append(tn)
                    
                    if not tracking_nums:
                        st.toast("⚠️ לא נמצאו מספרי משלוח בשורות שנבחרו")
                    else:
                        joined_nums = ", ".join(tracking_nums)
                        
                        # כאן הניסוח תמיד אותו דבר בערך, אבל אפשר לדייק
                        subject = f"{joined_nums} להחזיר אלינו בבקשה"
                        
                        if send_custom_email(subject):
                            st.success(f"נשלח מייל בנושא: {subject}")

        with col_copy:
            if not target_rows.empty:
                final_excel_lines = target_rows["_excel_line"].tolist()
                st.code("\n".join(final_excel_lines), language="csv")
            else:
                st.code("", language="csv")

        # --- פרטים מלאים (למטה) ---
        if not target_rows.empty:
            final_text_lines = target_rows["_text_line"].tolist()
        else:
            final_text_lines = []
            
        with st.expander("📝 העתקת פרטים מלאים (טקסט)"):
            st.code("\n".join(final_text_lines), language=None)
        
    else:
        st.warning(f"לא נמצאו תוצאות עבור: {clean_text_query}")
