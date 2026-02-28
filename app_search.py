import streamlit as st
import pandas as pd
import psycopg2
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import time
import re

# --- הגדרת תצוגה ---
st.set_page_config(layout="wide", page_title="איתור הזמנות", page_icon="🔎")

# ==========================================
# 🔐 מנגנון אבטחה (Login)
# ==========================================
def check_password():
    st.markdown("""
        <style>
            h1, h2, h3, h4, h5, h6, .stTextInput > label, .stTextInput input, div[data-testid="stMarkdownContainer"] p {
                direction: rtl !important;
                text-align: right !important;
            }
            .stTextInput > label {
                width: 100%;
                display: flex;
                justify-content: flex-start;
            }
            .stButton button {
                text-align: center;
            }
        </style>
    """, unsafe_allow_html=True)

    if "app_password" not in st.secrets:
        st.warning("⚠️ לא הוגדרה סיסמה ב-Secrets. הכניסה חופשית.")
        return True

    def password_entered():
        if st.session_state["password"] == st.secrets["app_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown("### 🔒 התחברות למערכת")
        st.text_input("הזמן סיסמה", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown("### 🔒 התחברות למערכת")
        st.text_input("הזמן סיסמה", type="password", on_change=password_entered, key="password")
        st.error("❌ סיסמה שגויה")
        return False
    else:
        return True

if not check_password():
    st.stop()

# ==========================================
# ⚙️ הגדרות וחיבורים
# ==========================================

SQL_TO_APP_COLS = {
    'order_num': 'מספר הזמנה',
    'customer_name': 'שם לקוח',
    'phone': 'טלפון',
    'city': 'עיר',
    'street': 'רחוב',
    'house_num': 'מספר בית',
    'sku': 'מוצר',
    'quantity': 'כמות',
    'shipping_num': 'סטטוס משלוח',
    'order_date': 'תאריך',
    'message_log': 'לוג מיילים',
    'order_type': 'סוג הזמנה',
    'delivery_time': 'raw_delivery_time',
    'notes': 'הערות' 
}

LOG_COLUMN_NAME = "לוג מיילים"
# שליפת אימיילים
EMAIL_ACE = st.secrets["suppliers"].get("ace_email") if "suppliers" in st.secrets else None
EMAIL_PAYNGO = st.secrets["suppliers"].get("payngo_email") if "suppliers" in st.secrets else None
EMAIL_KSP = st.secrets["suppliers"].get("ksp_email", "sapak@ksp.co.il") if "suppliers" in st.secrets else "sapak@ksp.co.il"
EMAIL_LASTPRICE = st.secrets["suppliers"].get("lastprice_email", "hen@lastprice.co.il") if "suppliers" in st.secrets else "hen@lastprice.co.il"

# כתובת המתקין
EMAIL_INSTALLER = st.secrets["suppliers"].get("installer_email", "meir22101@gmail.com") if "suppliers" in st.secrets else "meir22101@gmail.com"

INSTALLATION_PHONE = st.secrets["ultramsg"].get("installation_phone", "0528448382") if "ultramsg" in st.secrets else "0528448382"

def get_db_connection():
    return psycopg2.connect(
        host=st.secrets["supabase"]["DB_HOST"],
        port=st.secrets["supabase"]["DB_PORT"],
        database=st.secrets["supabase"]["DB_NAME"],
        user=st.secrets["supabase"]["DB_USER"],
        password=st.secrets["supabase"]["DB_PASS"],
        sslmode='require'
    )

# -------------------------------------------
# 📝 פונקציה לעדכון סטטוס "בטיפול"
# -------------------------------------------
def start_service_treatment(order_id):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = "UPDATE orders SET service_start_date = CURRENT_DATE WHERE id = %s"
        cur.execute(query, (order_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        st.error(f"שגיאה בעדכון טיפול: {e}")
        return False
    finally:
        if conn:
            conn.close()

# -------------------------------------------
# 📥 טעינת נתונים
# -------------------------------------------
@st.cache_data
def load_data():
    conn = get_db_connection()
    query = """
        SELECT 
            id, order_num, customer_name, phone, city, street, house_num, 
            sku, quantity, shipping_num, order_date, message_log, order_type, delivery_time, notes
        FROM all_orders_view
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df = df.rename(columns=SQL_TO_APP_COLS)
    df = df.fillna("")
    if LOG_COLUMN_NAME not in df.columns:
        df[LOG_COLUMN_NAME] = ""
        
    return df

# -------------------------------------------
# 📝 עדכון לוג
# -------------------------------------------
def update_log_in_db(order_num, sku, message, order_type_val="Regular Order", row_id=None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # === תיקון: זיהוי הטבלה החדשה ===
        if "Pre-Order" in str(order_type_val):
            target_table = "pre_orders"
        elif "Pickup" in str(order_type_val):
             target_table = "pickups"
        elif "Spare Part" in str(order_type_val):
             target_table = "spare_parts"
        elif "Double Delivery" in str(order_type_val): # <--- הוספנו את זה
             target_table = "double_deliveries"
        else:
            target_table = "orders"
        
        timestamp = datetime.now().strftime("%d/%m %H:%M")
        new_entry = f"{message} ({timestamp})"
        
        if row_id:
            condition_sql = "WHERE id = %s"
            params_select = (row_id,)
        else:
            condition_sql = "WHERE order_num = %s AND sku = %s"
            params_select = (str(order_num), str(sku))
        
        select_sql = f"SELECT message_log FROM {target_table} {condition_sql}"
        cursor.execute(select_sql, params_select)
        result = cursor.fetchone()
        current_log = result[0] if result and result[0] else ""
        
        if current_log:
            full_log = f"{current_log} | {new_entry}"
        else:
            full_log = new_entry
            
        update_sql = f"UPDATE {target_table} SET message_log = %s {condition_sql}"
        
        if row_id:
            cursor.execute(update_sql, (full_log, row_id))
        else:
            cursor.execute(update_sql, (full_log, str(order_num), str(sku)))
            
        conn.commit()
        cursor.close()
        conn.close()
        load_data.clear()
        return full_log
    except Exception as e:
        print(f"Error updating log: {e}") 
        return None

# --- פונקציות עזר ---
def normalize_phone(phone_input):
    if not phone_input: return ""
    clean_digits = ''.join(filter(str.isdigit, str(phone_input)))
    if clean_digits.startswith('972'): clean_digits = clean_digits[3:]
    if clean_digits.startswith('0'): return clean_digits[1:]
    return clean_digits

def normalize_phone_for_api(phone_input):
    if not phone_input: return None
    digits = ''.join(filter(str.isdigit, str(phone_input)))
    if not digits: return None
    if digits.startswith('972'): return digits 
    if digits.startswith('0'): return '972' + digits[1:] 
    if len(digits) == 9: return '972' + digits
    return digits 

def clean_input_garbage(val):
    if not isinstance(val, str): val = str(val)
    garbage_chars = ['\u200f', '\u200e', '\u202a', '\u202b', '\u202c', '\u202d', '\u202e', '\u00a0', '\t', '\n', '\r']
    cleaned_val = val
    for char in garbage_chars:
        cleaned_val = cleaned_val.replace(char, '')
    return cleaned_val.strip()

def format_date_il(d):
    if not d: return ""
    try:
        dt = pd.to_datetime(d)
        return dt.strftime('%d/%m/%Y')
    except:
        return str(d)

def format_quantity(q):
    try:
        return str(int(float(q)))
    except:
        return str(q).replace('.0', '')

# --- שליחה (ווצאפ / מייל) ---
def send_whatsapp_message(phone, message_body):
    if "ultramsg" not in st.secrets:
        st.error("חסרות הגדרות UltraMsg ב-Secrets.")
        return False
    instance_id = st.secrets["ultramsg"]["instance_id"]
    token = st.secrets["ultramsg"]["token"]
    clean_phone = normalize_phone_for_api(phone)
    if not clean_phone: return False
    url = f"https://api.ultramsg.com/{instance_id}/messages/chat"
    payload = {"token": token, "to": clean_phone, "body": message_body}
    try:
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200 and 'sent' in response.text: return True
        else:
            st.error(f"שגיאה בשליחת וואטסאפ: {response.text}")
            return False
    except Exception as e:
        st.error(f"תקלה בשליחה: {e}")
        return False

def send_custom_email(subject_line, body_text="", target_email=None):
    if "email" not in st.secrets:
        st.error("חסרות הגדרות אימייל ב-Secrets.")
        return False
    sender = st.secrets["email"]["sender_address"]
    password = st.secrets["email"]["password"]
    recipient = target_email if target_email else st.secrets["email"]["recipient_address"]
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject_line
    msg.attach(MIMEText(body_text, 'plain', 'utf-8'))
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

# --- Dialog Function for Updating Details (מפוצל) ---
@st.dialog("עדכון פרטים")
def open_update_dialog(rows_df):
    st.write("הזן את המלל שיישלח בגוף המייל:")
    user_input = st.text_area("תוכן ההודעה", height=100)
    
    if st.button("שלח"):
        if not user_input.strip():
            st.error("חובה להזין תוכן להודעה")
        else:
            emails_sent = 0
            
            mask_has_tracking = rows_df['_real_tracking'].apply(lambda x: True if (x and str(x).strip().lower() not in ['none', '', 'nan']) else False)
            df_shipping = rows_df[mask_has_tracking]
            df_installer = rows_df[~mask_has_tracking]
            
            # שליחה לחברת שליחויות
            if not df_shipping.empty:
                trackings = list(set([str(t).strip() for t in df_shipping['_real_tracking']]))
                subj = ", ".join(trackings)
                if send_custom_email(subj, user_input, target_email=None):
                    emails_sent += 1
                    for _, r in df_shipping.iterrows():
                        update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח עדכון פרטים", r['_order_type_key'])

            # שליחה למתקין
            if not df_installer.empty:
                orders = list(set([str(o).strip() for o in df_installer['מספר הזמנה']]))
                subj = ", ".join(orders)
                if send_custom_email(subj, user_input, target_email=EMAIL_INSTALLER):
                    emails_sent += 1
                    for _, r in df_installer.iterrows():
                        update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח עדכון למתקין", r['_order_type_key'])
            
            if emails_sent > 0:
                st.success("הבקשה נשלחה בהצלחה!")
                time.sleep(1.5)
                st.rerun()
            else:
                st.error("לא נשלח (אולי שגיאה בחיבור)")

# --- Dialog Function for Manual Supplier Email ---
@st.dialog("📧 שליחה לספק ידני (לא זוהה ספק)")
def open_manual_supplier_dialog(rows_df):
    st.write("לא זוהה ספק אוטומטי עבור ההזמנות שנבחרו.")
    st.write("אנא הזן כתובת מייל ידנית לשליחת הודעת 'אין מענה':")
    target_email = st.text_input("כתובת מייל לספק")
    
    if st.button("שלח הודעה"):
        if not target_email or "@" not in target_email:
            st.error("אנא הזן כתובת מייל תקינה")
        else:
            u_orders = ", ".join(rows_df['מספר הזמנה'].unique())
            u_tracking = ", ".join([t for t in rows_df['סטטוס משלוח'].unique() if t and t!="התקנה"]) or "ללא מס' משלוח"
            u_phones = ", ".join(rows_df['טלפון'].unique())
            
            subj = f"{u_orders} {u_tracking} - אין מענה מהלקוח - האם יש מספר טלפון אחר?"
            body = f"הטלפון שיש לנו כרגע הוא: {u_phones}\nנא בדקו אם יש מספר אחר."
            
            if send_custom_email(subj, body, target_email):
                st.success(f"נשלח ל-{target_email}")
                for _, r in rows_df.iterrows():
                    update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח ספק (ידני)", r['_order_type_key'])
                time.sleep(1.5)
                st.rerun()

# --- Dialog Function for Refund (זיכוי ללקוח) ---
@st.dialog("💸 בקשת זיכוי ללקוח")
def open_refund_dialog(rows_df):
    st.write("הזן את סיבת הזיכוי (המלל יתווסף למספר ההזמנה והמק\"ט בנושא ובגוף המייל):")
    user_input = st.text_area("פרטי הזיכוי", height=100)
    
    # בדוק מראש אם יש ספק מזוהה כדי להציג התראה במידת הצורך
    ace_g = rows_df[rows_df['מספר הזמנה'].astype(str).str.upper().str.startswith("PO")]
    pay_g = rows_df[rows_df['מספר הזמנה'].astype(str).str.startswith("9")]
    ksp_g = rows_df[(rows_df['מספר הזמנה'].astype(str).str.startswith("31")) & (rows_df['מספר הזמנה'].astype(str).str.len() == 8)]
    lp_g = rows_df[(rows_df['מספר הזמנה'].astype(str).str.startswith("32")) & (rows_df['מספר הזמנה'].astype(str).str.len() == 7)]
    
    has_auto_supplier = not (ace_g.empty and pay_g.empty and ksp_g.empty and lp_g.empty)
    
    manual_email = ""
    if not has_auto_supplier:
        st.warning("לא זוהה ספק אוטומטי (למשל אייס או מחסני חשמל). אנא הזן כתובת מייל ידנית לשליחה:")
        manual_email = st.text_input("כתובת מייל לספק")
        
    if st.button("שלח בקשת זיכוי"):
        if not user_input.strip():
            st.error("חובה להזין פרטים עבור הזיכוי")
            return
            
        if not has_auto_supplier and (not manual_email or "@" not in manual_email):
            st.error("אנא הזן כתובת מייל תקינה לספק")
            return

        emails_sent = 0
        
        def send_refund_to_supplier(df_group, email_address, supplier_name):
            if df_group.empty or not email_address: return False
            u_orders = " ".join(df_group['מספר הזמנה'].astype(str).unique())
            u_skus = " ".join(df_group['מוצר'].astype(str).unique())
            # הדרישה: נושא וגוף זהים. מס' הזמנה -> רווח -> מק"ט -> רווח -> המלל.
            text_to_send = f"{u_orders} {u_skus} {user_input.strip()}"
            
            if send_custom_email(text_to_send, text_to_send, email_address):
                st.toast(f"בקשת הזיכוי נשלחה ל-{supplier_name} ✅")
                for _, r in df_group.iterrows(): 
                    update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלחה בקשת זיכוי לספק", r['_order_type_key'])
                return True
            return False

        if has_auto_supplier:
            if send_refund_to_supplier(ace_g, EMAIL_ACE, "אייס"): emails_sent+=1
            if send_refund_to_supplier(pay_g, EMAIL_PAYNGO, "מחסני חשמל"): emails_sent+=1
            if send_refund_to_supplier(ksp_g, EMAIL_KSP, "KSP"): emails_sent+=1
            if send_refund_to_supplier(lp_g, EMAIL_LASTPRICE, "Last Price"): emails_sent+=1
        else:
            u_orders = " ".join(rows_df['מספר הזמנה'].astype(str).unique())
            u_skus = " ".join(rows_df['מוצר'].astype(str).unique())
            text_to_send = f"{u_orders} {u_skus} {user_input.strip()}"
            
            if send_custom_email(text_to_send, text_to_send, manual_email):
                st.toast(f"בקשת הזיכוי נשלחה ל-{manual_email} ✅")
                for _, r in rows_df.iterrows(): 
                    update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלחה בקשת זיכוי (ידני)", r['_order_type_key'])
                emails_sent+=1

        if emails_sent > 0:
            time.sleep(1.5)
            st.rerun()

# ==========================================
# 🖥️ ממשק משתמש
# ==========================================
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
    .stButton button { width: 100%; border-radius: 6px; height: 3em; }
    .block-container { padding-top: 2rem; padding-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)

# --- כותרת ---
col_title, col_refresh = st.columns([6, 1])
with col_title:
    st.title("🔎 איתור הזמנות מהיר (משולב)")
with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True) 
    if st.button("🔄 רענן"):
        load_data.clear()
        st.rerun()

try:
    with st.spinner('טוען נתונים מהענן...'):
        df = load_data()
    st.success(f"הנתונים נטענו בהצלחה! סה\"כ {len(df)} שורות.")
except Exception as e:
    st.error(f"שגיאה בטעינה: {e}")
    st.stop()

# --- חיפוש ---
search_query = st.text_input("הכנס טלפון, מספר הזמנה או מספר משלוח:", "")

if search_query:
    filtered_df = pd.DataFrame()
    clean_text_query = clean_input_garbage(search_query)
    clean_phone_query = normalize_phone(clean_text_query)

    conditions = []
    
    # 1. חיפוש הזמנה
    mask_order = df['מספר הזמנה'].astype(str).str.contains(clean_text_query, case=False, na=False, regex=False)
    conditions.append(mask_order)

    # 2. חיפוש משלוח
    if 'סטטוס משלוח' in df.columns:
        mask_tracking = df['סטטוס משלוח'].astype(str).str.contains(clean_text_query, case=False, na=False, regex=False)
        conditions.append(mask_tracking)

    # 3. חיפוש טלפון
    if clean_phone_query and 'טלפון' in df.columns:
        phone_col_norm = df['טלפון'].astype(str).apply(normalize_phone)
        mask_phone = phone_col_norm == clean_phone_query
        conditions.append(mask_phone)

    if conditions:
        final_mask = pd.concat(conditions, axis=1).any(axis=1)
        filtered_df = df[final_mask].copy()

    # --- הצגת תוצאות ---
    if not filtered_df.empty:
        try:
            filtered_df['temp_date'] = pd.to_datetime(filtered_df['תאריך'], errors='coerce')
            filtered_df = filtered_df.sort_values(by='temp_date', ascending=True)
        except: pass

        display_rows = []
        for index, row in filtered_df.iterrows():
            
            order_num = str(row['מספר הזמנה']).strip()
            qty = format_quantity(row['כמות'])
            date_val = format_date_il(row['תאריך'])
            sku = str(row['מוצר']).strip()
            full_name = str(row['שם לקוח']).strip()
            street = str(row['רחוב']).strip()
            house = str(row['מספר בית']).strip()
            city = str(row['עיר']).strip()
            address_display = f"{street} {house} {city}".strip()
            
            phone_raw = row['טלפון']
            phone_clean = normalize_phone(phone_raw)
            phone_display = "0" + phone_clean if phone_clean else ""
            
            notes_val = str(row.get('הערות', '')).strip()
            order_type_raw = str(row.get('סוג הזמנה', 'Regular Order'))
            delivery_time_raw = str(row.get('raw_delivery_time', '')).strip()
            
            # --- לוגיקות תצוגה ---
            if "Pickup" in order_type_raw:
                display_delivery_text = "" 
            elif "Spare Part" in order_type_raw:
                display_delivery_text = "עד 10 ימי עסקים"
            elif "Double Delivery" in order_type_raw: # <--- חדש
                display_delivery_text = "אספקה ואיסוף (עד 14 ימי עסקים)"
            elif "Pre-Order" in order_type_raw:
                if delivery_time_raw and delivery_time_raw.lower() != 'none':
                    display_delivery_text = f"עד {delivery_time_raw} ימי עסקים"
                else:
                    display_delivery_text = "זמן אספקה ארוך"
            else:
                display_delivery_text = "עד 10-14 ימי עסקים"

            # שמירת המספר המקורי ללוגיקה
            raw_tracking_val = str(row['סטטוס משלוח']).strip() 
            tracking = raw_tracking_val 
            
            # לוגיקת תצוגה (לטבלה בלבד)
            if not tracking or tracking == "None":
                # הוספנו את Double Delivery לרשימה של דברים שאין להם "התקנה" כברירת מחדל
                if any(x in order_type_raw for x in ["Pre-Order", "Pickup", "Spare Part", "Double Delivery"]):
                    tracking = "" 
                else:
                    tracking = "התקנה"
            
            # תגיות יפות לטבלה
            if "Pickup" in order_type_raw:
                tracking = "איסוף"
            elif "Spare Part" in order_type_raw:
                tracking = "חלקי חילוף"
            elif "Double Delivery" in order_type_raw: # <--- חדש
                tracking = "משלוח כפול"

            log_val = str(row.get(LOG_COLUMN_NAME, ""))
            first_name = full_name.split()[0] if full_name else ""
            
            # טקסט להעתקה - מציג מספר משלוח אם קיים, גם באיסוף/חלקים
            text_line_tracking = tracking
            if raw_tracking_val and raw_tracking_val != "None" and tracking in ["איסוף", "חלקי חילוף", "משלוח כפול"]:
                text_line_tracking = raw_tracking_val

            base_text_line = f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, שם: {full_name}, כתובת: {address_display}, טלפון: {phone_display}, מספר משלוח: {text_line_tracking}, תאריך: {date_val}, זמן אספקה: {display_delivery_text}"
            if notes_val:
                base_text_line += f", הערות: {notes_val}"
            
            display_rows.append({
                "מספר הזמנה": order_num,
                "שם לקוח": full_name,
                "טלפון": phone_display,
                "כתובת מלאה": address_display,
                "מוצר": sku,
                "כמות": qty,
                "סטטוס משלוח": tracking, # Table shows "Pickup"/"Spare Part"
                "תאריך": date_val,
                "זמן אספקה": display_delivery_text,
                "הערות": notes_val,
                LOG_COLUMN_NAME: log_val,
                "בחר": False,
                "_excel_line": f"{order_num}\t{qty}\t{sku}\t{first_name}\t{street}\t{house}\t{city}\t{phone_display}",
                "_text_line": base_text_line,
                "_raw_phone": str(phone_raw).strip(),
                "_order_key": order_num,
                "_sku_key": sku,
                "_order_type_key": order_type_raw,
                "_row_id": row.get('id'),
                "_real_tracking": raw_tracking_val # המספר האמיתי ללוגיקת כפתורים
            })
        
        display_df = pd.DataFrame(display_rows)
        cols_order = [LOG_COLUMN_NAME, "הערות", "סטטוס משלוח", "מוצר", "כמות", "זמן אספקה", "מספר הזמנה", "בחר"]
        
        edited_df = st.data_editor(
            display_df[cols_order],
            use_container_width=False,  
            hide_index=True,
            column_config={
                "בחר": st.column_config.CheckboxColumn("בחר", default=False, width="small"),
                "מספר הזמנה": st.column_config.TextColumn("מספר הזמנה", width="medium"),
                "זמן אספקה": st.column_config.TextColumn("זמן אספקה", width="medium"),
                "הערות": st.column_config.TextColumn("הערות", width="medium"),
                "כמות": st.column_config.TextColumn("כמות", width="small"),
                "מוצר": st.column_config.TextColumn("מוצר", width="large"),
                "סטטוס משלוח": st.column_config.TextColumn("מס משלוח", width="medium"),
                LOG_COLUMN_NAME: st.column_config.TextColumn("לוג", disabled=True, width="large")
            },
            disabled=["מספר הזמנה", "מוצר", "כמות", "סטטוס משלוח", LOG_COLUMN_NAME, "זמן אספקה", "הערות"]
        )

        selected_indices = edited_df[edited_df["בחר"] == True].index
        rows_for_action = display_df.loc[selected_indices] if not selected_indices.empty else display_df 
        is_implicit_select_all = selected_indices.empty
        show_bulk_warning = (is_implicit_select_all and len(rows_for_action) > 10)

# --- כפתורים (חלוקה חכמה עם Popovers) ---
        st.markdown("<br>", unsafe_allow_html=True)
        col_wa, col_delivery, col_supplier, col_system = st.columns(4, gap="small")
        
        # 1. עמודת וואטסאפ (תפריט נפתח)
        with col_wa:
            with st.popover("💬 פעולות וואטסאפ (לקוח/מתקין)", use_container_width=True):
                # מדיניות
                if not show_bulk_warning and st.button("💬 שלח מדיניות", use_container_width=True):
                    if rows_for_action.empty: st.toast("⚠️ אין נתונים")
                    else:
                        count = 0
                        for phone, group in rows_for_action.groupby('_raw_phone'):
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
                                count += 1
                                for _, r in group.iterrows():
                                    update_log_in_db(r['_order_key'], r['_sku_key'], "💬 נשלח ווצאפ מדיניות", r['_order_type_key'])
                                st.toast(f"נשלח ל-{client_name} ✅")
                        if count > 0:
                            time.sleep(1)
                            st.rerun()

                # חזרנו אליך
                if not show_bulk_warning and st.button("📞 חזרנו אליך", use_container_width=True):
                    if rows_for_action.empty: st.toast("⚠️ אין נתונים")
                    else:
                        count = 0
                        for phone, group in rows_for_action.groupby('_raw_phone'):
                            if not phone: continue
                            orders_str = ", ".join(group['מספר הזמנה'].unique())
                            skus_str = ", ".join(group['מוצר'].unique())
                            tracking_str = ", ".join(group['סטטוס משלוח'].unique())
                            client_name = group.iloc[0]['שם לקוח'].split()[0]
                            msg_body = f"""היי {client_name},
חוזרים אלייך מסלימפרייס לגבי הזמנה/ות: {orders_str}
מוצרים: {skus_str}
מס משלוח/ים: {tracking_str}
קיבלנו פנייה שחיפשת אותנו, איך אפשר לעזור?"""
                            if send_whatsapp_message(phone, msg_body):
                                count += 1
                                for _, r in group.iterrows():
                                    update_log_in_db(r['_order_key'], r['_sku_key'], "💬 נשלח 'חזרנו אליך'", r['_order_type_key'])
                                st.toast(f"נשלח ל-{client_name} ✅")
                        if count > 0:
                            time.sleep(1)
                            st.rerun()

                # התקנה
                if not show_bulk_warning and st.button("🔧 התקנה", use_container_width=True):
                    if rows_for_action.empty: st.toast("⚠️ אין נתונים")
                    else:
                        all_msgs = []
                        for order_num, group in rows_for_action.groupby('מספר הזמנה'):
                            r = group.iloc[0]
                            items = ", ".join([f"{row['כמות']} X {row['מוצר']}" for _, row in group.iterrows()])
                            line = f"{order_num} | {items} | {r['שם לקוח']} | {r['כתובת מלאה']} | {r['טלפון']} | התקנה"
                            all_msgs.append(line)
                        if send_whatsapp_message(INSTALLATION_PHONE, "\n\n".join(all_msgs)):
                            st.toast("נשלח למחסני חשמל")
                            for _, r in rows_for_action.iterrows():
                                 update_log_in_db(r['_order_key'], r['_sku_key'], "💬 נשלח למתקין", r['_order_type_key'])
                            time.sleep(1)
                            st.rerun()

        # 2. עמודת חברת שליחויות (תפריט נפתח)
        with col_delivery:
            with st.popover("📦 פעולות ח' שליחויות (מיילים)", use_container_width=True):
                # מה קורה?
                if not show_bulk_warning and st.button("❓ מה קורה?", use_container_width=True):
                    duplicate_alert = False
                    for _, r in rows_for_action.iterrows():
                         if "נשלח בדיקה" in str(r[LOG_COLUMN_NAME]): duplicate_alert = True
                    if duplicate_alert:
                         st.toast("⚠️ שים לב: כבר נשלח בעבר")
                         time.sleep(1)
                    
                    emails_sent = 0
                    mask_has_tracking = rows_for_action['_real_tracking'].apply(lambda x: True if (x and str(x).strip().lower() not in ['none', '', 'nan']) else False)
                    df_shipping = rows_for_action[mask_has_tracking]
                    df_installer = rows_for_action[~mask_has_tracking]
                    
                    if not df_shipping.empty:
                        trackings = list(set([str(t).strip() for t in df_shipping['_real_tracking']]))
                        subj = f"{', '.join(trackings)} מה קורה עם זה בבקשה?" if len(trackings)==1 else f"{', '.join(trackings)} מה קורה עם אלה בבקשה?"
                        if send_custom_email(subj, target_email=None):
                            emails_sent += 1
                            for _, r in df_shipping.iterrows():
                                update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח בדיקה", r['_order_type_key'])
                    
                    if not df_installer.empty:
                        orders = list(set([str(o).strip() for o in df_installer['מספר הזמנה']]))
                        subj = f"{', '.join(orders)} מה קורה עם זה בבקשה?"
                        if send_custom_email(subj, target_email=EMAIL_INSTALLER):
                            emails_sent += 1
                            for _, r in df_installer.iterrows():
                                update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח בדיקה למתקין", r['_order_type_key'])

                    if emails_sent > 0:
                        st.success(f"נשלחו {emails_sent} מיילים")
                        time.sleep(1)
                        st.rerun()

                # להחזיר
                if not show_bulk_warning and st.button("↩️ להחזיר", use_container_width=True):
                    emails_sent = 0
                    mask_has_tracking = rows_for_action['_real_tracking'].apply(lambda x: True if (x and str(x).strip().lower() not in ['none', '', 'nan']) else False)
                    df_shipping = rows_for_action[mask_has_tracking]
                    df_installer = rows_for_action[~mask_has_tracking]
                    
                    if not df_shipping.empty:
                        trackings = list(set([str(t).strip() for t in df_shipping['_real_tracking']]))
                        subj = f"{', '.join(trackings)} להחזיר אלינו בבקשה"
                        if send_custom_email(subj, target_email=None):
                            emails_sent += 1
                    
                    if not df_installer.empty:
                        orders = list(set([str(o).strip() for o in df_installer['מספר הזמנה']]))
                        subj = f"{', '.join(orders)} להחזיר אלינו בבקשה"
                        if send_custom_email(subj, target_email=EMAIL_INSTALLER):
                            emails_sent += 1

                    if emails_sent > 0:
                        st.success(f"נשלחו {emails_sent} בקשות החזרה")

                # עדכון פרטים
                if not show_bulk_warning and st.button("📝 עדכון פרטים", use_container_width=True):
                    if rows_for_action.empty: st.toast("⚠️ לא נבחרו שורות")
                    else:
                         open_update_dialog(rows_for_action)

        # 3. עמודת ספקים (תפריט נפתח)
        with col_supplier:
            with st.popover("📧 פעולות ספקים (מיילים)", use_container_width=True):
                # אין מענה
                if not show_bulk_warning and st.button("📞 אין מענה", use_container_width=True):
                    ace_g = rows_for_action[rows_for_action['מספר הזמנה'].astype(str).str.upper().str.startswith("PO")]
                    pay_g = rows_for_action[rows_for_action['מספר הזמנה'].astype(str).str.startswith("9")]
                    ksp_g = rows_for_action[(rows_for_action['מספר הזמנה'].astype(str).str.startswith("31")) & (rows_for_action['מספר הזמנה'].astype(str).str.len() == 8)]
                    lp_g = rows_for_action[(rows_for_action['מספר הזמנה'].astype(str).str.startswith("32")) & (rows_for_action['מספר הזמנה'].astype(str).str.len() == 7)]

                    found_supplier = False
                    if not ace_g.empty and EMAIL_ACE:
                        found_supplier = True
                        u_orders = ", ".join(ace_g['מספר הזמנה'].unique())
                        u_tracking = ", ".join([t for t in ace_g['סטטוס משלוח'].unique() if t and t!="התקנה"]) or "ללא מס' משלוח"
                        u_phones = ", ".join(ace_g['טלפון'].unique())
                        subj = f"{u_orders} {u_tracking} - אין מענה מהלקוח - האם יש מספר טלפון אחר?"
                        body = f"הטלפון שיש לנו כרגע הוא: {u_phones}\nנא בדקו אם יש מספר אחר."
                        if send_custom_email(subj, body, EMAIL_ACE):
                            st.toast("נשלח לאייס")
                            for _, r in ace_g.iterrows(): update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח ספק (אין מענה)", r['_order_type_key'])
                    
                    if not pay_g.empty and EMAIL_PAYNGO:
                        found_supplier = True
                        u_orders = ", ".join(pay_g['מספר הזמנה'].unique())
                        u_tracking = ", ".join([t for t in pay_g['סטטוס משלוח'].unique() if t and t!="התקנה"]) or "ללא מס' משלוח"
                        u_phones = ", ".join(pay_g['טלפון'].unique())
                        subj = f"{u_orders} {u_tracking} - אין מענה מהלקוח - האם יש מספר טלפון אחר?"
                        body = f"הטלפון שיש לנו כרגע הוא: {u_phones}\nנא בדקו אם יש מספר אחר."
                        if send_custom_email(subj, body, EMAIL_PAYNGO):
                            st.toast("נשלח למחסני חשמל")
                            for _, r in pay_g.iterrows(): update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח ספק (אין מענה)", r['_order_type_key'])

                    if not ksp_g.empty and EMAIL_KSP:
                        found_supplier = True
                        u_orders = ", ".join(ksp_g['מספר הזמנה'].unique())
                        u_tracking = ", ".join([t for t in ksp_g['סטטוס משלוח'].unique() if t and t!="התקנה"]) or "ללא מס' משלוח"
                        u_phones = ", ".join(ksp_g['טלפון'].unique())
                        subj = f"{u_orders} {u_tracking} - אין מענה מהלקוח - האם יש מספר טלפון אחר?"
                        body = f"הטלפון שיש לנו כרגע הוא: {u_phones}\nנא בדקו אם יש מספר אחר."
                        if send_custom_email(subj, body, EMAIL_KSP):
                            st.toast("נשלח ל-KSP")
                            for _, r in ksp_g.iterrows(): update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח ספק (אין מענה)", r['_order_type_key'])

                    if not lp_g.empty and EMAIL_LASTPRICE:
                        found_supplier = True
                        u_orders = ", ".join(lp_g['מספר הזמנה'].unique())
                        u_tracking = ", ".join([t for t in lp_g['סטטוס משלוח'].unique() if t and t!="התקנה"]) or "ללא מס' משלוח"
                        u_phones = ", ".join(lp_g['טלפון'].unique())
                        subj = f"{u_orders} {u_tracking} - אין מענה מהלקוח - האם יש מספר טלפון אחר?"
                        body = f"הטלפון שיש לנו כרגע הוא: {u_phones}\nנא בדקו אם יש מספר אחר."
                        if send_custom_email(subj, body, EMAIL_LASTPRICE):
                            st.toast("נשלח ל-Last Price")
                            for _, r in lp_g.iterrows(): update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח ספק (אין מענה)", r['_order_type_key'])
                    
                    if not found_supplier: 
                        open_manual_supplier_dialog(rows_for_action)
                    else: 
                        time.sleep(1)
                        st.rerun()

                # זיכוי
                if not show_bulk_warning and st.button("💸 זיכוי", use_container_width=True):
                    if rows_for_action.empty: 
                        st.toast("⚠️ לא נבחרו שורות")
                    else:
                        open_refund_dialog(rows_for_action)

        # 4. עמודת מערכת (כפתור רגיל בולט)
        with col_system:
            if not show_bulk_warning and st.button("🛠️ סמן 'בטיפול'", use_container_width=True):
                if rows_for_action.empty: st.toast("⚠️ לא נבחרו הזמנות")
                else:
                    success_count = 0
                    for index, row in rows_for_action.iterrows():
                        if "Regular Order" in str(row['_order_type_key']) and row['_row_id']:
                            if start_service_treatment(row['_row_id']):
                                update_log_in_db(row['_order_key'], row['_sku_key'], "🛠️ סומן 'בטיפול'", row['_order_type_key'], row_id=row['_row_id'])
                                success_count += 1
                    
                    if success_count > 0:
                        st.toast(f"✅ {success_count} הזמנות עברו לסטטוס 'בטיפול'!", icon="👨‍🔧")
                        time.sleep(1)
                        load_data.clear() 
                        st.rerun()
                    else:
                        st.toast("⚠️ לא נבחרו הזמנות רגילות לטיפול", icon="🛑")

        st.divider()
        if not rows_for_action.empty and not show_bulk_warning:
            st.caption("העתקה לאקסל:")
            st.code("\n".join(rows_for_action["_excel_line"]), language="csv")
            st.caption("פרטים מלאים:")
            st.code("\n".join(rows_for_action["_text_line"]), language=None)
            
    else:
        st.warning(f"לא נמצאו תוצאות עבור: {clean_text_query}")


