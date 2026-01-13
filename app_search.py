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
            /* יישור כפתורים כללי */
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
    'message_log': 'לוג מיילים'
}

LOG_COLUMN_NAME = "לוג מיילים"
EMAIL_ACE = st.secrets["suppliers"].get("ace_email") if "suppliers" in st.secrets else None
EMAIL_PAYNGO = st.secrets["suppliers"].get("payngo_email") if "suppliers" in st.secrets else None
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
# 📥 טעינת נתונים
# -------------------------------------------
@st.cache_data(ttl=600)
def load_data():
    conn = get_db_connection()
    # שולפים הכל
    query = """
        SELECT 
            order_num, customer_name, phone, city, street, house_num, 
            sku, quantity, shipping_num, order_date, message_log
        FROM orders
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # המרה לעברית
    df = df.rename(columns=SQL_TO_APP_COLS)
    
    # מילוי ריקים
    df = df.fillna("")
    if LOG_COLUMN_NAME not in df.columns:
        df[LOG_COLUMN_NAME] = ""
        
    return df

# -------------------------------------------
# 📝 עדכון לוג (SQL UPDATE)
# -------------------------------------------
def update_log_in_db(order_num, sku, message):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        timestamp = datetime.now().strftime("%d/%m %H:%M")
        new_entry = f"{message} ({timestamp})"
        
        # 1. שליפת לוג קיים
        select_sql = "SELECT message_log FROM orders WHERE order_num = %s AND sku = %s"
        cursor.execute(select_sql, (str(order_num), str(sku)))
        result = cursor.fetchone()
        current_log = result[0] if result and result[0] else ""
        
        # 2. שרשור
        if current_log:
            full_log = f"{current_log} | {new_entry}"
        else:
            full_log = new_entry
            
        # 3. עדכון
        update_sql = "UPDATE orders SET message_log = %s WHERE order_num = %s AND sku = %s"
        cursor.execute(update_sql, (full_log, str(order_num), str(sku)))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        load_data.clear() # ניקוי מטמון כדי לראות את השינוי
        return full_log
        
    except Exception as e:
        st.error(f"שגיאה בעדכון מסד הנתונים: {e}")
        return None

# --- פונקציות עזר וניקוי ---

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
    """ממיר תאריך SQL (YYYY-MM-DD) לפורמט ישראלי"""
    if not d: return ""
    try:
        dt = pd.to_datetime(d)
        return dt.strftime('%d/%m/%Y')
    except:
        return str(d)

def format_quantity(q):
    """מנקה אפסים אחרי הנקודה (1.0 -> 1)"""
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

# --- כותרת + כפתור רענון בשורה אחת ---
col_title, col_refresh = st.columns([6, 1])
with col_title:
    st.title("🔎 איתור הזמנות מהיר (SQL)")
with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True) # רווח קטן ליישור
    if st.button("🔄 רענן"):
        load_data.clear()
        st.rerun()

try:
    with st.spinner('טוען נתונים מהענן...'):
        df = load_data()
    st.success(f"הנתונים נטענו בהצלחה! סה\"כ {len(df)} שורות בהיסטוריה.")
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
    
    # שימוש ב-regex=False כדי למנוע קריסה מסימנים מיוחדים
    # 1. חיפוש הזמנה
    mask_order = df['מספר הזמנה'].astype(str).str.contains(clean_text_query, case=False, na=False, regex=False)
    conditions.append(mask_order)

    # 2. חיפוש משלוח
    if 'סטטוס משלוח' in df.columns:
        mask_tracking = df['סטטוס משלוח'].astype(str).str.contains(clean_text_query, case=False, na=False, regex=False)
        conditions.append(mask_tracking)

    # 3. חיפוש טלפון (נרמול)
    if clean_phone_query and 'טלפון' in df.columns:
        phone_col_norm = df['טלפון'].astype(str).apply(normalize_phone)
        mask_phone = phone_col_norm == clean_phone_query
        conditions.append(mask_phone)

    if conditions:
        final_mask = pd.concat(conditions, axis=1).any(axis=1)
        filtered_df = df[final_mask].copy()

    # --- הצגת תוצאות ---
    if not filtered_df.empty:
        # מיון
        try:
            filtered_df['temp_date'] = pd.to_datetime(filtered_df['תאריך'], errors='coerce')
            filtered_df = filtered_df.sort_values(by='temp_date', ascending=False)
        except: pass

        display_rows = []
        for index, row in filtered_df.iterrows():
            order_num = str(row['מספר הזמנה']).strip()
            
            # פורמטים מתוקנים
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
            
            tracking = str(row['סטטוס משלוח']).strip()
            if not tracking and "התקנות" in str(row.get('מקור', '')): tracking = "התקנה"
            
            first_name = full_name.split()[0] if full_name else ""
            log_val = str(row.get(LOG_COLUMN_NAME, ""))
            
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
                "_raw_phone": str(phone_raw).strip(),
                "_order_key": order_num,
                "_sku_key": sku
            })
        
        display_df = pd.DataFrame(display_rows)
        
        cols_order = [LOG_COLUMN_NAME, "סטטוס משלוח", "מוצר", "כמות", "מספר הזמנה", "בחר"]
        
        edited_df = st.data_editor(
            display_df[cols_order],
            use_container_width=False,  
            hide_index=True,
            column_config={
                "בחר": st.column_config.CheckboxColumn("בחר", default=False, width="small"),
                "מספר הזמנה": st.column_config.TextColumn("מספר הזמנה", width="medium"),
                "כמות": st.column_config.TextColumn("כמות", width="small"),
                "מוצר": st.column_config.TextColumn("מוצר", width="large"),
                "סטטוס משלוח": st.column_config.TextColumn("מס משלוח", width="medium"),
                LOG_COLUMN_NAME: st.column_config.TextColumn("לוג", disabled=True, width="large")
            },
            disabled=["מספר הזמנה", "מוצר", "כמות", "סטטוס משלוח", LOG_COLUMN_NAME]
        )

        selected_indices = edited_df[edited_df["בחר"] == True].index
        rows_for_action = display_df.loc[selected_indices] if not selected_indices.empty else display_df 
        is_implicit_select_all = selected_indices.empty
        show_bulk_warning = (is_implicit_select_all and len(rows_for_action) > 10)

        # --- כפתורים (בדיוק לפי הלוגיקה המקורית) ---
        col_wa_policy, col_wa_contact, col_wa_install, col_mail_status, col_mail_return, col_mail_supplier = st.columns(6, gap="small")
        
        # 1. מדיניות
        with col_wa_policy:
            if not show_bulk_warning and st.button("💬 שלח מדיניות"):
                if rows_for_action.empty: st.toast("⚠️ אין נתונים")
                else:
                    count = 0
                    # שימור לוגיקה: קיבוץ לפי טלפון
                    for phone, group in rows_for_action.groupby('_raw_phone'):
                        if not phone: continue
                        orders_str = ", ".join(group['מספר הזמנה'].unique())
                        skus_str = ", ".join(group['מוצר'].unique())
                        client_name = group.iloc[0]['שם לקוח'].split()[0] if group.iloc[0]['שם לקוח'] else "לקוח"
                        
                        # הטקסט המקורי בדיוק
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
                                update_log_in_db(r['_order_key'], r['_sku_key'], "💬 נשלח ווצאפ מדיניות")
                            st.toast(f"נשלח ל-{client_name} ✅")
                    if count > 0:
                        time.sleep(1)
                        st.rerun()

        # 2. חזרנו אליך
        with col_wa_contact:
            if not show_bulk_warning and st.button("📞 חזרנו אליך"):
                if rows_for_action.empty: st.toast("⚠️ אין נתונים")
                else:
                    count = 0
                    for phone, group in rows_for_action.groupby('_raw_phone'):
                        if not phone: continue
                        orders_str = ", ".join(group['מספר הזמנה'].unique())
                        skus_str = ", ".join(group['מוצר'].unique())
                        tracking_str = ", ".join(group['סטטוס משלוח'].unique())
                        client_name = group.iloc[0]['שם לקוח'].split()[0]
                        
                        # הטקסט המקורי בדיוק
                        msg_body = f"""היי {client_name},
חוזרים אלייך מסלימפרייס לגבי הזמנה/ות: {orders_str}
מוצרים: {skus_str}
מס משלוח/ים: {tracking_str}

קיבלנו פנייה שחיפשת אותנו, איך אפשר לעזור?"""
                        if send_whatsapp_message(phone, msg_body):
                            count += 1
                            for _, r in group.iterrows():
                                update_log_in_db(r['_order_key'], r['_sku_key'], "💬 נשלח 'חזרנו אליך'")
                            st.toast(f"נשלח ל-{client_name} ✅")
                    if count > 0:
                        time.sleep(1)
                        st.rerun()

        # 3. התקנה
        with col_wa_install:
            if not show_bulk_warning and st.button("🔧 התקנה"):
                if rows_for_action.empty: st.toast("⚠️ אין נתונים")
                else:
                    all_msgs = []
                    # שימור לוגיקה: קיבוץ לפי הזמנה
                    for order_num, group in rows_for_action.groupby('מספר הזמנה'):
                        r = group.iloc[0]
                        items = ", ".join([f"{row['כמות']} X {row['מוצר']}" for _, row in group.iterrows()])
                        line = f"{order_num} | {items} | {r['שם לקוח']} | {r['כתובת מלאה']} | {r['טלפון']} | התקנה"
                        all_msgs.append(line)
                    
                    if send_whatsapp_message(INSTALLATION_PHONE, "\n\n".join(all_msgs)):
                        st.toast("נשלח למתקין ✅")
                        for _, r in rows_for_action.iterrows():
                             update_log_in_db(r['_order_key'], r['_sku_key'], "💬 נשלח למתקין")
                        time.sleep(1)
                        st.rerun()

        # 4. מייל סטטוס
        with col_mail_status:
            if not show_bulk_warning and st.button("❓ מה קורה?"):
                tn_list = [t for t in rows_for_action['סטטוס משלוח'].unique() if t and t != "התקנה"]
                
                # בדיקת כפילויות בלוג (כמו במקור)
                duplicate_alert = False
                for _, r in rows_for_action.iterrows():
                     if "נשלח בדיקה" in str(r[LOG_COLUMN_NAME]): duplicate_alert = True
                if duplicate_alert:
                     st.toast("⚠️ שים לב: כבר נשלח בעבר")
                     time.sleep(1)

                if not tn_list: st.toast("⚠️ אין מספרי משלוח")
                else:
                    tn_list = list(set(tn_list))
                    joined_nums = ", ".join(tn_list)
                    subj = f"{joined_nums} מה קורה עם זה בבקשה?" if len(tn_list)==1 else f"{joined_nums} מה קורה עם אלה בבקשה?"
                    
                    if send_custom_email(subj):
                        st.success(f"נשלח: {subj}")
                        for _, r in rows_for_action.iterrows():
                            if r['סטטוס משלוח'] in tn_list:
                                update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח בדיקה")
                        time.sleep(1)
                        st.rerun()

        # 5. מייל החזרה
        with col_mail_return:
            if not show_bulk_warning and st.button("↩️ להחזיר"):
                tn_list = [t for t in rows_for_action['סטטוס משלוח'].unique() if t and t != "התקנה"]
                if not tn_list: st.toast("⚠️ אין מספרי משלוח")
                else:
                    tn_list = list(set(tn_list))
                    joined_nums = ", ".join(tn_list)
                    subj = f"{joined_nums} להחזיר אלינו בבקשה"
                    if send_custom_email(subj):
                        st.success(f"נשלח: {subj}")

        # 6. ספקים (PO / 9)
        with col_mail_supplier:
            if not show_bulk_warning and st.button("📞 אין מענה"):
                # סינון לפי לוגיקה מקורית
                ace_g = rows_for_action[rows_for_action['מספר הזמנה'].astype(str).str.upper().str.startswith("PO")]
                pay_g = rows_for_action[rows_for_action['מספר הזמנה'].astype(str).str.startswith("9")]
                
                found_supplier = False

                # ACE
                if not ace_g.empty and EMAIL_ACE:
                    found_supplier = True
                    u_orders = ", ".join(ace_g['מספר הזמנה'].unique())
                    u_tracking = ", ".join([t for t in ace_g['סטטוס משלוח'].unique() if t and t!="התקנה"]) or "ללא מס' משלוח"
                    u_phones = ", ".join(ace_g['טלפון'].unique())
                    
                    subj = f"{u_orders} {u_tracking} - אין מענה מהלקוח - האם יש מספר טלפון אחר?"
                    body = f"הטלפון שיש לנו כרגע הוא: {u_phones}\nנא בדקו אם יש מספר אחר."
                    
                    if send_custom_email(subj, body, EMAIL_ACE):
                        st.toast("נשלח לאייס")
                        for _, r in ace_g.iterrows(): update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח ספק (אין מענה)")

                # Payngo
                if not pay_g.empty and EMAIL_PAYNGO:
                    found_supplier = True
                    u_orders = ", ".join(pay_g['מספר הזמנה'].unique())
                    u_tracking = ", ".join([t for t in pay_g['סטטוס משלוח'].unique() if t and t!="התקנה"]) or "ללא מס' משלוח"
                    u_phones = ", ".join(pay_g['טלפון'].unique())

                    subj = f"{u_orders} {u_tracking} - אין מענה מהלקוח - האם יש מספר טלפון אחר?"
                    body = f"הטלפון שיש לנו כרגע הוא: {u_phones}\nנא בדקו אם יש מספר אחר."

                    if send_custom_email(subj, body, EMAIL_PAYNGO):
                        st.toast("נשלח למחסני חשמל")
                        for _, r in pay_g.iterrows(): update_log_in_db(r['_order_key'], r['_sku_key'], "📧 נשלח ספק (אין מענה)")
                
                if not found_supplier: st.toast("⚠️ לא זוהו הזמנות ספקים תואמות")
                else: 
                    time.sleep(1)
                    st.rerun()

        st.divider()
        if not rows_for_action.empty and not show_bulk_warning:
            st.caption("העתקה לאקסל:")
            st.code("\n".join(rows_for_action["_excel_line"]), language="csv")
            st.caption("פרטים מלאים:")
            st.code("\n".join(rows_for_action["_text_line"]), language=None)
            
    else:
        st.warning(f"לא נמצאו תוצאות עבור: {clean_text_query}")
