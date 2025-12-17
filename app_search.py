import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# --- הגדרת תצוגה רחבה ---
st.set_page_config(layout="wide", page_title="איתור הזמנות", page_icon="🔎")

# --- הגדרות קבועות ---
SPREADSHEET_ID = '1xUABIGIhnLxO2PYrpAOXZdk48Q-hNYOHkht2vUyaVdE'
WORKSHEET_NAME = "הזמנות"

# --- JS להעתקה ללוח ---
# הפונקציה הזו מוזרקת לדפדפן ומאפשרת את פעולת ההעתקה
clipboard_script = """
<script>
    function copyRowToClipboard(text) {
        navigator.clipboard.writeText(text).then(function() {
            // אפשר להוסיף כאן התראה קטנה אם רוצים, כרגע זה שקט
            console.log('Copied to clipboard');
            
            // אפקט ויזואלי קטן על הכפתור
            var activeElement = document.activeElement;
            var originalText = activeElement.innerText;
            activeElement.innerText = "הועתק! ✅";
            setTimeout(function() {
                activeElement.innerText = originalText;
            }, 1000);
        }, function(err) {
            console.error('Could not copy text: ', err);
        });
    }
</script>
"""
st.components.v1.html(clipboard_script, height=0, width=0)

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
    
    /* עיצוב לטבלה המותאמת אישית */
    .custom-table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 20px;
        direction: rtl;
        font-size: 0.9em;
    }
    .custom-table th {
        background-color: #262730;
        color: white;
        padding: 12px;
        text-align: right;
        border-bottom: 2px solid #444;
    }
    .custom-table td {
        padding: 10px;
        border-bottom: 1px solid #444;
        text-align: right;
        color: #e0e0e0;
    }
    .custom-table tr:hover {
        background-color: #363945;
    }
    
    /* עיצוב כפתור ההעתקה בתוך הטבלה */
    .copy-btn {
        background-color: #4CAF50;
        border: none;
        color: white;
        padding: 5px 10px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 12px;
        margin: 2px 1px;
        cursor: pointer;
        border-radius: 4px;
        transition-duration: 0.4s;
    }
    .copy-btn:hover {
        background-color: #45a049;
    }

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
        
        # מיון לפי תאריך
        if df.shape[1] > 9:
            try:
                filtered_df['temp_date'] = pd.to_datetime(filtered_df.iloc[:, 9], dayfirst=True, errors='coerce')
                filtered_df = filtered_df.sort_values(by='temp_date', ascending=True)
            except: pass

        # --- בניית טבלת HTML מותאמת אישית ---
        
        # כותרות הטבלה
        html_table = """
        <table class="custom-table">
            <thead>
                <tr>
                    <th>פעולה</th>
                    <th>תאריך</th>
                    <th>מספר הזמנה</th>
                    <th>שם לקוח</th>
                    <th>טלפון</th>
                    <th>כתובת מלאה</th>
                    <th>מוצר</th>
                    <th>כמות</th>
                    <th>סטטוס משלוח</th>
                </tr>
            </thead>
            <tbody>
        """

        copy_texts = [] # עבור הבלוק התחתון שביקשת לא לגעת בו

        for index, row in filtered_df.iterrows():
            try:
                # שליפת הנתונים הגולמיים
                order_num = str(row.iloc[0]).strip()
                qty = str(row.iloc[1]).strip()
                sku = str(row.iloc[2]).strip()
                full_name = str(row.iloc[3]).strip()
                
                # פירוק כתובת לעמודות נפרדות לאקסל
                street = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else ""
                house = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ""
                city = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""
                
                # כתובת מלאה לתצוגה בטבלה
                address_display = f"{street} {house} {city}".strip()
                
                phone_raw = row.iloc[7]
                phone_clean = normalize_phone(phone_raw)
                phone_display = "0" + phone_clean if phone_clean else ""
                
                tracking = row.iloc[8]
                if pd.isna(tracking) or str(tracking).strip() == "": tracking = "התקנה"
                
                date_val = str(row.iloc[9]).strip()

                # לוגיקה לשם פרטי (לוקח את המילה הראשונה)
                first_name = full_name.split()[0] if full_name else ""

                # --- יצירת המחרוזת להעתקה לאקסל (טאבים מפרידים בין תאים) ---
                # סדר: מספר הזמנה, כמות, שם פרטי, רחוב, בית, עיר, טלפון
                excel_string = f"{order_num}\t{qty}\t{first_name}\t{street}\t{house}\t{city}\t{phone_display}"
                # מנקה מרכאות שעלולות לשבור את ה-JS
                excel_string_safe = excel_string.replace("'", "").replace('"', '')

                # הוספת שורה לטבלה ב-HTML
                html_table += f"""
                <tr>
                    <td>
                        <button class="copy-btn" onclick="copyRowToClipboard('{excel_string_safe}')">
                            העתק לאקסל 📋
                        </button>
                    </td>
                    <td>{date_val}</td>
                    <td>{order_num}</td>
                    <td>{full_name}</td>
                    <td>{phone_display}</td>
                    <td>{address_display}</td>
                    <td>{sku}</td>
                    <td>{qty}</td>
                    <td>{tracking}</td>
                </tr>
                """

                # בניית הטקסט לבלוק ההעתקה המהירה התחתון (שמרנו עליו כמו שביקשת)
                formatted_text = (f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, "
                                  f"שם: {full_name}, כתובת: {address_display}, טלפון: {phone_display}, "
                                  f"מספר משלוח: {tracking}, תאריך: {date_val}")
                copy_texts.append(formatted_text)
                
            except IndexError: continue

        html_table += "</tbody></table>"
        
        # הזרקת ה-JS שוב כדי לוודא זמינות (בטוח)
        st.markdown(clipboard_script, unsafe_allow_html=True)
        # הצגת הטבלה
        st.markdown(html_table, unsafe_allow_html=True)

        # הבלוק התחתון שנשאר ללא שינוי
        st.markdown("### 📋 העתקה מהירה (טקסט מלא)")
        st.code("\n".join(copy_texts), language=None)
        
    else:
        st.warning(f"לא נמצאו הזמנות עבור {search_type}: {clean_query}")
