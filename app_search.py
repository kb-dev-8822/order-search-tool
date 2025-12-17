import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# --- הגדרת תצוגה רחבה ---
st.set_page_config(layout="wide", page_title="איתור הזמנות", page_icon="🔎")

# --- הגדרות קבועות ---
SPREADSHEET_ID = '1xUABIGIhnLxO2PYrpAOXZdk48Q-hNYOHkht2vUyaVdE'
WORKSHEET_NAME = "הזמנות"

# --- JS להעתקה ללוח (חובה כדי שהכפתור יעבוד) ---
clipboard_script = """
<script>
    function copyRowToClipboard(text) {
        navigator.clipboard.writeText(text).then(function() {
            console.log('Copied to clipboard');
        }, function(err) {
            console.error('Could not copy text: ', err);
        });
    }
</script>
"""
# מזריק את הסקריפט לדף בצורה נסתרת
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
    
    /* עיצוב הטבלה */
    .custom-table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 20px;
        direction: rtl;
        font-size: 0.95em;
        font-family: sans-serif;
    }
    .custom-table th {
        background-color: #262730;
        color: white;
        padding: 12px;
        text-align: right;
        border-bottom: 2px solid #555;
    }
    .custom-table td {
        padding: 10px;
        border-bottom: 1px solid #444;
        text-align: right;
        color: #ddd;
        vertical-align: middle;
    }
    .custom-table tr:hover {
        background-color: #363945;
    }
    
    /* כפתור העתקה משופר */
    .copy-btn {
        background-color: #4CAF50;
        border: none;
        color: white;
        padding: 6px 12px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 13px;
        font-weight: bold;
        border-radius: 4px;
        cursor: pointer;
        transition: 0.2s;
    }
    .copy-btn:hover {
        background-color: #45a049;
        transform: scale(1.05);
    }
    .copy-btn:active {
        transform: scale(0.95);
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
        
        # מיון לפי תאריך (לצורך סידור, גם אם לא מציגים אותו)
        if df.shape[1] > 9:
            try:
                filtered_df['temp_date'] = pd.to_datetime(filtered_df.iloc[:, 9], dayfirst=True, errors='coerce')
                filtered_df = filtered_df.sort_values(by='temp_date', ascending=True)
            except: pass

        # --- בניית הטבלה ---
        # בניית הכותרות (ללא תאריך)
        # שים לב: הכל בשורה אחת או צמוד לשמאל כדי למנוע זיהוי כקוד
        html_table = """
        <table class="custom-table">
            <thead>
                <tr>
                    <th style="width: 100px;">פעולה</th>
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

        copy_texts = []

        for index, row in filtered_df.iterrows():
            try:
                # נתונים
                order_num = str(row.iloc[0]).strip()
                qty = str(row.iloc[1]).strip()
                sku = str(row.iloc[2]).strip()
                full_name = str(row.iloc[3]).strip()
                
                # כתובת מפורקת להעתקה
                street = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else ""
                house = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ""
                city = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""
                
                # כתובת לתצוגה
                address_display = f"{street} {house} {city}".strip()
                
                phone_raw = row.iloc[7]
                phone_clean = normalize_phone(phone_raw)
                phone_display = "0" + phone_clean if phone_clean else ""
                
                tracking = row.iloc[8]
                if pd.isna(tracking) or str(tracking).strip() == "": tracking = "התקנה"
                
                date_val = str(row.iloc[9]).strip() # שומרים בצד לטקסט למטה

                first_name = full_name.split()[0] if full_name else ""

                # סטרינג להעתקה לאקסל (טאבים)
                excel_string = f"{order_num}\t{qty}\t{first_name}\t{street}\t{house}\t{city}\t{phone_display}"
                excel_string_safe = excel_string.replace("'", "").replace('"', '')

                # בניית השורה ב-HTML (חשוב! ללא הזחות מיותרות)
                row_html = f"""
                <tr>
                    <td><button class="copy-btn" onclick="copyRowToClipboard('{excel_string_safe}')">העתק 📋</button></td>
                    <td>{order_num}</td>
                    <td>{full_name}</td>
                    <td>{phone_display}</td>
                    <td>{address_display}</td>
                    <td>{sku}</td>
                    <td>{qty}</td>
                    <td>{tracking}</td>
                </tr>"""
                
                html_table += row_html

                # טקסט לבלוק התחתון
                formatted_text = (f"פרטי הזמנה: מספר הזמנה: {order_num}, כמות: {qty}, מק\"ט: {sku}, "
                                  f"שם: {full_name}, כתובת: {address_display}, טלפון: {phone_display}, "
                                  f"מספר משלוח: {tracking}, תאריך: {date_val}")
                copy_texts.append(formatted_text)
                
            except IndexError: continue

        html_table += "</tbody></table>"
        
        # הזרקת הסקריפט והטבלה
        st.markdown(clipboard_script, unsafe_allow_html=True)
        st.markdown(html_table, unsafe_allow_html=True)

        st.markdown("### 📋 העתקה מהירה (טקסט מלא)")
        st.code("\n".join(copy_texts), language=None)
        
    else:
        st.warning(f"לא נמצאו הזמנות עבור {search_type}: {clean_query}")
