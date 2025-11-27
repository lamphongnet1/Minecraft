from vnstock import Quote
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
import time
from datetime import datetime

# --- Cấu hình database ---
DB_USER = 'postgres'
DB_PASS = '2004'
DB_HOST = 'db'
DB_PORT = '5432'
DB_NAME = 'vnstock_data'

# --- Kết nối DB với retry logic ---
def create_db_engine():
    """Thử kết nối database với retry logic"""
    for i in range(20):  # thử 20 lần (60 giây)
        try:
            engine = create_engine(
                f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
            )
            with engine.connect() as conn:
                print(f"[{datetime.now()}] ✅ Kết nối DB thành công!")
            return engine
        except OperationalError as e:
            print(f"[{datetime.now()}] ⏳ DB chưa sẵn sàng, thử lại sau 3s... (lần {i+1}/20)")
            time.sleep(3)
    raise Exception("❌ Không thể kết nối DB sau 20 lần thử!")

engine = create_db_engine()

# --- Tạo table nếu chưa tồn tại ---
def create_table_if_not_exists():
    """Tạo table vci_history nếu chưa có"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS vci_history (
        time TIMESTAMP PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
    );
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print(f"[{datetime.now()}] ✅ Đã kiểm tra/tạo table vci_history")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi tạo table: {e}")
        raise

# --- Hàm lấy dữ liệu ---
def fetch_vci_data():
    """Lấy dữ liệu VCI từ vnstock"""
    try:
        quote = Quote(symbol='VCI', source='VCI')
        df = quote.history(start='2022-01-01')
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        
        # Chỉ giữ các cột cần thiết
        df = df[['open', 'high', 'low', 'close', 'volume']]
        
        print(f"[{datetime.now()}] ✅ Lấy được {len(df)} dòng dữ liệu từ vnstock")
        return df
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi lấy dữ liệu vnstock: {e}")
        return pd.DataFrame()

# --- Hàm lưu vào database ---
def save_to_db(df):
    """Lưu dữ liệu vào DB, bỏ qua dòng trùng"""
    if df.empty:
        print(f"[{datetime.now()}] ⚠️ DataFrame rỗng, bỏ qua lưu DB")
        return
    
    try:
        # Lấy những time đã có trong DB
        existing = pd.read_sql("SELECT time FROM vci_history", engine)
        
        # Chỉ giữ những dòng chưa có trong DB
        df_to_insert = df[~df.index.isin(existing['time'])]
        
        if not df_to_insert.empty:
            df_to_insert.to_sql(
                'vci_history',
                con=engine,
                if_exists='append',
                index=True,
                index_label='time',
                method='multi'
            )
            print(f"[{datetime.now()}] ✅ Đã lưu {len(df_to_insert)} dòng mới vào DB")
        else:
            print(f"[{datetime.now()}] ℹ️ Không có dòng mới để lưu")
    except ProgrammingError:
        # Table chưa tồn tại, tạo mới và insert toàn bộ
        print(f"[{datetime.now()}] ⚠️ Table chưa tồn tại, tạo mới...")
        create_table_if_not_exists()
        df.to_sql(
            'vci_history',
            con=engine,
            if_exists='append',
            index=True,
            index_label='time',
            method='multi'
        )
        print(f"[{datetime.now()}] ✅ Đã tạo table và lưu {len(df)} dòng")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi lưu DB: {e}")

# --- Main: Tạo table và lấy dữ liệu ban đầu ---
print(f"[{datetime.now()}] 🚀 Bắt đầu Ingestion Service...")

# Tạo table nếu chưa có
create_table_if_not_exists()

# Lấy dữ liệu lần đầu
print(f"[{datetime.now()}] 📥 Đang lấy dữ liệu ban đầu...")
df_initial = fetch_vci_data()
save_to_db(df_initial)

# --- Loop lấy dữ liệu định kỳ ---
print(f"[{datetime.now()}] 🔄 Bắt đầu loop cập nhật mỗi 5 phút...")
while True:
    try:
        time.sleep(5*60)  # Đợi 5 phút
        
        df = fetch_vci_data()
        save_to_db(df)
        
    except KeyboardInterrupt:
        print(f"\n[{datetime.now()}] ⛔ Dừng Ingestion Service")
        break
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Lỗi trong loop: {e}")
        time.sleep(60)  # Đợi 1 phút rồi thử lại