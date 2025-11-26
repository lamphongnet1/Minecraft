from vnstock import Quote
import pandas as pd
from sqlalchemy import create_engine
import time
from datetime import datetime

# --- Cấu hình database ---
DB_USER = 'postgres'
DB_PASS = '2004'
DB_HOST = 'db'
DB_PORT = '5432'
DB_NAME = 'vnstock_data'

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# --- Hàm lấy dữ liệu ---
def fetch_vci_data():
    quote = Quote(symbol='VCI', source='VCI')
    df = quote.history(start='2022-01-01')  # từ 2022 đến hiện tại
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    return df

# --- Hàm lưu vào database ---
def save_to_db(df):
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
        print(f"[{datetime.now()}] Đã lưu {len(df_to_insert)} dòng mới vào DB")
    else:
        print(f"[{datetime.now()}] Không có dòng mới để lưu")

# --- Loop lấy dữ liệu mỗi 5 phút ---
while True:
    try:
        df = fetch_vci_data()
        save_to_db(df)
        print(f"[{datetime.now()}] Đã lưu dữ liệu VCI vào DB, số dòng: {len(df)}")
    except Exception as e:
        print(f"[{datetime.now()}] Lỗi: {e}")
    
    time.sleep(5*60)  # 5 phút
