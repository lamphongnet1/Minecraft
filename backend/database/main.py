# database/main.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import time

# -------------------------
# Database config
# -------------------------
DB_USER = "postgres"
DB_PASS = "2004"
DB_HOST = "db"       # phải là tên service trong docker-compose
DB_PORT = "5432"
DB_NAME = "vnstock_data"

# -------------------------
# Create engine with retry
# -------------------------
def create_db_engine():
    for i in range(10):  # thử connect 10 lần
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            )
            with engine.connect() as conn:
                print("DB connected!")
            return engine
        except OperationalError:
            print("DB not ready, retrying in 3s...")
            time.sleep(3)
    raise Exception("Cannot connect to DB after multiple retries")

engine = create_db_engine()

# -------------------------
# Fetch data
# -------------------------
def get_vci_data():
    try:
        df = pd.read_sql("SELECT * FROM vci_history ORDER BY time ASC", engine)
    except Exception as e:
        print("Error fetching data:", e)
        return pd.DataFrame()  # trả DataFrame rỗng
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace=True)
    return df

# -------------------------
# FastAPI app
# -------------------------
app = FastAPI(title="Database Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/history")
def history(period: str = Query("D")):
    """
    period: resample rule, e.g., 'D' = day, 'W' = week, 'M' = month
    """
    df = get_vci_data()
    if df.empty:
        return {"error": "No data available"}

    df_resampled = df.resample(period).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }).dropna()

    return df_resampled.reset_index().to_dict(orient="records")
