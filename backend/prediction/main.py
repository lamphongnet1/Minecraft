# prediction/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sklearn.linear_model import LinearRegression
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import time

# -------------------------
# Database config
# -------------------------
DB_USER = "postgres"
DB_PASS = "2004"
DB_HOST = "db"       # tên service trong docker-compose
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
            with engine.connect():
                print("DB connected!")
            return engine
        except OperationalError:
            print("DB not ready, retry in 3s...")
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
        return pd.DataFrame()
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"])
    df.set_index("time", inplace=True)
    return df

# -------------------------
# FastAPI app
# -------------------------
app = FastAPI(title="Prediction Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Endpoint
# -------------------------
@app.get("/prediction")
def prediction():
    df = get_vci_data()
    if df.empty:
        return {"error": "No data available for prediction"}

    features = ['open','high','low','close','volume']
    df['close_next'] = df['close'].shift(-1)
    df_train = df.dropna()

    if df_train.empty:
        return {"error": "Not enough data to train model"}

    model = LinearRegression().fit(df_train[features], df_train['close_next'])

    current = df.iloc[[-1]][features]
    current_price = float(current["close"])
    pred_price = float(model.predict(current)[0])

    trend = (
        "Tăng" if pred_price > current_price
        else "Giảm" if pred_price < current_price
        else "Đi ngang"
    )

    return {
        "current_price": current_price,
        "pred_price": pred_price,
        "pred_class": trend
    }
