# clustering/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
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
    for i in range(10):
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
app = FastAPI(title="Clustering Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Endpoint
# -------------------------
@app.get("/clustering")
def clustering(n_clusters: int = 2):
    df = get_vci_data()
    if df.empty:
        return {"error": "No data available for clustering"}

    df['close_prev'] = df['close'].shift(1)
    df.dropna(inplace=True)

    if len(df) < n_clusters:
        return {"error": f"Not enough data points for {n_clusters} clusters"}

    features = ['open','high','low','close','volume']
    X = df[features]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['cluster'] = kmeans.fit_predict(X_scaled)

    return df.reset_index().to_dict(orient="records")
