from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests

app = FastAPI(title="PolySignal API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

GAMMA = "https://gamma-api.polymarket.com"

@app.get("/")
def home():
    return {"status": "ok", "service": "PolySignal", "version": "1.0"}

@app.get("/status")
def status():
    return {
        "status": "online",
        "total_markets": 0,
        "total_snapshots": 0,
        "worker_healthy": False,
    }

@app.get("/markets")
def get_markets():
    try:
        r = requests.get(f"{GAMMA}/markets", params={
            "active": True,
            "closed": False,
            "limit": 50,
        }, timeout=10)
        data = r.json()
        markets = []
        for m in data:
            tokens = m.get("tokens", [])
            yes_price = no_price = None
            for t in tokens:
                if t.get("outcome", "").upper() == "YES":
                    yes_price = round(float(t.get("price", 0)) * 100, 1)
                elif t.get("outcome", "").upper() == "NO":
                    no_price = round(float(t.get("price", 0)) * 100, 1)
            markets.append({
                "question": m.get("question", ""),
                "slug": m.get("marketSlug", ""),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": m.get("volume", 0),
                "end_date": m.get("endDate", ""),
            })
        return markets
    except Exception as e:
        return {"error": str(e)}