from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import json

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
            try:
                outcomes = json.loads(m.get("outcomes", "[]"))
                prices = json.loads(m.get("outcomePrices", "[]"))
            except Exception:
                continue

            yes_price = no_price = None
            for i, outcome in enumerate(outcomes):
                try:
                    price = round(float(prices[i]) * 100, 1)
                except Exception:
                    price = None
                if str(outcome).upper() == "YES":
                    yes_price = price
                elif str(outcome).upper() == "NO":
                    no_price = price

            if yes_price is None and no_price is None:
                continue

            markets.append({
                "question": m.get("question", ""),
                "slug": m.get("slug", ""),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": round(float(m.get("volume", 0) or 0), 2),
                "end_date": m.get("endDate", ""),
            })
        return markets
    except Exception as e:
        return {"error": str(e)}