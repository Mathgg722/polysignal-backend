from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import json
from datetime import datetime, timezone

app = FastAPI(title="PolySignal API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

GAMMA = "https://gamma-api.polymarket.com"

def fetch_all_markets():
    all_markets = []
    offset = 0
    limit = 100
    while True:
        try:
            r = requests.get(f"{GAMMA}/markets", params={
                "active": True,
                "closed": False,
                "limit": limit,
                "offset": offset,
            }, timeout=15)
            data = r.json()
            if not data:
                break
            all_markets.extend(data)
            if len(data) < limit:
                break
            offset += limit
            if offset > 300:
                break
        except Exception:
            break
    return all_markets

def parse_market(m, now):
    try:
        outcomes = json.loads(m.get("outcomes", "[]"))
        prices = json.loads(m.get("outcomePrices", "[]"))
    except Exception:
        return None

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

    if yes_price is None or no_price is None:
        return None
    if yes_price < 5 or yes_price > 95:
        return None

    end_date_str = m.get("endDate", "")
    try:
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        if end_date < now:
            return None
    except Exception:
        return None

    return {
        "question": m.get("question", ""),
        "slug": m.get("slug", ""),
        "yes_price": yes_price,
        "no_price": no_price,
        "volume": round(float(m.get("volume", 0) or 0), 2),
        "volume_24h": round(float(m.get("volume24hr", 0) or 0), 2),
        "end_date": end_date_str,
        "last_trade": m.get("lastTradePrice", None),
        "change_24h": m.get("oneDayPriceChange", None),
    }

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
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        markets = []
        for m in all_data:
            parsed = parse_market(m, now)
            if parsed:
                markets.append(parsed)
        markets.sort(key=lambda x: x["volume_24h"], reverse=True)
        return markets
    except Exception as e:
        return {"error": str(e)}

@app.get("/signals")
def get_signals():
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        signals = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue

            change = parsed["change_24h"]
            volume_24h = parsed["volume_24h"]

            if change is None:
                continue
            if abs(change) < 0.01:
                continue
            if volume_24h < 1000:
                continue

            sinal = "BUY" if change > 0 else "SELL"
            confianca = min(round(abs(change) * 200, 0), 95)

            p = parsed["yes_price"] / 100 if sinal == "BUY" else parsed["no_price"] / 100
            edge = abs(change)
            kelly = round((edge / max(1 - p, 0.01)) * 0.25 * 100, 1)
            kelly = min(kelly, 5.0)

            signals.append({
                "question": parsed["question"],
                "slug": parsed["slug"],
                "sinal": sinal,
                "yes_price": parsed["yes_price"],
                "no_price": parsed["no_price"],
                "change_24h": round(change * 100, 2),
                "confianca": confianca,
                "kelly_pct": kelly,
                "volume_24h": volume_24h,
            })

        signals.sort(key=lambda x: abs(x["change_24h"]), reverse=True)
        return signals
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/kalshi")
def get_kalshi():
    try:
        r = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"limit": 200, "status": "open"},
            timeout=10
        )
        data = r.json()
        markets = []
        for m in data.get("markets", []):
            yes_price = m.get("yes_ask", None)
            no_price = m.get("no_ask", None)
            if yes_price is None or no_price is None:
                continue
            yes_price = round(yes_price, 1)
            no_price = round(no_price, 1)
            if yes_price < 5 or yes_price > 95:
                continue
            markets.append({
                "question": m.get("title", ""),
                "slug": m.get("ticker", ""),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": round(float(m.get("volume", 0) or 0), 2),
                "volume_24h": round(float(m.get("volume_24h", 0) or 0), 2),
                "end_date": m.get("close_time", ""),
                "platform": "kalshi",
            })
        markets.sort(key=lambda x: x["volume_24h"], reverse=True)
        return markets
    except Exception as e:
        return {"error": str(e)}    
    
    