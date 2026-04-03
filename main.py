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
            "limit": 100,
        }, timeout=10)
        data = r.json()
        now = datetime.now(timezone.utc)
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

            if yes_price is None or no_price is None:
                continue

            # Filtro 1: só mercados com end_date no futuro
            end_date_str = m.get("endDate", "")
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                if end_date < now:
                    continue
            except Exception:
                continue

            # Filtro 2: preço entre 5% e 95% (mercados vivos)
            if yes_price < 5 or yes_price > 95:
                continue

            markets.append({
                "question": m.get("question", ""),
                "slug": m.get("slug", ""),
                "yes_price": yes_price,
                "no_price": no_price,
                "volume": round(float(m.get("volume", 0) or 0), 2),
                "volume_24h": round(float(m.get("volume24hr", 0) or 0), 2),
                "end_date": end_date_str,
                "last_trade": m.get("lastTradePrice", None),
                "change_24h": m.get("oneDayPriceChange", None),
            })

        # Ordena por volume 24h decrescente
        markets.sort(key=lambda x: x["volume_24h"], reverse=True)
        return markets

    except Exception as e:
        return {"error": str(e)}
    
@app.get("/signals")
def get_signals():
    try:
        r = requests.get(f"{GAMMA}/markets", params={
            "active": True,
            "closed": False,
            "limit": 100,
        }, timeout=10)
        data = r.json()
        now = datetime.now(timezone.utc)
        signals = []

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

            if yes_price is None or no_price is None:
                continue
            if yes_price < 5 or yes_price > 95:
                continue

            end_date_str = m.get("endDate", "")
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                if end_date < now:
                    continue
            except Exception:
                continue

            change = m.get("oneDayPriceChange", None)
            volume_24h = float(m.get("volume24hr", 0) or 0)

            if change is None:
                continue
            if abs(change) < 0.01:
                continue
            if volume_24h < 1000:
                continue

            sinal = "BUY" if change > 0 else "SELL"
            confianca = min(round(abs(change) * 200, 0), 95)

            # Kelly simples
            p = yes_price / 100 if sinal == "BUY" else no_price / 100
            edge = abs(change)
            kelly = round((edge / (1 - p)) * 0.25 * 100, 1)
            kelly = min(kelly, 5.0)

            signals.append({
                "question": m.get("question", ""),
                "slug": m.get("slug", ""),
                "sinal": sinal,
                "yes_price": yes_price,
                "no_price": no_price,
                "change_24h": round(change * 100, 2),
                "confianca": confianca,
                "kelly_pct": kelly,
                "volume_24h": round(volume_24h, 2),
            })

        signals.sort(key=lambda x: abs(x["change_24h"]), reverse=True)
        return signals

    except Exception as e:
        return {"error": str(e)}    