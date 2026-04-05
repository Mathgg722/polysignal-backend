from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
import json
import os
import threading
import time
from datetime import datetime, timezone

app = FastAPI(title="PolySignal API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── PostgreSQL ──────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")
engine = None
Session = None
Base = declarative_base()

class Snapshot(Base):
    __tablename__ = "snapshots"
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(String, index=True)
    question = Column(String)
    yes_price = Column(Float)
    no_price = Column(Float)
    volume = Column(Float)
    volume_24h = Column(Float)
    change_24h = Column(Float)
    captured_at = Column(DateTime, default=datetime.utcnow)

if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL.replace("postgres://", "postgresql://", 1))
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        print("✅ PostgreSQL conectado")
    except Exception as e:
        print(f"❌ PostgreSQL erro: {e}")

# ── Estado global ───────────────────────────────────────────
state = {
    "total_markets": 0,
    "total_snapshots": 0,
    "last_collection": None,
    "worker_healthy": False,
}

GAMMA = "https://gamma-api.polymarket.com"
NTFY_TOPIC = "polysignal-matheus"
alerted_slugs = set()

# ── Ntfy ────────────────────────────────────────────────────
def send_alert(title, message, tags="chart_with_upwards_trend"):
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title": title,
                "Priority": "high",
                "Tags": tags,
            },
            timeout=5
        )
        print(f"🔔 Alerta enviado: {title}")
    except Exception as e:
        print(f"❌ Ntfy erro: {e}")

# ── Coleta ──────────────────────────────────────────────────
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

    change = m.get("oneDayPriceChange", None)
    if change is not None:
        try:
            change = float(change)
        except Exception:
            change = None

    return {
        "question": m.get("question", ""),
        "slug": m.get("slug", ""),
        "yes_price": yes_price,
        "no_price": no_price,
        "volume": round(float(m.get("volume", 0) or 0), 2),
        "volume_24h": round(float(m.get("volume24hr", 0) or 0), 2),
        "end_date": end_date_str,
        "change_24h": change,
    }

def save_snapshots(markets):
    if not Session:
        print("❌ Session não existe")
        return
    try:
        session = Session()
        for m in markets:
            snap = Snapshot(
                slug=m["slug"],
                question=m["question"],
                yes_price=m["yes_price"],
                no_price=m["no_price"],
                volume=m["volume"],
                volume_24h=m["volume_24h"],
                change_24h=m["change_24h"],
                captured_at=datetime.utcnow(),
            )
            session.add(snap)
        session.commit()
        result = session.execute(text("SELECT COUNT(*) FROM snapshots")).scalar()
        state["total_snapshots"] = result
        session.close()
        print(f"✅ Snapshots salvos: {result}")
    except Exception as e:
        print(f"❌ Snapshot erro detalhado: {e}")
        import traceback
        traceback.print_exc()

def check_alerts(markets):
    for m in markets:
        change = m.get("change_24h")
        volume_24h = m.get("volume_24h", 0)
        slug = m.get("slug", "")

        if change is None:
            continue
        if abs(change) < 0.01:
            continue
        if volume_24h < 5000:
            continue
        if slug in alerted_slugs:
            continue

        sinal = "BUY" if change > 0 else "SELL"
        emoji = "🟢" if change > 0 else "🔴"
        tags = "green_circle" if change > 0 else "red_circle"

        send_alert(
            f"{emoji} PolySignal — {sinal} FORTE",
            f"{m['question']}\n\nVariação 24h: {round(change*100,1)}%\nVol 24h: ${round(volume_24h/1000,1)}k\nYES: {m['yes_price']}% | NO: {m['no_price']}%",
            tags=tags
        )
        alerted_slugs.add(slug)

# ── Worker ──────────────────────────────────────────────────
def worker_loop():
    print("🔄 Worker iniciado")
    while True:
        try:
            now = datetime.now(timezone.utc)
            all_data = fetch_all_markets()
            markets = [parse_market(m, now) for m in all_data]
            markets = [m for m in markets if m]
            state["total_markets"] = len(markets)
            state["last_collection"] = datetime.utcnow().isoformat()
            state["worker_healthy"] = True
            save_snapshots(markets)
            check_alerts(markets)
            print(f"✅ {len(markets)} mercados coletados · {state['total_snapshots']} snapshots")
        except Exception as e:
            state["worker_healthy"] = False
            print(f"❌ Worker erro: {e}")
        time.sleep(60)

threading.Thread(target=worker_loop, daemon=True).start()

# ── Endpoints ────────────────────────────────────────────────
@app.get("/")
def home():
    return {"status": "ok", "service": "PolySignal", "version": "2.0"}

@app.get("/status")
def status():
    return {
        "status": "online",
        "total_markets": state["total_markets"],
        "total_snapshots": state["total_snapshots"],
        "last_collection": state["last_collection"],
        "worker_healthy": state["worker_healthy"],
        "db_connected": Session is not None,
        "alerts_sent": len(alerted_slugs),
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

            signal = "BUY" if change > 0 else "SELL"
            confidence = round(min(abs(change) * 200, 95) / 100, 2)

            p = parsed["yes_price"] / 100 if signal == "BUY" else parsed["no_price"] / 100
            edge = abs(change)
            kelly = round((edge / max(1 - p, 0.01)) * 0.25 * 100, 1)
            kelly = min(kelly, 5.0)

            signals.append({
                "question": parsed["question"],
                "slug": parsed["slug"],
                "signal": signal,
                "yes_price": parsed["yes_price"],
                "no_price": parsed["no_price"],
                "change_24h": round(change * 100, 2),
                "confidence": confidence,
                "kelly": kelly,
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
            "https://api.kalshi.com/trade-api/v2/markets",
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