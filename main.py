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
                "Title": title.encode("utf-8").decode("latin-1", errors="replace"),
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
        if abs(change) < 0.05:
            continue
        if volume_24h < 5000:
            continue
        if slug in alerted_slugs:
            continue

        if change > 0:
            sinal = "BUY"
            acao = f"Compre YES a {m['yes_price']}%"
            tags = "green_circle"
        else:
            sinal = "SELL"
            acao = f"Compre NO a {m['no_price']}%"
            tags = "red_circle"

        send_alert(
            f"PolySignal {sinal}",
            f"{m['question']}\n\nAcao: {acao}\nVariacao 24h: {round(change*100,1)}%\nVol 24h: ${round(volume_24h/1000,1)}k\nKelly: ate 5% da banca",
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
            if abs(change) < 0.05:
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

@app.get("/orphans")
def get_orphans():
    """Motor #44 — Mercados Órfãos: volume < $50k, máxima ineficiência"""
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        orphans = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue

            volume_24h = parsed["volume_24h"]
            volume = parsed["volume"]
            change = parsed["change_24h"]

            # Órfão: volume total < $50k e volume 24h < $5k
            if volume > 50000:
                continue
            if volume_24h > 5000:
                continue

            # Score de ineficiência — quanto menor o volume, maior o edge potencial
            ineficiencia = round(100 - (volume / 500), 1)
            ineficiencia = max(0, min(100, ineficiencia))

            # Distância do 50% — mercados longe do meio têm mais opinião formada
            yes = parsed["yes_price"]
            distancia_50 = abs(yes - 50)

            orphans.append({
                "question": parsed["question"],
                "slug": parsed["slug"],
                "yes_price": yes,
                "no_price": parsed["no_price"],
                "volume": volume,
                "volume_24h": volume_24h,
                "change_24h": round(change * 100, 2) if change else 0,
                "ineficiencia_score": ineficiencia,
                "distancia_50": round(distancia_50, 1),
                "tier": "orfao" if volume < 10000 else "niche",
            })

        orphans.sort(key=lambda x: x["ineficiencia_score"], reverse=True)
        return orphans[:30]
    except Exception as e:
        return {"error": str(e)}

@app.get("/narrative")
def get_narrative():
    """Narrative Drift Engine — detecta força e direção da narrativa de cada mercado"""
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        results = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue

            change = parsed["change_24h"]
            volume_24h = parsed["volume_24h"]

            if change is None or volume_24h < 1000:
                continue

            abs_change = abs(change)
            direction = "BULLISH" if change > 0 else "BEARISH"

            # Momentum narrativo — velocidade da mudança de preço
            momentum = min(round(abs_change * 300, 1), 100)

            # Convicção do mercado — distância do 50%
            yes = parsed["yes_price"]
            distancia_50 = abs(yes - 50)
            convicao = round(distancia_50 * 2, 1)

            # Volume score — mercados com mais volume têm narrativa mais forte
            vol_score = min(round((volume_24h / 10000) * 10, 1), 30)

            # Score final composto
            narrative_score = round((momentum * 0.5) + (convicao * 0.3) + (vol_score * 0.2), 1)

            if narrative_score < 15:
                continue

            # Classificação de força
            if narrative_score >= 65:
                forca = "FORTE"
                forca_color = "#30d158"
            elif narrative_score >= 35:
                forca = "MEDIA"
                forca_color = "#ff9f0a"
            else:
                forca = "FRACA"
                forca_color = "#ff453a"

            # Interpretação estratégica
            if direction == "BULLISH" and forca == "FORTE":
                interpretacao = "Narrativa bullish consolidada — momentum favorece YES"
            elif direction == "BEARISH" and forca == "FORTE":
                interpretacao = "Narrativa bearish consolidada — momentum favorece NO"
            elif forca == "MEDIA":
                interpretacao = "Narrativa em formacao — aguardar confirmacao"
            else:
                interpretacao = "Sinal fraco — ruido provavel"

            results.append({
                "question": parsed["question"],
                "slug": parsed["slug"],
                "direction": direction,
                "forca": forca,
                "forca_color": forca_color,
                "narrative_score": narrative_score,
                "momentum": momentum,
                "convicao": convicao,
                "interpretacao": interpretacao,
                "yes_price": parsed["yes_price"],
                "no_price": parsed["no_price"],
                "change_24h": round(change * 100, 2),
                "volume_24h": volume_24h,
            })

        results.sort(key=lambda x: x["narrative_score"], reverse=True)
        return results[:25]
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