from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
import json
import os
import threading
import time
from datetime import datetime, timezone

app = FastAPI(title="PolySignal API", version="3.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        engine = create_engine(
            DATABASE_URL.replace("postgres://", "postgresql://", 1),
            pool_pre_ping=True,
            pool_size=3,
            max_overflow=0,
            pool_recycle=300,
        )
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        print("✅ PostgreSQL conectado")
    except Exception as e:
        print(f"❌ PostgreSQL erro: {e}")

state = {
    "total_markets": 0,
    "total_snapshots": 0,
    "last_collection": None,
    "worker_healthy": False,
}

GAMMA = "https://gamma-api.polymarket.com"
NTFY_TOPIC = "polysignal-matheus"
alerted_slugs = set()

# ── Filtros universais ────────────────────────────────────────────────────────
FILTER_MIN_PRICE  = 5.0    # 5%
FILTER_MAX_PRICE  = 95.0   # 95%
FILTER_MIN_VOLUME = 1_000  # $1k volume total mínimo

# ── Tiers de volume (dossiê v4.1 insight #2) ─────────────────────────────────
def get_volume_tier(volume: float) -> dict:
    if volume < 50_000:
        return {"tier": "orfao",        "label": "Órfão",        "kelly_mult": 1.0, "cor": "#bf5af2", "acuracia": "62%"}
    elif volume < 500_000:
        return {"tier": "niche",        "label": "Niche",        "kelly_mult": 0.75,"cor": "#0a84ff", "acuracia": "68%"}
    elif volume < 2_000_000:
        return {"tier": "medio",        "label": "Médio",        "kelly_mult": 0.5, "cor": "#ff9f0a", "acuracia": "80%"}
    else:
        return {"tier": "institucional","label": "Institucional","kelly_mult": 0.0, "cor": "#ff453a", "acuracia": "95%"}

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

def fetch_all_markets():
    all_markets = []
    offset = 0
    limit = 100
    while True:
        try:
            r = requests.get(f"{GAMMA}/markets", params={
                "active": True, "closed": False,
                "limit": limit, "offset": offset,
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
        prices   = json.loads(m.get("outcomePrices", "[]"))
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
    if yes_price < FILTER_MIN_PRICE or yes_price > FILTER_MAX_PRICE:
        return None

    end_date_str = m.get("endDate", "")
    days_to_close = None
    try:
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        if end_date < now:
            return None
        days_to_close = (end_date - now).days
    except Exception:
        return None

    change = m.get("oneDayPriceChange", None)
    if change is not None:
        try:
            change = float(change)
        except Exception:
            change = None

    volume    = round(float(m.get("volume", 0) or 0), 2)
    volume_24h= round(float(m.get("volume24hr", 0) or 0), 2)

    if volume < FILTER_MIN_VOLUME:
        return None

    tier = get_volume_tier(volume)

    return {
        "question":     m.get("question", ""),
        "slug":         m.get("slug", ""),
        "yes_price":    yes_price,
        "no_price":     no_price,
        "volume":       volume,
        "volume_24h":   volume_24h,
        "end_date":     end_date_str,
        "days_to_close":days_to_close,
        "change_24h":   change,
        "tier":         tier["tier"],
        "tier_label":   tier["label"],
        "tier_cor":     tier["cor"],
        "tier_kelly_mult": tier["kelly_mult"],
        "tier_acuracia":   tier["acuracia"],
    }

def save_snapshots(markets):
    if not Session:
        return
    try:
        session = Session()
        for m in markets:
            snap = Snapshot(
                slug=m["slug"], question=m["question"],
                yes_price=m["yes_price"], no_price=m["no_price"],
                volume=m["volume"], volume_24h=m["volume_24h"],
                change_24h=m["change_24h"], captured_at=datetime.utcnow(),
            )
            session.add(snap)
        session.commit()
        result = session.execute(text("SELECT COUNT(*) FROM snapshots")).scalar()
        state["total_snapshots"] = result
        session.close()
        print(f"✅ Snapshots salvos: {result}")
    except Exception as e:
        print(f"❌ Snapshot erro: {e}")

def check_alerts(markets):
    for m in markets:
        change     = m.get("change_24h")
        volume_24h = m.get("volume_24h", 0)
        slug       = m.get("slug", "")
        tier       = m.get("tier", "institucional")
        if change is None or abs(change) < 0.05 or volume_24h < 5000 or slug in alerted_slugs:
            continue
        if tier == "institucional":
            continue
        sinal = "BUY" if change > 0 else "SELL"
        acao  = f"Compre YES a {m['yes_price']}%" if change > 0 else f"Compre NO a {m['no_price']}%"
        tags  = "green_circle" if change > 0 else "red_circle"
        dias  = m.get("days_to_close", "?")
        send_alert(
            f"PolySignal {sinal} [{m.get('tier_label','?')}]",
            f"{m['question']}\n\nAcao: {acao}\nVariacao 24h: {round(change*100,1)}%\nVol 24h: ${round(volume_24h/1000,1)}k\nFecha em: {dias} dias\nTier: {m.get('tier_label','?')} ({m.get('tier_acuracia','?')} acuracia)\nKelly: ate 5% da banca",
            tags=tags
        )
        alerted_slugs.add(slug)

def build_recommendations_snapshot(markets, historico):
    results = []
    for m in markets:
        slug       = m["slug"]
        change     = m["change_24h"]
        volume_24h = m["volume_24h"]
        yes        = m["yes_price"]
        no         = m["no_price"]
        days       = m["days_to_close"]
        kelly_mult = m.get("tier_kelly_mult", 0.5)

        if kelly_mult == 0.0:
            continue
        if volume_24h < 1000:
            continue

        sinal_score = 0; sinal_dir = None
        if change is not None and abs(change) >= 0.01:
            sinal_score = min(abs(change) * 200, 40)
            sinal_dir   = "BUY" if change > 0 else "SELL"

        anomalia_score = 0; reversao_score = 0; reversao_dir = None; avg_yes = None
        if slug in historico:
            h       = historico[slug]
            avg_yes = h["avg_yes"]; std_yes = h["std_yes"]
            avg_vol = h["avg_vol"]; std_vol = h["std_vol"]
            if avg_yes and std_yes > 0.5:
                preco_zscore   = (yes - avg_yes) / std_yes
                vol_zscore     = (volume_24h - avg_vol) / std_vol if std_vol > 100 else 0
                anomalia_score = round(min(abs(preco_zscore) * 15, 20) + min(abs(vol_zscore) * 10, 20), 1)
                desvio         = yes - avg_yes
                desvio_pct     = abs((desvio / avg_yes) * 100) if avg_yes > 0 else 0
                reversao_score = min(round(abs(preco_zscore) * 10 + desvio_pct, 1), 40)
                reversao_dir   = "SELL" if desvio > 0 else "BUY"

        prazo_bonus = 0
        if days is not None:
            if days <= 1:   prazo_bonus = 20
            elif days <= 3: prazo_bonus = 15
            elif days <= 7: prazo_bonus = 10
            elif days <= 30:prazo_bonus = 5

        score_total = round((sinal_score * 0.35) + (anomalia_score * 0.15) + (reversao_score * 0.35) + (prazo_bonus * 0.15), 1)
        if score_total < 8:
            continue

        votos_buy  = (1 if sinal_dir == "BUY" else 0) + (1 if reversao_dir == "BUY" else 0)
        votos_sell = (1 if sinal_dir == "SELL" else 0) + (1 if reversao_dir == "SELL" else 0)
        if votos_buy > votos_sell:
            direcao = "COMPRE SIM"; acao = f"Compre SIM (YES) a {yes}%"; p = yes / 100
        elif votos_sell > votos_buy:
            direcao = "COMPRE NAO"; acao = f"Compre NAO (NO) a {no}%"; p = no / 100
        else:
            continue

        edge  = abs(change) if change else 0.01
        kelly = min(round((edge / max(1 - p, 0.01)) * 0.25 * kelly_mult * 100, 1), 5.0)

        results.append({
            "question": m["question"], "direcao": direcao, "acao": acao,
            "kelly": kelly, "score_total": score_total, "days_to_close": days,
            "change_24h": round(change * 100, 2) if change else 0,
            "volume_24h": volume_24h,
            "tier": m.get("tier"), "tier_label": m.get("tier_label"),
            "tier_acuracia": m.get("tier_acuracia"),
        })

    results.sort(key=lambda x: x["score_total"], reverse=True)
    return results[:5]

def send_hourly_summary(markets, historico):
    try:
        hora = datetime.now().strftime("%H:%M")
        recs = build_recommendations_snapshot(markets, historico)
        if not recs:
            send_alert(f"PolySignal {hora} — Sem apostas agora",
                "Nenhuma oportunidade detectada.\n\nO sistema continua monitorando.",
                tags="hourglass_flowing_sand")
            return
        linhas = [f"RESUMO HORARIO — {hora}\n", f"{len(recs)} APOSTA(S):\n", "=" * 30]
        for i, r in enumerate(recs):
            emoji   = "COMPRE SIM" if "SIM" in r["direcao"] else "COMPRE NAO"
            dias_txt= f"Fecha em {r['days_to_close']} dias" if r['days_to_close'] is not None else "Sem prazo"
            var_txt = f"+{r['change_24h']}%" if r['change_24h'] > 0 else f"{r['change_24h']}%"
            linhas += [
                f"\n#{i+1} — {emoji}",
                f"Pergunta: {r['question']}",
                f"O que fazer: {r['acao']}",
                f"Variacao hoje: {var_txt}",
                f"Volume: ${round(r['volume_24h']/1000,1)}k",
                f"Prazo: {dias_txt}",
                f"Tier: {r.get('tier_label','?')} ({r.get('tier_acuracia','?')} acuracia)",
                f"Quanto apostar: ate {r['kelly']}% do seu dinheiro",
                f"Confianca: {r['score_total']}/100",
            ]
        linhas += ["\n" + "=" * 30, "REGRAS:", "- Nunca aposte mais do sugerido", "- Em duvida, nao aposte"]
        send_alert(f"PolySignal {hora} — {len(recs)} aposta(s)", "\n".join(linhas), tags="bar_chart")
    except Exception as e:
        print(f"❌ Resumo horario erro: {e}")

def worker_loop():
    print("🔄 Worker iniciado")
    last_summary_hour = -1
    while True:
        try:
            now      = datetime.now(timezone.utc)
            all_data = fetch_all_markets()
            markets  = [parse_market(m, now) for m in all_data]
            markets  = [m for m in markets if m]
            state["total_markets"]  = len(markets)
            state["last_collection"]= datetime.utcnow().isoformat()
            state["worker_healthy"] = True
            save_snapshots(markets)
            check_alerts(markets)
            hora_atual = datetime.now().hour
            if hora_atual != last_summary_hour:
                historico = {}
                if Session:
                    try:
                        session = Session()
                        rows = session.execute(text("""
                            SELECT slug, AVG(yes_price), STDDEV(yes_price), AVG(volume_24h), STDDEV(volume_24h), COUNT(*)
                            FROM snapshots GROUP BY slug HAVING COUNT(*) >= 5
                        """)).fetchall()
                        session.close()
                        historico = {r[0]: {"avg_yes": r[1], "std_yes": r[2] or 0, "avg_vol": r[3] or 0, "std_vol": r[4] or 0} for r in rows}
                    except Exception as e:
                        print(f"❌ Historico erro: {e}")
                send_hourly_summary(markets, historico)
                last_summary_hour = hora_atual
            print(f"✅ {len(markets)} mercados · {state['total_snapshots']} snapshots")
        except Exception as e:
            state["worker_healthy"] = False
            print(f"❌ Worker erro: {e}")
        time.sleep(60)

threading.Thread(target=worker_loop, daemon=True).start()

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
def home():
    return {"status": "ok", "service": "PolySignal", "version": "3.1"}

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
def get_markets(
    max_days:   int = Query(default=0),
    max_volume: int = Query(default=0),
    tier:       str = Query(default=""),
):
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        markets  = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            if max_volume > 0 and parsed["volume"] > max_volume:
                continue
            if tier and parsed["tier"] != tier:
                continue
            markets.append(parsed)
        markets.sort(key=lambda x: x["volume_24h"], reverse=True)
        return markets
    except Exception as e:
        return {"error": str(e)}

@app.get("/niche")
def get_niche(
    max_days:   int = Query(default=90),
    min_change: float = Query(default=0.0),
):
    """
    Motor #44 expandido — Mercados Órfãos e Niche.
    Retorna apenas tiers 'orfao' e 'niche' (volume < $500k).
    Máxima zona de edge: acurácia histórica 62-68% vs 95% nos institucionais.
    """
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        results  = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if parsed["tier"] not in ("orfao", "niche"):
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            change = parsed["change_24h"]
            if min_change > 0 and (change is None or abs(change) < min_change):
                continue

            # score de oportunidade
            edge_score = 0
            if change is not None and abs(change) >= 0.01:
                edge_score = min(abs(change) * 200, 50)
            vol_score  = 10 if parsed["tier"] == "orfao" else 5
            days       = parsed["days_to_close"]
            prazo_score= 0
            if days is not None:
                if days <= 3:  prazo_score = 20
                elif days <= 7:prazo_score = 15
                elif days <= 30:prazo_score= 10

            opp_score = round(edge_score * 0.5 + vol_score * 0.2 + prazo_score * 0.3, 1)

            p     = parsed["yes_price"] / 100 if (change or 0) > 0 else parsed["no_price"] / 100
            edge  = abs(change) if change else 0.01
            kelly = min(round((edge / max(1 - p, 0.01)) * 0.25 * parsed["tier_kelly_mult"] * 100, 1), 5.0)

            results.append({
                **parsed,
                "opp_score":   opp_score,
                "kelly":       kelly,
                "signal":      "BUY" if (change or 0) > 0 else ("SELL" if (change or 0) < 0 else "HOLD"),
                "change_24h_pct": round(change * 100, 2) if change else 0,
            })

        results.sort(key=lambda x: x["opp_score"], reverse=True)
        return results[:30]
    except Exception as e:
        return {"error": str(e)}

@app.get("/closing_soon")
def get_closing_soon(max_days: int = Query(default=7)):
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        results  = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            days = parsed["days_to_close"]
            if days is None or days > max_days:
                continue
            change     = parsed["change_24h"]
            volume_24h = parsed["volume_24h"]
            yes        = parsed["yes_price"]
            urgencia   = max(0, 100 - (days * 14))
            movimento  = 0; direcao = None
            if change is not None and abs(change) >= 0.005:
                movimento = min(abs(change) * 300, 50)
                direcao   = "BUY" if change > 0 else "SELL"
            vol_score = min((volume_24h / 5000) * 10, 20)
            score     = round(urgencia * 0.5 + movimento * 0.3 + vol_score * 0.2, 1)
            if score < 10:
                continue
            if direcao == "BUY":  acao = f"Compre YES a {yes}%"; acao_cor = "#30d158"
            elif direcao == "SELL": acao = f"Compre NO a {parsed['no_price']}%"; acao_cor = "#ff453a"
            else: acao = "Monitorar"; acao_cor = "#ff9f0a"
            if days == 0:   urgencia_label = "HOJE";   urgencia_cor = "#ff453a"
            elif days == 1: urgencia_label = "AMANHA"; urgencia_cor = "#ff453a"
            elif days <= 3: urgencia_label = f"{days} DIAS"; urgencia_cor = "#ff9f0a"
            else:           urgencia_label = f"{days} DIAS"; urgencia_cor = "#0a84ff"
            results.append({
                "question": parsed["question"], "slug": parsed["slug"],
                "days_to_close": days, "urgencia_label": urgencia_label, "urgencia_cor": urgencia_cor,
                "direcao": direcao, "acao": acao, "acao_cor": acao_cor, "score": score,
                "yes_price": yes, "no_price": parsed["no_price"],
                "change_24h": round(change * 100, 2) if change else 0,
                "volume_24h": volume_24h, "end_date": parsed["end_date"],
                "tier": parsed["tier"], "tier_label": parsed["tier_label"],
            })
        results.sort(key=lambda x: (x["days_to_close"], -x["score"]))
        return results
    except Exception as e:
        return {"error": str(e)}

@app.get("/signals")
def get_signals(
    max_days:   int   = Query(default=0),
    max_volume: int   = Query(default=0),
    tier:       str   = Query(default=""),
):
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        signals  = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            if max_volume > 0 and parsed["volume"] > max_volume:
                continue
            if tier and parsed["tier"] != tier:
                continue
            # pula institucionais por padrão (sem edge)
            if parsed["tier"] == "institucional":
                continue
            change     = parsed["change_24h"]
            volume_24h = parsed["volume_24h"]
            if change is None or abs(change) < 0.05 or volume_24h < 1000:
                continue
            signal     = "BUY" if change > 0 else "SELL"
            confidence = round(min(abs(change) * 200, 95) / 100, 2)
            p          = parsed["yes_price"] / 100 if signal == "BUY" else parsed["no_price"] / 100
            kelly_mult = parsed.get("tier_kelly_mult", 0.5)
            kelly      = min(round((abs(change) / max(1 - p, 0.01)) * 0.25 * kelly_mult * 100, 1), 5.0)
            signals.append({
                "question": parsed["question"], "slug": parsed["slug"],
                "signal": signal, "yes_price": parsed["yes_price"], "no_price": parsed["no_price"],
                "change_24h": round(change * 100, 2), "confidence": confidence,
                "kelly": kelly, "volume_24h": volume_24h, "days_to_close": parsed["days_to_close"],
                "tier": parsed["tier"], "tier_label": parsed["tier_label"],
                "tier_acuracia": parsed["tier_acuracia"],
            })
        signals.sort(key=lambda x: abs(x["change_24h"]), reverse=True)
        return signals
    except Exception as e:
        return {"error": str(e)}

@app.get("/recommendations")
def get_recommendations(
    max_days:   int = Query(default=0),
    max_volume: int = Query(default=0),
):
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        session  = Session()
        rows     = session.execute(text("""
            SELECT slug, AVG(yes_price), STDDEV(yes_price), AVG(volume_24h), STDDEV(volume_24h), COUNT(*)
            FROM snapshots GROUP BY slug HAVING COUNT(*) >= 5
        """)).fetchall()
        session.close()
        historico = {r[0]: {"avg_yes": r[1], "std_yes": r[2] or 0, "avg_vol": r[3] or 0, "std_vol": r[4] or 0, "total_snaps": r[5]} for r in rows}

        results = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if parsed["tier"] == "institucional":
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            if max_volume > 0 and parsed["volume"] > max_volume:
                continue

            slug       = parsed["slug"]; change = parsed["change_24h"]
            volume_24h = parsed["volume_24h"]; yes = parsed["yes_price"]
            no         = parsed["no_price"];   days = parsed["days_to_close"]
            kelly_mult = parsed.get("tier_kelly_mult", 0.5)

            if volume_24h < 1000:
                continue

            sinal_score = 0; sinal_dir = None
            if change is not None and abs(change) >= 0.01:
                sinal_score = min(abs(change) * 200, 40)
                sinal_dir   = "BUY" if change > 0 else "SELL"

            anomalia_score = 0; reversao_score = 0; reversao_dir = None; avg_yes = None
            if slug in historico:
                h       = historico[slug]; avg_yes = h["avg_yes"]; std_yes = h["std_yes"]
                avg_vol = h["avg_vol"];    std_vol  = h["std_vol"]
                if avg_yes and std_yes > 0.5:
                    preco_zscore   = (yes - avg_yes) / std_yes
                    vol_zscore     = (volume_24h - avg_vol) / std_vol if std_vol > 100 else 0
                    anomalia_score = round(min(abs(preco_zscore) * 15, 20) + min(abs(vol_zscore) * 10, 20), 1)
                    desvio         = yes - avg_yes
                    desvio_pct     = abs((desvio / avg_yes) * 100) if avg_yes > 0 else 0
                    reversao_score = min(round(abs(preco_zscore) * 10 + desvio_pct, 1), 40)
                    reversao_dir   = "SELL" if desvio > 0 else "BUY"

            prazo_bonus = 0
            if days is not None:
                if days <= 1:    prazo_bonus = 20
                elif days <= 3:  prazo_bonus = 15
                elif days <= 7:  prazo_bonus = 10
                elif days <= 30: prazo_bonus = 5

            score_total = round((sinal_score * 0.35) + (anomalia_score * 0.15) + (reversao_score * 0.35) + (prazo_bonus * 0.15), 1)
            if score_total < 8:
                continue

            votos_buy  = (1 if sinal_dir == "BUY" else 0) + (1 if reversao_dir == "BUY" else 0)
            votos_sell = (1 if sinal_dir == "SELL" else 0) + (1 if reversao_dir == "SELL" else 0)
            if votos_buy > votos_sell:
                direcao = "BUY"; direcao_cor = "#30d158"; acao = f"Compre YES a {yes}%"; p = yes / 100
            elif votos_sell > votos_buy:
                direcao = "SELL"; direcao_cor = "#ff453a"; acao = f"Compre NO a {no}%"; p = no / 100
            else:
                continue

            edge  = abs(change) if change else 0.01
            kelly = min(round((edge / max(1 - p, 0.01)) * 0.25 * kelly_mult * 100, 1), 5.0)
            motores = []
            if sinal_dir == direcao:   motores.append("Sinal")
            if reversao_dir == direcao:motores.append("Reversao")
            if anomalia_score > 15:    motores.append("Anomalia")
            if days is not None and days <= 7: motores.append("Prazo")
            forca = "FORTE" if score_total >= 60 else "MEDIA" if score_total >= 30 else "FRACA"

            results.append({
                "question": parsed["question"], "slug": slug,
                "direcao": direcao, "direcao_cor": direcao_cor, "forca": forca,
                "score_total": score_total, "sinal_score": round(sinal_score, 1),
                "anomalia_score": anomalia_score, "reversao_score": reversao_score,
                "prazo_bonus": prazo_bonus, "kelly": kelly, "acao": acao,
                "yes_price": yes, "no_price": no,
                "change_24h": round(change * 100, 2) if change else 0,
                "volume_24h": volume_24h, "days_to_close": days, "motores": motores,
                "yes_price_media": round(avg_yes, 1) if avg_yes else None,
                "tier": parsed["tier"], "tier_label": parsed["tier_label"],
                "tier_acuracia": parsed["tier_acuracia"],
            })

        results.sort(key=lambda x: x["score_total"], reverse=True)
        return results[:15]
    except Exception as e:
        return {"error": str(e)}

@app.get("/anomalies")
def get_anomalies(
    max_days:   int = Query(default=0),
    max_volume: int = Query(default=0),
):
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        session  = Session()
        rows     = session.execute(text("""
            SELECT slug, AVG(yes_price), STDDEV(yes_price), AVG(volume_24h), STDDEV(volume_24h), COUNT(*), MAX(captured_at)
            FROM snapshots GROUP BY slug HAVING COUNT(*) >= 10
        """)).fetchall()
        session.close()
        historico = {r[0]: {"avg_yes": r[1], "std_yes": r[2] or 0, "avg_vol": r[3] or 0, "std_vol": r[4] or 0, "total_snaps": r[5]} for r in rows}

        results = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            if max_volume > 0 and parsed["volume"] > max_volume:
                continue
            slug = parsed["slug"]
            if slug not in historico:
                continue
            h           = historico[slug]
            avg_yes     = h["avg_yes"]; std_yes = h["std_yes"]
            avg_vol     = h["avg_vol"]; std_vol = h["std_vol"]
            current_yes = parsed["yes_price"]; current_vol = parsed["volume_24h"]
            if avg_yes is None:
                continue
            preco_zscore  = round((current_yes - avg_yes) / std_yes, 2) if std_yes > 0.5 else 0
            preco_desvio  = round(((current_yes - avg_yes) / avg_yes) * 100, 1) if avg_yes > 0 else 0
            vol_zscore    = round((current_vol - avg_vol) / std_vol, 2) if std_vol > 100 else 0
            anomaly_score = round(min(abs(preco_zscore) * 25, 40) + min(abs(vol_zscore) * 15, 30) + (30 if abs(preco_zscore) > 1 and abs(vol_zscore) > 1 else 0), 1)
            if anomaly_score < 15:
                continue
            if preco_zscore > 1.5 and vol_zscore > 1:   tipo = "SPIKE BULLISH"; tipo_cor = "#30d158"; interpretacao = "Alta anormal com volume elevado"
            elif preco_zscore < -1.5 and vol_zscore > 1: tipo = "SPIKE BEARISH"; tipo_cor = "#ff453a"; interpretacao = "Queda anormal com volume elevado"
            elif abs(preco_zscore) > 2:                  tipo = "PRECO EXTREMO"; tipo_cor = "#ff9f0a"; interpretacao = "Preco muito distante da media historica"
            elif vol_zscore > 2:                         tipo = "VOLUME ANORMAL";tipo_cor = "#bf5af2"; interpretacao = "Volume muito acima do normal"
            else:                                        tipo = "ANOMALIA";      tipo_cor = "#0a84ff"; interpretacao = "Comportamento fora do padrao historico"
            forca = "FORTE" if anomaly_score >= 60 else "MEDIA" if anomaly_score >= 30 else "FRACA"
            results.append({
                "question": parsed["question"], "slug": slug,
                "tipo": tipo, "tipo_cor": tipo_cor, "forca": forca, "anomaly_score": anomaly_score,
                "yes_price_atual": current_yes, "yes_price_media": round(avg_yes, 1),
                "preco_zscore": preco_zscore, "preco_desvio_pct": preco_desvio,
                "volume_24h_atual": current_vol, "volume_24h_media": round(avg_vol, 1),
                "vol_zscore": vol_zscore, "total_snaps": h["total_snaps"], "interpretacao": interpretacao,
                "yes_price": parsed["yes_price"], "no_price": parsed["no_price"],
                "days_to_close": parsed["days_to_close"],
                "tier": parsed["tier"], "tier_label": parsed["tier_label"],
            })
        results.sort(key=lambda x: x["anomaly_score"], reverse=True)
        return results[:20]
    except Exception as e:
        return {"error": str(e)}

@app.get("/reversion")
def get_reversion(
    max_days:   int = Query(default=0),
    max_volume: int = Query(default=0),
):
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        session  = Session()
        rows     = session.execute(text("""
            SELECT slug, AVG(yes_price), STDDEV(yes_price), COUNT(*), MIN(yes_price), MAX(yes_price)
            FROM snapshots GROUP BY slug HAVING COUNT(*) >= 5
        """)).fetchall()
        session.close()
        historico = {r[0]: {"avg_yes": r[1], "std_yes": r[2] or 0, "total_snaps": r[3], "min_yes": r[4], "max_yes": r[5]} for r in rows}

        results = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            if max_volume > 0 and parsed["volume"] > max_volume:
                continue
            slug = parsed["slug"]
            if slug not in historico:
                continue
            h       = historico[slug]; avg = h["avg_yes"]; std = h["std_yes"]; current = parsed["yes_price"]
            if avg is None or std is None:
                continue
            desvio     = current - avg
            desvio_pct = round((desvio / avg) * 100, 2) if avg > 0 else 0
            zscore     = round(desvio / std, 2) if std > 1 else 0
            if abs(desvio_pct) < 3 and abs(zscore) < 1:
                continue
            direcao      = "SELL" if desvio > 0 else "BUY"
            direcao_cor  = "#ff453a" if desvio > 0 else "#30d158"
            interpretacao= f"Preco {desvio_pct:.1f}% acima da media" if desvio > 0 else f"Preco {abs(desvio_pct):.1f}% abaixo da media"
            acao         = f"Compre NO a {parsed['no_price']}%" if desvio > 0 else f"Compre YES a {parsed['yes_price']}%"
            score        = min(round(abs(zscore) * 25 + abs(desvio_pct) * 2, 1), 100)
            forca        = "FORTE" if score >= 60 else "MEDIA" if score >= 30 else "FRACA"
            results.append({
                "question": parsed["question"], "slug": slug,
                "direcao": direcao, "direcao_cor": direcao_cor, "forca": forca, "score": score,
                "yes_price_atual": current, "yes_price_media": round(avg, 1),
                "yes_price_min": round(h["min_yes"], 1), "yes_price_max": round(h["max_yes"], 1),
                "desvio_pct": desvio_pct, "zscore": zscore, "total_snaps": h["total_snaps"],
                "interpretacao": interpretacao, "acao": acao,
                "volume_24h": parsed["volume_24h"], "no_price": parsed["no_price"],
                "days_to_close": parsed["days_to_close"],
                "tier": parsed["tier"], "tier_label": parsed["tier_label"],
            })
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:20]
    except Exception as e:
        return {"error": str(e)}

@app.get("/history/{slug}")
def get_history(slug: str, hours: int = Query(default=24)):
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        session = Session()
        rows    = session.execute(text("""
            SELECT yes_price, no_price, volume_24h, captured_at
            FROM snapshots WHERE slug = :slug
            ORDER BY captured_at ASC LIMIT 500
        """), {"slug": slug}).fetchall()
        session.close()
        points = [{"yes_price": r[0], "no_price": r[1], "volume_24h": r[2], "time": r[3].isoformat() if r[3] else None} for r in rows]
        if not points:
            return {"slug": slug, "points": [], "total": 0}
        first      = points[0]["yes_price"]; last = points[-1]["yes_price"]
        change     = round(last - first, 1)
        change_pct = round((change / first) * 100, 2) if first > 0 else 0
        return {"slug": slug, "points": points, "total": len(points), "yes_first": first, "yes_last": last, "change": change, "change_pct": change_pct}
    except Exception as e:
        return {"error": str(e)}

@app.get("/kalshi")
def get_kalshi():
    try:
        r    = requests.get("https://api.kalshi.com/trade-api/v2/markets", params={"limit": 200, "status": "open"}, timeout=10)
        data = r.json()
        markets = []
        for m in data.get("markets", []):
            yes_price = m.get("yes_ask"); no_price = m.get("no_ask")
            if yes_price is None or no_price is None:
                continue
            yes_price = round(yes_price, 1); no_price = round(no_price, 1)
            if yes_price < 5 or yes_price > 95:
                continue
            markets.append({
                "question": m.get("title", ""), "slug": m.get("ticker", ""),
                "yes_price": yes_price, "no_price": no_price,
                "volume": round(float(m.get("volume", 0) or 0), 2),
                "volume_24h": round(float(m.get("volume_24h", 0) or 0), 2),
                "end_date": m.get("close_time", ""), "platform": "kalshi",
            })
        markets.sort(key=lambda x: x["volume_24h"], reverse=True)
        return markets
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════════════════
# PATCH — Adicionar no FINAL do main.py (antes do último endpoint /kalshi ou depois)
# Motor #52 — GTI (Global Tension Index)
# Motor — Shannon Entropy (Detector de Regime)
# ═══════════════════════════════════════════════════════════════════════════════

import math

# ── Mapa de mercados geopolíticos e peso de tensão ───────────────────────────
GEO_SIGNALS = [
    # (keywords no slug/question, peso, "tensao se YES alto" ou "tensao se NO alto")
    {"keywords": ["russia", "ukraine", "ceasefire"],     "weight": 15, "tension_if": "NO"},   # sem cessar-fogo = tensão
    {"keywords": ["china", "invade", "taiwan"],          "weight": 20, "tension_if": "YES"},  # invasão = tensão
    {"keywords": ["putin", "out"],                       "weight": 8,  "tension_if": "YES"},  # Putin cai = instabilidade
    {"keywords": ["netanyahu", "out"],                   "weight": 8,  "tension_if": "YES"},
    {"keywords": ["zelenskyy", "out"],                   "weight": 8,  "tension_if": "YES"},
    {"keywords": ["erdogan", "out"],                     "weight": 6,  "tension_if": "YES"},
    {"keywords": ["xi", "jinping", "out"],               "weight": 10, "tension_if": "YES"},
    {"keywords": ["war", "invad", "militar"],            "weight": 10, "tension_if": "YES"},
    {"keywords": ["trump", "impeach"],                   "weight": 5,  "tension_if": "YES"},
    {"keywords": ["nuclear"],                            "weight": 15, "tension_if": "YES"},
    {"keywords": ["recession", "recessao"],              "weight": 8,  "tension_if": "YES"},
    {"keywords": ["bitcoin", "btc", "crypto"],           "weight": 3,  "tension_if": "NO"},   # cripto caindo = tensão macro
]

def compute_gti(markets: list) -> dict:
    """
    Motor #52 — Global Tension Index
    Score 0-100 de tensão geopolítica global baseado nos mercados Polymarket ativos.
    """
    total_weight  = 0
    tension_score = 0
    contributors  = []

    for m in markets:
        q    = (m.get("question", "") + " " + m.get("slug", "")).lower()
        yes  = m.get("yes_price", 50)
        no   = m.get("no_price",  50)

        for sig in GEO_SIGNALS:
            if not all(kw in q for kw in sig["keywords"]):
                continue

            weight = sig["weight"]
            total_weight += weight

            # qual probabilidade representa tensão?
            if sig["tension_if"] == "YES":
                tension_prob = yes / 100
            else:
                tension_prob = no / 100

            contribution = weight * tension_prob
            tension_score += contribution

            contributors.append({
                "question":      m["question"],
                "slug":          m.get("slug", ""),
                "tension_prob":  round(tension_prob * 100, 1),
                "weight":        weight,
                "contribution":  round(contribution, 1),
                "yes_price":     yes,
                "no_price":      no,
            })
            break  # um mercado só conta uma vez

    if total_weight == 0:
        gti = 50  # sem dados = neutro
    else:
        gti = round((tension_score / total_weight) * 100, 1)

    # classificação
    if gti >= 75:
        nivel = "CRÍTICO";  cor = "#ff453a"; acao = "Kill Switch — Kelly mínimo em tudo"
    elif gti >= 60:
        nivel = "ALTO";     cor = "#ff9f0a"; acao = "Reduzir sizing — favorece mercados doom"
    elif gti >= 40:
        nivel = "MÉDIO";    cor = "#ffd60a"; acao = "Operar normalmente com cautela"
    elif gti >= 25:
        nivel = "BAIXO";    cor = "#30d158"; acao = "Sizing normal — economia estável"
    else:
        nivel = "MÍNIMO";   cor = "#0a84ff"; acao = "Máxima agressividade — ambiente favorável"

    contributors.sort(key=lambda x: x["contribution"], reverse=True)

    return {
        "gti":          gti,
        "nivel":        nivel,
        "cor":          cor,
        "acao":         acao,
        "total_weight": total_weight,
        "contributors": contributors[:10],
        "mercados_geo": len(contributors),
        "timestamp":    datetime.utcnow().isoformat(),
    }

def compute_entropy(markets: list) -> dict:
    """
    Entropia de Shannon — Detector de Regime (dossiê seção 2.2)
    H = -p*log2(p) - (1-p)*log2(1-p)
    Máx = 1.0 bit (mercado 50/50 = máxima incerteza)
    Mín = 0.0 bits (mercado resolvido = certeza total)
    """
    if not markets:
        return {"error": "Sem mercados"}

    entropies = []
    for m in markets:
        p = m.get("yes_price", 50) / 100
        q = 1 - p
        if p <= 0 or p >= 1:
            h = 0.0
        else:
            h = -p * math.log2(p) - q * math.log2(q)
        entropies.append({
            "question":  m["question"],
            "slug":      m.get("slug", ""),
            "yes_price": m.get("yes_price"),
            "entropy":   round(h, 4),
        })

    avg_entropy = round(sum(e["entropy"] for e in entropies) / len(entropies), 4)

    # regime
    if avg_entropy >= 0.92:
        regime = "CAOS";           cor = "#ff453a"; kelly_mult = 0.0
        descricao = "Mercado em máxima incerteza. Zero novas posições."
    elif avg_entropy >= 0.80:
        regime = "LATERAL";        cor = "#ff9f0a"; kelly_mult = 0.5
        descricao = "Mercado indeciso. Kelly 50% — posições reduzidas."
    elif avg_entropy >= 0.65:
        regime = "TENDÊNCIA FRACA";cor = "#ffd60a"; kelly_mult = 0.75
        descricao = "Alguma direção detectável. Kelly 75%."
    else:
        regime = "TENDÊNCIA FORTE";cor = "#30d158"; kelly_mult = 1.0
        descricao = "Mercado com direção clara. Kelly cheio."

    # mercados mais incertos (próximos de 50/50)
    mais_incertos = sorted(entropies, key=lambda x: x["entropy"], reverse=True)[:5]
    # mercados mais resolvidos (próximos de 0 ou 1)
    mais_resolvidos = sorted(entropies, key=lambda x: x["entropy"])[:5]

    return {
        "avg_entropy":    avg_entropy,
        "max_entropy":    1.0,
        "regime":         regime,
        "cor":            cor,
        "kelly_mult":     kelly_mult,
        "descricao":      descricao,
        "total_mercados": len(entropies),
        "mais_incertos":  mais_incertos,
        "mais_resolvidos":mais_resolvidos,
        "timestamp":      datetime.utcnow().isoformat(),
    }


@app.get("/gti")
def get_gti():
    """
    Motor #52 — Global Tension Index
    Score 0-100 de tensão geopolítica global.
    Baseado nos mercados geopolíticos ativos no Polymarket.
    """
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        markets  = [parse_market(m, now) for m in all_data]
        markets  = [m for m in markets if m]
        return compute_gti(markets)
    except Exception as e:
        return {"error": str(e)}


@app.get("/entropy")
def get_entropy():
    """
    Entropia de Shannon — Detector de Regime de Mercado
    Alta entropia = caos = não operar
    Baixa entropia = tendência = operar com confiança
    """
    try:
        now      = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        markets  = [parse_market(m, now) for m in all_data]
        markets  = [m for m in markets if m]
        return compute_entropy(markets)
    except Exception as e:
        return {"error": str(e)}
    
# ═══════════════════════════════════════════════════════════════════════════════
# PATCH — Adicionar no FINAL do main.py
# Motor #56 — Polymarket Seismograph
# Detecta micro-tremores antes de grandes movimentos
# ═══════════════════════════════════════════════════════════════════════════════


@app.get("/seismograph")
def get_seismograph(
    min_snaps:  int   = Query(default=10),
    top:        int   = Query(default=20),
    max_volume: int   = Query(default=0),
):
    """
    Motor #56 — Polymarket Seismograph
    Detecta micro-tremores: mercados que ficaram estáveis e começaram a se mover.
    Usa os snapshots históricos do banco — quanto mais snapshots, mais preciso.

    Métricas calculadas:
    - velocity:     taxa de mudança média por snapshot (pontos percentuais/snap)
    - acceleration: se a velocidade está aumentando (tremor se intensificando)
    - stability:    quão estável o mercado ficou antes do tremor
    - quake_score:  score final 0-100
    """
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        now     = datetime.now(timezone.utc)
        session = Session()

        # busca últimos snapshots por mercado
        rows = session.execute(text("""
            SELECT
                slug,
                yes_price,
                captured_at,
                ROW_NUMBER() OVER (PARTITION BY slug ORDER BY captured_at DESC) as rn
            FROM snapshots
            WHERE captured_at >= NOW() - INTERVAL '24 hours'
            ORDER BY slug, captured_at DESC
        """)).fetchall()
        session.close()

        # agrupa por slug
        from collections import defaultdict
        slug_snaps = defaultdict(list)
        for slug, yes_price, captured_at, rn in rows:
            slug_snaps[slug].append({"yes_price": yes_price, "captured_at": captured_at})

        # busca mercados ativos
        all_data = fetch_all_markets()
        market_map = {}
        for m in all_data:
            parsed = parse_market(m, now)
            if parsed:
                market_map[parsed["slug"]] = parsed

        results = []

        for slug, snaps in slug_snaps.items():
            if len(snaps) < min_snaps:
                continue
            if slug not in market_map:
                continue

            market = market_map[slug]
            if max_volume > 0 and market.get("volume", 0) > max_volume:
                continue

            # ordena por tempo (mais antigo primeiro)
            snaps_sorted = sorted(snaps, key=lambda x: x["captured_at"])
            prices = [s["yes_price"] for s in snaps_sorted]

            # divide em duas metades: passado vs recente
            mid = len(prices) // 2
            past_prices   = prices[:mid]
            recent_prices = prices[mid:]

            if not past_prices or not recent_prices:
                continue

            # estabilidade no passado (baixo desvio = mercado estava parado)
            past_mean = sum(past_prices) / len(past_prices)
            past_std  = (sum((p - past_mean) ** 2 for p in past_prices) / len(past_prices)) ** 0.5

            # velocidade recente (mudança média por snapshot)
            recent_changes = [abs(recent_prices[i] - recent_prices[i-1]) for i in range(1, len(recent_prices))]
            velocity = sum(recent_changes) / len(recent_changes) if recent_changes else 0

            # direção dominante
            direction_changes = [recent_prices[i] - recent_prices[i-1] for i in range(1, len(recent_prices))]
            net_direction = sum(direction_changes)

            # aceleração: velocidade da segunda metade vs primeira metade do período recente
            half = len(recent_changes) // 2
            if half > 0:
                v_early = sum(recent_changes[:half]) / half
                v_late  = sum(recent_changes[half:]) / max(len(recent_changes[half:]), 1)
                acceleration = v_late - v_early
            else:
                acceleration = 0

            # quake score: alta velocidade + baixa estabilidade anterior + aceleração positiva
            stability_bonus = max(0, 2.0 - past_std) * 10  # mercado mais estável antes = mais suspeito agora
            velocity_score  = min(velocity * 40, 50)
            accel_score     = min(max(acceleration * 30, 0), 30)
            quake_score     = round(velocity_score + accel_score + stability_bonus, 1)

            if quake_score < 5:
                continue

            # classificação
            if quake_score >= 60:
                nivel = "TERREMOTO"; cor = "#ff453a"
            elif quake_score >= 35:
                nivel = "TREMOR FORTE"; cor = "#ff9f0a"
            elif quake_score >= 15:
                nivel = "MICRO-TREMOR"; cor = "#ffd60a"
            else:
                nivel = "RUÍDO"; cor = "rgba(255,255,255,0.3)"

            direcao = "↑ SUBINDO" if net_direction > 0.5 else ("↓ CAINDO" if net_direction < -0.5 else "→ LATERAL")
            direcao_cor = "#30d158" if net_direction > 0.5 else ("#ff453a" if net_direction < -0.5 else "#ff9f0a")
            signal = "BUY" if net_direction > 0.5 else ("SELL" if net_direction < -0.5 else "HOLD")

            results.append({
                "question":      market["question"],
                "slug":          slug,
                "yes_price":     market["yes_price"],
                "no_price":      market["no_price"],
                "quake_score":   quake_score,
                "nivel":         nivel,
                "cor":           cor,
                "velocity":      round(velocity, 3),
                "acceleration":  round(acceleration, 3),
                "past_std":      round(past_std, 3),
                "net_direction": round(net_direction, 2),
                "direcao":       direcao,
                "direcao_cor":   direcao_cor,
                "signal":        signal,
                "total_snaps":   len(snaps),
                "volume_24h":    market.get("volume_24h", 0),
                "days_to_close": market.get("days_to_close"),
                "tier":          market.get("tier"),
                "tier_label":    market.get("tier_label"),
            })

        results.sort(key=lambda x: x["quake_score"], reverse=True)
        return results[:top]

    except Exception as e:
        return {"error": str(e)}