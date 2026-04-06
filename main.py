from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta

app = FastAPI(title="PolySignal API", version="3.0")

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
        # pool_pre_ping=True é essencial pro Neon (escala a zero quando inativo)
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

    return {
        "question": m.get("question", ""),
        "slug": m.get("slug", ""),
        "yes_price": yes_price,
        "no_price": no_price,
        "volume": round(float(m.get("volume", 0) or 0), 2),
        "volume_24h": round(float(m.get("volume24hr", 0) or 0), 2),
        "end_date": end_date_str,
        "days_to_close": days_to_close,
        "change_24h": change,
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
        change = m.get("change_24h")
        volume_24h = m.get("volume_24h", 0)
        slug = m.get("slug", "")
        if change is None or abs(change) < 0.05 or volume_24h < 5000 or slug in alerted_slugs:
            continue
        sinal = "BUY" if change > 0 else "SELL"
        acao = f"Compre YES a {m['yes_price']}%" if change > 0 else f"Compre NO a {m['no_price']}%"
        tags = "green_circle" if change > 0 else "red_circle"
        dias = m.get("days_to_close", "?")
        send_alert(
            f"PolySignal {sinal}",
            f"{m['question']}\n\nAcao: {acao}\nVariacao 24h: {round(change*100,1)}%\nVol 24h: ${round(volume_24h/1000,1)}k\nFecha em: {dias} dias\nKelly: ate 5% da banca",
            tags=tags
        )
        alerted_slugs.add(slug)

def build_recommendations_snapshot(markets, historico):
    results = []
    for m in markets:
        slug = m["slug"]
        change = m["change_24h"]
        volume_24h = m["volume_24h"]
        yes = m["yes_price"]
        no = m["no_price"]
        days = m["days_to_close"]
        if volume_24h < 1000:
            continue
        sinal_score = 0; sinal_dir = None
        if change is not None and abs(change) >= 0.01:
            sinal_score = min(abs(change) * 200, 40)
            sinal_dir = "BUY" if change > 0 else "SELL"
        anomalia_score = 0; reversao_score = 0; reversao_dir = None; avg_yes = None
        if slug in historico:
            h = historico[slug]
            avg_yes = h["avg_yes"]; std_yes = h["std_yes"]
            avg_vol = h["avg_vol"]; std_vol = h["std_vol"]
            if avg_yes and std_yes > 0.5:
                preco_zscore = (yes - avg_yes) / std_yes
                vol_zscore = (volume_24h - avg_vol) / std_vol if std_vol > 100 else 0
                anomalia_score = round(min(abs(preco_zscore) * 15, 20) + min(abs(vol_zscore) * 10, 20), 1)
                desvio = yes - avg_yes
                desvio_pct = abs((desvio / avg_yes) * 100) if avg_yes > 0 else 0
                reversao_score = min(round(abs(preco_zscore) * 10 + desvio_pct, 1), 40)
                reversao_dir = "SELL" if desvio > 0 else "BUY"
        prazo_bonus = 0
        if days is not None:
            if days <= 1: prazo_bonus = 20
            elif days <= 3: prazo_bonus = 15
            elif days <= 7: prazo_bonus = 10
            elif days <= 30: prazo_bonus = 5
        score_total = round((sinal_score * 0.35) + (anomalia_score * 0.15) + (reversao_score * 0.35) + (prazo_bonus * 0.15), 1)
        if score_total < 8:
            continue
        votos_buy = (1 if sinal_dir == "BUY" else 0) + (1 if reversao_dir == "BUY" else 0)
        votos_sell = (1 if sinal_dir == "SELL" else 0) + (1 if reversao_dir == "SELL" else 0)
        if votos_buy > votos_sell:
            direcao = "COMPRE SIM"; acao = f"Compre SIM (YES) a {yes}%"; p = yes / 100
        elif votos_sell > votos_buy:
            direcao = "COMPRE NAO"; acao = f"Compre NAO (NO) a {no}%"; p = no / 100
        else:
            continue
        edge = abs(change) if change else 0.01
        kelly = min(round((edge / max(1 - p, 0.01)) * 0.25 * 100, 1), 5.0)
        results.append({
            "question": m["question"], "direcao": direcao, "acao": acao,
            "kelly": kelly, "score_total": score_total, "days_to_close": days,
            "change_24h": round(change * 100, 2) if change else 0, "volume_24h": volume_24h,
        })
    results.sort(key=lambda x: x["score_total"], reverse=True)
    return results[:5]

def send_hourly_summary(markets, historico):
    try:
        hora = datetime.now().strftime("%H:%M")
        recs = build_recommendations_snapshot(markets, historico)
        if not recs:
            send_alert(f"PolySignal {hora} — Sem apostas agora",
                "Nenhuma oportunidade com consenso detectada neste momento.\n\nO sistema continua monitorando.",
                tags="hourglass_flowing_sand")
            return
        linhas = [f"RESUMO HORARIO — {hora}\n", f"{len(recs)} APOSTA(S) IDENTIFICADA(S):\n", "=" * 30]
        for i, r in enumerate(recs):
            emoji = "COMPRE SIM" if "SIM" in r["direcao"] else "COMPRE NAO"
            dias_txt = f"Fecha em {r['days_to_close']} dias" if r['days_to_close'] is not None else "Sem prazo"
            var_txt = f"+{r['change_24h']}%" if r['change_24h'] > 0 else f"{r['change_24h']}%"
            linhas += [f"\n#{i+1} — {emoji}", f"Pergunta: {r['question']}", f"O que fazer: {r['acao']}",
                f"Variacao hoje: {var_txt}", f"Volume: ${round(r['volume_24h']/1000,1)}k",
                f"Prazo: {dias_txt}", f"Quanto apostar: ate {r['kelly']}% do seu dinheiro",
                f"Confianca: {r['score_total']}/100"]
        linhas += ["\n" + "=" * 30, "REGRAS:", "- Nunca aposte mais do que o sistema indica",
            "- Diversifique entre as apostas", "- Em caso de duvida, nao aposte"]
        send_alert(f"PolySignal {hora} — {len(recs)} aposta(s)", "\n".join(linhas), tags="bar_chart")
        print(f"📊 Resumo horario enviado: {len(recs)} apostas")
    except Exception as e:
        print(f"❌ Resumo horario erro: {e}")

def worker_loop():
    print("🔄 Worker iniciado")
    last_summary_hour = -1
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

@app.get("/")
def home():
    return {"status": "ok", "service": "PolySignal", "version": "3.0"}

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
def get_markets(max_days: int = Query(default=0)):
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        markets = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            markets.append(parsed)
        markets.sort(key=lambda x: x["volume_24h"], reverse=True)
        return markets
    except Exception as e:
        return {"error": str(e)}

@app.get("/closing_soon")
def get_closing_soon(max_days: int = Query(default=7)):
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        results = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            days = parsed["days_to_close"]
            if days is None or days > max_days:
                continue
            change = parsed["change_24h"]; volume_24h = parsed["volume_24h"]
            yes = parsed["yes_price"]
            urgencia = max(0, 100 - (days * 14))
            movimento = 0; direcao = None
            if change is not None and abs(change) >= 0.005:
                movimento = min(abs(change) * 300, 50)
                direcao = "BUY" if change > 0 else "SELL"
            vol_score = min((volume_24h / 5000) * 10, 20)
            score = round(urgencia * 0.5 + movimento * 0.3 + vol_score * 0.2, 1)
            if score < 10:
                continue
            if direcao == "BUY": acao = f"Compre YES a {yes}%"; acao_cor = "#30d158"
            elif direcao == "SELL": acao = f"Compre NO a {parsed['no_price']}%"; acao_cor = "#ff453a"
            else: acao = "Monitorar — sem sinal claro"; acao_cor = "#ff9f0a"
            if days == 0: urgencia_label = "HOJE"; urgencia_cor = "#ff453a"
            elif days == 1: urgencia_label = "AMANHA"; urgencia_cor = "#ff453a"
            elif days <= 3: urgencia_label = f"{days} DIAS"; urgencia_cor = "#ff9f0a"
            else: urgencia_label = f"{days} DIAS"; urgencia_cor = "#0a84ff"
            results.append({
                "question": parsed["question"], "slug": parsed["slug"],
                "days_to_close": days, "urgencia_label": urgencia_label, "urgencia_cor": urgencia_cor,
                "direcao": direcao, "acao": acao, "acao_cor": acao_cor, "score": score,
                "yes_price": yes, "no_price": parsed["no_price"],
                "change_24h": round(change * 100, 2) if change else 0,
                "volume_24h": volume_24h, "end_date": parsed["end_date"],
            })
        results.sort(key=lambda x: (x["days_to_close"], -x["score"]))
        return results
    except Exception as e:
        return {"error": str(e)}

@app.get("/signals")
def get_signals(max_days: int = Query(default=0)):
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        signals = []
        for m in all_data:
            parsed = parse_market(m, now)
            if not parsed:
                continue
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            change = parsed["change_24h"]; volume_24h = parsed["volume_24h"]
            if change is None or abs(change) < 0.05 or volume_24h < 1000:
                continue
            signal = "BUY" if change > 0 else "SELL"
            confidence = round(min(abs(change) * 200, 95) / 100, 2)
            p = parsed["yes_price"] / 100 if signal == "BUY" else parsed["no_price"] / 100
            kelly = min(round((abs(change) / max(1 - p, 0.01)) * 0.25 * 100, 1), 5.0)
            signals.append({
                "question": parsed["question"], "slug": parsed["slug"],
                "signal": signal, "yes_price": parsed["yes_price"], "no_price": parsed["no_price"],
                "change_24h": round(change * 100, 2), "confidence": confidence,
                "kelly": kelly, "volume_24h": volume_24h, "days_to_close": parsed["days_to_close"],
            })
        signals.sort(key=lambda x: abs(x["change_24h"]), reverse=True)
        return signals
    except Exception as e:
        return {"error": str(e)}

@app.get("/recommendations")
def get_recommendations(max_days: int = Query(default=0)):
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        session = Session()
        rows = session.execute(text("""
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
            if max_days > 0 and (parsed["days_to_close"] is None or parsed["days_to_close"] > max_days):
                continue
            slug = parsed["slug"]; change = parsed["change_24h"]; volume_24h = parsed["volume_24h"]
            yes = parsed["yes_price"]; no = parsed["no_price"]; days = parsed["days_to_close"]
            if volume_24h < 1000:
                continue
            sinal_score = 0; sinal_dir = None
            if change is not None and abs(change) >= 0.01:
                sinal_score = min(abs(change) * 200, 40)
                sinal_dir = "BUY" if change > 0 else "SELL"
            anomalia_score = 0; reversao_score = 0; reversao_dir = None; avg_yes = None
            if slug in historico:
                h = historico[slug]; avg_yes = h["avg_yes"]; std_yes = h["std_yes"]
                avg_vol = h["avg_vol"]; std_vol = h["std_vol"]
                if avg_yes and std_yes > 0.5:
                    preco_zscore = (yes - avg_yes) / std_yes
                    vol_zscore = (volume_24h - avg_vol) / std_vol if std_vol > 100 else 0
                    anomalia_score = round(min(abs(preco_zscore) * 15, 20) + min(abs(vol_zscore) * 10, 20), 1)
                    desvio = yes - avg_yes
                    desvio_pct = abs((desvio / avg_yes) * 100) if avg_yes > 0 else 0
                    reversao_score = min(round(abs(preco_zscore) * 10 + desvio_pct, 1), 40)
                    reversao_dir = "SELL" if desvio > 0 else "BUY"
            prazo_bonus = 0
            if days is not None:
                if days <= 1: prazo_bonus = 20
                elif days <= 3: prazo_bonus = 15
                elif days <= 7: prazo_bonus = 10
                elif days <= 30: prazo_bonus = 5
            score_total = round((sinal_score * 0.35) + (anomalia_score * 0.15) + (reversao_score * 0.35) + (prazo_bonus * 0.15), 1)
            if score_total < 8:
                continue
            votos_buy = (1 if sinal_dir == "BUY" else 0) + (1 if reversao_dir == "BUY" else 0)
            votos_sell = (1 if sinal_dir == "SELL" else 0) + (1 if reversao_dir == "SELL" else 0)
            if votos_buy > votos_sell:
                direcao = "BUY"; direcao_cor = "#30d158"; acao = f"Compre YES a {yes}%"; p = yes / 100
            elif votos_sell > votos_buy:
                direcao = "SELL"; direcao_cor = "#ff453a"; acao = f"Compre NO a {no}%"; p = no / 100
            else:
                continue
            edge = abs(change) if change else 0.01
            kelly = min(round((edge / max(1 - p, 0.01)) * 0.25 * 100, 1), 5.0)
            motores = []
            if sinal_dir == direcao: motores.append("Sinal")
            if reversao_dir == direcao: motores.append("Reversao")
            if anomalia_score > 15: motores.append("Anomalia")
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
            })
        results.sort(key=lambda x: x["score_total"], reverse=True)
        return results[:15]
    except Exception as e:
        return {"error": str(e)}

@app.get("/anomalies")
def get_anomalies(max_days: int = Query(default=0)):
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        session = Session()
        rows = session.execute(text("""
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
            slug = parsed["slug"]
            if slug not in historico:
                continue
            h = historico[slug]; avg_yes = h["avg_yes"]; std_yes = h["std_yes"]
            avg_vol = h["avg_vol"]; std_vol = h["std_vol"]
            current_yes = parsed["yes_price"]; current_vol = parsed["volume_24h"]
            if avg_yes is None:
                continue
            preco_zscore = round((current_yes - avg_yes) / std_yes, 2) if std_yes > 0.5 else 0
            preco_desvio = round(((current_yes - avg_yes) / avg_yes) * 100, 1) if avg_yes > 0 else 0
            vol_zscore = round((current_vol - avg_vol) / std_vol, 2) if std_vol > 100 else 0
            anomaly_score = round(min(abs(preco_zscore) * 25, 40) + min(abs(vol_zscore) * 15, 30) + (30 if abs(preco_zscore) > 1 and abs(vol_zscore) > 1 else 0), 1)
            if anomaly_score < 15:
                continue
            if preco_zscore > 1.5 and vol_zscore > 1: tipo = "SPIKE BULLISH"; tipo_cor = "#30d158"; interpretacao = "Alta anormal com volume elevado"
            elif preco_zscore < -1.5 and vol_zscore > 1: tipo = "SPIKE BEARISH"; tipo_cor = "#ff453a"; interpretacao = "Queda anormal com volume elevado"
            elif abs(preco_zscore) > 2: tipo = "PRECO EXTREMO"; tipo_cor = "#ff9f0a"; interpretacao = "Preco muito distante da media historica"
            elif vol_zscore > 2: tipo = "VOLUME ANORMAL"; tipo_cor = "#bf5af2"; interpretacao = "Volume muito acima do normal"
            else: tipo = "ANOMALIA"; tipo_cor = "#0a84ff"; interpretacao = "Comportamento fora do padrao historico"
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
            })
        results.sort(key=lambda x: x["anomaly_score"], reverse=True)
        return results[:20]
    except Exception as e:
        return {"error": str(e)}

@app.get("/reversion")
def get_reversion(max_days: int = Query(default=0)):
    if not Session:
        return {"error": "Banco nao conectado"}
    try:
        now = datetime.now(timezone.utc)
        all_data = fetch_all_markets()
        session = Session()
        rows = session.execute(text("""
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
            slug = parsed["slug"]
            if slug not in historico:
                continue
            h = historico[slug]; avg = h["avg_yes"]; std = h["std_yes"]; current = parsed["yes_price"]
            if avg is None or std is None:
                continue
            desvio = current - avg
            desvio_pct = round((desvio / avg) * 100, 2) if avg > 0 else 0
            zscore = round(desvio / std, 2) if std > 1 else 0
            if abs(desvio_pct) < 3 and abs(zscore) < 1:
                continue
            direcao = "SELL" if desvio > 0 else "BUY"
            direcao_cor = "#ff453a" if desvio > 0 else "#30d158"
            interpretacao = f"Preco {desvio_pct:.1f}% acima da media" if desvio > 0 else f"Preco {abs(desvio_pct):.1f}% abaixo da media"
            acao = f"Compre NO a {parsed['no_price']}%" if desvio > 0 else f"Compre YES a {parsed['yes_price']}%"
            score = min(round(abs(zscore) * 25 + abs(desvio_pct) * 2, 1), 100)
            forca = "FORTE" if score >= 60 else "MEDIA" if score >= 30 else "FRACA"
            results.append({
                "question": parsed["question"], "slug": slug,
                "direcao": direcao, "direcao_cor": direcao_cor, "forca": forca, "score": score,
                "yes_price_atual": current, "yes_price_media": round(avg, 1),
                "yes_price_min": round(h["min_yes"], 1), "yes_price_max": round(h["max_yes"], 1),
                "desvio_pct": desvio_pct, "zscore": zscore, "total_snaps": h["total_snaps"],
                "interpretacao": interpretacao, "acao": acao,
                "volume_24h": parsed["volume_24h"], "no_price": parsed["no_price"],
                "days_to_close": parsed["days_to_close"],
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
        rows = session.execute(text("""
            SELECT yes_price, no_price, volume_24h, captured_at
            FROM snapshots WHERE slug = :slug
            ORDER BY captured_at ASC LIMIT 500
        """), {"slug": slug}).fetchall()
        session.close()
        points = [{"yes_price": r[0], "no_price": r[1], "volume_24h": r[2], "time": r[3].isoformat() if r[3] else None} for r in rows]
        if not points:
            return {"slug": slug, "points": [], "total": 0}
        first = points[0]["yes_price"]; last = points[-1]["yes_price"]
        change = round(last - first, 1)
        change_pct = round((change / first) * 100, 2) if first > 0 else 0
        return {"slug": slug, "points": points, "total": len(points), "yes_first": first, "yes_last": last, "change": change, "change_pct": change_pct}
    except Exception as e:
        return {"error": str(e)}

@app.get("/kalshi")
def get_kalshi():
    try:
        r = requests.get("https://api.kalshi.com/trade-api/v2/markets", params={"limit": 200, "status": "open"}, timeout=10)
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