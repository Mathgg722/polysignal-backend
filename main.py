from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="PolySignal API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        "message": "Backend novo — construindo do zero"
    }