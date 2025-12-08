# edge/main.py
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import os
import requests

INGESTION_URL = os.getenv("INGESTION_URL", "http://ingestion:8002/ingest")

app = FastAPI(title="UDT-EM Edge Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RawEvent(BaseModel):
    sensor_id: str
    road_id: str
    congestion: int


@app.post("/event")
def receive_event(e: RawEvent):
    """
    Edge receives raw sensor events and forwards them to ingestion.
    """
    payload = e.dict()
    try:
        r = requests.post(INGESTION_URL, json=payload, timeout=2)
        return {"status": "forwarded", "ingestion_status": r.status_code}
    except Exception as ex:
        print("[EDGE] Failed to forward to ingestion:", ex)
        return {"status": "ingestion_error", "error": str(ex)}
