# ingestion/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import paho.mqtt.publish as publish
import datetime
import time
import os

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "traffic/events"

app = FastAPI(title="UDT-EM Ingestion Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class IngestedEvent(BaseModel):
    sensor_id: str
    road_id: str
    congestion: int


event_count = 0
start_time = time.time()


@app.post("/ingest")
def ingest(e: IngestedEvent):
    """
    Receives events from edge, stamps them with a timestamp,
    and publishes to the MQTT broker.
    """
    global event_count, start_time

    event_out = {
        "sensor_id": e.sensor_id,
        "road_id": e.road_id,
        "congestion": e.congestion,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }
    payload = str(event_out)

    try:
        publish.single(
            MQTT_TOPIC,
            payload,
            hostname=MQTT_HOST,
            port=MQTT_PORT,
        )
        event_count += 1

        if event_count % 1000 == 0:
            elapsed = time.time() - start_time
            rate = event_count / elapsed if elapsed > 0 else 0
            print(f"[INGESTION] {event_count} events processed "
                  f"({rate:.1f} events/s)")

        return {"status": "published"}
    except Exception as ex:
        print("[INGESTION] Failed to publish to MQTT:", ex)
        return {"status": "mqtt_error", "error": str(ex)}
