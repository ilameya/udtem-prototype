# twin/main.py
import ast
import os
import time
import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "traffic/events"

app = FastAPI(title="UDT-EM Twin Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory twin state: {road_id: {congestion, timestamp, last_sensor}}
state = {}

# Simple metrics
twin_start_time = time.time()
total_events = 0


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[TWIN] Connected to MQTT at {MQTT_HOST}:{MQTT_PORT}, "
              f"subscribing to {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[TWIN] MQTT connection failed with code {rc}")


def on_message(client, userdata, msg):
    global total_events
    try:
        payload = msg.payload.decode()
        event = ast.literal_eval(payload)

        sensor_id = event.get("sensor_id", "unknown")
        road_id = event["road_id"]
        congestion = event["congestion"]
        timestamp = event.get("timestamp", "unknown")

        # Latency measurement
        latency_secs = None
        try:
            event_ts = datetime.datetime.fromisoformat(timestamp)
            now_ts = datetime.datetime.utcnow()
            latency = now_ts - event_ts
            latency_secs = latency.total_seconds()
        except Exception:
            pass

        state[road_id] = {
            "congestion": congestion,
            "timestamp": timestamp,
            "last_sensor": sensor_id,
        }

        total_events += 1

        if latency_secs is not None:
            print(
                f"[TWIN] Updated {road_id} -> {congestion} "
                f"(sensor={sensor_id}), latency ~ {latency_secs:.3f} s"
            )
        else:
            print(
                f"[TWIN] Updated {road_id} -> {congestion} "
                f"(sensor={sensor_id})"
            )

    except Exception as e:
        print("[TWIN] Failed to process message:", e)


@app.on_event("startup")
def start_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"[TWIN] Connecting to MQTT {MQTT_HOST}:{MQTT_PORT} ...")
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        print("[TWIN] Initial MQTT connect failed:", e)

    client.loop_start()
    app.state.mqtt_client = client


@app.get("/state")
def get_state():
    return state


@app.get("/state/{road_id}")
def get_road_state(road_id: str):
    return state.get(road_id, {})


@app.get("/metrics")
def get_metrics():
    """
    Basic metrics for dashboard KPI bar.
    """
    now = time.time()
    elapsed = max(now - twin_start_time, 1e-6)
    events_per_sec = total_events / elapsed

    # count unique sensors (based on last_sensor per road)
    unique_sensors = set()
    for info in state.values():
        s = info.get("last_sensor")
        if s:
            unique_sensors.add(s)

    return {
        "active_roads": len(state),
        "active_sensors": len(unique_sensors),
        "events_total": total_events,
        "events_per_sec_approx": events_per_sec,
        "uptime_seconds": elapsed,
    }
