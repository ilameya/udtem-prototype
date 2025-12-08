# routing/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import requests
import os

TWIN_URL = os.getenv("TWIN_URL", "http://twin:8003/state")
BASE_TRAVEL_TIME_MIN = 10

app = FastAPI(title="UDT-EM Routing Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/route/{road_id}")
def get_route(road_id: str):
    """
    Simple route estimate based on twin congestion for the given road_id.
    """
    try:
        twin_state = requests.get(TWIN_URL, timeout=2).json()
        road = twin_state.get(road_id)
        if not road:
            return {"road_id": road_id, "known": False}

        congestion = road["congestion"]
        travel_time = BASE_TRAVEL_TIME_MIN + congestion * 0.1

        return {
            "road_id": road_id,
            "congestion": congestion,
            "estimated_travel_time_min": round(travel_time, 1),
        }
    except Exception as e:
        return {"error": str(e)}
