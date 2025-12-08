import time
import random
import requests

EDGE_URL = "http://localhost:8001/event"

def main():
    print("Starting dummy sensor. Ctrl+C to stop.")
    while True:
        event = {
            "road_id": "R42",
            "congestion": random.randint(10, 100)
        }
        try:
            r = requests.post(EDGE_URL, json=event, timeout=2)
            print("Sent:", event, "status:", r.status_code)
        except Exception as e:
            print("Failed to send:", e)
        time.sleep(3)

if __name__ == "__main__":
    main()
