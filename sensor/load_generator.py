# load_generator.py
import threading
import time
import random
import requests

EDGE_URL = "http://127.0.0.1:8001/event"

NUM_SENSORS = 100             # number of simulated streams (aggregated)
NUM_ROADS = 100              # distinct roads (R1..R100)
TARGET_EVENTS_PER_SEC = 800 # ≈ 48,000 events per minute
RUN_DURATION_SECONDS = 60    

PER_SENSOR_RATE = TARGET_EVENTS_PER_SEC / NUM_SENSORS
PER_SENSOR_INTERVAL = 1.0 / PER_SENSOR_RATE  # seconds between sends per sensor

print(f"Simulating {NUM_SENSORS} sensors across {NUM_ROADS} roads")
print(f"Target total rate: {TARGET_EVENTS_PER_SEC} events/s (~{TARGET_EVENTS_PER_SEC*60} events/min)")
print(f"Per sensor rate: {PER_SENSOR_RATE:.1f} events/s")
print(f"Per sensor interval: {PER_SENSOR_INTERVAL:.4f} s")
print(f"Run duration: {RUN_DURATION_SECONDS} s")

stop_flag = False
success_count = 0
failure_count = 0
lock = threading.Lock()

def sensor_thread(sensor_idx: int):
    global success_count, failure_count
    sensor_id = f"S{sensor_idx + 1}"
    road_id = f"R{(sensor_idx % NUM_ROADS) + 1}"

    while not stop_flag:
        event = {
            "sensor_id": sensor_id,
            "road_id": road_id,
            "congestion": random.randint(10, 100),
        }
        try:
            resp = requests.post(EDGE_URL, json=event, timeout=5)
            with lock:
                if resp.status_code == 200:
                    success_count += 1
                else:
                    failure_count += 1
        except Exception:
            with lock:
                failure_count += 1
            # Uncomment if you want error spam:
            # print(f"[{sensor_id}] Failed: {e}")

        time.sleep(PER_SENSOR_INTERVAL)

def main():
    global stop_flag
    threads = []
    start_time = time.time()

    for i in range(NUM_SENSORS):
        t = threading.Thread(target=sensor_thread, args=(i,), daemon=True)
        threads.append(t)
        t.start()

    print("Load generator running...")
    last_report = start_time

    while True:
        now = time.time()
        elapsed = now - start_time
        if elapsed >= RUN_DURATION_SECONDS:
            break

        if now - last_report >= 5:
            with lock:
                s = success_count
                f = failure_count
            total = s + f
            eps = total / elapsed if elapsed > 0 else 0.0
            print(
                f"[{elapsed:6.1f}s] total={total}, success={s}, "
                f"fail={f}, approx {eps:.1f} events/s ({eps*60:.0f} events/min)"
            )
            last_report = now

        time.sleep(0.5)

    # Stop threads
    stop_flag = True
    time.sleep(PER_SENSOR_INTERVAL * 2)

    elapsed = time.time() - start_time
    with lock:
        s = success_count
        f = failure_count
    total = s + f
    eps = total / elapsed if elapsed > 0 else 0.0

    print("\n=== Load test finished ===")
    print(f"Elapsed time: {elapsed:.1f} s")
    print(f"Total events attempted: {total}")
    print(f"Successful: {s}")
    print(f"Failed: {f}")
    print(f"Approx throughput: {eps:.1f} events/s "
          f"({eps * 60:.0f} events/min)")

if __name__ == "__main__":
    main()
