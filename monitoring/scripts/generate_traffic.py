import requests
import sys

url = sys.argv[1] if len(sys.argv) > 1 else 'http://localhost:8000/predict'

payload = {
    "lag_1": 70,
    "lag_3": 68,
    "lag_6": 65,
    "rolling_mean_3": 69,
    "wti_price": 72,
    "wti_lag_1": 71,
    "wti_rolling_mean_3": 71.5
}

success_count = 0
for i in range(30):
    try:
        response = requests.post(url, json=payload, timeout=2)
        if response.status_code == 200:
            success_count += 1
            print(f"[{i+1}/30] Success: 200 OK")
        else:
            print(f"[{i+1}/30] Failed: {response.status_code}")
    except Exception as e:
        print(f"[{i+1}/30] Error: {e}")

print(f"Traffic generation complete. {success_count}/30 successful.")
