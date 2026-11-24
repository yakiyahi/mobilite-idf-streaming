import json, time, requests
from datetime import datetime, timezone
from confluent_kafka import Producer

URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
p = Producer({"bootstrap.servers":"localhost:19092"})

while True:
    try:
        r = requests.get(URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        for s in data["data"]["stations"]:
            msg = {"ingestion_ts": ingestion_ts, "source": "gbfs_station_status", "payload": s}
            p.produce("velib.station_status.raw",
                      key=str(s["station_id"]),
                      value=json.dumps(msg))
        p.flush()
    except Exception as e:
        print("Erreur ingestion:", e)   # en prod : logs + alerting
    time.sleep(60)