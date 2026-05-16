"""Real-time event producer.

Emits three Kafka topic streams:
  hmis.bed_events      shelter check-in / check-out
  hmis.outreach        street-outreach encounters with geo
  hmis.case_updates    case-management updates (CDC-ish payloads)
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


TOPICS = ["hmis.bed_events", "hmis.outreach", "hmis.case_updates"]
PROJECTS = [f"P{i:04d}" for i in range(40)]
OUTREACH_ZONES = [f"Z{i:02d}" for i in range(20)]

_running = True


def _stop(*_):
    global _running
    _running = False


def bed_event() -> dict:
    return {
        "event_id":  f"BE-{int(time.time()*1000)}-{random.randint(0,9999)}",
        "event_type": random.choices(["CHECKIN", "CHECKOUT"], weights=[5, 4])[0],
        "client_id":  f"C{random.randint(0, 1999):08d}",
        "project_id": random.choice(PROJECTS),
        "bed_id":     f"B{random.randint(1, 500):04d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def outreach_event() -> dict:
    return {
        "event_id":   f"OE-{int(time.time()*1000)}-{random.randint(0,9999)}",
        "zone":       random.choice(OUTREACH_ZONES),
        "client_id":  f"C{random.randint(0, 1999):08d}" if random.random() > 0.3 else None,
        "encounter_type": random.choice(["NEW_CONTACT", "REPEAT_CONTACT",
                                         "REFERRAL_OFFERED", "REFERRAL_ACCEPTED"]),
        "needs":      random.sample(["food", "water", "hygiene", "medical",
                                     "mental_health", "housing", "id_recovery"],
                                    k=random.randint(1, 3)),
        "lat":        round(random.uniform(33.6, 33.9), 5),
        "lon":        round(random.uniform(-84.5, -84.2), 5),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def case_update_event() -> dict:
    return {
        "event_id":   f"CU-{int(time.time()*1000)}-{random.randint(0,9999)}",
        "op":         random.choices(["c", "u"], weights=[1, 9])[0],
        "client_id":  f"C{random.randint(0, 1999):08d}",
        "enrollment_id": f"E{random.randint(0, 3999):09d}",
        "field_changed": random.choice(["DisablingCondition", "HousingStatus",
                                        "IncomeAmount", "ContactStatus"]),
        "new_value":  random.choice(["1", "0", "stable", "engaged", "lost_contact"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


GENERATORS = {
    "hmis.bed_events":   bed_event,
    "hmis.outreach":     outreach_event,
    "hmis.case_updates": case_update_event,
}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--rate", type=int, default=20)
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 25})
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    sleep = 1.0 / max(args.rate, 1)
    sent = 0
    while _running:
        topic = random.choices(TOPICS, weights=[5, 3, 4])[0]
        ev = GENERATORS[topic]()
        producer.produce(topic, key=ev.get("client_id") or "anon",
                         value=json.dumps(ev).encode("utf-8"))
        sent += 1
        if sent % 100 == 0:
            producer.poll(0); print(f"sent={sent}")
        time.sleep(sleep)
    producer.flush(10)
    print(f"flushed. total={sent}")


if __name__ == "__main__":
    main()
