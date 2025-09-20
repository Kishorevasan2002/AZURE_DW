import json
import random
import time
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData

# --- Event Hub configs ---
FLEET_CONN = "<place_holder>"
TRUCK_CONN = "<place_holder>"
FLEET_HUB = "fleet_data"
TRUCK_HUB = "truck_data"

# --- Clients ---
producer_fleet = EventHubProducerClient.from_connection_string(conn_str=FLEET_CONN, eventhub_name=FLEET_HUB)
producer_truck = EventHubProducerClient.from_connection_string(conn_str=TRUCK_CONN, eventhub_name=TRUCK_HUB)

# --- Simulation constants ---
BASE_KM_PER_LITER = 3.5
WEIGHT_PENALTY = 0.05      # per ton
AVG_SPEED = 70             # km/h
CO2_PER_LITER = 2.68       # kg

# --- Fleet Event Generator ---
def generate_fleet_event():
    delivery_id = f"D-{random.randint(1000, 9999)}"
    truck_id = random.randint(1, 50)
    now = datetime.utcnow()
    trip_distance_km = random.randint(50, 300)
    cargo_weight_ton = random.randint(5, 20)

    delivery_time = now + timedelta(hours=trip_distance_km / AVG_SPEED)

    return {
        "delivery_id": delivery_id,
        "truck_id": truck_id,
        "driver_id": random.randint(200, 300),
        "pickup_time": now.isoformat() + "Z",
        "delivery_time": delivery_time.isoformat() + "Z",
        "trip_distance_km": trip_distance_km,
        "cargo_weight_ton": cargo_weight_ton,
        "cargo_type": random.choice(["Food", "Electronics", "Furniture", "Clothing"]),
        "delivery_status": "Scheduled",
        "location": random.choice(["Mumbai", "Delhi", "Bangalore", "Hyderabad"]),
        "event_type": "FleetEvent",
        "event_date": now.date().isoformat()
    }

# --- Truck Telemetry Generator ---
def generate_truck_events(delivery):
    truck_id = delivery["truck_id"]
    trip_distance = delivery["trip_distance_km"]
    cargo_weight = delivery["cargo_weight_ton"]

    # fuel efficiency decreases with weight
    km_per_liter = max(BASE_KM_PER_LITER - cargo_weight * WEIGHT_PENALTY, 1.5)
    total_fuel = trip_distance / km_per_liter
    total_co2 = total_fuel * CO2_PER_LITER

    # simulate ~1 telemetry per 25km
    num_events = max(2, trip_distance // 25)
    events = []
    pickup = datetime.fromisoformat(delivery["pickup_time"].replace("Z", ""))
    drop = datetime.fromisoformat(delivery["delivery_time"].replace("Z", ""))
    duration = (drop - pickup).total_seconds()

    for i in range(num_events):
        progress = (i + 1) / num_events
        timestamp = pickup + timedelta(seconds=progress * duration)

        telemetry = {
            "delivery_id": delivery["delivery_id"],  # 🔑 join with fleet
            "truck_id": truck_id,
            "speed": round(random.gauss(AVG_SPEED, 10), 1),
            "fuel_used": round(total_fuel * progress, 2),
            "co2_emitted": round(total_co2 * progress, 2),
            "engine_temp": round(85 + cargo_weight * 0.3 + random.uniform(-2, 2), 1),
            "event_type": "TruckTelemetry",
            "event_date": timestamp.date().isoformat(),
            "EventTimestamp": timestamp.isoformat() + "Z"
        }
        events.append(telemetry)

    return events

# --- Send helper ---
def send_to_eventhub(producer, payloads):
    batch = producer.create_batch()
    for p in payloads:
        batch.add(EventData(json.dumps(p)))
    producer.send_batch(batch)

# --- Main ---
def main():
    while True:
        # 1. Fleet event
        fleet_event = generate_fleet_event()
        send_to_eventhub(producer_fleet, [fleet_event])
        print("✅ Sent Fleet Event:", fleet_event)

        # 2. Truck telemetry (linked + correlated)
        truck_events = generate_truck_events(fleet_event)
        send_to_eventhub(producer_truck, truck_events)
        print(f"✅ Sent {len(truck_events)} Truck Events for Delivery {fleet_event['delivery_id']}")

        time.sleep(0.1)

if __name__ == "__main__":
    main()




