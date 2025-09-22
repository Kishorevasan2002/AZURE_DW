import json
import random
import time
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData

# --- Event Hub configs ---
FLEET_CONN = "<connection string>"
TRUCK_CONN = "<connection string>"
FLEET_HUB = "fleet_data"
TRUCK_HUB = "truck_data"

# --- Clients ---
producer_fleet = EventHubProducerClient.from_connection_string(conn_str=FLEET_CONN, eventhub_name=FLEET_HUB)
producer_truck = EventHubProducerClient.from_connection_string(conn_str=TRUCK_CONN, eventhub_name=TRUCK_HUB)

# --- Simulation constants ---
BASE_KM_PER_LITER = 3.5
WEIGHT_PENALTY = 0.05      # per ton
AVG_SPEED = 60             # km/h
CO2_PER_LITER = 2.68       # kg
ANOMALY_CHANCE = 0.15      # 15% chance of an anomaly in a trip's telemetry

# --- Simulation enhancements ---
SIMULATION_START_DATE = datetime.utcnow() - timedelta(days=30)
current_simulated_time = SIMULATION_START_DATE

TAMIL_NADU_LOCATIONS = [
    "Tirunelveli", "Chennai", "Coimbatore", "Madurai", "Tiruchirappalli",
    "Salem", "Thoothukudi", "Kanyakumari", "Nagercoil", "Vellore"
]
TRAFFIC_CONDITIONS = {"Low": 1.0, "Medium": 0.85, "High": 0.70} # Speed multiplier

# --- Event Generators ---
def generate_fleet_event(sim_time):
    delivery_id = f"D-{random.randint(1000, 9999)}"
    truck_id = random.randint(1, 50)
    now = sim_time
    trip_distance_km = random.randint(50, 400)
    cargo_weight_ton = random.randint(5, 20)
    pickup_location, delivery_location = random.sample(TAMIL_NADU_LOCATIONS, 2)
    traffic = random.choice(list(TRAFFIC_CONDITIONS.keys()))
    effective_speed = AVG_SPEED * TRAFFIC_CONDITIONS[traffic]
    delivery_time = now + timedelta(hours=trip_distance_km / effective_speed)
    return {
        "delivery_id": delivery_id, "truck_id": truck_id, "driver_id": random.randint(200, 300),
        "customer_id": f"CUST-{random.randint(100, 150)}", "pickup_time": now.isoformat() + "Z",
        "delivery_time": delivery_time.isoformat() + "Z", "trip_distance_km": trip_distance_km,
        "cargo_weight_ton": cargo_weight_ton,
        "cargo_type": random.choice(["Textiles", "Electronics", "Automotive Parts", "Agricultural Produce"]),
        "delivery_status": "Scheduled", "pickup_location": pickup_location,
        "delivery_location": delivery_location, "traffic_condition": traffic,
        "event_type": "FleetEvent", "event_date": now.date().isoformat()
    }

def generate_fleet_update_event(original_event, actual_delivery_time):
    planned_delivery_time = datetime.fromisoformat(original_event["delivery_time"].replace("Z", ""))
    status = "Completed" if actual_delivery_time <= planned_delivery_time else "Delayed"
    return {
        "delivery_id": original_event["delivery_id"], "truck_id": original_event["truck_id"],
        "delivery_status": status, "actual_delivery_time": actual_delivery_time.isoformat() + "Z",
        "event_type": "FleetUpdateEvent", "event_date": actual_delivery_time.date().isoformat()
    }

def generate_truck_events(delivery):
    truck_id = delivery["truck_id"]
    trip_distance = delivery["trip_distance_km"]
    cargo_weight = delivery["cargo_weight_ton"]
    traffic = delivery["traffic_condition"]
    effective_speed = AVG_SPEED * TRAFFIC_CONDITIONS[traffic]
    km_per_liter = max(BASE_KM_PER_LITER - cargo_weight * WEIGHT_PENALTY - (1 if traffic == "High" else 0), 1.5)
    total_fuel = trip_distance / km_per_liter
    total_co2 = total_fuel * CO2_PER_LITER
    num_events = max(2, trip_distance // 25)
    events = []
    pickup = datetime.fromisoformat(delivery["pickup_time"].replace("Z", ""))
    drop = datetime.fromisoformat(delivery["delivery_time"].replace("Z", ""))
    duration_seconds = (drop - pickup).total_seconds()
    has_anomaly = random.random() < ANOMALY_CHANCE
    anomaly_type = random.choice(["Speeding", "HighEngineTemp"])
    anomaly_point = random.randint(1, num_events - 1) if num_events > 1 else 0
    for i in range(num_events):
        progress = (i + 1) / num_events
        timestamp = pickup + timedelta(seconds=progress * duration_seconds)
        current_speed = round(random.gauss(effective_speed, 10), 1)
        telemetry = {
            "delivery_id": delivery["delivery_id"], "truck_id": truck_id, "speed": current_speed,
            "fuel_used": round(total_fuel * progress, 2), "co2_emitted": round(total_co2 * progress, 2),
            "engine_temp": round(85 + cargo_weight * 0.3 + (5 if traffic == "High" else 0) + random.uniform(-2, 2), 1),
            "alert_type": None, "traffic_condition": traffic, "event_type": "TruckTelemetry",
            "event_date": timestamp.date().isoformat(), "EventTimestamp": timestamp.isoformat() + "Z"
        }
        if has_anomaly and i == anomaly_point:
            if anomaly_type == "Speeding":
                telemetry["speed"] = round(random.uniform(effective_speed + 20, effective_speed + 40), 1)
                telemetry["alert_type"] = "Speeding"
            elif anomaly_type == "HighEngineTemp":
                telemetry["engine_temp"] = round(random.uniform(105, 115), 1)
                telemetry["alert_type"] = "HighEngineTemp"
        events.append(telemetry)
    random_adjustment_minutes = random.randint(-30, 30)
    actual_delivery_time = timestamp + timedelta(minutes=random_adjustment_minutes)
    return events, actual_delivery_time

def send_to_eventhub(producer, payloads):
    if not payloads: return
    batch = producer.create_batch()
    for p in payloads:
        try: batch.add(EventData(json.dumps(p)))
        except ValueError:
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(p)))
    if len(batch) > 0: producer.send_batch(batch)

def main():
    global current_simulated_time
    print(f"Starting simulation from: {current_simulated_time.isoformat()}")
    while True:
        if current_simulated_time > datetime.utcnow():
            print("--- Simulation caught up to real-time. Resetting clock. ---")
            current_simulated_time = SIMULATION_START_DATE
        
        # Generate a full delivery lifecycle
        fleet_event = generate_fleet_event(current_simulated_time)
        send_to_eventhub(producer_fleet, [fleet_event])
        print(f"\n🚚 [{current_simulated_time.strftime('%Y-%m-%d %H:%M')}] NEW DELIVERY: {fleet_event['delivery_id']} ({fleet_event['pickup_location']} -> {fleet_event['delivery_location']})")

        truck_events, actual_delivery_time = generate_truck_events(fleet_event)
        send_to_eventhub(producer_truck, truck_events)
        print(f"   - Sent {len(truck_events)} telemetry events.")
        for event in truck_events:
            if event.get("alert_type"):
                print(f"   - ❗ ANOMALY: {event['alert_type']}")

        fleet_update = generate_fleet_update_event(fleet_event, actual_delivery_time)
        send_to_eventhub(producer_fleet, [fleet_update])
        print(f"   - ✅ DELIVERY {fleet_update['delivery_status'].upper()}: {fleet_update['delivery_id']}")

        time_jump_minutes = random.randint(30, 300)
        current_simulated_time += timedelta(minutes=time_jump_minutes)
        time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
    finally:
        producer_fleet.close()
        producer_truck.close()
        print("Event Hub producers closed.")
