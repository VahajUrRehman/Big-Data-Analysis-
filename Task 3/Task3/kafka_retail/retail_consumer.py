import json
import time
from collections import deque
from kafka import KafkaConsumer

# Connect to the retail Kafka topic
consumer = KafkaConsumer(
    "retail_transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="retail_consumer_group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Retail consumer started...")
print("Waiting for messages...\n")

# Keep recent message times for a simple 10-second window count
recent_messages = deque()

for message in consumer:
    event = message.value
    current_time = time.time()

    print("Received event:")
    print(event)

    # Simple anomaly rule:
    # flag higher-value retail transactions
    revenue = float(event.get("Revenue", 0))

    if revenue >= 20:
        print(f"ALERT: High-value transaction detected: Revenue = {revenue}")
    else:
        print(f"Normal transaction: Revenue = {revenue}")

    # Add current message time to the window
    recent_messages.append(current_time)

    # Remove messages older than 10 seconds
    while recent_messages and current_time - recent_messages[0] > 10:
        recent_messages.popleft()

    print(
        f"Messages received in last 10 seconds: "
        f"{len(recent_messages)}"
    )

    print("-" * 60)