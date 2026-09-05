import json
import time
from kafka import KafkaProducer

# Connect to Kafka on your Windows machine
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

topic_name = "retail_transactions"

# Example retail transaction events
retail_events = [
    {
        "InvoiceNo": "536365",
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": 6,
        "UnitPrice": 2.55,
        "Country": "United Kingdom",
        "Revenue": 15.30
    },
    {
        "InvoiceNo": "536366",
        "StockCode": "22633",
        "Description": "HAND WARMER UNION JACK",
        "Quantity": 6,
        "UnitPrice": 1.85,
        "Country": "United Kingdom",
        "Revenue": 11.10
    },
    {
        "InvoiceNo": "536367",
        "StockCode": "22745",
        "Description": "POPPY'S PLAYHOUSE BEDROOM",
        "Quantity": 12,
        "UnitPrice": 2.10,
        "Country": "United Kingdom",
        "Revenue": 25.20
    },
    {
        "InvoiceNo": "536368",
        "StockCode": "22960",
        "Description": "JAM MAKING SET WITH JARS",
        "Quantity": 6,
        "UnitPrice": 4.25,
        "Country": "United Kingdom",
        "Revenue": 25.50
    }
]

for event in retail_events:
    producer.send(topic_name, value=event)
    print(f"Sent: {event}")
    time.sleep(1)

producer.flush()
producer.close()

print("Finished sending retail events.")