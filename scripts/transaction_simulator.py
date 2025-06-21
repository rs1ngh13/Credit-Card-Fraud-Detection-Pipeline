#import libraries
import random
import json
import time
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

#configuration for azure eventhubs
CONNECTION_STRING = "Endpoint=sb://fraud-detection-project-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=jqE/NKbtmuz6h3cAc1PfBPai8DUkWdnHt+AEhEvO9dQ="
EVENT_HUB_NAME = "transaction-streams"

#fields for transaction details
user_ids = [f"user_{i}" for i in range(1,51)]
merchants = ["Amazon", "Uber", "DoorDash", "Target", "Exxon", "Taco Bell", "Costco"]
locations = ["CT", "NY", "MA", "NJ"]

#generate randomized transaction with a chance for an anomaly transaction 
def generate_transaction(anomaly = False):
    transaction = {
        "transaction_id": f"txn_{random.randint(100000, 999999)}",      #choose random transaction ID
        "user_id": random.choice(user_ids),                             #choose random user
        "amount": round(random.uniform(5, 250), 2),                     #random amount between $5 to $250
        "merchant_id": random.choice(merchants),                        #choose random merchant
        "location": random.choice(locations),                           #choose random state from list of EXPECTED states
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")    #current time of transaction
    }

    #if an anomaly is to occur...
    if anomaly:
        anomaly_type = random.choice(["high_amount", "abnormal_location"])          #anomaly will be either a high amount spent or take place in a random state outside of east coast
        if anomaly_type == "high_amount":
            transaction["amount"] = round(random.uniform(500, 10000), 2)            #exceedingly high amount for transaction
        elif anomaly_type == "abnormal_location":
            transaction["location"] = random.choice(["TX", "CA", "WA", "OH", "FL"]) #random state outside of the east coast

    return transaction

#send batch of events to the Event Hubs
def transaction_to_eventhubs(producer, event_data_batch):
    try:
        producer.send_batch(event_data_batch)                                       #send batches of events to event hubs
        print(f"[{datetime.now()}] Sent {len(event_data_batch)} transactions")
    except Exception as e: 
        print(f"Error sending batch: {e}")

def main():
    #Initialize the EventHubProducerClient
    producer = EventHubProducerClient.from_connection_string(
        conn_str = CONNECTION_STRING,
        eventhub_name = EVENT_HUB_NAME
    )
    try:
        while True:
            event_data_batch = producer.create_batch()      #new batrch of events

            for _ in range(5):                              #5 transactions per batch
                anomaly_chance = random.random() < 0.10     #10% chance of anomaly transaction 
                transaction = generate_transaction(anomaly = anomaly_chance)
                event_data_batch.add(EventData(json.dumps(transaction)))    #transaction added as JSON

            transaction_to_eventhubs(producer, event_data_batch)
            time.sleep(5)

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
