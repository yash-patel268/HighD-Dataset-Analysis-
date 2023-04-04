from google.cloud import pubsub_v1
import pandas as pd
import matplotlib.pyplot as plt
import json
import os

timeout = 5.0   

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="nodal-time-375522-de68b184f726.json"
project_id = "nodal-time-375522"
subscription_id = "projects/nodal-time-375522/subscriptions/test4-sub"

subscriber = pubsub_v1.SubscriberClient()

# Initialize variable to hold cars
car_counts = {}

# Define a callback function to process each received message
def callback(message):
    # Parse the message
    data = json.loads(message.data.decode("utf-8"))

    # Extract the car type and count
    car_type = data["class"]
    count = data["count"]

    # Add the car count for every car type
    if car_type in car_counts:
        car_counts[car_type] += count
    else:
        car_counts[car_type] = count
        
    # Convert the car_counts to data frame
    df = pd.DataFrame(list(car_counts.items()), columns=["class", "count"])

    # Plot the number of cars for each car type
    plt.bar(df["class"], df["count"])
    plt.xlabel("Car Type")
    plt.ylabel("Number of Cars")
    plt.title("Car Count by Type")
    plt.show()

    # Acknowledge the message
    message.ack()

# Start the subscriber to listen for messages
streaming_pull_future = subscriber.subscribe(subscription_id, callback=callback)
print(f"Listening for messages on {subscription_id}...")

# Block the main thread until the subscriber is closed
with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)

    except TimeoutError:
        streaming_pull_future.cancel()                        
        streaming_pull_future.result() 


