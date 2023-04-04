from google.cloud import pubsub_v1
import pandas as pd
import matplotlib.pyplot as plt
import json
import os

timeout = 5.0   

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="nodal-time-375522-de68b184f726.json"
project_id = "nodal-time-375522"
subscription_id = "projects/nodal-time-375522/subscriptions/test6-sub"

subscriber = pubsub_v1.SubscriberClient()

# Initialize variable to hold lane ids and average velocity
lane_ids = []
avg_velocities = []

# Define a callback function to process each message
def callback(message):
    # Parse the message 
    row_dict = json.loads(message.data.decode("utf-8"))

    # Extract the laneId and avg_xVelocity
    lane_id = row_dict["laneId"]
    avg_velocity = row_dict["avg_xVelocity"]
    
    # Append the values to the respective variable
    lane_ids.append(lane_id)
    avg_velocities.append(avg_velocity)
    
    # Create data frame using lane ids and average velocity
    df = pd.DataFrame({
        "laneId": lane_ids,
        "avg_xVelocity": avg_velocities
    })
    
    # Group the data frame by laneId and find the mean average velocity of each lane
    grouped_df = df.groupby("laneId").mean().reset_index()

    # Plot the data using a horizontal bar chart
    plt.bar(grouped_df["laneId"], grouped_df["avg_xVelocity"])
    plt.xlabel("Lane ID")
    plt.ylabel("Average Velocity")
    plt.title("Average Velocity by Lane")
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

