from google.cloud import bigquery
from google.cloud import pubsub_v1
import pandas as pd
import os
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="nodal-time-375522-de68b184f726.json"
project_id = "nodal-time-375522"
topic_id = "test6"

bq_client = bigquery.Client()

# Set id of big query table
table_id = "nodal-time-375522.road.01_tracks"

# Query to get cars id, and average xVelocity with name avg_xVelocity
query = f"""
    SELECT laneId, id, AVG(xVelocity) AS avg_xVelocity
    FROM `{table_id}`
    GROUP BY laneId, id
"""

# Run query and convert results to data frame
df = bq_client.query(query).to_dataframe()

# Group the data frame by laneId and find the average speed of each lane
avg_lane_speed = df.groupby("laneId")["avg_xVelocity"].mean().reset_index()

publisher = pubsub_v1.PublisherClient()
topic_name = publisher.topic_path(project_id, topic_id)

# Publish each row of dataframe to topic
for _, row in avg_lane_speed.iterrows():
    message_data = json.dumps(row.to_dict()).encode("utf-8")
    future = publisher.publish(topic_name, message_data)
    print(future.result())

print(f"Published {len(avg_lane_speed)} messages to {topic_name}.")