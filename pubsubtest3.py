import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import pubsub_v1
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "nodal-time-375522-de68b184f726.json"
project_id = "nodal-time-375522"
topic_id = "test3"

bq_client = bigquery.Client()

# Set id of big query table
table_id = "nodal-time-375522.road.01_tracks"

# Query to get cars id, front sight distance, and back sight distance
query = f"""
    SELECT id, frontSightDistance, backSightDistance
    FROM `{table_id}`
"""

# Run query and convert results to data frame
df = bq_client.query(query).to_dataframe()

# Find average front and back sight distance
avg_front_sight_distance = df.groupby('id')['frontSightDistance'].mean().astype(int)
avg_back_sight_distance = df.groupby('id')['backSightDistance'].mean().astype(int)

# Publish the results to topic
publisher = pubsub_v1.PublisherClient()
topic_name = publisher.topic_path(project_id, topic_id)

for car_id in avg_front_sight_distance.index:
    data = {
        'car_id': int(car_id),
        'avg_front_sight_distance': int(avg_front_sight_distance[car_id]),
        'avg_back_sight_distance': int(avg_back_sight_distance[car_id])
    }
    message_data = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_name, message_data)
    # print(future.result())
    
print(f"Published {len(avg_front_sight_distance)} messages to {topic_name}.")