from google.cloud import bigquery
from google.cloud import pubsub_v1
import pandas as pd
import os
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="nodal-time-375522-de68b184f726.json"
project_id = "nodal-time-375522"
topic_id = "test4"

bq_client = bigquery.Client()

# Set id of big query table
table_id = "nodal-time-375522.road.01_tracksMeta"

# Query to get data
query = f"""
    SELECT class
    FROM `{table_id}`
"""

# Run query and convert results to data frame
df = bq_client.query(query).to_dataframe()

# Count the number of different types of cars
num_car_types = df['class'].nunique()
print(f"Number of different types of cars: {num_car_types}")

# Count the number of cars for each type
car_counts = df.groupby('class').size().reset_index(name='count')
print("Number of cars for each type:")
print(car_counts)

publisher = pubsub_v1.PublisherClient()
topic_name = publisher.topic_path(project_id, topic_id)

# Publish each row of dataframe to topic
for _, row in car_counts.iterrows():
    message_data = json.dumps(row.to_dict()).encode("utf-8")
    future = publisher.publish(topic_name, message_data)
    print(future.result())

print(f"Published {len(car_counts)} messages to {topic_name}.")