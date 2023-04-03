import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "nodal-time-375522-de68b184f726.json"
project_id = "nodal-time-375522"
topic_id = "test5"

bq_client = bigquery.Client()

# Set id of big query table
table_id = "nodal-time-375522.road.01_tracksMeta"

# Query to get cars id, and maxXVelocity
query = f"""
    SELECT id, maxXVelocity
    FROM `{table_id}`
"""

# Run query and convert results to data frame
df = bq_client.query(query).to_dataframe()

# Calculate the average speed of each car
average_speeds = df.groupby('id')['maxXVelocity'].mean()

# Calculate the number of cars going faster than the average speed
faster_than_average = (average_speeds > average_speeds.mean()).sum()

print(average_speeds.mean())
print(f'Number of cars going faster than the average speed: {faster_than_average}')

# Publish the results to topic
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
message_data = f"Average speed: {average_speeds.mean()}, Number of cars going faster than average: {faster_than_average}"
message_bytes = message_data.encode("utf-8")
future = publisher.publish(topic_path, message_bytes)
print(future.result())