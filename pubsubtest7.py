import os
from google.cloud import bigquery
from google.cloud import pubsub_v1
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "nodal-time-375522-de68b184f726.json"
project_id = "nodal-time-375522"
car_count_topic = "test7"
lane_change_topic = "test8"

bq_client = bigquery.Client()

# Set id of big query table
table_id = "nodal-time-375522.road.01_tracks"

# Query to get cars id, and laneId
query = f"""
    SELECT id, laneId
    FROM `{table_id}`
"""

# Run query and convert results to data frame
df = bq_client.query(query).to_dataframe()

# Keep only one frame per car unless it changes lanes
df = df.groupby(['id', 'laneId']).first().reset_index()

# Group by the 'laneId' column and get the number of cars in each lane
cars_per_lane = df.groupby('laneId')['id'].count()

# Publish each row of dataframe to topic
publisher = pubsub_v1.PublisherClient()
topic_name = publisher.topic_path(project_id, car_count_topic)

for lane_id, car_count in cars_per_lane.iteritems():
    data = {
        'lane_id': int(lane_id),
        'car_count': int(car_count)
    }
    message_data = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_name, message_data)
    print(future.result())
    
print(f"Published {len(cars_per_lane)} messages to {topic_name}.")

# Group by id column and get the number of lane changes for each car
lane_changes_per_car = df.groupby('id')['laneId'].nunique()

# Calculate the total number of lane changes
num_lane_changes = lane_changes_per_car.sum() - len(lane_changes_per_car)

# Publish results to topic
topic_name = publisher.topic_path(project_id, lane_change_topic)

data = {
    'num_lane_changes': int(num_lane_changes)
}
message_data = json.dumps(data).encode("utf-8")
future = publisher.publish(topic_name, message_data)
print(future.result())
print(f"Published 1 message to {topic_name}.")