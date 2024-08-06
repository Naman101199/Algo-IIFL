from confluent_kafka.admin import AdminClient
import os

# Initialize Admin Client
admin_client = AdminClient({
    'bootstrap.servers': '43.205.25.254:9092'
})

# Get a list of all topics
metadata = admin_client.list_topics(timeout=10)
topics = metadata.topics.keys()

print(f"Topics to be deleted: {topics}")

# Delete all topics
delete_topics_result = admin_client.delete_topics(list(topics))

# Wait for each deletion to complete
for topic, future in delete_topics_result.items():
    try:
        future.result()  # The result itself is None
        print(f"Deleted topic: {topic}")
    except Exception as e:
        print(f"Failed to delete topic {topic}: {e}")