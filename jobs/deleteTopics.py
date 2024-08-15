from confluent_kafka.admin import AdminClient
import os
import logging
import sys
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import configuration

todays_date = str(datetime.today().date()).replace('-','_')
log_file = f'logs/producer_{todays_date}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)
PUBLIC_IP = configuration.get("PUBLIC_IP")

# Initialize Admin Client
admin_client = AdminClient({
    'bootstrap.servers': f'{PUBLIC_IP}:9092'
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
        logger.info(f"Deleted topic: {topic}")
    except Exception as e:
        logger.error(f"Failed to delete topic {topic}: {e}")