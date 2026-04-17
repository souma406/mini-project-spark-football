import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time

KAFKA_SERVER = 'kafka:9092' 
TOPIC_NAME = 'mauritania_matches'
DATA_PATH = '/home/jovyan/work/data/final_mauritania_football_dataset.parquet'

print("--- Kafka Producer (Auto-setup) ---")


try:
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER, client_id='test')
    topic_list = admin_client.list_topics()
    if TOPIC_NAME not in topic_list:
        print(f"Creating topic: {TOPIC_NAME}")
        topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
    else:
        print(f"Topic {TOPIC_NAME} already exists.")
except Exception as e:
    print(f"Note on topic creation: {e}")


try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    df = pd.read_parquet(DATA_PATH)
    print(f"Dataset loaded: {len(df)} matches. Streaming...")

    for index, row in df.iterrows():
        message = {
            'event_date': str(row['date']),
            'season': row['season'],
            'home_team': row['home_team'],
            'away_team': row['away_team'],
            'home_goals': int(row['home_goals']),
            'away_goals': int(row['away_goals'])
        }
        producer.send(TOPIC_NAME, value=message)
        print(f"Sent: {message['home_team']} vs {message['away_team']}")
        time.sleep(1)

except Exception as e:
    print(f"Fatal Error: {e}")
finally:
    print("Producer finished.")
