import json
import logging
import os

from kafka import KafkaConsumer
from geoalchemy2.functions import ST_Point
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Location  # import local para evitar dependencias circulares

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("udaconnect-location-consumer")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "locations")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

SQLALCHEMY_DATABASE_URI = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def save_location(session, location: dict):
    logging.info("Saving Location...")
    new_location = Location()
    new_location.person_id = location["person_id"]
    new_location.creation_time = location["creation_time"]
    new_location.coordinate = ST_Point(location["latitude"], location["longitude"])

    session.add(new_location)
    session.commit()
    logging.info("Location saved successfully with ID: %s", new_location.id)


def main():
    logging.info("Starting consumer...")
    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    logging.info("Connecting to Kafka...")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            request_timeout_ms=30000,  
            session_timeout_ms=10000,
            max_poll_interval_ms=30000,
        ) 
    except Exception as e:
        logging.error(f"ERROR: {e}")
        return

    for message in consumer:
        logging.info(f"Message received: {message.value}")
        try:
            save_location(session, message.value)
        except Exception as e:
            logging.error(f"Error saving location: {e}")
            session.rollback()
            
if __name__ == "__main__":
    main()