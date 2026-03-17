import json
import logging
import os

from kafka import KafkaConsumer
from geoalchemy2.functions import ST_Point
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Location  # import local para evitar dependencias circulares

logging.basicConfig(level=logging.WARNING)
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
    new_location = Location()
    new_location.person_id = location["person_id"]
    new_location.creation_time = location["creation_time"]
    new_location.coordinate = ST_Point(location["latitude"], location["longitude"])

    session.add(new_location)
    session.commit()


def main():

    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="location-consumer-group",
        auto_offset_reset="earliest",
    )

    logger.info("Consumidor Kafka arrancado, escuchando topic: %s", KAFKA_TOPIC)

    for message in consumer:
        location = message.value
        logger.info("Menssage: %s", location)
        try:
            save_location(session, location)
        except Exception as e:
            logger.error("Error save: %s", e)
            session.rollback()


if __name__ == "__main__":
    main()