import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from app import db
from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-api")
from kafka import KafkaProducer

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-api-location")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "locations")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
class LocationService:
    @staticmethod
    def retrieve(location_id) -> Location:
        location, coord_text = (
            db.session.query(Location, Location.coordinate.ST_AsText())
            .filter(Location.id == location_id)
            .one()
        )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return location

    @staticmethod
    def create(location: Dict) -> Location:
        validation_results: Dict = LocationSchema().validate(location)
        if validation_results:
            logger.warning(f"Unexpected data format in payload: {validation_results}")
            raise Exception(f"Invalid payload: {validation_results}")

        message = {
            "person_id": location["person_id"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "creation_time": str(location["creation_time"])
        }
        producer.send(KAFKA_TOPIC, value=message)
        producer.flush()
        logger.info(f"Mensaje enviado a Kafka: {message}")

        new_location = Location()
        new_location.person_id = location["person_id"]
        new_location.creation_time = location["creation_time"]
        new_location.coordinate = ST_Point(location["latitude"], location["longitude"])

        return new_location
