from datetime import datetime

from app.udaconnect.models import Location
from app.udaconnect.schemas import (
    LocationSchema,
)
from app.udaconnect.services import LocationService
from flask import request, jsonify
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from typing import Optional, List

DATE_FORMAT = "%Y-%m-%d"

api = Namespace("UdaConnect", description="Connections via geolocation.")  # noqa


# TODO: This needs better exception handling


@api.route("/locations")
@api.route("/locations/<location_id>")
@api.param("location_id", "Unique ID for a given Location", _in="query")
class LocationResource(Resource):
    @accepts(schema=LocationSchema, api=api)
    def post(self) -> Location:
        request.get_json()
        LocationService.create(request.get_json())
        return {"message": "Location received"}, 201
    @responds(schema=LocationSchema)
    def get(self, location_id) -> Location:
        location: Location = LocationService.retrieve(location_id)
        return location
