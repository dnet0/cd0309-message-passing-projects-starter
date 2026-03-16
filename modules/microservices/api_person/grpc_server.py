import time 
import os
import sys
from concurrent import futures

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "proto"))

import person_pb2
import person_pb2_grpc

from app import create_app
from app.udaconnect.services import PersonService

class PersonServicer(person_pb2_grpc.PersonServiceServicer):
    def __init__(self):
        self.app = create_app(os.getenv("FLASK_ENV", "test"))

    def GetAllPersons(self, request, context):
        with self.app.app_context():
            persons_result = PersonService.retrieve_all()
            return person_pb2.PersonList(
                persons=[
                    person_pb2.PersonMessage(
                        id=p.id,
                        first_name=p.first_name,
                        last_name=p.last_name,
                        company_name=p.company_name,
                    )
                    for p in persons_result
                ]
            )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    person_pb2_grpc.add_PersonServiceServicer_to_server(PersonServicer(), server)

    port = os.getenv("GRPC_PORT", "5005")
    server.add_insecure_port(f"[::]:{port}")
    server.start()

    server.wait_for_termination()


if __name__ == "__main__":
    serve()