FROM python:3.9-slim

WORKDIR .

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 5000

COPY . .

# proto
RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./proto \
    --grpc_python_out=./proto \
    ./proto/person.proto
EXPOSE 5000
EXPOSE 5005
CMD ["sh", "-c", "python grpc_server.py & flask run --host 0.0.0.0"]