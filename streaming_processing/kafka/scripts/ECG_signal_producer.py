import argparse
import json
import random
from datetime import datetime, timedelta
from time import sleep

import numpy as np
from bson import json_util
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default="localhost:9092",
    help="Where the bootstrap server is",
)
parser.add_argument(
    "-c",
    "--schemas_path",
    default="./avro_schemas",
    help="Folder containing all generated avro schemas",
)

args = parser.parse_args()

NUM_DEVICES = 1


def generate_ecg_message(t=0, frequency=1.3):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ecg_signal = 0.6 * np.sin(2 * np.pi * frequency * t) + np.random.normal(0, 0.05)
    monitor_id = "monitor_" + str(random.randint(1, 10))
    room = random.choice(["ICU", "ICC"])
    lead = "II"
    message = {
        "monitor_id": monitor_id,
        "room": room,
        "created": timestamp,
        "ecg_signal": float(ecg_signal),
        "lead": lead,
        "sampling_rate": 500,
    }
    return message


def create_topic(admin, topic_name):
    try:
        topic = NewTopic(name=topic_name, num_partitions=5, replication_factor=1)
        admin.create_topic([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def create_stream(servers, schemas_path):
    producer = None
    admin = None
    for _ in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass

    sampling_rate = 500  # Hz
    period = 1 / sampling_rate  # seconds
    t = 0
    while True:
        data = generate_ecg_message(t=0, frequency=1.3)
        t += period
        schema_path = f"{schemas_path}/schema_0.avsc"
        with open(schema_path, "r") as f:
            parsed_schema = json.loads(f.read())

        topic_name = f'{data["room"]}_room'
        # Create a new topic for this device id if not exists
        create_topic(admin, topic_name=topic_name)
        # Create the record including schema, and data,
        # ref: https://stackoverflow.com/a/76511956
        record = {
            "schema": {"type": "struct", "fields": parsed_schema["fields"]},
            "payload": data,
        }
        # record = data # Message without schema
        # Send messages to this topic
        producer.send(
            topic_name, json.dumps(record, default=json_util.default).encode("utf-8")
        )
        sleep(2)


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]

    # # Tear down all previous streams
    # print("Tearing down all existing topics!")
    # for device_id in range(NUM_DEVICES):
    #     try:
    #         teardown_stream(f"device_{device_id}", [servers])
    #     except Exception as e:
    #         print(f"Topic device_{device_id} does not exist. Skipping...!")

    if mode == "setup":
        schemas_path = parsed_args["schemas_path"]
        create_stream([servers], schemas_path)
