"""
Common utilities for Kafka and Flink processing.

This module contains shared functions and classes used across multiple
Flink streaming jobs to avoid code duplication.
"""
import json
import os
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from kafka import KafkaAdminClient, KafkaProducer


def parse_json(value):
    """
    Parse JSON message from Kafka and extract ECG signal data.
    
    Args:
        value: JSON string containing the message payload
        
    Returns:
        tuple: (monitor_id, patient_id, room, created, ecg_signal, lead, sampling_rate)
    """
    data = json.loads(value)["payload"]
    return (
        data["monitor_id"],  # 0
        data["patient_id"],  # 1
        data["room"],  # 2
        data["created"],  # 3
        float(data["ecg_signal"]),  # 4
        data["lead"],  # 5
        int(data["sampling_rate"]),  # 6
    )


class CustomTimestampAssigner(TimestampAssigner):
    """
    Custom timestamp assigner for extracting timestamps from Kafka messages.
    
    Extracts the 'created' field from the message payload to use as the
    event timestamp for watermark generation.
    """
    
    def extract_timestamp(self, element, record_timestamp) -> int:
        """
        Extract timestamp from JSON message.
        
        Args:
            element: JSON string containing the message
            record_timestamp: Record timestamp from Kafka
            
        Returns:
            int: Timestamp in milliseconds
        """
        element = json.loads(element)
        timestamp = int(element["payload"]["created"])
        return timestamp


# Common type definition for ECG signal tuples
ECG_SIGNAL_TUPLE_TYPE = Types.TUPLE(
    [
        Types.STRING(),  # monitor_id
        Types.STRING(),  # patient_id
        Types.STRING(),  # room
        Types.STRING(),  # timestamp
        Types.FLOAT(),  # ecg_signal
        Types.STRING(),  # lead
        Types.INT(),  # sampling_rate
    ]
)

# Column names for ECG signal data
ECG_SIGNAL_COLUMNS = [
    "monitor_id",
    "patient_id",
    "room",
    "timestamp",
    "ecg_signal",
    "lead",
    "sampling_rate",
]


def setup_flink_environment(jars_path=None):
    """
    Set up Flink execution environment with required JAR files.
    
    Args:
        jars_path: Path to JAR files directory. If None, uses default path.
        
    Returns:
        StreamExecutionEnvironment: Configured Flink environment
    """
    if jars_path is None:
        jars_path = f"{os.getcwd()}/kafka_connect/jars"
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        f"file://{jars_path}/flink-connector-kafka-1.17.1.jar",
        f"file://{jars_path}/kafka-clients-3.4.0.jar",
    )
    return env


def create_kafka_clients(bootstrap_servers="localhost:9092"):
    """
    Create Kafka producer and admin client.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers address
        
    Returns:
        tuple: (KafkaProducer, KafkaAdminClient)
    """
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    return producer, admin_client


def create_kafka_consumer(topic, bootstrap_servers="localhost:9092", group_id="test_group"):
    """
    Create a Flink Kafka consumer.
    
    Args:
        topic: Kafka topic to consume from
        bootstrap_servers: Kafka bootstrap servers address
        group_id: Consumer group ID
        
    Returns:
        FlinkKafkaConsumer: Configured Kafka consumer
    """
    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id
        },
    )


def create_watermark_strategy(idleness_seconds=30):
    """
    Create a watermark strategy for monotonous timestamps.
    
    Args:
        idleness_seconds: Seconds to wait before marking source as idle
        
    Returns:
        WatermarkStrategy: Configured watermark strategy
    """
    return (
        WatermarkStrategy.for_monotonous_timestamps()
        .with_timestamp_assigner(CustomTimestampAssigner())
        .with_idleness(Duration.of_seconds(idleness_seconds))
    )

