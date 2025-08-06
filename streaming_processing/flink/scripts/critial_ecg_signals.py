import json
import os
from typing import Iterable

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import (
    Duration,
    TimestampAssigner,
    WatermarkStrategy,
)
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.window import TimeWindow, TumblingEventTimeWindows


def parse_json(value):
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
    def extract_timestamp(self, element, record_timestamp) -> int:
        element = json.loads(element)
        timestamp = int(element["payload"]["created"])
        return timestamp


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements: Iterable[tuple],
    ) -> tuple:
        max_val = max(e[4] for e in elements)
        room = elements[0][1]
        if abs(max_val) > 2.0:
            return [
                json.dumps(
                    {
                        "monitor_id": key,
                        "alert_type": "VOLTAGE_SPIKE",
                        "max_ecg_sinal": max_val,
                        "room": room,
                        "start": context.window().start,
                        "end": context.window().end,
                    }
                )
            ]


if __name__ == "__main__":
    JARS_PATH = f"{os.getcwd()}/kafka_connect/jars"
    print(JARS_PATH)
    servers = "localhost:9092"
    producer = KafkaProducer(bootstrap_servers=servers)
    admin_client = KafkaAdminClient(bootstrap_servers=servers)
    topic_name = "ecg_signal_alert"
    if topic_name not in admin_client.list_topics():
        topic = NewTopic(name=topic_name, num_partitions=5, replication_factor=1)
        admin_client.create_topics([topic])
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
    )
    # Define the source to take data from
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("http://localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    kafka_consumer = FlinkKafkaConsumer(
        topics="ICU_room",
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": "localhost:9092", "group.id": "test_group"},
    )
    print("kafka_consumer: ", kafka_consumer)
    watermark_strategy = (
        WatermarkStrategy.for_monotonous_timestamps()
        .with_timestamp_assigner(CustomTimestampAssigner())
        .with_idleness(Duration.of_seconds(30))
    )
    stream = env.add_source(kafka_consumer).map(
        parse_json,
        output_type=Types.TUPLE(
            [
                Types.STRING(),  # monitor_id
                Types.STRING(),  # patient_id
                Types.STRING(),  # room
                Types.STRING(),  # timestamp
                Types.FLOAT(),  # ecg_signal
                Types.STRING(),  # lead
                Types.INT(),  # sampling_rate
            ]
        ),
    )
    ds = (
        stream.assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda x: x[0])
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10000)))
        .process(CountWindowProcessFunction(), output_type=Types.STRING())
        .sink_to(sink=sink)
        .set_parallelism(1)
    )
    env.execute()
