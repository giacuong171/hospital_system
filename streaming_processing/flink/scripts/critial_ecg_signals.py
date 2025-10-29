import json
import os
import sys
from pathlib import Path
from typing import Iterable

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import (
    Duration,
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

# Add parent directory to path to import from src
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from utils.kafka_flink_common import (
    parse_json,
    CustomTimestampAssigner,
    ECG_SIGNAL_TUPLE_TYPE,
    setup_flink_environment,
    create_kafka_clients,
    create_kafka_consumer,
    create_watermark_strategy,
)


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
    producer, admin_client = create_kafka_clients()
    topic_name = "ecg_signal_alert"
    if topic_name not in admin_client.list_topics():
        topic = NewTopic(name=topic_name, num_partitions=5, replication_factor=1)
        admin_client.create_topics([topic])
    env = setup_flink_environment(JARS_PATH)
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
    kafka_consumer = create_kafka_consumer("ICU_room")
    print("kafka_consumer: ", kafka_consumer)
    watermark_strategy = create_watermark_strategy()
    stream = env.add_source(kafka_consumer).map(
        parse_json,
        output_type=ECG_SIGNAL_TUPLE_TYPE,
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
