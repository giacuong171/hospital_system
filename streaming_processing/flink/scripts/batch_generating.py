import datetime
import json
import os
from typing import Iterable, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from minio import Minio
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
    def __init__(
        self,
        bucket="raw-ecg-parquet",
        prefix="windows",
        endpoint_url="http://localhost:9000",
        columns=None,
    ):
        # client = Minio(
        #     endpoint=endpoint_url,
        #     access_key="minio_access_key",
        #     secret_key="minio_secret_key",
        #     http_client=http_client,
        #     secure=False,
        # )

        self.fs = s3fs.S3FileSystem(
            key="minio_access_key",
            secret="minio_secret_key",
            client_kwargs={"endpoint_url": endpoint_url},
            config_kwargs={"s3": {"addressing_style": "path"}},
        )
        self.bucket = bucket
        # self.prefix = prefix
        self.columns = columns
        if not self.fs.exists(self.bucket):
            self.fs.mkdir(self.bucket)
            print(f"Bucket '{self.bucket}' created.")
        else:
            print(f"Bucket '{self.bucket}' already exists.")

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements: Iterable[tuple],
    ):
        data = list(elements)
        if not data:
            return
        df = pd.DataFrame(data, columns=self.columns)
        # Create filename based on key and window end timestamp
        window_end = context.window().end  # epoch ms
        dt_str = datetime.datetime.fromtimestamp(
            window_end / 1000, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{key}_{dt_str}.parquet"
        s3_path = f"{self.bucket}/{filename}"
        table = pa.Table.from_pandas(df)
        with self.fs.open(s3_path, "wb") as f:
            pq.write_table(table, f, compression="snappy")
        return [s3_path]


if __name__ == "__main__":
    JARS_PATH = f"{os.getcwd()}/kafka_connect/jars"
    servers = "localhost:9092"
    producer = KafkaProducer(bootstrap_servers=servers)
    admin_client = KafkaAdminClient(bootstrap_servers=servers)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
    )
    kafka_consumer = FlinkKafkaConsumer(
        topics="ICU_room",
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": "localhost:9092", "group.id": "test_group"},
    )
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
    columns = [
        "monitor_id",
        "patient_id",
        "room",
        "timestamp",
        "ecg_signal",
        "lead",
        "sampling_rate",
    ]
    ds = (
        stream.assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda x: x[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(
            CountWindowProcessFunction(
                bucket="ecg-parquet",
                prefix="windowed",
                endpoint_url="http://localhost:9000",
                columns=columns,
            )
        )
        .set_parallelism(1)
        .print()
    )
    env.execute("Generating batch ecg files")
