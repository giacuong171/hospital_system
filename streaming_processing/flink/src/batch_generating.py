import datetime
import json
import os
import sys
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import urllib3

# import s3fs
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
from pyflink.datastream.window import TimeWindow, TumblingEventTimeWindows
from utils.checksum import write_md5_file


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


class WindowBatchProcess(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def __init__(
        self,
        bucket="raw-ecg-parquet",
        endpoint_url="localhost:9000",
        columns=None,
        tmp_path="/tmp",
    ):
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.columns = columns
        self.tmp_path = tmp_path

        if not os.path.isdir(self.tmp_path):
            os.makedirs(self.tmp_path, exist_ok=True)

        self.client = None  # delay creation

    def _init_minio_client(self):
        if self.client is None:
            try:
                self.client = Minio(
                    endpoint=self.endpoint_url,
                    access_key="minio_access_key",
                    secret_key="minio_secret_key",
                    secure=False,
                )
                if not self.client.bucket_exists(self.bucket):
                    self.client.make_bucket(self.bucket)
                    print(f"Bucket '{self.bucket}' created.")
                else:
                    print(f"Bucket '{self.bucket}' already exists.")
            except Exception as e:
                print(f"Error initializing Minio client: {e}")
                raise

    # Tasks: Add logging
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements: Iterable[tuple],
    ):
        self._init_minio_client()

        data = list(elements)
        if not data:
            return

        df = pd.DataFrame(data, columns=self.columns)
        window_end = context.window().end
        dt_str = datetime.datetime.fromtimestamp(
            window_end / 1000, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{key}_{dt_str}.parquet"
        file_path = os.path.join(self.tmp_path, filename)

        try:
            pq.write_table(pa.Table.from_pandas(df), file_path, compression="snappy")
            checksum_file = write_md5_file(file_path)
            print(f"MD5 file generated: {checksum_file}")

            # Upload .parquet file
            self.client.fput_object(
                bucket_name=self.bucket,
                object_name=f"{key}/{filename}",
                file_path=file_path,
                metadata={
                    "patient_id": key,
                    "timestamp": dt_str,
                    "data_type": "ecg_signal",
                },
            )

            # Upload checksum file
            self.client.fput_object(
                bucket_name=self.bucket,
                object_name=f"{key}/{checksum_file}",
                file_path=checksum_file,
                metadata={
                    "patient_id": key,
                    "timestamp": dt_str,
                    "data_type": "ecg_checksum",
                },
            )

            os.remove(file_path)
            os.remove(checksum_file)
            print(f"Cleaned up: {file_path}, {checksum_file}")
        except Exception as e:
            print(f"Upload or cleanup error: {e}")

        return [filename]


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
        .key_by(lambda x: x[1])  # key by patient ID
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(
            WindowBatchProcess(
                bucket="ecg-parquet",
                endpoint_url="localhost:9000",
                columns=columns,
                tmp_path="tmp",
            )
        )
        .set_parallelism(1)
        .print()
    )
    env.execute("Generating batch ecg files")
