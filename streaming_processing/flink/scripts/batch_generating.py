import datetime
import json
import os
import sys
from pathlib import Path
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
    ECG_SIGNAL_COLUMNS,
    setup_flink_environment,
    create_kafka_clients,
    create_kafka_consumer,
    create_watermark_strategy,
)


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
    producer, admin_client = create_kafka_clients()
    env = setup_flink_environment(JARS_PATH)
    kafka_consumer = create_kafka_consumer("ICU_room")
    watermark_strategy = create_watermark_strategy()
    stream = env.add_source(kafka_consumer).map(
        parse_json,
        output_type=ECG_SIGNAL_TUPLE_TYPE,
    )
    ds = (
        stream.assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda x: x[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(
            CountWindowProcessFunction(
                bucket="ecg-parquet",
                prefix="windowed",
                endpoint_url="http://localhost:9000",
                columns=ECG_SIGNAL_COLUMNS,
            )
        )
        .set_parallelism(1)
        .print()
    )
    env.execute("Generating batch ecg files")
