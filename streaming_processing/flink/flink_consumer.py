import json
import time

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


def parse_json(value):
    data = json.loads(value)
    return (
        data["monitor_id"],  # 0
        data["room"],  # 1
        data["timestamp"],  # 2
        float(data["ecg_signal"]),  # 3
        data["lead"],  # 4
        int(data["sampling_rate"]),  # 5
    )


def to_json(alert):
    return json.dumps(
        {
            "patient_id": alert[0],
            "alert_type": alert[1],
            "value": alert[2],
            "timestamp": alert[3],
        }
    )


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

# Kafka consumer setup
properties = {"bootstrap.servers": "localhost:9092", "group.id": "ecg-consumer"}

kafka_source = FlinkKafkaConsumer(
    topics="ICU_room",
    deserialization_schema=SimpleStringSchema(),
    properties=properties,
)

stream = env.add_source(kafka_source).map(
    parse_json,
    output_type=Types.TUPLE(
        [
            Types.STRING(),  # monitor_id
            Types.STRING(),  # room
            Types.STRING(),  # timestamp
            Types.FLOAT(),  # ecg_signal
            Types.STRING(),  # lead
            Types.INT(),  # sampling_rate
        ]
    ),
)

# Assign timestamp and watermark
stream = stream.assign_timestamps_and_watermarks(
    WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(lambda event, ts: event[4])
)

from pyflink.datastream import Collector
from pyflink.datastream.functions import ProcessWindowFunction

# Sliding window: 10s window every 2s
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


class AnomalyDetectionFunction(ProcessWindowFunction):

    def process(
        self, key, context: ProcessWindowFunction.Context, elements, out: Collector
    ):
        max_val = max(e[3] for e in elements)
        if abs(max_val) > 2.0:
            out.collect((key, "VOLTAGE_SPIKE", max_val, context.window().get_end()))


# Keyed by patient_id
alerts = (
    stream.key_by(lambda x: x[0])  # patient_id
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
    .process(
        AnomalyDetectionFunction(),
        output_type=Types.TUPLE(
            [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.LONG()]
        ),
    )
)

# Serialize alerts as JSON and send to Kafka (or use print sink)
alert_json = alerts.map(to_json, output_type=Types.STRING())

# Kafka sink
kafka_sink = FlinkKafkaProducer(
    topic="ecg-alerts",
    serialization_schema=SimpleStringSchema(),
    producer_config={"bootstrap.servers": "localhost:9092"},
)

alert_json.add_sink(kafka_sink)

env.execute("ECG Anomaly Detection - Max Voltage")
