"""
Common utilities for Kafka and Flink processing.

This module contains shared functions and classes used across multiple
Flink streaming jobs to avoid code duplication.
"""
import json
from pyflink.common import Types
from pyflink.common.watermark_strategy import TimestampAssigner


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
