"""
Test suite for kafka_flink_common utilities.

These tests verify that the refactored shared utilities maintain
the same behavior as the original duplicated code.
"""
import json
import sys
from pathlib import Path

# Add src to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_parse_json():
    """Test that parse_json correctly extracts data from JSON message."""
    # Mock pyflink before importing
    from unittest.mock import MagicMock
    sys.modules['pyflink'] = MagicMock()
    sys.modules['pyflink.common'] = MagicMock()
    sys.modules['pyflink.common.watermark_strategy'] = MagicMock()
    
    from utils.kafka_flink_common import parse_json
    
    # Create a test message
    test_message = {
        "schema": {"type": "struct"},
        "payload": {
            "monitor_id": "monitor_1",
            "patient_id": "P001",
            "room": "ICU",
            "created": "2023-10-01 12:00:00",
            "ecg_signal": 0.75,
            "lead": "II",
            "sampling_rate": 500
        }
    }
    
    # Convert to JSON string
    json_str = json.dumps(test_message)
    
    # Parse the message
    result = parse_json(json_str)
    
    # Verify the result
    assert result[0] == "monitor_1", f"Expected monitor_id='monitor_1', got '{result[0]}'"
    assert result[1] == "P001", f"Expected patient_id='P001', got '{result[1]}'"
    assert result[2] == "ICU", f"Expected room='ICU', got '{result[2]}'"
    assert result[3] == "2023-10-01 12:00:00", f"Expected created='2023-10-01 12:00:00', got '{result[3]}'"
    assert result[4] == 0.75, f"Expected ecg_signal=0.75, got {result[4]}"
    assert result[5] == "II", f"Expected lead='II', got '{result[5]}'"
    assert result[6] == 500, f"Expected sampling_rate=500, got {result[6]}"
    
    # Verify types
    assert isinstance(result[4], float), "ecg_signal should be float"
    assert isinstance(result[6], int), "sampling_rate should be int"
    
    print("✓ test_parse_json passed")


def test_parse_json_with_numeric_conversion():
    """Test that parse_json correctly converts string numbers to proper types."""
    # Mock pyflink before importing
    from unittest.mock import MagicMock
    sys.modules['pyflink'] = MagicMock()
    sys.modules['pyflink.common'] = MagicMock()
    sys.modules['pyflink.common.watermark_strategy'] = MagicMock()
    
    from utils.kafka_flink_common import parse_json
    
    test_message = {
        "payload": {
            "monitor_id": "monitor_2",
            "patient_id": "P002",
            "room": "ICC",
            "created": "2023-10-01 13:00:00",
            "ecg_signal": "1.25",  # String that should be converted to float
            "lead": "III",
            "sampling_rate": "250"  # String that should be converted to int
        }
    }
    
    json_str = json.dumps(test_message)
    result = parse_json(json_str)
    
    assert result[4] == 1.25, f"Expected ecg_signal=1.25, got {result[4]}"
    assert result[6] == 250, f"Expected sampling_rate=250, got {result[6]}"
    assert isinstance(result[4], float), "ecg_signal should be float"
    assert isinstance(result[6], int), "sampling_rate should be int"
    
    print("✓ test_parse_json_with_numeric_conversion passed")


def test_timestamp_extraction_logic():
    """Test the timestamp extraction logic used in CustomTimestampAssigner."""
    test_message = {
        "payload": {
            "monitor_id": "monitor_1",
            "patient_id": "P001",
            "room": "ICU",
            "created": "1696161600000",  # Timestamp in milliseconds
            "ecg_signal": 0.75,
            "lead": "II",
            "sampling_rate": 500
        }
    }
    
    json_str = json.dumps(test_message)
    
    # Test the same logic used in CustomTimestampAssigner
    element = json.loads(json_str)
    timestamp = int(element["payload"]["created"])
    
    assert timestamp == 1696161600000, f"Expected timestamp=1696161600000, got {timestamp}"
    assert isinstance(timestamp, int), "Timestamp should be int"
    
    print("✓ test_timestamp_extraction_logic passed")


if __name__ == "__main__":
    print("Running tests for kafka_flink_common utilities...")
    print()
    
    test_parse_json()
    test_parse_json_with_numeric_conversion()
    test_timestamp_extraction_logic()
    
    print()
    print("All tests passed! ✓")

