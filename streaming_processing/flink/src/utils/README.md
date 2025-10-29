# Kafka and Flink Common Utilities

This module provides shared utilities for Kafka and Flink streaming processing in the hospital system.

## Overview

The `kafka_flink_common.py` module consolidates common functionality that was previously duplicated across multiple Flink streaming jobs. This refactoring improves code maintainability, reduces bugs, and ensures consistent behavior across all streaming processing scripts.

## Components

### Data Processing Functions

#### `parse_json(value)`
Parses JSON messages from Kafka and extracts ECG signal data.

**Parameters:**
- `value` (str): JSON string containing the message payload

**Returns:**
- tuple: `(monitor_id, patient_id, room, created, ecg_signal, lead, sampling_rate)`

**Example:**
```python
from utils.kafka_flink_common import parse_json

message = '{"payload": {"monitor_id": "M001", "patient_id": "P001", ...}}'
data = parse_json(message)
# data = ('M001', 'P001', 'ICU', '2023-10-01 12:00:00', 0.75, 'II', 500)
```

### Classes

#### `CustomTimestampAssigner`
Custom timestamp assigner for extracting timestamps from Kafka messages for watermark generation.

Inherits from `pyflink.common.watermark_strategy.TimestampAssigner`.

**Methods:**
- `extract_timestamp(element, record_timestamp) -> int`: Extracts the 'created' field from the message payload as the event timestamp.

### Type Definitions

#### `ECG_SIGNAL_TUPLE_TYPE`
PyFlink type definition for ECG signal tuples.

**Structure:**
```
Types.TUPLE([
    Types.STRING(),  # monitor_id
    Types.STRING(),  # patient_id
    Types.STRING(),  # room
    Types.STRING(),  # timestamp
    Types.FLOAT(),   # ecg_signal
    Types.STRING(),  # lead
    Types.INT(),     # sampling_rate
])
```

#### `ECG_SIGNAL_COLUMNS`
List of column names corresponding to the ECG signal tuple.

```python
['monitor_id', 'patient_id', 'room', 'timestamp', 'ecg_signal', 'lead', 'sampling_rate']
```

### Setup Helper Functions

#### `setup_flink_environment(jars_path=None)`
Sets up Flink execution environment with required JAR files.

**Parameters:**
- `jars_path` (str, optional): Path to JAR files directory. Defaults to `{cwd}/kafka_connect/jars`.

**Returns:**
- `StreamExecutionEnvironment`: Configured Flink environment

**Example:**
```python
from utils.kafka_flink_common import setup_flink_environment

env = setup_flink_environment("/path/to/jars")
```

#### `create_kafka_clients(bootstrap_servers="localhost:9092")`
Creates Kafka producer and admin client.

**Parameters:**
- `bootstrap_servers` (str): Kafka bootstrap servers address

**Returns:**
- tuple: `(KafkaProducer, KafkaAdminClient)`

**Example:**
```python
from utils.kafka_flink_common import create_kafka_clients

producer, admin_client = create_kafka_clients()
```

#### `create_kafka_consumer(topic, bootstrap_servers="localhost:9092", group_id="test_group")`
Creates a Flink Kafka consumer.

**Parameters:**
- `topic` (str): Kafka topic to consume from
- `bootstrap_servers` (str): Kafka bootstrap servers address
- `group_id` (str): Consumer group ID

**Returns:**
- `FlinkKafkaConsumer`: Configured Kafka consumer

**Example:**
```python
from utils.kafka_flink_common import create_kafka_consumer

consumer = create_kafka_consumer("ICU_room")
```

#### `create_watermark_strategy(idleness_seconds=30)`
Creates a watermark strategy for monotonous timestamps.

**Parameters:**
- `idleness_seconds` (int): Seconds to wait before marking source as idle

**Returns:**
- `WatermarkStrategy`: Configured watermark strategy with custom timestamp assigner

**Example:**
```python
from utils.kafka_flink_common import create_watermark_strategy

watermark_strategy = create_watermark_strategy()
```

## Usage

### Before Refactoring

Each script contained duplicated code:

```python
# Duplicated in 4 files
def parse_json(value):
    data = json.loads(value)["payload"]
    return (data["monitor_id"], ...)

class CustomTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp) -> int:
        element = json.loads(element)
        return int(element["payload"]["created"])

# Setup code duplicated in 4 files
env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(...)
producer = KafkaProducer(bootstrap_servers=servers)
admin_client = KafkaAdminClient(bootstrap_servers=servers)
```

### After Refactoring

All scripts now import from the shared module:

```python
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

# Clean, maintainable code
producer, admin_client = create_kafka_clients()
env = setup_flink_environment(JARS_PATH)
kafka_consumer = create_kafka_consumer("ICU_room")
watermark_strategy = create_watermark_strategy()
```

## Testing

Run the test suite to verify functionality:

```bash
cd streaming_processing/flink/src
python3 utils/test_kafka_flink_common.py
```

All tests should pass:
```
Running tests for kafka_flink_common utilities...

✓ test_parse_json passed
✓ test_parse_json_with_numeric_conversion passed
✓ test_timestamp_extraction_logic passed

All tests passed! ✓
```

## Benefits

1. **DRY Principle**: Single source of truth for common functionality
2. **Easier Maintenance**: Changes only need to be made in one place
3. **Better Documentation**: Centralized docstrings explain functionality
4. **Reduced Bugs**: No risk of inconsistent implementations
5. **Improved Testability**: Shared code can be tested independently
6. **Code Reusability**: Easy to use in new streaming jobs

## Migration Guide

To migrate existing scripts to use this module:

1. Add import statement:
   ```python
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
   ```

2. Remove local implementations of these functions/classes

3. Replace local type definitions with `ECG_SIGNAL_TUPLE_TYPE`

4. Replace manual setup code with helper functions

5. Test to ensure behavior remains unchanged

## Compatibility

- Python 3.7+
- Apache Flink 1.17.1+
- kafka-python 2.0.2+
