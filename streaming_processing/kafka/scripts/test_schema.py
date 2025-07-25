import json
import sys

from confluent_kafka import Consumer, KafkaException

# Kafka consumer configuration
# Replace 'localhost:9092' with your Kafka broker address
# Replace 'my_consumer_group' with a unique group ID for your consumer application
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my_consumer_group",
    "auto.offset.reset": "earliest",  # Start reading from the beginning of the topic if no offset is committed
    "enable.auto.commit": True,  # Automatically commit offsets
    "auto.commit.interval.ms": 5000,  # Commit offsets every 5 seconds
}

consumer = Consumer(conf)

# Replace 'your_topic_name' with the actual Kafka topic you want to consume from
topic = "ICU_room"
consumer.subscribe([topic])

print(f"Subscribed to topic '{topic}'. Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with a 1-second timeout

        if msg is None:
            # No message available within the timeout
            # print("Waiting for messages...")
            continue
        if msg.error():
            # Error occurred during message consumption
            if msg.error().code() == KafkaException.PARTITION_EOF:
                # End of partition event, not an actual error, just means no more messages for now
                sys.stderr.write(
                    f"%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n"
                )
                continue
            elif msg.error():
                # Other Kafka error
                sys.stderr.write(f"Consumer error: {msg.error()}\n")
                break

        # --- Message Processing and Validation ---
        try:
            msg_value_bytes = msg.value()
            if msg_value_bytes is None:
                print(
                    f"Received null message value from topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}"
                )
                continue

            # Attempt to decode the message value as UTF-8 string
            msg_value_str = msg_value_bytes.decode("utf-8")
            print(
                f"\n--- Raw Message from Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()} ---"
            )
            print(msg_value_str)

            # Try to parse the string as JSON
            data = json.loads(msg_value_str)

            # Check for Kafka Connect's JSON Converter format ("schema" and "payload")
            if "schema" in data and "payload" in data:
                print(
                    "Message detected as Kafka Connect JSON Converter format (has 'schema' and 'payload' fields)."
                )
                schema = data["schema"]
                payload = data["payload"]

                # Perform schema validation
                if schema.get("type") != "struct":
                    print(
                        f"  WARNING: Schema 'type' is not 'struct'. Actual type: {schema.get('type')}"
                    )

                schema_fields_info = {
                    f.get("field", f.get("name")): f for f in schema.get("fields", [])
                }

                # Validate "field" vs "name" if "name" is found
                for f_def in schema.get("fields", []):
                    if "name" in f_def and "field" not in f_def:
                        print(
                            f"  CRITICAL ERROR: Schema field definition uses 'name' ('{f_def['name']}') instead of 'field'. This is likely the cause of your issue."
                        )
                        print(
                            f"  The problematic field definition: {json.dumps(f_def)}"
                        )
                        break  # Stop checking further schema fields for this error type
                else:  # This else runs if the loop completes without 'break'
                    print("  Schema fields use 'field' key correctly.")

                # Check if payload fields match schema fields
                payload_keys = set(payload.keys())
                schema_field_names = set(schema_fields_info.keys())

                missing_in_payload = schema_field_names - payload_keys
                extra_in_payload = payload_keys - schema_field_names

                if missing_in_payload:
                    for field_name in missing_in_payload:
                        field_def = schema_fields_info.get(field_name, {})
                        if not field_def.get("optional", False):
                            print(
                                f"  ERROR: Required field '{field_name}' is missing from payload."
                            )
                        else:
                            print(
                                f"  INFO: Optional field '{field_name}' is missing from payload."
                            )

                if extra_in_payload:
                    print(
                        f"  WARNING: Extra fields in payload not defined in schema: {extra_in_payload}"
                    )

                # Basic type checking (can be expanded)
                for field_name, field_def in schema_fields_info.items():
                    if field_name in payload:
                        payload_value = payload[field_name]
                        schema_type = field_def.get("type")

                        if schema_type == "string" and not isinstance(
                            payload_value, str
                        ):
                            print(
                                f"  WARNING: Field '{field_name}' (type '{schema_type}') has non-string value: '{payload_value}' (type {type(payload_value).__name__})"
                            )
                        elif schema_type == "int" and not isinstance(
                            payload_value, int
                        ):
                            print(
                                f"  WARNING: Field '{field_name}' (type '{schema_type}') has non-integer value: '{payload_value}' (type {type(payload_value).__name__})"
                            )
                        elif schema_type == "double" and not (
                            isinstance(payload_value, (int, float))
                        ):
                            # Python floats cover Kafka Connect 'double'
                            print(
                                f"  WARNING: Field '{field_name}' (type '{schema_type}') has non-numeric value: '{payload_value}' (type {type(payload_value).__name__})"
                            )
                        # Add more type checks as needed (e.g., boolean, array, map, bytes)

            else:
                print(
                    "Message detected as plain JSON (does not have 'schema' and 'payload' fields)."
                )
                print(
                    "If your consumer/connector expects 'schemas.enable=true', this is likely the issue."
                )
                print(
                    "Consider setting 'value.converter.schemas.enable=false' if this is intended."
                )

            print(f"  Successfully processed message from offset {msg.offset()}")

        except json.JSONDecodeError as e:
            # Message value is not valid JSON
            print(
                f"  ERROR: Could not decode message value as JSON. Message value might be malformed or not JSON."
            )
            print(f"  Decoding error: {e}")
            print(
                f"  Problematic raw value (first 200 chars): {msg_value_str[:200]}..."
            )
        except UnicodeDecodeError:
            # Message value is not valid UTF-8
            print(
                f"  ERROR: Could not decode message value as UTF-8. It might be binary data or a different encoding."
            )
            print(f"  Raw bytes (first 50): {msg_value_bytes[:50]}...")
        except Exception as e:
            # Catch any other unexpected errors during processing
            print(f"  An unexpected error occurred during message processing: {e}")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
    print("Consumer closed.")
