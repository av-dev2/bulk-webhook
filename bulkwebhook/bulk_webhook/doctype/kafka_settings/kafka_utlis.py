import json

import frappe
import bulkwebhook
from kafka import KafkaProducer


def get_kafka_client(settings_name: str):
    """
        Create a KafkaProducer instance for the given settings name.
        args:
            settings_name: Kafka Settings document name
    """
    settings = frappe.get_cached_doc("Kafka Settings", settings_name)

    return KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
        value_serializer=lambda e: serialize_data(e),
        key_serializer=lambda e: serialize_data(e),
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=settings.get_password("api_key"),
        sasl_plain_password=settings.get_password("api_secret"),
    )

def get_kafka_producer(settings_name: str) -> KafkaProducer: 
    """
        Return a KafkaProducer instance for the given settings name. If the producer is already
        created, return the same instance. Otherwise, create a new instance and return it.

        args:
            settings_name: Kafka Settings document name
    """
    if frappe.local.site not in bulkwebhook.PRODUCER_MAP:
        bulkwebhook.PRODUCER_MAP[frappe.local.site] = {}

    if settings_name not in bulkwebhook.PRODUCER_MAP[frappe.local.site]:
        bulkwebhook.PRODUCER_MAP[frappe.local.site][settings_name] = get_kafka_client(
            settings_name
        )

    return bulkwebhook.PRODUCER_MAP[frappe.local.site][settings_name]

def send_kafka(settings_name, topic, key, value):
    """
        Send the given data to kafka for a given topic.
        args:
            settings_name: Kafka Settings document name
            topic: Kafka topic name
            key: Kafka message key
            value: Kafka message value
    """
    producer = get_kafka_producer(settings_name)
    future = (
        producer.send(topic=topic, key=key, value=value)
        .add_callback(on_send_success)
        .add_errback(on_send_error)
    )
    res = future.get(timeout=120)

    return res

# NOTE: The on_send_success function is not working.
def on_send_success(record_metadata):
    frappe.log_error(
        str(
            {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            }
        )
    )

# NOTE: the on_send_error function is not working.
def on_send_error(excp):
    frappe.log_error(str(excp))
    # handle exception

def serialize_data(data):
    """Serialize data to be sent to Kafka"""
    serialized_data = None
    try:
        serialized_data = json.dumps(data).encode("ascii")
    except TypeError:
        try:
            serialized_data = data.SerializeToString()

        except Exception as e:
            frappe.log_error(str(e), frappe.get_traceback())
            frappe.throw(str(e))

    return serialized_data
