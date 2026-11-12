from common.store import encode
from kafka.config import BOOTSTRAP_SERVERS, TOPIC_RAW
from kafka.schemas import normalize_article

_producer = None


def get_producer():
    global _producer
    if _producer is None:
        from confluent_kafka import Producer
        _producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS,
                              'enable.idempotence': True, 'acks': 'all',
                              'delivery.timeout.ms': 30000})
    return _producer


def send_message(topic, key, value):
    producer = get_producer()
    payload = encode(value).encode('utf-8')
    if len(payload) > 800_000:
        raise ValueError('Message exceeds 800KB; store large content separately first')
    reports = []
    producer.produce(topic, key=key.encode('utf-8'), value=payload,
                     on_delivery=lambda error, message: reports.append(error))
    remaining = producer.flush(35)
    if remaining or not reports or reports[0] is not None:
        raise RuntimeError(f'Delivery not confirmed: {topic}. Source offset/outbox is unchanged.')


def send_article(article: dict):
    article = normalize_article(article)
    send_message(TOPIC_RAW, article['document_id'], article)


def flush():
    if get_producer().flush(35):
        raise RuntimeError('Kafka messages remain undelivered')
