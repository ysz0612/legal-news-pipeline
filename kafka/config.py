import os
from common.settings import DATA  # loads local .env first

BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_RAW = 'raw-news'
TOPIC_LEGAL = 'legal-news'
TOPIC_CATEGORY = 'categorized-news'
TOPIC_FAILED = 'failed-news'
