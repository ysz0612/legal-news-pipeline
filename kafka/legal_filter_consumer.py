from common.rules import legal_filter
from kafka.config import TOPIC_RAW, TOPIC_LEGAL
from kafka.runner import run
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--idle-timeout', type=int, default=None)
    args = parser.parse_args()
    run('legal-filter', TOPIC_RAW, TOPIC_LEGAL, legal_filter, args.idle_timeout)
