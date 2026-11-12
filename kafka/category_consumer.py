from common.openai_classifier import openai_categorize
from kafka.config import TOPIC_LEGAL, TOPIC_CATEGORY
from kafka.runner import run
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--idle-timeout', type=int, default=None)
    args = parser.parse_args()
    run('category-filter', TOPIC_LEGAL, TOPIC_CATEGORY, openai_categorize, args.idle_timeout)
