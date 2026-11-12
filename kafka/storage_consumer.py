from kafka.config import TOPIC_CATEGORY
from kafka.runner import run
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--idle-timeout', type=int, default=None)
    args = parser.parse_args()
    run('article-storage', TOPIC_CATEGORY, idle_timeout=args.idle_timeout)
