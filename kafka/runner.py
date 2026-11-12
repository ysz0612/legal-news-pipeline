import base64
import json
import sys
import time
import argparse
from common.settings import filters
from common.store import connect, encode, now, save_stage, save_final
from psycopg.types.json import Jsonb
from kafka.config import BOOTSTRAP_SERVERS, TOPIC_FAILED
from kafka.producer import send_message
from kafka.schemas import normalize_article


def run(stage, input_topic, output_topic=None, transform=None, idle_timeout=None):
    from confluent_kafka import Consumer
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    config = filters()
    db = connect()
    consumer = Consumer({'bootstrap.servers': BOOTSTRAP_SERVERS, 'group.id': stage + '-v1',
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe([input_topic])
    print(f'{stage}: listening to {input_topic}', flush=True)
    last_message = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                if idle_timeout is not None and time.monotonic() - last_message >= idle_timeout:
                    print(f'{stage}: idle timeout reached ({idle_timeout}s)', flush=True)
                    return
                continue
            last_message = time.monotonic()
            if msg.error():
                raise RuntimeError(str(msg.error()))
            try:
                article = normalize_article(json.loads(msg.value()))
                if transform:
                    article = transform(article, config)
            except (ValueError, TypeError, KeyError) as exc:
                fid = f'{stage}:{msg.topic()}:{msg.partition()}:{msg.offset()}'
                failed = {'failure_id': fid, 'input_topic': input_topic, 'stage': stage,
                          'error': str(exc), 'raw_base64': base64.b64encode(msg.value() or b'').decode()}
                # Disk failure or DLQ failure stops the worker; no offset commit.
                with db.transaction():
                    db.execute(
                        'INSERT INTO news_processing_failures '
                        '(failure_id, input_topic, error_message, payload) '
                        'VALUES (%s, %s, %s, %s) '
                        'ON CONFLICT (failure_id) DO NOTHING',
                        (fid, input_topic, str(exc), Jsonb(failed)),
                    )
                send_message(TOPIC_FAILED, fid, failed)
                consumer.commit(message=msg, asynchronous=False)
                print('Quarantined:', fid, flush=True)
                continue
            if output_topic:
                save_stage(db, stage, article)
                if article['filter_status'] != 'excluded_stage1':
                    send_message(output_topic, article['document_id'], article)
                else:
                    # A keyword miss is only a hand-off to semantic recovery.  Do
                    # not erase previous candidates or treat it as a final reject.
                    # The recovery worker uses this durable checkpoint instead of
                    # sending the record through the OpenAI category topic first.
                    with db.transaction():
                        db.execute("ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS semantic_recovery_status TEXT")
                        db.execute(
                            "UPDATE news_articles "
                            "SET semantic_recovery_status=CASE "
                            "WHEN semantic_recovery_status IN "
                            "('RECOVERED_LEGAL','RECOVERED_CANDIDATE','NON_LEGAL','REVIEW','FAILED') "
                            "THEN semantic_recovery_status ELSE 'PENDING' END, "
                            "updated_at=CURRENT_TIMESTAMP WHERE document_id=%s",
                            (article['document_id'],),
                        )
            else:
                save_final(db, article)
            consumer.commit(message=msg, asynchronous=False)
            print(stage, article['filter_status'], article['source'], article['title'][:70], flush=True)
    except KeyboardInterrupt:
        print('Stopped; committed progress retained.')
    finally:
        consumer.close()
        db.close()
