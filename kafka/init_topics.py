from kafka.config import BOOTSTRAP_SERVERS, TOPIC_RAW, TOPIC_LEGAL, TOPIC_CATEGORY, TOPIC_FAILED


def main():
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaException, KafkaError
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    topics = [TOPIC_RAW, TOPIC_LEGAL, TOPIC_CATEGORY, TOPIC_FAILED]
    for name, future in admin.create_topics([NewTopic(t, 3, 1) for t in topics], request_timeout=30).items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        print('Ready:', name)


if __name__ == '__main__':
    main()
