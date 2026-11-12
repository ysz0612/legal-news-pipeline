from common.store import connect


def main():
    db = connect()
    try:
        with db.transaction():
            result = db.execute(
                "UPDATE news_articles SET embedding_status='PENDING', embedding_error=NULL "
                "WHERE filter_status='categorized' AND (embedding_status IS DISTINCT FROM 'COMPLETED' "
                "OR embedding_model IS DISTINCT FROM %s)",
                ('BAAI/bge-m3',),
            )
        print('Embedding pending marked:', result.rowcount, flush=True)
    finally:
        db.close()


if __name__ == '__main__':
    main()
