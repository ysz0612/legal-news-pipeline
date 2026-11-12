import os
import time

import torch

from common.embedding import embed_rows, eligible_rows, ensure_schema, load_model, model_name
from common.store import connect


LOCK_KEY = 891274631
POLL_SECONDS = int(os.environ.get('EMBEDDING_WORKER_POLL_SECONDS', '60'))


def main():
    db = connect()
    lock = db.execute('SELECT pg_try_advisory_lock(%s) AS acquired', (LOCK_KEY,)).fetchone()['acquired']
    db.commit()
    if not lock:
        print('Another embedding worker owns the advisory lock; exiting.', flush=True)
        db.close()
        return
    try:
        model, device = load_model()
        dimension = model.get_embedding_dimension() if hasattr(model, 'get_embedding_dimension') else model.get_sentence_embedding_dimension()
        ensure_schema(db, dimension)
        print(f'Embedding worker ready: model={model_name()} device={device} dimension={dimension}', flush=True)
        while True:
            rows = eligible_rows(db, None, False)
            db.commit()
            if not rows:
                time.sleep(POLL_SECONDS)
                continue
            for offset in range(0, len(rows), 100):
                chunk = rows[offset:offset + 100]
                embed_rows(db, model, chunk, batch_size=16, commit_size=100)
                db.commit()
                print(f'Worker committed {min(offset + len(chunk), len(rows))}/{len(rows)}', flush=True)
    finally:
        db.execute('SELECT pg_advisory_unlock(%s)', (LOCK_KEY,))
        db.commit()
        db.close()


if __name__ == '__main__':
    main()
