from common.settings import filters
from common.store import digest, encode, now


def normalize_article(article: dict) -> dict:
    if not isinstance(article, dict):
        raise ValueError('article must be an object')
    result = dict(article)  # preserve metadata instead of silently dropping it
    for name in ('title', 'url', 'source'):
        if not isinstance(result.get(name), str) or not result[name].strip():
            raise ValueError(f'Missing required string: {name}')
    if result['source'] not in {p['name'] for p in filters()['publishers']}:
        raise ValueError('Unknown publisher')
    if not result['url'].startswith(('https://', 'http://')):
        raise ValueError('Invalid article link')
    result['content'] = result.get('content') or ''
    if not isinstance(result['content'], str):
        raise ValueError('content must be string')
    result.setdefault('content_type', 'unknown')
    result.setdefault('published_at', None)
    result.setdefault('collected_at', now())
    if not isinstance(result['collected_at'], str):
        raise ValueError('collected_at must be an ISO timestamp string')
    result.setdefault('document_id', digest(result['url']))
    stable = {k: result.get(k) for k in ('document_id','title','content','source','published_at','content_type')}
    result.setdefault('event_id', digest(encode(stable)))
    result.setdefault('schema_version', 1)
    return result
