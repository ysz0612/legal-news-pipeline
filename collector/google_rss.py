import gzip
import html
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from common.settings import DATA
from common.store import digest, encode, now, save_raw
from kafka.schemas import normalize_article


def text_only(value):
    return re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]*>', ' ', value or ''))).strip()


def publisher_for(source_url, publishers):
    try:
        parts = urlsplit(source_url)
        if parts.scheme not in ('https', 'http'):
            return None
        host = (parts.hostname or '').lower().rstrip('.')
    except ValueError:
        return None
    for p in publishers:
        if any(host == d or host.endswith('.' + d) for d in p['domains']):
            return p['name']
    return None


def meaningful_description(description, title, source):
    # Google RSS often repeats the title/source or supplies lists of other headlines.
    if re.search(r'<(?:ol|ul)\b', description, re.I):
        return ''
    plain = text_only(description)
    compact = lambda s: re.sub(r'[\s\W_]+', '', unicodedata.normalize('NFKC', s)).casefold()
    if compact(plain) in ('', compact(title), compact(title + source)):
        return ''
    return plain


def parse_feed(xml_bytes, config, query):
    root = ET.fromstring(xml_bytes)
    if root.tag != 'rss' or root.find('channel') is None:
        raise ValueError('Response is not RSS (possibly consent/error HTML)')
    articles, skipped = [], 0
    for item in root.findall('./channel/item'):
        source = item.find('source')
        source_url = source.get('url', '') if source is not None else ''
        publisher = publisher_for(source_url, config['publishers'])
        link = item.findtext('link') or ''
        title = text_only(item.findtext('title'))
        if not publisher or not title or not link.startswith(('http://', 'https://')):
            skipped += 1
            continue
        source_text = text_only(source.text if source is not None else '')
        for label in (source_text, publisher):
            if label and title.endswith(' - ' + label):
                title = title[:-(len(label) + 3)]
                break
        raw_description = item.findtext('description') or ''
        content = meaningful_description(raw_description, title, source_text)
        try:
            published = parsedate_to_datetime(item.findtext('pubDate')).astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError, OverflowError):
            published = None
        is_google = (urlsplit(link).hostname or '').lower() == 'news.google.com'
        article = {'title': title, 'content': content,
                   'content_type': 'rss_description' if content else 'title_only',
                   'url': link, 'url_type': 'google_news_redirect' if is_google else 'publisher_link',
                   'original_url': None if is_google else link,
                   'source': publisher, 'source_home_url': source_url,
                   'published_at': published, 'collected_at': now(),
                   'rss_guid': item.findtext('guid'), 'raw_description': raw_description,
                   'collection_query': query, 'body': None, 'body_status': 'not_collected',
                   'document_id': digest(publisher + '|' + (item.findtext('guid') or link))}
        articles.append(normalize_article(article))
    return articles, skipped


def query_plan(config, mode, publishers=None, days=7, day=None):
    selected = [p for p in config['publishers'] if not publishers or p['name'] in publishers]
    if publishers and set(publishers) - {p['name'] for p in selected}:
        raise ValueError('Unknown publisher; check settings/filters.json')
    if day:
        start = datetime.strptime(day, '%Y-%m-%d').date()
        window = f'after:{start.isoformat()} before:{(start + timedelta(days=1)).isoformat()}'
    else:
        window = f'when:{days}d'
    keywords = [''] if mode == 'broad' else config['first_keywords']
    # Keyword-first interleaving prevents low max-feeds from serving only one publisher.
    return [f'site:{domain} {keyword} {window}'.strip()
            for keyword in keywords for p in selected for domain in p['domains']]


def fetch(url):
    request = Request(url, headers={'User-Agent': 'LegalNewsResearch/1.0 RSS collector', 'Accept': 'application/rss+xml,application/xml'})
    for attempt in range(3):
        try:
            with urlopen(request, timeout=25) as response:
                data = response.read(8_000_001)
                if len(data) > 8_000_000:
                    raise ValueError('RSS response too large')
                return data
        except HTTPError as exc:
            if exc.code in (403, 429):
                raise RuntimeError(f'RSS access/rate limit {exc.code}. Stop and retry later; no bypass.') from None
            if exc.code < 500 or attempt == 2:
                raise
        except (URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)


def collect(db, config, args):
    plan = query_plan(config, args.mode, args.publisher, args.days, args.date)
    selected = plan[args.offset:args.offset + args.max_feeds]
    if not selected:
        raise ValueError('No queries selected; reduce --offset')
    folder = DATA / 'rss_raw'
    folder.mkdir(parents=True, exist_ok=True)
    failures = 0
    for position, query in enumerate(selected, args.offset):
        url = 'https://news.google.com/rss/search?' + urlencode({'q': query, 'hl': 'ko', 'gl': 'KR', 'ceid': 'KR:ko'})
        status, count, error = 'ok', 0, ''
        try:
            xml = fetch(url)
            articles, skipped = parse_feed(xml, config, query)
            filename = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f') + '_' + digest(query)[:12] + '.xml.gz'
            with gzip.open(folder / filename, 'wb') as output:
                output.write(xml)
            count = len(articles)
            changed = sum(save_raw(db, a) for a in articles)
            print(f'[{position+1}/{len(plan)}] {query}: source-verified={count}, new/changed={changed}, rejected-source/item={skipped}', flush=True)
        except (URLError, TimeoutError, ValueError, ET.ParseError) as exc:
            status, error = 'failed', str(exc)[:500]
            failures += 1
            print('Feed failed:', query, error, flush=True)
        except RuntimeError as exc:
            status, error = 'blocked', str(exc)
            raise
        finally:
            with db.transaction():
                db.execute(
                    'INSERT INTO news_feed_logs '
                    '(feed_url, source, status, collected_count, error_message) '
                    'VALUES (%s, %s, %s, %s, %s)',
                    (url, None, status, count, error or None),
                )
        time.sleep(args.delay)
    if args.offset + len(selected) < len(plan):
        print('Remaining queries. Continue same options with --offset', args.offset + len(selected))
    if failures:
        raise RuntimeError(f'{failures} feed(s) failed. Successful records retained; rerun or inspect news_feed_logs.')
