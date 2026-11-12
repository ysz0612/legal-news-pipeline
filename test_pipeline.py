import unittest

from collector.google_rss import parse_feed, publisher_for, query_plan
from common.rules import categorize, hit, legal_filter
from common.settings import filters
from common.store import encode
from kafka.schemas import normalize_article


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.config = filters()

    def article(self, title, content=''):
        publisher = self.config['publishers'][0]['name']
        domain = self.config['publishers'][0]['domains'][0]
        return normalize_article({'title': title, 'content': content,
                                  'content_type': 'title_only', 'source': publisher,
                                  'url': f'https://{domain}/test', 'is_test': True})

    def classify(self, title, content=''):
        return categorize(legal_filter(self.article(title, content), self.config), self.config)

    def test_config_counts(self):
        self.assertEqual(len(self.config['publishers']), 15)
        self.assertEqual(len(self.config['categories']), 30)
        self.assertEqual(len({x['group'] for x in self.config['categories']}), 10)
        self.assertEqual(len(self.config['first_keywords']), 27)

    def test_filter_and_classification_shape(self):
        article = self.classify(self.config['first_keywords'][0])
        self.assertIn(article['filter_status'], {'categorized', 'unclassified'})
        self.assertIn('legal_keywords', article)
        self.assertIn('categories', article)
        self.assertIn('category_candidates', article)

    def test_spacing_and_ascii_word_boundaries(self):
        self.assertTrue(hit('alpha beta', 'alpha'))
        self.assertFalse(hit('daily mail', 'AI'))
        self.assertTrue(hit('AI article', 'AI'))

    def test_domain_boundary(self):
        domain = self.config['publishers'][0]['domains'][0]
        self.assertIsNone(publisher_for(f'https://{domain}.evil.test', self.config['publishers']))
        self.assertEqual(publisher_for(f'https://www.{domain}', self.config['publishers']),
                         self.config['publishers'][0]['name'])

    def test_parse_rss_and_no_fake_body(self):
        publisher = self.config['publishers'][0]
        xml = f'''<rss><channel><item><title>Test title</title>
        <link>https://news.google.com/rss/articles/test</link><guid>abc123</guid>
        <description>&lt;a href="x"&gt;Test title&lt;/a&gt;</description>
        <source url="https://{publisher['domains'][0]}">{publisher['name']}</source>
        <pubDate>Tue, 08 Sep 2026 10:00:00 +0900</pubDate></item></channel></rss>'''
        articles, skipped = parse_feed(xml.encode(), self.config, 'q')
        self.assertEqual(skipped, 0)
        article = articles[0]
        self.assertEqual(article['content'], '')
        self.assertIsNone(article['body'])
        self.assertIsNone(article['original_url'])
        self.assertEqual(article['published_at'], '2026-09-08T01:00:00+00:00')

    def test_non_rss_rejected(self):
        with self.assertRaises(ValueError):
            parse_feed(b'<html><body>blocked</body></html>', self.config, 'q')

    def test_date_plan(self):
        publisher = self.config['publishers'][0]['name']
        queries = query_plan(self.config, 'expanded', [publisher], day='2026-09-08')
        self.assertEqual(len(queries), len(self.config['first_keywords']))
        self.assertIn('after:2026-09-08 before:2026-09-09', queries[0])

    def test_original_fields_preserved(self):
        article = self.article('test')
        article['categories'] = [{'category': 'preserved'}]
        self.assertEqual(normalize_article(article)['categories'], article['categories'])

    def test_json_encoding_for_postgres_payloads(self):
        encoded = encode(self.classify(self.config['first_keywords'][0]))
        self.assertIn('document_id', encoded)
        self.assertIn('filter_status', encoded)


if __name__ == '__main__':
    unittest.main()
