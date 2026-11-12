import json
import os
from pathlib import Path


_CATEGORY_CONFIG = json.loads(
    (Path(__file__).resolve().parents[1] / 'settings' / 'filters.json').read_text(
        encoding='utf-8-sig'
    )
)
CATEGORIES = [item['name'] for item in _CATEGORY_CONFIG['categories']]
CATEGORY_IDS = {item['name']: item['id'] for item in _CATEGORY_CONFIG['categories']}

_SCHEMA = {
    'type': 'object',
    'properties': {
        'primary_category': {'type': 'string', 'enum': CATEGORIES},
        'secondary_categories': {
            'type': 'array',
            'items': {'type': 'string', 'enum': CATEGORIES},
            'maxItems': 2,
        },
        'confidence': {'type': 'number', 'minimum': 0, 'maximum': 1},
    },
    'required': ['primary_category', 'secondary_categories', 'confidence'],
    'additionalProperties': False,
}


def classification_model():
    return os.environ.get('OPENAI_CLASSIFICATION_MODEL', 'gpt-5.6-luna')


def _client():
    from openai import OpenAI
    if not os.environ.get('OPENAI_API_KEY'):
        raise RuntimeError('OPENAI_API_KEY is not set')
    return OpenAI()


def _validate(result):
    primary = result.get('primary_category')
    secondary = result.get('secondary_categories')
    confidence = result.get('confidence')
    if primary not in CATEGORIES:
        raise ValueError('Invalid primary_category')
    if not isinstance(secondary, list) or len(secondary) > 2:
        raise ValueError('secondary_categories must contain 0 to 2 items')
    if any(category not in CATEGORIES for category in secondary):
        raise ValueError('Invalid secondary category')
    if len(set(secondary)) != len(secondary) or primary in secondary:
        raise ValueError('Duplicate or primary secondary category')
    if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        raise ValueError('confidence must be between 0 and 1')
    return {
        'primary_category': primary,
        'secondary_categories': secondary,
        'confidence': float(confidence),
    }


def classify_article(article):
    title = article.get('title', '')
    content = article.get('content') or article.get('description') or ''
    response = _client().responses.create(
        model=classification_model(),
        store=False,
        instructions=(
            'Classify this legal news article using only the supplied category list. '
            'Return exactly one primary category, zero to two distinct secondary categories, '
            'and confidence from 0 to 1. Use \uAE30\uD0C0 when no category fits. Do not explain.'
        ),
        input=f'Title:\n{title}\n\nArticle text:\n{content}',
        text={
            'format': {
                'type': 'json_schema',
                'name': 'legal_news_category',
                'strict': True,
                'schema': _SCHEMA,
            }
        },
    )
    if not response.output_text:
        raise ValueError('OpenAI returned an empty classification')
    return _validate(json.loads(response.output_text))


def openai_categorize(article, _config=None):
    result = dict(article)
    classification = classify_article(article)
    primary = classification['primary_category']
    secondary = classification['secondary_categories']
    result.update(classification)
    result['categories'] = [
        {
            'id': CATEGORY_IDS[category],
            'name': category,
            'category_id': CATEGORY_IDS[category],
            'category': category,
            'role': 'primary' if index == 0 else 'secondary',
            'source': 'openai',
            'confidence': classification['confidence'],
        }
        for index, category in enumerate([primary, *secondary])
    ]
    result['category_candidates'] = []
    result['article_types'] = result.get('article_types', [])
    result['filter_status'] = 'categorized'
    return result
