import re
import unicodedata
from common.store import digest, encode


def hit(text, word):
    text = unicodedata.normalize('NFKC', text).casefold()
    word = unicodedata.normalize('NFKC', word).casefold()
    if word.isascii() and word.isalpha():
        return re.search(r'(?<![a-z0-9])' + re.escape(word) + r'(?![a-z0-9])', text) is not None
    return re.sub(r'\s+', '', word) in re.sub(r'\s+', '', text)


def evidence(text, words):
    found = [w for w in words if hit(text, w)]
    # 계약해지/계약, 개정안/개정: count longer keyword only inside a group.
    return [w for w in found if not any(w != other and w in other for other in found)]


def legal_filter(article, config):
    result = dict(article)
    text = result['title'] + '\n' + result['content']
    result['legal_keywords'] = [w for w in config['first_keywords'] if hit(text, w)]
    result['filter_scope'] = 'title_and_' + result['content_type']
    result['filter_version'] = digest(encode(config))
    result['filter_status'] = 'legal' if result['legal_keywords'] else 'excluded_stage1'
    return result


def categorize(article, config):
    result = dict(article)
    title, content = result['title'], result['content']
    text = title + '\n' + content
    labels, candidates = [], []
    for rule in config['categories']:
        strong = evidence(text, rule['strong'])
        weak = evidence(text, rule['weak'])
        if not strong and not weak:
            continue
        entry = {'category_id': rule['id'], 'category': rule['name'], 'group': rule['group'],
                 'keywords': strong + weak, 'strong_matches': strong, 'weak_matches': weak}
        if strong or len(weak) >= 2:
            labels.append(entry)
        else:
            candidates.append(entry)
    result['categories'] = labels
    result['category_candidates'] = candidates
    result['primary_category'] = None  # do not imply a legal judgement by picking first label
    result['article_types'] = [name for name, words in config['article_types'].items() if evidence(text, words)]
    result['filter_status'] = 'categorized' if labels else 'unclassified'
    return result
