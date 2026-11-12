CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS news_articles (
    id BIGSERIAL PRIMARY KEY,

    document_id VARCHAR(64) NOT NULL UNIQUE,
    event_id VARCHAR(64) NOT NULL,

    title TEXT NOT NULL,
    description TEXT,
    source VARCHAR(100) NOT NULL,
    url TEXT NOT NULL,

    published_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL,

    legal_keywords JSONB NOT NULL DEFAULT '[]'::jsonb,
    categories JSONB NOT NULL DEFAULT '[]'::jsonb,
    category_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    article_types JSONB NOT NULL DEFAULT '[]'::jsonb,

    filter_status VARCHAR(30) NOT NULL DEFAULT 'raw',
    filter_version VARCHAR(64),

    kafka_sent_at TIMESTAMPTZ,

    embedding VECTOR(1024),
    embedding_model TEXT,
    embedding_status TEXT,
    embedding_error TEXT,
    embedded_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_news_source
    ON news_articles(source);

CREATE INDEX IF NOT EXISTS idx_news_published_at
    ON news_articles(published_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_collected_at
    ON news_articles(collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_filter_status
    ON news_articles(filter_status);

CREATE INDEX IF NOT EXISTS idx_news_categories
    ON news_articles USING GIN(categories);

CREATE TABLE IF NOT EXISTS kafka_outbox (
    event_id VARCHAR(64) PRIMARY KEY,
    document_id VARCHAR(64) NOT NULL,
    topic VARCHAR(100) NOT NULL DEFAULT 'raw-news',
    payload JSONB NOT NULL,

    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_kafka_outbox_article
        FOREIGN KEY (document_id)
        REFERENCES news_articles(document_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_kafka_outbox_unsent
    ON kafka_outbox(created_at)
    WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS news_processing_failures (
    id BIGSERIAL PRIMARY KEY,
    failure_id VARCHAR(200) NOT NULL UNIQUE,
    input_topic VARCHAR(100),
    error_message TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS news_feed_logs (
    id BIGSERIAL PRIMARY KEY,
    feed_url TEXT NOT NULL,
    source VARCHAR(100),
    status VARCHAR(30) NOT NULL,
    collected_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
