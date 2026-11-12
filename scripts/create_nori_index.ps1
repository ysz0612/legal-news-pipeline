$ErrorActionPreference = 'Stop'
$index = 'legal-news'
$exists = $true
try { Invoke-RestMethod "http://localhost:9200/$index" | Out-Null } catch { $exists = $false }
if ($exists) {
    Write-Host "Index $index already exists. Existing data was not changed."
    exit 0
}
$mapping = @'
{
  "settings": {"analysis": {"analyzer": {"nori": {"type": "custom", "tokenizer": "nori_tokenizer"}}}},
  "mappings": {"properties": {
    "article_id": {"type": "keyword"}, "title": {"type": "text", "analyzer": "nori"},
    "content": {"type": "text", "analyzer": "nori"}, "source": {"type": "keyword"},
    "url": {"type": "keyword"}, "published_at": {"type": "date"},
    "document_type": {"type": "keyword"}
  }}
}
'@
Invoke-RestMethod -Method Put -Uri "http://localhost:9200/$index" -ContentType 'application/json' -Body $mapping | ConvertTo-Json
Write-Host "Empty Nori index created. Run indexer.py or index_text_only.py before searching."
