$ErrorActionPreference = 'Stop'
Write-Host '--- Docker Compose ---'
docker compose ps
Write-Host '--- PostgreSQL ---'
docker exec legal-news-postgres pg_isready -U legal_news -d legal_news
docker exec legal-news-postgres psql -U legal_news -d legal_news -c "SELECT COUNT(*) AS articles FROM articles; SELECT COUNT(*) AS chunks FROM article_chunks;"
Write-Host '--- Elasticsearch ---'
Invoke-RestMethod http://localhost:9200/_cluster/health | ConvertTo-Json
Invoke-RestMethod http://localhost:9200/_cat/plugins?v
Invoke-RestMethod http://localhost:9200/legal-news/_count | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:9200/legal-news/_analyze -ContentType 'application/json' -Body '{"analyzer":"nori","text":"개인정보 유출"}' | ConvertTo-Json
