import os,psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase
load_dotenv(); p=dict(host='127.0.0.1',port=55432,user='postgres',password=os.getenv('POSTGRES_PASSWORD','1234')); c=psycopg.connect(dbname='law_db',**p); print('NEWS TOTAL:',c.execute('SELECT count(*) FROM news_articles').fetchone()[0]); print('LEGAL RELATIONS TOTAL:',c.execute('SELECT count(*) FROM news_legal_relations').fetchone()[0]); print('STATUS:',c.execute('SELECT filter_status,count(*) FROM news_articles GROUP BY filter_status').fetchall()); c.close(); d=GraphDatabase.driver(os.getenv('NEO4J_URI','bolt://127.0.0.1:7687'),auth=('neo4j',os.getenv('NEO4J_PASSWORD'))); d.verify_connectivity()
with d.session() as s:
    for label,q in [('NEO4J NEWS','MATCH (n:News) RETURN count(n)'),('NEO4J RELATED_TO','MATCH ()-[r:RELATED_TO]->() RETURN count(r)'),('NEO4J CATEGORY','MATCH (c:Category) RETURN count(c)'),('NEO4J BELONGS_TO','MATCH ()-[r:BELONGS_TO]->() RETURN count(r)')]: print(label+':',s.run(q).single()[0])
d.close()
