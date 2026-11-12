from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT='/opt/law_project'
ENV={'POSTGRES_HOST':'postgres','POSTGRES_PORT':'5432','POSTGRES_DB':'law_db','POSTGRES_USER':'postgres','LEGAL_POSTGRES_HOST':'postgres','LEGAL_POSTGRES_PORT':'5432','LEGAL_POSTGRES_DB':'legal_shared','LEGAL_POSTGRES_USER':'postgres','KAFKA_BOOTSTRAP_SERVERS':'kafka:29092','PYTHONPATH':PROJECT}
ARGS={'owner':'law-project','retries':2,'retry_delay':timedelta(minutes=5)}
with DAG(dag_id='law_daily_pipeline',schedule='0 8 * * *',start_date=pendulum.datetime(2026,1,1,tz='Asia/Seoul'),catchup=False,max_active_runs=1,default_args=ARGS,tags=['law','rss','kafka','nemotron','neo4j']) as dag:
    collect_news=BashOperator(task_id='collect_news',bash_command=f'cd {PROJECT} && python main.py collect --mode broad --days 1 --max-feeds 100 --delay 2',env=ENV,append_env=True)
    publish_raw_news=BashOperator(task_id='publish_raw_news',bash_command=f'cd {PROJECT} && python main.py publish',env=ENV,append_env=True)
    legal_filter=BashOperator(task_id='legal_filter',bash_command=f'cd {PROJECT} && python kafka/legal_filter_consumer.py --idle-timeout 30',env=ENV,append_env=True)
    category_filter=BashOperator(task_id='category_filter',bash_command=f'cd {PROJECT} && python kafka/category_consumer.py --idle-timeout 30',env=ENV,append_env=True)
    store_categorized_news=BashOperator(task_id='store_categorized_news',bash_command=f'cd {PROJECT} && python kafka/storage_consumer.py --idle-timeout 30',env=ENV,append_env=True)
    semantic_recovery=BashOperator(task_id='semantic_recovery_new_rejects',bash_command=f'cd {PROJECT} && python pipeline/semantic_recovery_new.py',env=ENV,append_env=True,retries=0)
    merge_legal_news=EmptyOperator(task_id='merge_legal_news',trigger_rule='none_failed_min_one_success')
    embed_new_legal_news=BashOperator(task_id='embed_new_legal_news',bash_command=f'cd {PROJECT} && python main.py embed-news-nemotron --batch-size 32',env=ENV,append_env=True)
    link_new_news_to_legal=BashOperator(task_id='link_new_news_to_legal',bash_command=f'cd {PROJECT} && python main.py link-news-to-legal --top-k 5',env=ENV,append_env=True)
    sync_new_news_to_neo4j=BashOperator(task_id='sync_new_news_to_neo4j',bash_command=f'cd {PROJECT} && python pipeline/incremental_neo4j_sync.py',env=ENV,append_env=True)
    validate_daily_pipeline=BashOperator(task_id='validate_daily_pipeline',bash_command=f'cd {PROJECT} && python pipeline/validate_daily_pipeline.py',env=ENV,append_env=True)
    collect_news >> publish_raw_news >> legal_filter
    legal_filter >> [category_filter,semantic_recovery]
    category_filter >> store_categorized_news
    [store_categorized_news,semantic_recovery] >> merge_legal_news
    merge_legal_news >> embed_new_legal_news >> link_new_news_to_legal >> sync_new_news_to_neo4j >> validate_daily_pipeline
