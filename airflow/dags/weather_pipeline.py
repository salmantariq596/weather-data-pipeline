"""
Airflow DAG for Weather ETL Pipeline
Schedules the full ETL pipeline to run every 6 hours
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from etl_full import run_pipeline

# DAG configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='Weather data ETL pipeline for Bahawalpur',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'etl', 'bahawalpur'],
) as dag:

    # Task 1: Run the ETL pipeline
    run_etl = PythonOperator(
        task_id='run_weather_etl',
        python_callable=run_pipeline,
    )

    # Task 2: Data quality check
    check_quality = BashOperator(
        task_id='check_data_quality',
        bash_command="""
        psql -h postgres -U weather_user -d weather_db -c 
            "SELECT COUNT(*) as total_records, 
                    AVG(data_quality_score) as avg_quality,
                    COUNT(*) FILTER (WHERE data_quality_score < 0.5) as low_quality_count
             FROM stg_weather_current 
             WHERE fetched_at >= NOW() - INTERVAL '6 hours';"
        """,
        env={'PGPASSWORD': 'weather_pass'}
    )

    # Task 3: Refresh daily mart
    refresh_mart = BashOperator(
        task_id='refresh_daily_mart',
        bash_command="""
        psql -h postgres -U weather_user -d weather_db -c 
            "SELECT build_daily_mart();" || true
        """,
        env={'PGPASSWORD': 'weather_pass'}
    )

    # Task dependencies
    run_etl >> check_quality >> refresh_mart
