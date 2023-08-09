from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

from krx.crawler import KRXCrawler


dag = DAG(
    dag_id="daily_krx_index",
    description="Download daily Korean stock index",
    start_date=pendulum.today("UTC").add(days=-10),
    schedule_interval="35 6 * * 1-5",
    template_searchpath="/tmp",
    max_active_runs=1,
)


def _get_data(market_date, market_code, output_path):
    krx_crawler = KRXCrawler(market_date)
    krx_data = krx_crawler.run(market_code)
    krx_data.to_csv(output_path)


get_kospi_index = PythonOperator(
    task_id="get_kospi_index",
    python_callable=_get_data,
    op_kwargs={
        "market_date": "{{ logical_date.strftime('%Y%m%d') }}",
        "market_code": "02",
        "output_path": "/tmp/daily_kospi_index.csv",
    },
    dag=dag,
)

get_kosdaq_index = PythonOperator(
    task_id="get_kosdaq_index",
    python_callable=_get_data,
    op_kwargs={
        "market_date": "{{ logical_date.strftime('%Y%m%d') }}",
        "market_code": "03",
        "output_path": "/tmp/daily_kosdaq_index.csv",
    },
    dag=dag,
)
