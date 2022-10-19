"""
Адаптиранный пайплайн для текущей задачи!
"""
import json
import logging

import pandas as pd
import requests

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

from utils import simple_retry

POSTGRES_CONN_ID = '1_postgresql'
API_CONN_ID = '1_api'

http_conn = HttpHook.get_connection(API_CONN_ID)
api_endpoint = http_conn.host
api_token = http_conn.extra_dejson.get('X-API-KEY')
nickname = http_conn.extra_dejson.get('X-Nickname')
cohort = http_conn.extra_dejson.get('X-Cohort')

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_token,
    'Content-Type': 'application/x-www-form-urlencoded',
}

s3_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{INCREMENT_ID}/{FILE_NAME}'

business_dt = '{{ ds }}'
date_last_success = '{{ prev_start_date_success }}'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# TASKS
def upload_report(ti: TaskInstance, header: dict, pg_table: str,
                  file_name: str, start_at: str,
                  s3_file_url: str) -> None:
    """
    Функция обрабатывает два случая: первичный отчёт и инкремент.

    Args:
        ti: Context.
        header: Заголовки запроса.
        pg_table: Табоица-цель загрузки.
        file_name: Название импортируемого файла.
        start_at: Дата исполнения таска.
        s3_file_url: URL до файла.
    """
    # Проверка на тип загружаемых данных
    report_ids = ti.xcom_pull(key='increment_id', task_ids=['t_get_increment'])
    report_id = report_ids[0]

    s3_file_url = s3_file_url.replace('{INCREMENT_ID}', report_id)
    s3_file_url = s3_file_url.replace('{COHORT}', header['X-Cohort'])
    s3_file_url = s3_file_url.replace('{NICKNAME}', header['X-Nickname'])
    s3_file_url = s3_file_url.replace('{FILE_NAME}', file_name)

    local_file_name = start_at.replace('-', '') + '_' + file_name

    logging.info(f"Load file from {s3_file_url}")
    response = simple_retry(
        requests.get, {'url': s3_file_url},
    )

    open(local_file_name, 'wb').write(response.content)

    df = pd.read_csv(local_file_name)
    cols = ','.join(list(df.columns))

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"truncate {pg_table};")
            conn.commit()

            if df.shape[0] > 0:
                insert_cr = f"INSERT INTO {pg_table} ({cols}) VALUES " + "{cr_val};"
                i = 0
                step = int(df.shape[0] / 100)
                logging.info(f"{pg_table}, step-{step}")
                while i <= df.shape[0]:

                    if step == 0:
                        step = df.shape[0]
                    cr_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                    cur.execute(insert_cr.replace('{cr_val}', cr_val))

                    conn.commit()

                    i += step + 1


def get_increment(start_at: str, ti: TaskInstance, header: dict, endpoint: str) -> str:
    """Получения данных за те даты, которые не вошли в основной отчёт.

    Args:
        start_at: дата запуска инкремента.
        ti: Context.
        header: Заголовки запроса.
        endpoint: Точка входа запросов.

    Returns:
        str: Выбрираем группу задач инкремента или идём дальше.
    """
    start_at = start_at
    logging.info('Making request get_increment')
    report_id = Variable.get('report_id')

    url = f"https://{endpoint}/get_increment?report_id={report_id}&date={str(start_at)}T00:00:00"
    logging.info(f"Skip. {url}")
    response = simple_retry(
        requests.get,
        {
            'url': url,
            'headers': header,
        })
    # Пропускаем ситуацию когда дата меньше даты формирования отчёта
    logging.info(f"Skip. {response.content}")
    response_content = json.loads(response.content)
    status = response_content['status']
    if status == 'NOT FOUND':
        logging.info(f"Skip. {response_content}")
        raise AirflowFailException


    response.raise_for_status()


    increment_id = response_content['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    logging.info(f"increment_id={increment_id}")

# DAG#
with DAG(
    'increment-report-load',
    default_args=args,
    description='Задача',
    start_date=datetime.today() - timedelta(days=8),
    catchup=True,
    max_active_runs=1,
    schedule_interval='@daily',
) as dag:
    with TaskGroup('group_update_tables') as group_update_tables:
        dimension_tasks = list()
        for i in ['d_city', 'd_item', 'd_customer', 'update_d_calendar', ]:
            dimension_tasks.append(PostgresOperator(
                task_id=f'load_{i}',
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=f'sql/mart.{i}.sql',
                dag=dag
            )
            )
        update_f_sales_incr = PostgresOperator(
            task_id='update_f_sales_incr',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='sql/mart.f_sales_incr.sql',
            parameters={'date': {business_dt}},
        )
        dimension_tasks >> update_f_sales_incr
        dimension_tasks = list()

    with TaskGroup('group_load_data') as group_load_data:
        load_customer_research_inc = PythonOperator(
            task_id='load_customer_research_inc',
            python_callable=upload_report,
            op_kwargs={
                'file_name': 'customer_research_inc.csv',
                'pg_table': 'staging.customer_research',
                'header': headers,
                'start_at': business_dt,
                's3_file_url': s3_url,
            },
            provide_context=True,
        )
        load_user_activity_log_inc = PythonOperator(
            task_id='load_user_activity_log_inc',
            python_callable=upload_report,
            op_kwargs={
                'file_name': 'user_activity_log_inc.csv',
                'pg_table': 'staging.user_activity_log',
                'header': headers,
                'start_at': business_dt,
                's3_file_url': s3_url,
            },
            provide_context=True,
        )
        load_user_order_log_inc = PythonOperator(
            task_id='load_user_order_log_inc',
            python_callable=upload_report,
            op_kwargs={
                'file_name': 'user_orders_log_inc.csv',
                'pg_table': 'staging.user_order_log',
                'header': headers,
                'start_at': business_dt,
                's3_file_url': s3_url,
            },
            provide_context=True,
        )

        [load_customer_research_inc, load_user_activity_log_inc, load_user_order_log_inc]

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    t_get_increment = PythonOperator(
        task_id='t_get_increment',
        python_callable=get_increment,
        op_kwargs={
            'start_at': business_dt,
            'header': headers,
            'endpoint': api_endpoint,
        },
    )
    start >> t_get_increment >> group_load_data >> group_update_tables >> end
