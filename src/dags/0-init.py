"""
Адаптиранный пайплайн для текущей задачи!
"""
import json
import logging
import time

import pandas as pd
import requests

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.utils.task_group import TaskGroup

from utils import simple_retry
from airflow.models import Variable

POSTGRES_CONN_ID='1_postgresql'
API_CONN_ID='1_api'

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

s3_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{REPORT_ID}/{FILE_NAME}'

business_dt = '{{ ds }}'
date_last_success = '{{ prev_start_date_success }}'

args = {
    'owner': 'ragim',
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
}

def generate_report(ti: TaskInstance, header: dict, endpoint: str) -> None:
    """Запрос на генерацию очёта.

    Args:
        ti: Context.
        header: Заголовок запроса.
        endpoint: Точка входа запросов.
    """
    logging.info('Making request generate_report')

    url = 'https://{0}/generate_report'.format(endpoint)
    response = simple_retry(
        requests.post,
        {
            'url': url,
            'headers': header,
        },
    )
    response.raise_for_status()

    response_dict = json.loads(response.content)
    ti.xcom_push(key='task_id', value=response_dict['task_id'])


def get_report(ti: TaskInstance, header: dict, endpoint: str) -> None:
    """Получение id отчёта после того, как он будет сформирован на сервере.

    Args:
        ti: Context.
        header: Заголовок запроса.
        endpoint: Точка входа запросов.

    Raises:
        TimeoutError: Нет ответа от сервреа.
    """
    logging.info('Making request get_report')

    task_id = ti.xcom_pull(key='task_id')
    report_id = None
    url = 'https://{endpoint}/get_report?task_id={task_id}'.format(endpoint=endpoint, task_id=task_id)

    for _ in range(20):

        response = simple_retry(
            requests.get,
            {
                'url': url,
                'headers': header,
            },
        )

        response.raise_for_status()
        logging.info(f"Response is {response.content}")
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    Variable.set('report_id', report_id)
    ti.xcom_push(key='report_id', value=report_id)


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
    report_ids = ti.xcom_pull(key='report_id', task_ids=['t_get_report'])
    report_id = report_ids[0]

    s3_file_url = s3_file_url.replace('{REPORT_ID}', report_id)
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

                    cr_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                    cur.execute(insert_cr.replace('{cr_val}', cr_val))

                    conn.commit()

                    i += step + 1

# DAG#
with DAG(
    'init-report-load',
    default_args=args,
    description='Initialize report dag',
    start_date=datetime.today(),
    schedule_interval='@once',
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    with TaskGroup('group_update_tables') as group_update_tables:
        dimension_tasks = list()
        for i in ['d_city', 'd_item', 'd_customer', 'd_calendar', ]:
            dimension_tasks.append(PostgresOperator(
                task_id=f'update_{i}',
                postgres_conn_id=POSTGRES_CONN_ID,
                sql=f'sql/mart.{i}.sql',
                dag=dag
            )
            )
        update_f_sales = PostgresOperator(
            task_id='update_f_sales',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='sql/mart.f_sales.sql',
            parameters={'date': {business_dt}},
        )
        dimension_tasks >> update_f_sales


    t_generate_report = PythonOperator(
        task_id='t_generate_report',
        python_callable=generate_report,
        op_kwargs={'header': headers, 'endpoint': api_endpoint},
        provide_context=True,
    )
    t_get_report = PythonOperator(
        task_id='t_get_report',
        python_callable=get_report,
        op_kwargs={'header': headers, 'endpoint': api_endpoint},
        provide_context=True,
    )
    t_load_customer_research = PythonOperator(
        task_id='t_load_customer_research',
        python_callable=upload_report,
        op_kwargs={
           'file_name': 'customer_research.csv',
           'pg_table': 'staging.customer_research',
           'header': headers,
           'start_at': business_dt,
           's3_file_url': s3_url,
        },
        provide_context=True,
    )
    t_load_user_activity_log = PythonOperator(
        task_id='t_load_user_activity_log',
        python_callable=upload_report,
        op_kwargs={
           'file_name': 'user_activity_log.csv',
           'pg_table': 'staging.user_activity_log',
           'header': headers,
           'start_at': business_dt,
           's3_file_url': s3_url,
        },
        provide_context=True,
    )
    t_load_user_order_log = PythonOperator(
        task_id='t_load_user_order_log',
        python_callable=upload_report,
        op_kwargs={
            'file_name': 'user_orders_log.csv',
            'pg_table': 'staging.user_order_log',
            'header': headers,
            'start_at': business_dt,
            's3_file_url': s3_url,
        },
        provide_context=True,
    )

    start >> t_generate_report >> t_get_report >> \
        [t_load_customer_research, t_load_user_activity_log, t_load_user_order_log] >> group_update_tables >> end
