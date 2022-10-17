"""
Адаптиранный пайплайн для текущей задачи!
"""
import json
import logging
import time

import pandas as pd
import requests

from collections.abc import Callable
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.utils.task_group import TaskGroup

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

s3_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{REPORT_ID}/{FILE_NAME}'
s3_url_inc = s3_url.replace('{REPORT_ID}', '{INCREMENT_ID}')

business_dt = '{{ ds }}'
date_last_success = '{{ prev_start_date_success }}'

args = {
    'owner': 'ragim',
    'poke_interval': 30,
    'email': ['ragimatamov@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


# UTILS
def simple_retry(request: Callable, kwargs: dict) -> requests.Response:
    """Простая функция повтора до 3 раз запроса.

    Args:
        request: Функция запроса get/post
        kwargs: Аргументы вызова функции request

    Returns:
        requests.Response

    """
    response = None
    for _ in range(3):
        response = request(**kwargs)
        if response.status_code == requests.codes.ok:
            break
    return response


# TASKS
def check_init(last_success: object) -> str:
    """Проверка, что даг запущен впервые.

    Args:
        last_success: Дата последнего успешного запуска дага.

    Returns:
          Возвращает имя задачи в зависимости от успешного запуска дага.
    """
    if last_success is not None:
        return 'get_increment'
    return 'group_report'


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
        requests.get,
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

    ti.xcom_push(key='report_id', value=report_id)


def upload_report(ti: TaskInstance, header: dict, pg_table: str,
                  file_name: str, start_at: datetime.date,
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
    if 'INCREMENT_ID' in s3_file_url:
        report_ids = ti.xcom_pull(key='increment_id', task_ids=['get_increment'])
    else:
        report_ids = ti.xcom_pull(key='report_id', task_ids=['check_report'])
    report_id = report_ids[0]

    s3_file_url = s3_file_url.replace('{REPORT_ID}', report_id)
    s3_file_url = s3_file_url.replace('{COHORT_NUMBER}', header['X-Cohort'])
    s3_file_url = s3_file_url.replace('{NICKNAME}', header['X-Nickname'])
    s3_file_url = s3_file_url.replace('{FILE_NAME}', file_name)

    local_file_name = start_at.replace('-', '') + '_' + file_name

    response = simple_retry(
        requests.get, {'url': s3_file_url},
    )

    open(local_file_name, 'wb').write(response.content)

    df = pd.read_csv(local_file_name)
    cols = ','.join(list(df.columns))

    # insert to database
    # psql_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    # conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    # cur = conn.cursor()

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"truncate {pg_table};")
            conn.commit()

            insert_cr = f"INSERT INTO {pg_table} ({cols}) VALUES " + "{cr_val};"
            i = 0
            step = int(df.shape[0] / 100)
            logging.info(f"{pg_table}, step-{step}")
            while i <= df.shape[0]:

                cr_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                cur.execute(insert_cr.replace('{cr_val}', cr_val))

                conn.commit()

                i += step + 1


def get_increment(start_at: datetime.date, ti: TaskInstance, header: dict, endpoint: str) -> str:
    """Получения данных за те даты, которые не вошли в основной отчёт.

    Args:
        start_at: дата запуска инкремента.
        ti: Context.
        header: Заголовки запроса.
        endpoint: Точка входа запросов.

    Returns:
        str: Выбрираем группу задач инкремента или идём дальше.
    """
    logging.info('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')

    url = f"https://{endpoint}/get_increment?report_id={report_id}&date={str(start_at)}T00:00:00"
    response = simple_retry(
        requests.get,
        {
            'url': url,
            'headers': header,
        })
    response.raise_for_status()

    response_content = json.loads(response.content)

    # Пропускаем ситуацию когда дата меньше даты формирования отчёта
    status = response_content['status']
    if status == 'NOT FOUND' and 'Date is too early' in response_content['debug_info']:
        logging.info(f"Skip. {response_content['debug_info']}")
        return 'end'

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    logging.info(f"increment_id={increment_id}")
    return 'group_increment'


# TODO check transaction

# DAG#
start = DummyOperator(task_id='start')

check_init = BranchPythonOperator(
    task_id='check_init',
    python_callable=check_init,
    op_kwargs={'last_success': date_last_success},
    )

with DAG(
    'customer_retention',
    default_args=args,
    description='Provide default dag for sprint3',
    catchup=True,
    start_date=datetime.today() - timedelta(days=8),
    end_date=datetime.today() - timedelta(days=1),
) as dag:
    with TaskGroup('group_update_table') as group_update_table:
        update_d_item_table = PostgresOperator(
            task_id='update_d_item',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='sql0/production.d_item.sql0',
        )
        update_d_customer_table = PostgresOperator(
            task_id='update_d_customer',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='sql0/production.d_customer.sql0',
        )
        update_d_city_table = PostgresOperator(
            task_id='update_d_city',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='sql0/production.d_city.sql0',
        )
        update_f_sales = PostgresOperator(
            task_id='update_f_sales',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql='sql0/production.f_sales.sql0',
            parameters={'date': {business_dt}},
        )

        update_d_item_table >> update_d_customer_table >> update_d_city_table >> update_f_sales

    with TaskGroup('group_report') as group_report:
        generate_report = PythonOperator(
            task_id='generate_report',
            python_callable=generate_report,
            op_kwargs={'header': headers, 'api_endpoint': api_endpoint},
            provide_context=True,
        )
        get_report = PythonOperator(
            task_id='get_report',
            python_callable=get_report,
            op_kwargs={'header': headers, 'api_endpoint': api_endpoint},
            provide_context=True,
        )
        load_customer_research = PythonOperator(
            task_id='load_customer_research',
            python_callable=upload_report,
            op_kwargs={
               'file_name': 'customer_research.csv',
               'pg_table': 'stage.customer_research',
               'header': headers,
               'start_at': business_dt,
               's3_file_url': s3_url,
            },
            provide_context=True,
        )
        load_user_activity_log = PythonOperator(
            task_id='load_user_activity_log',
            python_callable=upload_report,
            op_kwargs={
               'file_name': 'user_activity_log.csv',
               'pg_table': 'stage.user_activity_log',
               'header': headers,
               'start_at': business_dt,
               's3_file_url': s3_url,
            },
            provide_context=True,
        )
        load_user_order_log = PythonOperator(
            task_id='load_user_order_log',
            python_callable=upload_report,
            op_kwargs={
                'file_name': 'user_orders_log.csv',
                'pg_table': 'stage.user_order_log',
                'header': headers,
                'start_at': business_dt,
                's3_file_url': s3_url,
            },
            provide_context=True,
        )

        generate_report >> get_report >> load_customer_research >> load_user_activity_log >> load_user_order_log >> group_update_table

    get_increment = BranchPythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'start_at': business_dt},
    )

    with TaskGroup('group_increment') as group_increment:
        load_customer_research_inc = PythonOperator(
            task_id='load_customer_research_inc',
            python_callable=upload_report,
            op_kwargs={
                'file_name': 'customer_research_inc.csv',
                'pg_table': 'stage.customer_research',
                'header': headers,
                'start_at': business_dt,
                's3_file_url': s3_url_inc,
            },
            provide_context=True,
        )
        load_user_activity_log_inc = PythonOperator(
            task_id='load_user_activity_log_inc',
            python_callable=upload_report,
            op_kwargs={
                'file_name': 'user_activity_log_inc.csv',
                'pg_table': 'stage.user_activity_log',
                'header': headers,
                'start_at': business_dt,
                's3_file_url': s3_url_inc,
            },
            provide_context=True,
        )
        load_user_order_log_inc = PythonOperator(
            task_id='load_user_order_log_inc',
            python_callable=upload_report,
            op_kwargs={
                'file_name': 'user_orders_log_inc.csv',
                'pg_table': 'stage.user_order_log',
                'header': headers,
                'start_at': business_dt,
                's3_file_url': s3_url_inc,
            },
            provide_context=True,
        )

        load_customer_research >> load_user_activity_log >> load_user_order_log >> group_update_table
