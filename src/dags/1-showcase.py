import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

POSTGRES_CONN_ID = "1_postgresql"
API_CONN_ID = "1_api"

http_conn = HttpHook.get_connection(API_CONN_ID)
api_endpoint = http_conn.host
api_token = http_conn.extra_dejson.get("X-API-KEY")
nickname = http_conn.extra_dejson.get("X-Nickname")
cohort = http_conn.extra_dejson.get("X-Cohort")

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_token,
    'Content-Type': 'application/x-www-form-urlencoded'
}

s3_url = "https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{REPORT_ID}/{FILE_NAME}"
s3_url_inc = s3_url.replace("{REPORT_ID}","{INCREMENT_ID}")

business_dt = "{{ ds }}"
date_last_success = "{{ prev_start_date_success }}"

#UTILS#
# TODO: декоратор?
def simple_retry(request, kwargs: dict):
    """Простая функция повтора до 3 раз запроса

    Args:
        request: функция get/post
        kwargs: аргументы функции
    """
    for i in range(3):
        response = request(**kwargs)
        if response.status_code == requests.codes.ok:
            break

#TASKS#
def check_init(date_last_success):
    """Проверка, что даг запущен впервые."""
    if date_last_success is not None:
        return 'get_increment'
    else:
        return 'group_report'


def generate_report(ti, headers, api_endpoint):
    print('Making request generate_report')
    method_url = '/generate_report'

    r = simple_retry(
        requests.get,
        {
            "url": 'https://'+api_endpoint + method_url,
            "headers": headers
        })
    r.raise_for_status()

    response_dict = json.loads(r.content)
    ti.xcom_push(key='task_id', value=response_dict['task_id'])
    return response_dict['task_id']


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):

        response = simple_retry(
            requests.get,
            {
                "url": f"{base_url}/get_report?task_id={task_id}",
                "headers": headers
            })

        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)


def upload_report(ti,headers, pg_table, file_name, date, s3_file_url):

    if "INCREMENT_ID" in s3_file_url:
        report_ids = ti.xcom_pull(key='increment_id', task_ids=['get_increment'])    
    else:
        report_ids = ti.xcom_pull(key='report_id', task_ids=['check_report'])
    report_id = report_ids[0]

    s3_file_url = s3_file_url.replace("{REPORT_ID}", report_id)
    s3_file_url = s3_file_url.replace("{COHORT_NUMBER}", headers["X-Cohort"])
    s3_file_url = s3_file_url.replace("{NICKNAME}", headers["X-Nickname"])
    s3_file_url = s3_file_url.replace("{FILE_NAME}", file_name)

    local_file_name = date.replace('-', '') + '_' + file_name
    
    response = simple_retry(
        requests.get, {"url": s3_file_url})

    open(f"{local_file_name}", "wb").write(response.content)

    df = pd.read_csv(local_file_name)
    cols = ','.join(list(df.columns))

    #insert to database
    # psql_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    # conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    # cur = conn.cursor()

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"truncate {pg_table};")
            conn.commit()

            insert_cr = f"INSERT INTO {pg_table} ({cols}) VALUES "+"{cr_val};"
            i = 0
            step = int(df.shape[0] / 100)
            while i <= df.shape[0]:
                print(pg_table, i, end='\r')
                
                cr_val =  str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
                cur.execute(insert_cr.replace('{cr_val}',cr_val))

                conn.commit()
                
                i += step+1


def get_increment(date, ti, headers, api_endpoint):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    method_url = '/get_increment'

    r = simple_retry(
        requests.get,
        {
            "url": f"https://{api_endpoint}/{method_url}?report_id={report_id}&date={str(date)}T00:00:00",
            "headers": headers
        })
    r.raise_for_status()

    response_content = json.loads(r.content)

    status = response_content['status']
    if status == "NOT FOUND" and "Date is too early" in response_content['debug_info']:
        print(f"Skip. {response_content['debug_info']}")
        raise AirflowSkipException

    increment_id = json.loads(r.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')
    return "group_increment"


#TODO check transaction

#DAG#
start = DummyOperator(task_id="start")    

check_init = BranchPythonOperator(
    task_id='check_init',
    python_callable=check_init,
    op_kwargs={"date_last_success": date_last_success},
    dag=dag)

with TaskGroup("group_report") as group_report:

    generate_report = PythonOperator(task_id='generate_report',
        python_callable=generate_report,
        op_kwargs={"headers": headers, "api_endpoint": api_endpoint},
        provide_context=True,
        dag=dag)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    load_customer_research = PythonOperator(task_id='load_customer_research',
                                            python_callable=upload_report,
                                            op_kwargs={'file_name' : 'customer_research.csv',
                                                'pg_table': 'stage.customer_research',
                                                'headers': headers,
                                                'date': business_dt,
                                                's3_file_url': s3_url
                                            },
                                            provide_context=True,
                                            dag=dag)

    load_user_activity_log = PythonOperator(task_id='load_user_activity_log',
                                            python_callable=upload_report,
                                            op_kwargs={'file_name' : 'user_activity_log.csv',
                                                'pg_table': 'stage.user_activity_log',
                                                'headers': headers,
                                                'date': business_dt,
                                                's3_file_url': s3_url
                                            },
                                            provide_context=True,
                                            dag=dag)

    load_user_order_log = PythonOperator(task_id='load_user_order_log',
                                            python_callable=upload_report,
                                            op_kwargs={'file_name' : 'user_orders_log.csv',
                                                'pg_table': 'stage.user_order_log',
                                                'headers': headers,
                                                'date': business_dt,
                                                's3_file_url': s3_url
                                            },
                                            provide_context=True,
                                            dag=dag)


    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.d_city.sql")







    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

get_increment = BranchPythonOperator(
    task_id='get_increment',
    python_callable=get_increment,
    op_kwargs={'date': business_dt},
    dag=dag)

with TaskGroup("group_increment") as group_increment:
    load_customer_research_inc = PythonOperator(task_id='load_customer_research_inc',
                                            python_callable=upload_report,
                                            op_kwargs={'file_name' : 'customer_research_inc.csv',
                                                'pg_table': 'stage.customer_research',
                                                'headers': headers,
                                                'date': business_dt,
                                                's3_file_url': s3_url_inc
                                            },
                                            provide_context=True,
                                            dag=dag)

    load_user_activity_log_inc = PythonOperator(task_id='load_user_activity_log_inc',
                                            python_callable=upload_report,
                                            op_kwargs={'file_name' : 'user_activity_log_inc.csv',
                                                'pg_table': 'stage.user_activity_log',
                                                'headers': headers,
                                                'date': business_dt,
                                                's3_file_url': s3_url_inc
                                            },
                                            provide_context=True,
                                            dag=dag)

    load_user_order_log_inc = PythonOperator(task_id='load_user_order_log_inc',
                                            python_callable=upload_report,
                                            op_kwargs={'file_name' : 'user_orders_log_inc.csv',
                                                'pg_table': 'stage.user_order_log',
                                                'headers': headers,
                                                'date': business_dt,
                                                's3_file_url': s3_url_inc
                                            },
                                            provide_context=True,
                                            dag=dag)


    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart.d_city.sql")







    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )
    