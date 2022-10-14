import datetime
import time
import psycopg2
import logging
import urllib

import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom


#1
def create_files_request(ti, headers):
    logging.info(headers)
    api_conn = BaseHook.get_connection('create_files_api')
    api_endpoint = api_conn.host
    method_url = '/generate_report'
    r = requests.post('https://'+api_endpoint + method_url, headers=headers)

    response_dict = json.loads(r.content)

    ti.xcom_push(key='task_id', value=response_dict['task_id'])
    return response_dict['task_id']

#2. проверяем готовность файлов в success
#на выход получаем стринг идентификатор готового репорта который является ссылкой до файлов которые можем скачивать
def check_report(ti, headers):
    logging.info('check_report')
    logging.info(headers)
    logging.info(ti)
    api_conn = BaseHook.get_connection('create_files_api')
    api_endpoint = api_conn.host

    task_ids = ti.xcom_pull(key='task_id', task_ids=['create_files_request'])
    task_id = task_ids[0]
    
    method_url = '/get_report'
    payload = {'task_id': task_id}

    #отчет выгружается 60 секунд минимум - самое простое в слип на 70 секунд уводим - делаем 4 итерации - если нет то пусть валится в ошибку
    #значит с выгрузкой что-то не так по api и надо идти разбираться с генерацией данных - и в жизни такая же фигня бывает
    for i in range(4):
        time.sleep(70)
        r = requests.get('https://' + api_endpoint + method_url, params=payload, headers=headers)
        response_dict = json.loads(r.content)
        logging.info(f"{i}, {response_dict['status']}")
        if response_dict['status'] == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break
    
    #тут соответствуенно если report_id не объявлен то есть не было сукксесса то в ошибку упадет и инженер идет разбираться почему
    #можно сделать красивее но время
    ti.xcom_push(key='report_id', value=report_id)
    logging.info(f"report_id is {report_id}")
    return report_id

#3. загружаем 3 файлика в таблички (таблички stage)
def upload_from_s3_to_pg(ti,headers, pg_table, file_name):
    report_ids = ti.xcom_pull(key='report_id', task_ids=['check_report'])
    report_id = report_ids[0]

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/{REPORT_ID}/{FILE_NAME}'

    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", headers["X-Cohort"])
    personal_storage_url = personal_storage_url.replace("{NICKNAME}", headers["X-Nickname"])
    personal_storage_url = personal_storage_url.replace("{REPORT_ID}", report_id)

    #insert to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    count = 0
    while True:
        try:
            count += 1
            logging.info(personal_storage_url.replace("{FILE_NAME}", file_name))
            df = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", file_name) )

            cols = ','.join(list(df.columns))
            break
        except urllib.error.HTTPError as e:
            if count>3:
                raise e



    cur.execute(f"truncate stage.{pg_table};")
    conn.commit()
    insert_cr = f"INSERT INTO stage.{pg_table} ({cols}) VALUES "+"{cr_val};"
    i = 0
    step = int(df.shape[0] / 100)
    while i <= df.shape[0]:
        print(pg_table, i, end='\r')
        
        cr_val =  str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_cr.replace('{cr_val}',cr_val))
        #psycopg2.extras.execute_values(cur, insert_stmt, df.values)
        conn.commit()
        
        i += step+1



    cur.close()
    conn.close()

    #понятно что можно обернуть в функцию но для времени описал 3 разными запросами просто для экономии
    return 200

#3. обновляем таблички d по загруженными в stage
def update_mart_d_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #d_customer
    cur.execute("""
        truncate mart.d_customer cascade;
        with all_customer as (
            select
                c.customer_id,
                c.first_name,
                c.last_name,
                max(c.city_id) as city_id
            from stage.user_order_log c
            group by         c.customer_id,
                c.first_name,
                c.last_name
        )
        INSERT INTO mart.d_customer
        select *
        from all_customer;    
    """)
    conn.commit()

    #d_calendar
    cur.execute("""
        truncate mart.d_calendar cascade;
        with all_dates as (
            select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time from stage.user_activity_log
            union
            select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time from stage.user_order_log
            union
            select distinct to_date(date_id::TEXT,'YYYY-MM-DD') as date_time from stage.customer_research
            order by date_time
            )
        INSERT INTO mart.d_calendar
        select
        distinct ROW_NUMBER () OVER (
        ORDER BY date_time
        ) as date_id
        ,a.date_time as fact_date
        ,extract(day from a.date_time) as day_num
        ,extract(month from a.date_time) as month_num
        ,TO_CHAR(a.date_time, 'Month')::varchar(8) as month_name
        ,extract(year from a.date_time) as year_num
        from all_dates a;
    """)
    conn.commit()

    #d_item
    cur.execute("""
        truncate mart.d_item cascade ;

        INSERT INTO mart.d_item
        select distinct o.item_id, o.item_name
        from stage.user_order_log o;    
    """)
    conn.commit()

    cur.close()
    conn.close()

    return 200

#4. апдейт витринок (таблички f)
def update_mart_f_tables(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #f_activity
    cur.execute("""
        truncate mart.f_activity cascade ;
        INSERT INTO mart.f_activity
        select  ual.action_id
            ,dc.date_id
            ,sum(ual.quantity) as click_number
        from stage.user_activity_log as ual
        left join mart.d_calendar as dc on ual.date_time::date = dc.fact_date
        group by ual.action_id, dc.date_id;
    """)
    conn.commit()

    #f_daily_sales
    cur.execute("""
        truncate mart.f_daily_sales cascade;
        INSERT INTO mart.f_daily_sales
        select
            dc.date_id,
            uol.item_id,
            uol.customer_id,
            sum(uol.payment_amount) / sum(uol.quantity) as price,
            sum(uol.quantity) as quantity,
            sum(uol.payment_amount) as payment_amount
        from stage.user_order_log as uol
        left join mart.d_calendar as dc on uol.date_time::date = dc.fact_date
        group by dc.date_id, uol.item_id, uol.customer_id;
    """)
    conn.commit()

    cur.close()
    conn.close()

    return 200


nickname = 'RagimAtamov'
cohort = '6'
api_token = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'

headers = {
	"X-API-KEY": api_token,
	"X-Nickname": nickname,
	"X-Cohort": cohort
}

dag = DAG(
    dag_id='591_full_dag',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

t_create_files_request = PythonOperator(task_id='create_files_request',
										python_callable=create_files_request,
										op_kwargs={"headers": headers},
                                        provide_context=True,
										dag=dag)

t_check_report = PythonOperator(task_id='check_report',
                                        python_callable=check_report,
                                        op_kwargs={ 'headers': headers                                                                
                                        },
                                        provide_context=True,
                                        dag=dag)


t_load_customer_research = PythonOperator(task_id='load_customer_research',
                                        python_callable=upload_from_s3_to_pg,
                                        op_kwargs={'file_name' : 'customer_research.csv',
                                            'pg_table': 'customer_research',
                                            'headers': headers
                                        },
                                        provide_context=True,
                                        dag=dag)

t_load_user_activity_log = PythonOperator(task_id='load_user_activity_log',
                                        python_callable=upload_from_s3_to_pg,
                                        op_kwargs={'file_name' : 'user_activity_log.csv',
                                            'pg_table': 'user_activity_log',
                                            'headers': headers
                                        },
                                        provide_context=True,
                                        dag=dag)

t_load_user_order_log = PythonOperator(task_id='load_user_order_log',
                                        python_callable=upload_from_s3_to_pg,
                                        op_kwargs={'file_name' : 'user_orders_log.csv',
                                            'pg_table': 'user_order_log',
                                            'headers': headers
                                        },
                                        provide_context=True,
                                        dag=dag)


t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)

t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)


t_create_files_request >> t_check_report >> [t_load_user_order_log, t_load_user_activity_log, t_load_customer_research] >> t_update_mart_d_tables >> t_update_mart_f_tables
