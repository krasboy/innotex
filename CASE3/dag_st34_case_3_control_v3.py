from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import datetime

with DAG(
    'st34_case_3_control_v3',
    default_args={
        'depends_on_past': False,
        #'email': ['developer@yandex.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': datetime.timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='',
    schedule_interval='*/10 * * * *',
    start_date=datetime.datetime(2023, 5, 5),
    catchup=False,
    max_active_runs=1,
    tags=['case_3'],
) as dag:

    t_st_dum = DummyOperator(
        task_id='t_st_dum',
    )
    t_end_dum = DummyOperator(
        task_id='t_end_dum',
    )

 
    t_src_crt = PostgresOperator(
        task_id='oltp_src_system__create_request',
        postgres_conn_id = 'st34_postgree',
        sql = 'select * from oltp_src_system.create_request();'
    )



    t_src_upd = PostgresOperator(
        task_id='oltp_src_system__update_existed_request',
        postgres_conn_id = 'st34_postgree',
        sql = 'select * from oltp_src_system.update_existed_request();'
        
    )


    t_src_del = PostgresOperator(
        task_id='oltp_src_system__deleted_existed_request',
        postgres_conn_id = 'st34_postgree',
        sql = 'select * from oltp_src_system.deleted_existed_request();'
        
    )
    
    t_dwh_load = PostgresOperator(
        task_id='dwh_stage__load_dwh_stage',
        postgres_conn_id = 'st34_postgree',
        sql = 'select * from dwh_stage.load_dwh_stage();'
        
    )
    
    t_ods_hist = PostgresOperator(
        task_id='dwh_ods__load_from_stage_hist_request',
        postgres_conn_id = 'st34_postgree',
        sql = 'select * from dwh_ods.load_from_stage_hist_request();'
        
    )
    
    t_ods_agg = PostgresOperator(
        task_id='dwh_ods__load_agg_request_data_hist',
        postgres_conn_id = 'st34_postgree',
        sql = 'select * from dwh_ods.load_agg_request_data_hist();'
        
    )
    t_rep_load = PostgresOperator(
        task_id='dwh_ods__load_dim_work',
        postgres_conn_id = 'st34_postgree',
        sql = 'select * from dwh_ods.load_dim_work();'
        
    )

    t_st_dum >> [t_src_crt, t_src_upd, t_src_del] >> t_dwh_load >>t_ods_hist >>t_ods_agg>>t_rep_load>>t_end_dum