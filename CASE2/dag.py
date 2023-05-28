#даг для отслеживания изменений в метаданных airflow
import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

with DAG(
          dag_id="transfer_dag_md_from_airflow_to_etl_service_storage",
          start_date=airflow.utils.dates.days_ago(1),
          max_active_runs=1,
          tags = ['tech_dag'],
          schedule_interval = "@hourly"
) as dag:
#         scheduler_interval = "10 */1 * * *"

    run_f = DummyOperator(task_id='run_flg_dummy')

    #@task_group()
    #def gr_load_dag_code():
    with TaskGroup(group_id="gr_load_dag_code") as gr_load_dag_code:
        load_dag_code = GenericTransfer(
            task_id = 'load_dag_code_from_airflow_to_etl_service',
            source_conn_id = 'airflow_pg_metastorage',
            destination_conn_id = 'etl_service_pg_metastorage',
            destination_table = 'stage.airflow_dag_code_src',
            preoperator = ["truncate table stage.airflow_dag_code_src",],
            sql = """select fileloc, source_code from public.dag_code""")

        # RUN PostgreSQL operator
        # select cdc.load_cdc_airflow_dag_code()
        load_to_cdc_from_stage_dag_code = PostgresOperator(task_id="load_to_cdc_from_stage_dag_code"
                , sql="""select cdc.load_cdc_airflow_dag_code()"""
                , postgres_conn_id = 'etl_service_pg_metastorage'
                )
        # RUN PostgreSQL operator
        # select md.load_from_cdc_airflow_dag_code_hist()
        load_to_md_from_cdc_dag_code = PostgresOperator(task_id="load_to_md_from_cdc_dag_code"
                , sql="""select md.load_from_cdc_airflow_dag_code_hist()"""
                , postgres_conn_id = 'etl_service_pg_metastorage'
                )
        load_dag_code >> Label("CDC") >> load_to_cdc_from_stage_dag_code >> Label("Increment") >> load_to_md_from_cdc_dag_code

    #@task_group()
    #def gr_load_dag_information():
    with TaskGroup(group_id="gr_load_dag_information") as gr_load_dag_information:
        load_dag_info = GenericTransfer(
                                      task_id = 'load_dag_info_from_airflow_to_etl_service',
                                      source_conn_id = 'airflow_pg_metastorage',
                                      destination_conn_id = 'etl_service_pg_metastorage',
                                      destination_table = 'stage.airflow_dag_information_src',
                                      preoperator = ["truncate table stage.airflow_dag_information_src",],
                                      sql = """select dag_id, is_paused, is_subdag, is_active, last_parsed_time, last_pickled
                                                  , last_expired, scheduler_lock, pickle_id, fileloc, owners, description
                                                  , default_view, schedule_interval, root_dag_id, next_dagrun
                                                  , next_dagrun_create_after, max_active_tasks, has_task_concurrency_limits
                                                  , max_active_runs, next_dagrun_data_interval_start
                                                  , next_dagrun_data_interval_end, has_import_errors
                                              from public.dag""")
        # RUN PostgreSQL operator
        # select cdc.load_cdc_airflow_dag_information()
        load_to_cdc_from_stage_dag_information = PostgresOperator(task_id="load_to_cdc_from_stage_dag_information"
                , sql="""select cdc.load_cdc_airflow_dag_information()"""
                , postgres_conn_id = 'etl_service_pg_metastorage'
                )
        # RUN PostgreSQL operator
        # select md.load_from_cdc_airflow_dag_information_hist()
        load_to_md_from_cdc_dag_information = PostgresOperator(task_id="load_to_md_from_cdc_dag_information"
                , sql="""select md.load_from_cdc_airflow_dag_information_hist()"""
                , postgres_conn_id = 'etl_service_pg_metastorage'
                )
        load_dag_info >> Label("CDC") >> load_to_cdc_from_stage_dag_information >> Label("Increment") >> load_to_md_from_cdc_dag_information

    last_step = DummyOperator(task_id='last_run_flg_dummy')

    run_f >> gr_load_dag_code >> last_step
    run_f >> gr_load_dag_information >> last_step