"""DAG for era5 temperature weather variable"""

# pylint: disable=line-too-long,import-error,pointless-statement,protected-access
import ssl
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from era5_pipelines import constants,config,utility


#skipping ssl certification check
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022,9,12),
    'retries': 1,
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': constants.NOTIFICATION_EMAILS,
    'retry_delay': timedelta(minutes=1),
    'dataflow_default_options': {
        'project': constants.GCP_PROJECT_ID,
        'workerZone': constants.ZONE,
        'network': constants.NETWORK,
        'subnetwork': constants.SUBNET,
        'tempLocation': constants.DATAFLOW_TEMP_LOCATION,
        'stagingLocation': constants.DATAFLOW_STAGE_LOCATION,
        'serviceAccountEmail': constants.DATAFLOW_SERVICE_ACCOUNT,
        'ipConfiguration': constants.DATAFLOW_WORKER_IP_CONFIG,
        'machineType': constants.WORKER_MACHINE_TYPE,
        'additionalExperiments': [
            'shuffle_mode=service'
        ]
    }
}

with models.DAG(
        dag_id=constants.ERA5_WIND_DAG_ID,
        description=constants.ERA5_WIND_DAG_DESC,
        default_args=default_args,
        max_active_runs=1,
        concurrency=30,
        schedule_interval=constants.ERA5_WIND_DAG_SCHEDULE,
        catchup=False,
) as dag:

    task_start = DummyOperator(
        task_id="start_fetching_data"
    )

    task_end = DummyOperator(
        task_id="data_fetched"
    )

    email_task = PythonOperator(
                task_id='send_email',
                python_callable=utility.send_email,
                provide_context=True
                )
    populate_daily_index_table = BigQueryInsertJobOperator(
                task_id=f'wind_daily_index',
                configuration={
                    'query': {
                        'query': config.ERA5_WIND_DAILY_INDEX_SP,
                        "useLegacySql": False,

                    }
                },
                location=constants.REGION
                )
    populate_hourly_index_table = BigQueryInsertJobOperator(
        task_id=f"wind_hourly_index",
        configuration={
            'query': {
                    'query': config.ERA5_WIND_HOURLY_INDEX_SP,
                    "useLegacySql": False,
                }
            },
            location=constants.REGION
    )
    with TaskGroup(group_id=f'group_proxy_gen') as group_proxy_gen:

        for key,value in config.PROXY_POWER_GEN_QUERY_DICT.items():
            proxy_gen = BigQueryInsertJobOperator(
                task_id=f"proxy_gen_{key}",
                configuration={
                    'query':{
                        'query':value,
                        'useLegacySql':False,
                    }

                }
            )
       
        
            populate_hourly_index_table >> proxy_gen >> email_task
    

    for task_config in config.ERA5_WIND_CONFIGS:
        #calling api to get results and storing it in GCS
        with TaskGroup(group_id=f'group_{task_config["task_name"]}') as job_group:
            download_data_to_a_file = PythonOperator(
                task_id="download_data_to_a_gcs",
                python_callable = utility.download_data_to_a_file,
                op_kwargs={
                            "start_date_key" : task_config["start_date_key"],
                            "grid_details" : task_config["grid_details"],
                            "staging_bucket" : task_config["staging_bucket"],
                            "gcs_file_name_prefix" : task_config["gcs_file_name_prefix"],
                            "ID_element" : task_config["ID_element"],
                            "ID_data_provider" : task_config["ID_data_provider"]
                            },
                provide_context=True)

        # dataflow job to process data,attach grid_id and store the results in bigquery table

            load_data = DataflowTemplatedJobStartOperator(
                    task_id="gcs_to_bq",
                    job_name=f'{task_config["task_name"]}',
                    template=constants.TEMPLATE_LOCATION,
                    location=constants.REGION,
                    dataflow_default_options=default_args['dataflow_default_options'],
                    parameters={
                            "input" : task_config["input"],
                            "output" : task_config["destination_table_name"],
                            "grid_query" : config.ERA5_GRID_QUERY
                    }
                )
            download_data_to_a_file >> load_data
        task_start.set_downstream(job_group)
        job_group.set_downstream(populate_daily_index_table)
        populate_daily_index_table.set_downstream(populate_hourly_index_table)
        populate_hourly_index_table.set_downstream(group_proxy_gen)
        group_proxy_gen.set_downstream(email_task)
        email_task.set_downstream(task_end)
