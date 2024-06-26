"""constants for era5 pipelines"""

# pylint: disable=line-too-long,import-error

from airflow.models import Variable

#project_variables

GCP_PROJECT_ID= Variable.get("gcp_project_id")
ERA5_STAGE_DATASET_NAME = Variable.get("era5_stage_dataset_name")
SPEED_STAGE_DATASET_NAME = Variable.get('speed_dataset_stage')
BUCKET = Variable.get("era5_stage_bucket_name")
template_bucket = Variable.get("template_bucket")

# Network Variables
REGION = 'us-east4'
ZONE = 'us-east4-a'
NETWORK = Variable.get('network')
SUBNET = Variable.get('sub-network')

# DAG Variables

ERA5_WIND_DAG_ID= "ERA5_Wind"
ERA5_WIND_DAG_DESC= "ERA5 hourly data for wind"
ERA5_TEMPERATURE_DAG_ID = "ERA5_Temperature"
ERA5_TEMPERATURE_DAG_DESC = "ERA5 hourly data for temperature"
ERA5_PRECIP_DAG_ID = "ERA5_Precip"
ERA5_PRECIP_DAG_DESC = "ERA5 hourly data for precipitation"
ERA5_WIND_GLOBAL_DAG_ID= "ERA5_Wind_Global"
ERA5_WIND_GLOBAL_DAG_DESC= "ERA5 hourly data for wind global grids"

ERA5_WIND_DAG_SCHEDULE = Variable.get("era5_wind_dag_schedule")
ERA5_TEMPERATURE_DAG_SCHEDULE = Variable.get("era5_temperature_dag_schedule")
ERA5_PRECIP_DAG_SCHEDULE = Variable.get("era5_precip_dag_schedule")
ERA5_WIND_GLOBAL_DAG_SCHEDULE = Variable.get("era5_wind_global_dag_schedule	")



# Dataflow variables
DATAFLOW_SERVICE_ACCOUNT = Variable.get('dataflow_service_account')
TEMPLATE_LOCATION = 'gs://' + template_bucket + '/templates/speedwell_api/Speedwell_Api_Template'
WORKER_MACHINE_TYPE = 'n1-standard-4'
DATAFLOW_TEMP_LOCATION = 'gs://' + template_bucket + '/tmp'
DATAFLOW_STAGE_LOCATION = 'gs://' + template_bucket + '/stage'
DATAFLOW_WORKER_IP_CONFIG = 'WORKER_IP_PRIVATE'

# Api authorization details
wUserName = Variable.get("speedwell_api_username")
wPassword = Variable.get("speedwell_api_password")

NOTIFICATION_EMAILS = Variable.get("notif_emails")
AIRFLOW_URL = Variable.get('airflow_url')
ENVIRONMENT = Variable.get('environment')
