"""util functios for era5 pipelines"""

# pylint: disable=line-too-long,unused-import,import-error,logging-fstring-interpolation,invalid-name

import Speedwell_GriddedDataAPI_Core
from Speedwell_GriddedDataAPI_Core.APIHelper import APIError
import time

import logging
from datetime import datetime,timedelta
import numpy as np
import pandas as pd
from airflow.models import Variable
from airflow.utils.email import send_email as airflow_send_email
from ERA5.Speedwell_DataAPI_Common import SafeSerializationDateTime
from ERA5.Speedwell_GriddedDataAPI_Client import Speedwell_GriddedDataAPI_Client as SWDGridCatalogClient
from ERA5.Speedwell_GriddedDataAPI_Client import Speedwell_GriddedDataAPI_MathsClient as SWDGridMathClient
from jinja2 import Template
from era5_pipelines import constants
from google.cloud import bigquery


def send_email(**context):
    """send email function"""
    try:
        dag_id = context['dag'].dag_id
        exec_time = context['ds']
        subject = f'[ClimateDelta-{constants.ENVIRONMENT}] DAG {dag_id} Completed Successfully on {exec_time}'
        template_params = {
            'application_dag': dag_id,
            'airflow_link': constants.AIRFLOW_URL,
            'environment': constants.ENVIRONMENT,
            'date': exec_time
        }
        with open('/home/airflow/gcs/dags/template/email_notif.html', 'r') as f:
            email_template = f.read()
        email_content = Template(email_template).render(template_params)
        airflow_send_email(to=constants.NOTIFICATION_EMAILS,
                           subject=subject,
                           html_content=email_content)
    except Exception as e:
        logging.error(f'Error: {str(e)}')
        raise

def fn(row):
    """combine columns to give datetime object"""
    a = row
    Day = a['Day']
    Hour = a['Hour']
    Minute = a['Minute']
    Month = a['Month']
    Second = a['Second']
    Year = a['Year']
    x = datetime(Year,Month,Day,Hour,Minute,Second)
    # x = x.strftime('%Y-%m-%d %H:%M:%S')
    return x

def format_data(x):
    """format data """
    r = []
    for i in x["Measures"]:
        date = fn(i["MeasureDateTime"])
        dates = np.repeat(date,len(i["Measures"]))
        values = i["Measures"]
        df0 = pd.DataFrame()
        df0["MeasureDateTime"] = dates
        values_pd = pd.DataFrame(values)
        df0["Latitude"] = values_pd["Latitude"]
        df0["Longitude"] = values_pd["Longitude"]
        df0["MeasureValue"] = values_pd["Value"]
        r.append(df0)
    return r

def download_data_to_a_file(**context):
    """load data from api to GCS location"""
    task_instance = context['ti']

    #staging bucket

    storage_bucket = context["staging_bucket"]

    logging.info(f"the staging bucket for this era5 weather component is {storage_bucket}")

    # Api authorization details
    wUserName = constants.wUserName
    wPassword = constants.wPassword

    # Api client interface for data  fetching

    MathsAPIClient = SWDGridMathClient("griddeddata.speedwellweather.com",
                                         "/GriddedDataRegionMathsService/GriddedDataRegionMathsService.svc",
                                         wUserName, wPassword)
    wIDDataType = None
    wIncludeNulls = True

    start_date_key = context["start_date_key"]
    grid_details = context["grid_details"]
    gcs_file_name_prefix = context["gcs_file_name_prefix"]
    ID_element = context["ID_element"]
    ID_data_provider = context["ID_data_provider"]
    logging.info(f"the details of api request is {start_date_key},{grid_details},{gcs_file_name_prefix},{ID_element},{ID_data_provider} ")

    start_date = Variable.get(start_date_key)

    # formatting the dates as required by the api
    wStartDate = start_date.split('-')
    wStartDate = SafeSerializationDateTime(*wStartDate)
    wEndDate = None
    logging.info(f"the details of api request is {start_date_key},{start_date},{grid_details},{gcs_file_name_prefix},{ID_element},{ID_data_provider} ")

    data = MathsAPIClient.GetPointsInRectangle(ID_data_provider, ID_element, wIDDataType, grid_details[0],
                                            grid_details[1],grid_details[2],grid_details[3], wIncludeNulls, wStartDate, wEndDate)
    
    if type(data) == APIError:
        logging.info(f"Got API Error {data.ErrorMessage}: \n{data.UserMessage}")
        raise Exception(f"Got API Error {data.ErrorMessage}: \n{data.UserMessage}")
    elif data==None:
        raise Exception(f"Got API Error : No data available ")
                                            
    logging.info(f"data{type(data)}")
    logging.info(f"the response header is {data['Header']['Message']}")
    result =format_data(data)



    # this dataframe contains the data related to era5 wind in the period of 5 days
    df = pd.concat(result)
    max_date = df["MeasureDateTime"].max().to_pydatetime()

    logging.info(max_date)
    logging.info(df["MeasureDateTime"].max())

    # file name to store the data in cloud storage with

    file_name = storage_bucket+f'/{gcs_file_name_prefix}_{max_date}.csv'
    logging.info(f"the gcs location for the data is {file_name}")

    task_instance.xcom_push(key="gcs_file_location",value=file_name)
    task_instance.xcom_push(key="start_date",value=datetime.strptime(start_date,'%Y-%m-%d-%H-%M-%S').strftime("%Y-%m-%dT%H:%M:%S"))


    # setting start_date for next dag run here
    # the below date is start date for next run
    max_date = (max_date + timedelta(hours=1)).strftime('%Y-%m-%d-%H-%M-%S')
    logging.info(f"next start date is {max_date}")
    Variable.set(start_date_key,max_date)

    # Storing File in gcs
    df.to_csv(f"{file_name}")
    logging.info("file loaded to GCS")

def format_global_data(x):
    """format data """
    r = []
    for i in x['MeasureValues']:
        date = fn(i["MeasureDateTime"])
        dict_d = dict(MeasureDateTime = date, MeasureValue = i["MeasureValue"])
        r.append(dict_d)
    return r

def download_global_wind_data_to_a_file(**context):
    """load data from api to GCS location"""
    task_instance = context['ti']

    #staging bucket
    storage_bucket = context["staging_bucket"]

    logging.info(f"the staging bucket for this era5 weather component is {storage_bucket}")

    # Api authorization details
    wUserName = constants.wUserName
    wPassword = constants.wPassword
   
    wIDDataType = None
    wIncludeNulls = True

    wIDDataElement = context["ID_element"]
    wIDDataProvider = context["ID_data_provider"]
    start_date_key = context["start_date_key"]
    gcs_file_name_prefix = context["gcs_file_name_prefix"]

    start_date = Variable.get(start_date_key)

    wStartDate = start_date.split('-')
    wStartDate = SafeSerializationDateTime(*wStartDate)
    wEndDate = None

    client = bigquery.Client()
    query = f"""with 
        g_grids as (
         SELECT a.Latitude, a.Longitude FROM `climate-delta-dev.urd_stage.era5_grids_global` a inner join `climate-delta-dev.speed_stage.tblWindFarms` b on ST_CONTAINS(a.box,ST_GEOGPOINT(b.longitude,b.latitude)) = TRUE
         ),
         final_grids as (
         select distinct * from g_grids
         )
         select c.Latitude, c.Longitude from `climate-delta-dev.urd_datamart.grids_all` d right join
         final_grids c
         on c.Latitude=d.centroid_lat and c.Longitude=d.centroid_lon where d.centroid_lat is null and d.centroid_lon is null"""
    station_df = client.query(query).to_dataframe()
    # print(station_df)

    l = []
    df = pd.DataFrame()

    logging.info("Starting Data Download...")
    for index, row in station_df.iterrows():
        # print(row['Latitude'], row['Longitude'])

        try:
            while(True):
                wLatitude = row['Latitude']
                wLongitude = row['Longitude']
                
                logging.info(f"Getting Data from {start_date} for lat : {wLatitude} and long : {wLongitude}")
                
                MathsAPIClient = SWDGridMathClient("griddeddata.speedwellweather.com",
                                         "/GriddedDataRegionMathsService/GriddedDataRegionMathsService.svc",
                                         wUserName, wPassword)
                
                logging.info(f"the details of api request is {start_date_key},{gcs_file_name_prefix},{wIDDataElement},{wIDDataProvider} ")
                
                oDataFrame = MathsAPIClient.GetPointTimeSeries(wIDDataProvider, wIDDataElement, wIDDataType,
                                                       wLatitude, wLongitude, wIncludeNulls, wStartDate,
                                                       wEndDate)
                
                
                if type(oDataFrame) == APIError:
                    logging.info(f"Got API Error {oDataFrame.ErrorMessage}: \n{oDataFrame.UserMessage}")
                    logging.info(f"\n\nSkipping with Halt...")

                    time.sleep(250)
                    continue
                else:
                    oDataFrame = format_global_data(oDataFrame)
                    oDataFrame = pd.DataFrame.from_dict(oDataFrame)
                    
                    logging.info("Got Data.")

                    oDataFrame.insert(1,'Latitude',wLatitude)
                    oDataFrame.insert(2,'Longitude', wLongitude)
                    
                    l.append(oDataFrame)
                    
                    logging.info(f"Got Data for lat : {wLatitude} and long : {wLongitude}.")
                    break
        except Exception as e:
                logging.info(f"Got Error as {e}")
                logging.info("Sleeping API...")

                time.sleep(250)
                logging.info("API Woke up")
    logging.info("Inserting Data to BQ...")

    #print(df)
    df = pd.concat(l)
    max_date = df.groupby(['Latitude','Longitude']).max()["MeasureDateTime"].min(0).to_pydatetime()

    logging.info(max_date)  

    final_df = df[df['MeasureDateTime'] <= max_date]

    file_name = storage_bucket+f'/{gcs_file_name_prefix}_{max_date}.csv'
    logging.info(f"the gcs location for the data is {file_name}")

    task_instance.xcom_push(key="gcs_file_location",value=file_name)
    task_instance.xcom_push(key="start_date",value=datetime.strptime(start_date,'%Y-%m-%d-%H-%M-%S').strftime("%Y-%m-%dT%H:%M:%S"))

    # setting start_date for next dag run here
    # the below date is start date for next run
    max_date = (max_date + timedelta(hours=1)).strftime('%Y-%m-%d-%H-%M-%S')
    logging.info(f"next start date is {max_date}")
    Variable.set(start_date_key,max_date)

    # Storing File in gcs
    final_df.to_csv(f"{file_name}")
    logging.info("file loaded to GCS")    

def download_data_to_a_file_new(**context):
    '''Load data from API to a GCS location with new API'''

    task_instance = context['ti']

    # staging bucket
    storage_bucket = context["staging_bucket"]

    logging.info(f"The staging bucket for this ERA5 weather component is {storage_bucket}")

    start_date_key = context["start_date_key"]
    polygon_details = context["polygon_details"]
    gcs_file_name_prefix = context["gcs_file_name_prefix"]
    ID_element = context["ID_element"]
    ID_data_provider = context["ID_data_provider"]

    logging.info(f"The details of API request is {start_date_key}, {polygon_details}, {gcs_file_name_prefix}, {ID_element}, {ID_data_provider}.")

    start_date = Variable.get(start_date_key)

    # formatting the date as required for the API
    wStartDate = datetime.strptime(start_date,'%Y-%m-%d-%H-%M-%S')
    wEndDate = None

    logging.info(f"The details of API request is {start_date_key}, {start_date}, {polygon_details}, {gcs_file_name_prefix}, {ID_element}, {ID_data_provider}.")

    got_data = False
    oData = []
    while not got_data:
        oData = Speedwell_GriddedDataAPI_Core.GetMeasureValuesForAPolygonRegion(ID_data_provider, ID_element, polygon_details, wStartDate, wEndDate)

        if type(oData) == APIError:
            if oData.ErrorMessage == "Exceeded Usage Limit":
                got_data = False
                logging.info("got API error sleeping for 3 mins")
                time.sleep(200)
        else:
            got_data = True

            logging.info("Got the Data converting it to Dataframe")

    oDataFrame = Speedwell_GriddedDataAPI_Core.MathsServiceHelper.ConvertListDateTimeMeasuresToDataFrame(oData)

    max_date = oDataFrame["MeasureDateTime"].max().to_pydatetime()

    logging.info(max_date)
    logging.info(oDataFrame["MeasureDateTime"].max())

    # file name to store the data in cloud storage with
    file_name = storage_bucket+f"/{gcs_file_name_prefix}_{max_date}.csv"
    logging.info(f"The GCS location for the data is {file_name}")

    task_instance.xcom_push(key="gcs_file_location", value=file_name)
    task_instance.xcom_push(key="start_date", value=datetime.strptime(start_date,'%Y-%m-%d-%H-%M-%S').strftime('%Y-%m-%dT%H:%M:%S'))

    # setting start_date for next DAG run here
    # The below data is start date for next run
    max_date = (max_date + timedelta(hours=1)).strftime('%Y-%m-%d-%H-%M-%S')
    logging.info(f"Next start date is {max_date}")

    Variable.set(start_date_key, max_date)

    # Storing File in GCS
    oDataFrame.to_csv(f"{file_name}")
    logging.info("File loaded in GCS")
