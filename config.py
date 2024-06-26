"""config for era5 pipelines"""

# pylint: disable=line-too-long,import-error

from era5_pipelines import constants
from Speedwell_GriddedDataAPI_Core import eElement, GetDataProviderFromIDElement

ERA5_WIND_CONFIGS = [
            {
                'task_name': 'wind_usa',
                "destination_table_name": f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyWind',
                "start_date_key" : "start_date",
                "grid_details" : [50, 25, -146, -65],
                "staging_bucket" : constants.BUCKET + "era5_wind",
                "gcs_file_name_prefix" : "wind_data",
                "ID_element" : 262657 ,
                "ID_data_provider" : 100,
                "input" : '{{ti.xcom_pull(task_ids="group_wind_usa.download_data_to_a_gcs", key="gcs_file_location")}}'

            },
            {
                'task_name': 'wind_europe',
                "destination_table_name": f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyWind',
                "start_date_key" : "start_date_EU",
                "grid_details" : [72, 27, 40, -32],
                "staging_bucket" : constants.BUCKET + "era5_wind",
                "gcs_file_name_prefix" : "wind_data_eu",
                "ID_element" : 262657,
                "ID_data_provider" : 100,
                "input" : '{{ti.xcom_pull(task_ids="group_wind_europe.download_data_to_a_gcs", key="gcs_file_location")}}'
            },
            {
                'task_name': 'wind_australia',
                "destination_table_name": f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyWind',
                "start_date_key" : "start_date_aus",
                "grid_details" : [-8, -44, 155, 111],
                "staging_bucket" : constants.BUCKET + "era5_wind",
                "gcs_file_name_prefix" : "wind_data_aus",
                "ID_element" : 262657,
                "ID_data_provider" : 100,
                "input" : '{{ti.xcom_pull(task_ids="group_wind_australia.download_data_to_a_gcs", key="gcs_file_location")}}'
            }
]

ERA5_WIND_GLOBAL_CONFIGS = [
            {
                'task_name': 'wind_global',
                "destination_table_name": f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyWindGlobal',
                "start_date_key" : "wind_global_start_date",
                "staging_bucket" : constants.BUCKET + "era5_wind_global",
                "gcs_file_name_prefix" : "wind_global_data",
                "ID_element" : 262657,
                "ID_data_provider" : 100,
                "input" : '{{ti.xcom_pull(task_ids="group_wind_global.download_global_wind_data_to_a_file", key="gcs_file_location")}}'

            }]

ERA5_PRECIPITATION_CONFIGS = [
    {
      'task_name':'precip_aus',
      'destination_table_name':f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyPrecipitation',
      'start_date_key':'start_date_precip_aus',
      'polygon_details':"POLYGON((155 -44, 155 -8, 111 -8, 111 -44, 155 -44))",
      "grid_details" : [-8, -44, 155, 111],
      'staging_bucket': constants.BUCKET + "era5_precip",
      "gcs_file_name_prefix": "precip_data",
      "ID_element": 197110,
      "ID_data_provider": 4,
      "input":'{{ti.xcom_pull(task_ids="group_precip_aus.download_data_to_a_gcs", key="gcs_file_location")}}'
    },
    {
      'task_name':'precip_eu',
      'destination_table_name':f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyPrecipitation',
      'start_date_key':'start_date_precip_eu',
      'polygon_details':"POLYGON((40 27, 40 72, -32 72, -32 27, 40 27))",
      "grid_details" : [72, 27, 40, -32],
      'staging_bucket': constants.BUCKET + "era5_precip",
      "gcs_file_name_prefix": "precip_data_eu",
      "ID_element": 197110,
      "ID_data_provider": 4,
      "input":'{{ti.xcom_pull(task_ids="group_precip_eu.download_data_to_a_gcs", key="gcs_file_location")}}'
    },
    {
      'task_name':'precip_us',
      'destination_table_name':f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyPrecipitation',
      'start_date_key':'start_date_precip_us',
      'polygon_details':"POLYGON((-65 25, -65 50, -146 50, -146 25, -65 25))",
      "grid_details" : [50, 25, -146, -65],
      'staging_bucket': constants.BUCKET + "era5_precip",
      "gcs_file_name_prefix": "precip_data_us",
      "ID_element": 197110,
      "ID_data_provider": 4,
      "input":'{{ti.xcom_pull(task_ids="group_precip_us.download_data_to_a_gcs", key="gcs_file_location")}}'
    }
]


ERA5_WIND_DAILY_INDEX_SP = f"CALL `{constants.GCP_PROJECT_ID}.{constants.ERA5_STAGE_DATASET_NAME}.Wind_Daily_Index`();"

ERA5_WIND_HOURLY_INDEX_SP = f"CALL `{constants.GCP_PROJECT_ID}.{constants.ERA5_STAGE_DATASET_NAME}.Wind_Hourly_Index`();"

ERA5_TEMPERATURE_DAILY_INDEX_SP = f"CALL `{constants.GCP_PROJECT_ID}.{constants.ERA5_STAGE_DATASET_NAME}.Temperature_Daily_Index`();"

ERA5_TEMPERATURE_HOURLY_INDEX_SP = f"CALL `{constants.GCP_PROJECT_ID}.{constants.ERA5_STAGE_DATASET_NAME}.Temperature_Hourly_Index`();"

ERA5_PRECIP_DAILY_INDEX_SP = f"CALL `{constants.GCP_PROJECT_ID}.{constants.ERA5_STAGE_DATASET_NAME}.Precipitation_Daily_Index`();"

ERA5_PRECIP_HOURLY_INDEX_SP = f"CALL `{constants.GCP_PROJECT_ID}.{constants.ERA5_STAGE_DATASET_NAME}.Precipitation_Hourly_Index`();"

ERA5_TEMPERATURE_CONFIGS = [{
                'task_name': 'temperature_eu',
                "destination_table_name": f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyTemperature',
                "start_date_key" : "start_date_temperature",
                # "grid_details" : [55.1, 47.2,5.5,15.5],
                "grid_details" : [72, 27, 40, -32],
                "staging_bucket" : constants.BUCKET + "era5_temperature",
                "gcs_file_name_prefix" : "temperature_data",
                "ID_element" : 197109,
                "ID_data_provider" : 4,
                "input" : '{{ti.xcom_pull(task_ids="group_temperature_eu.download_data_to_a_gcs", key="gcs_file_location")}}'
            },
            {
                "task_name": "temperature_usa",
                "destination_table_name": f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyTemperature',
                "start_date_key" : "start_date_temp_usa",
                "polygon": "POLYGON((-65 25, -65 50, -146 50, -146 25, -65 25))",
                "grid_details" : [50, 25, -146, -65],
                "staging_bucket" : constants.BUCKET + "era5_temperature",
                "gcs_file_name_prefix" : "temperature_data_usa",
                "ID_element" : 197109,
                "ID_data_provider" : 4,
                "input" : '{{ti.xcom_pull(task_ids="group_temperature_usa.download_data_to_a_gcs", key="gcs_file_location")}}'

        },
        {
                "task_name": "temperature_aus",
                "destination_table_name": f'{constants.GCP_PROJECT_ID}:{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyTemperature',
                "start_date_key" : "start_date_temp_aus",
                "polygon": "POLYGON((155 -44, 155 -8, 111 -8, 111 -44, 155 -44))",
                "grid_details" : [-8, -44, 155, 111],
                "staging_bucket" : constants.BUCKET + "era5_temperature",
                "gcs_file_name_prefix" : "temperature_data_aus",
                "ID_element" : 197109,
                "ID_data_provider" : 4,
                "input" : '{{ti.xcom_pull(task_ids="group_temperature_aus.download_data_to_a_gcs", key="gcs_file_location")}}'

        }
        ]



ERA5_GRID_QUERY = f"""select grid_ID as gridID,centroid_lat as latitude,
                                  centroid_lon as longitude from 
                                {constants.GCP_PROJECT_ID}.{constants.ERA5_STAGE_DATASET_NAME}.grids
                                where type='era5'"""

ERA5_GRID_GLOBAL_QUERY = f"""select grid_id as gridID,Latitude as latitude,
                                  Longitude as longitude from `climate-delta-dev.urd_stage.era5_grids_global`"""

PROXY_POWER_GEN_QUERY = """
CREATE TEMP FUNCTION  string_to_float (string_val string)
  RETURNS FLOAT64
  as (
    cast(concat(split(string_val,'_')[offset(2)],'.',split(string_val,'_')[offset(3)]) as FLOAT64)
  );
  insert into `{speed_stage_dataset}.tblHourlyWindProxyGeneration`
  (code_wban,Id,gridid,Name,Manufacturer,Turbine,Number_of_turbines,MeasureDateTime,MeasureValue,lower_speed,higher_speed,lower_power,higher_power,Proxy_Gen,ingestion_time)
  (
  with wind_farms AS (
    SELECT
      DISTINCT ID,case when Second_Name is null then Name else concat(Name,' ',IFNULL(Second_Name,'')) end as  name,
      latitude,
      longitude,
      Manufacturer,
      Turbine,
      Number_of_turbines,
      Total_power,
      Commissioning_date,
      Decommissioning_date,
      gridid,
      Code_Wban
    FROM
      `{speed_stage_dataset}.tblWindFarms`
    WHERE
      gridid is not null and Number_of_turbines is not null and Status='Production' )
  ,
  wind_farms_data as
  (SELECT
    DISTINCT Code_Wban,ID,name,
      Manufacturer,
      Turbine,
      Number_of_turbines,
      wf.gridid,
      tws.MeasureDateTime,tws.MeasureValue
  FROM
    wind_farms wf
  inner join (select GridID,MeasureDateTime,MeasureValue from `{urd_stage_dataset}.tblHourlyWind` inner join `{grid_table}` grids on GridID = grids.grid_id where  MeasureDateTime >= {measure_date}) tws
  on   wf.gridid = tws.gridid )
  ,final_data as (
  select code_wban,ID,name,
    wf.Manufacturer,
    wf.Turbine,
    wf.Number_of_turbines,
    gridid,
    wf.MeasureDateTime,wf.MeasureValue,
    concat('kW_at_',SPLIT(cast(wf.MeasureValue as string),'.')[offset(0)],'_',IFNULL((SPLIT(cast(wf.MeasureValue as string),'.')[SAFE_OFFSET(1)]),'0'),'_m_s') as  input_speed,
  case
  when MeasureValue <= floor(MeasureValue) + 0.5 and MeasureValue > floor(MeasureValue) then concat('kW_at_',cast(floor(MeasureValue) as string),'_0_m_s')
  when MeasureValue > floor(MeasureValue) + 0.5  and MeasureValue < ceiling(MeasureValue) then concat('kW_at_',cast(floor(MeasureValue) as string),'_5_m_s')
  end as lower_speed,
  case 
  when MeasureValue <= ceiling(MeasureValue) - 0.5 and  MeasureValue > floor(MeasureValue) then concat('kW_at_',cast(floor(MeasureValue) as string),'_5_m_s')
  when MeasureValue > ceiling(MeasureValue) - 0.5 and MeasureValue < ceiling(MeasureValue) then concat('kW_at_',cast(ceiling(MeasureValue) as string),'_0_m_s')
  end as higher_speed
  from wind_farms_data wf)
  ,last_table as (
  select  
  ID,name,code_wban,
    Manufacturer,
    Turbine,
    Number_of_turbines,
    gridid,fd.MeasureDateTime,MeasureValue,input_speed,lower_speed,higher_speed,
    sum(case 
    when fd.lower_speed = pc.power_curve then pc.val 
    end ) as lower_power,
    sum(
    case 
    when fd.higher_speed = pc.power_curve then pc.val 
    end )as higher_power
    from final_data fd 
  inner join `{speed_stage_dataset}.vw_power_curves` pc
  on fd.manufacturer = pc.manufacturer_name and fd.turbine = pc.Turbine_name and (lower_speed = pc.power_curve or higher_speed = pc.power_curve)
  group by ID,name,code_wban,Manufacturer,Turbine,Number_of_turbines,gridid,fd.MeasureDateTime,MeasureValue,input_speed,lower_speed,higher_speed
  )
  select code_wban,Id,gridid,Name,Manufacturer,Turbine,Number_of_turbines,MeasureDateTime,MeasureValue,lower_speed,higher_speed,lower_power,higher_power,
  `{speed_stage_dataset}.linear_interpolation`(string_to_float(lower_speed), lower_power, string_to_float(higher_speed), higher_power, MeasureValue) * Number_of_turbines as Proxy_Gen,
  current_timestamp as ingestion_time
  from last_table)
"""

PROXY_POWER_GEN_FOR_GLOBAL_QUERY = f"""
CREATE TEMP FUNCTION  string_to_float (string_val string)
  RETURNS FLOAT64
  as (
    cast(concat(split(string_val,'_')[offset(2)],'.',split(string_val,'_')[offset(3)]) as FLOAT64)
  );
  insert into `{constants.SPEED_STAGE_DATASET_NAME}.tblHourlyWindProxyGeneration`
  (code_wban,Id,gridid,Name,Manufacturer,Turbine,Number_of_turbines,MeasureDateTime,MeasureValue,lower_speed,higher_speed,lower_power,higher_power,Proxy_Gen,ingestion_time)
  (
  with wind_farms AS (
    SELECT
      DISTINCT ID,case when Second_Name is null then Name else concat(Name,' ',IFNULL(Second_Name,'')) end as  name,
      latitude,
      longitude,
      Manufacturer,
      Turbine,
      Number_of_turbines,
      Total_power,
      Commissioning_date,
      Decommissioning_date,
      gridid,
      Code_Wban
    FROM
      `{constants.SPEED_STAGE_DATASET_NAME}.tblWindFarms`
    WHERE
      gridid is not null and Number_of_turbines is not null and Status='Production' )
  ,
  wind_farms_data as
  (SELECT
    DISTINCT Code_Wban,ID,name,
      Manufacturer,
      Turbine,
      Number_of_turbines,
      wf.gridid,
      tws.MeasureDateTime,tws.MeasureValue
  FROM
    wind_farms wf
  inner join (select GridID,MeasureDateTime,MeasureValue from `{constants.ERA5_STAGE_DATASET_NAME}.tblHourlyWindGlobal` where  MeasureDateTime >= "{{{{ti.xcom_pull(task_ids="group_wind_global.download_global_wind_data_to_a_file",key="start_date")}}}}") tws
  on   wf.gridid = tws.gridid)
  ,final_data as (
  select code_wban,ID,name,
    wf.Manufacturer,
    wf.Turbine,
    wf.Number_of_turbines,
    gridid,
    wf.MeasureDateTime,wf.MeasureValue,
    concat('kW_at_',SPLIT(cast(wf.MeasureValue as string),'.')[offset(0)],'_',IFNULL((SPLIT(cast(wf.MeasureValue as string),'.')[SAFE_OFFSET(1)]),'0'),'_m_s') as  input_speed,
  case
  when MeasureValue <= floor(MeasureValue) + 0.5 and MeasureValue > floor(MeasureValue) then concat('kW_at_',cast(floor(MeasureValue) as string),'_0_m_s')
  when MeasureValue > floor(MeasureValue) + 0.5  and MeasureValue < ceiling(MeasureValue) then concat('kW_at_',cast(floor(MeasureValue) as string),'_5_m_s')
  end as lower_speed,
  case 
  when MeasureValue <= ceiling(MeasureValue) - 0.5 and  MeasureValue > floor(MeasureValue) then concat('kW_at_',cast(floor(MeasureValue) as string),'_5_m_s')
  when MeasureValue > ceiling(MeasureValue) - 0.5 and MeasureValue < ceiling(MeasureValue) then concat('kW_at_',cast(ceiling(MeasureValue) as string),'_0_m_s')
  end as higher_speed
  from wind_farms_data wf)
  ,last_table as (
  select  
  ID,name,code_wban,
    Manufacturer,
    Turbine,
    Number_of_turbines,
    gridid,fd.MeasureDateTime,MeasureValue,input_speed,lower_speed,higher_speed,
    sum(case 
    when fd.lower_speed = pc.power_curve then pc.val 
    end ) as lower_power,
    sum(
    case 
    when fd.higher_speed = pc.power_curve then pc.val 
    end )as higher_power
    from final_data fd 
  inner join `{constants.SPEED_STAGE_DATASET_NAME}.vw_power_curves` pc
  on fd.manufacturer = pc.manufacturer_name and fd.turbine = pc.Turbine_name and (lower_speed = pc.power_curve or higher_speed = pc.power_curve)
  group by ID,name,code_wban,Manufacturer,Turbine,Number_of_turbines,gridid,fd.MeasureDateTime,MeasureValue,input_speed,lower_speed,higher_speed
  )
  select code_wban,Id,gridid,Name,Manufacturer,Turbine,Number_of_turbines,MeasureDateTime,MeasureValue,lower_speed,higher_speed,lower_power,higher_power,
  `{constants.SPEED_STAGE_DATASET_NAME}.linear_interpolation`(string_to_float(lower_speed), lower_power, string_to_float(higher_speed), higher_power, MeasureValue) * Number_of_turbines as Proxy_Gen,
  current_timestamp as ingestion_time
  from last_table)
"""

PROXY_POWER_GEN_QUERY_DICT = {
    "usa": PROXY_POWER_GEN_QUERY.format(speed_stage_dataset='speed_stage',urd_stage_dataset='urd_stage',grid_table='urd_stage.era5_grids_usa',measure_date='\'{{ti.xcom_pull(task_ids="group_wind_usa.download_data_to_a_gcs",key="start_date")}}\''),
    "eu" : PROXY_POWER_GEN_QUERY.format(speed_stage_dataset='speed_stage',urd_stage_dataset='urd_stage',grid_table='urd_stage.era5_grids_eu',measure_date='\'{{ti.xcom_pull(task_ids="group_wind_europe.download_data_to_a_gcs",key="start_date")}}\''),
    "aus": PROXY_POWER_GEN_QUERY.format(speed_stage_dataset='speed_stage',urd_stage_dataset='urd_stage',grid_table='urd_stage.era5_grids_aus',measure_date='\'{{ti.xcom_pull(task_ids="group_wind_australia.download_data_to_a_gcs",key="start_date")}}\'')
}

WIND_DAILY_INDEX_QUERY = f'''
create temp table hourly_table as (
WITH  all_dates AS (
  SELECT MeasureDateTime FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('1950-01-01T00:00:00'), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR),INTERVAL 1 HOUR)) MeasureDateTime),

  all_stations AS (
  SELECT DISTINCT GridID FROM `urd_stage.tblHourlyWind` WHERE MeasureDateTime >= {{{{ti.xcom_pull(task_ids="group_wind_usa.download_data_to_a_gcs",key="start_date")}}}}),

  all_pairs AS (
  SELECT GridID, DATETIME(MeasureDateTime) AS MeasureDateTime FROM all_stations CROSS JOIN all_dates),

  local_table_data AS(
  SELECT tbl.GridID, DATETIME_TRUNC((DATETIME( TIMESTAMP(tbl.MeasureDateTime),grid.timezone)),HOUR) AS MeasureDateTime, MeasureValue FROM `urd_stage.tblHourlyWind` tbl
  INNER JOIN
    `urd_stage.grids` grid ON grid.grid_id = tbl.GridID WHERE grid.type = "era5" AND MeasureDateTime >= {{{{ti.xcom_pull(task_ids="group_wind_usa.download_data_to_a_gcs",key="start_date")}}}}),

  filled_data AS (
  SELECT
    COALESCE(tbl.GridID, al.GridID) AS GridID,
    al.MeasureDateTime AS Date_Ranges,
    COALESCE(tbl.MeasureDateTime, al.MeasureDateTime) AS MeasureDateTime,
    CASE WHEN tbl.MeasureDateTime IS NULL THEN LAG(AVG(MeasureValue)) OVER (ORDER BY al.GridID,al.MeasureDateTime ASC) ELSE AVG(MeasureValue) END AS Measure, MeasureValue as missing_measure
  FROM all_pairs al
  LEFT JOIN
    local_table_data tbl
  ON
    tbl.GridID = al.GridID
    AND tbl.MeasureDateTime = al.MeasureDateTime
    GROUP BY
    GridID,al.GridID, al.MeasureDateTime, tbl.MeasureDateTime,MeasureValue
  ORDER BY
    MeasureDateTime 
    )

SELECT
  GridID, MeasureDateTime as Date, AVG(Measure) as Measure FROM filled_data WHERE Measure IS NOT NULL GROUP BY GridID, Date
);
select GridID,date, measure from hourly_table;
'''

PRECIP_HOURLY_INDEX = f'''
begin
create temp table hourly_table as (
WITH  all_dates AS (
  SELECT MeasureDateTime FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('1950-01-01T00:00:00'), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR),INTERVAL 1 HOUR)) MeasureDateTime),

  all_stations AS (
  SELECT DISTINCT GridID FROM `urd_stage.tblHourlyPrecipitation` {{{{ti.xcom_pull(task_ids=group_precip_us.download_data_to_a_gcs",key="start_date")}}}}),

  all_pairs AS (
  SELECT GridID, DATETIME(MeasureDateTime) AS MeasureDateTime FROM all_stations CROSS JOIN all_dates),

  local_table_data AS(
  SELECT tbl.GridID, DATETIME_TRUNC((DATETIME( TIMESTAMP(tbl.MeasureDateTime),grid.timezone)),HOUR) AS MeasureDateTime, MeasureValue FROM `urd_stage.tblHourlyPrecipitation` tbl
  INNER JOIN
    `urd_stage.grids` grid ON grid.grid_id = tbl.GridID WHERE grid.type = "era5" {{{{ti.xcom_pull(task_ids=group_precip_us.download_data_to_a_gcs",key="start_date")}}}}),

  filled_data AS (
  SELECT
    COALESCE(tbl.GridID, al.GridID) AS GridID,
    al.MeasureDateTime AS Date_Ranges,
    COALESCE(tbl.MeasureDateTime, al.MeasureDateTime) AS MeasureDateTime,
    CASE WHEN tbl.MeasureDateTime IS NULL THEN LAG(AVG(MeasureValue)) OVER (ORDER BY al.GridID,al.MeasureDateTime ASC) ELSE AVG(MeasureValue) END AS Measure, MeasureValue as missing_measure
  FROM all_pairs al
  LEFT JOIN
    local_table_data tbl
  ON
    tbl.GridID = al.GridID
    AND tbl.MeasureDateTime = al.MeasureDateTime
    GROUP BY
    GridID,al.GridID, al.MeasureDateTime, tbl.MeasureDateTime,MeasureValue
  ORDER BY
    MeasureDateTime 
    )

SELECT
  GridID, MeasureDateTime as Date, AVG(Measure) as Measure FROM filled_data WHERE Measure IS NOT NULL GROUP BY GridID, Date
);

#creating daily hourly wind speed table
truncate table  urd_datamart.tblHourlyPrecipitation;
insert into urd_datamart.tblHourlyPrecipitation 
select GridID,date, measure from hourly_table;
end
'''

TEMPERATURE_HOURLY_INDEX = f'''
create temp table hourly_table as (
WITH  all_dates AS (
  SELECT MeasureDateTime FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('1950-01-01T00:00:00'), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR),INTERVAL 1 HOUR)) MeasureDateTime),

  all_stations AS (
  SELECT DISTINCT GridID FROM `urd_stage.tblHourlyTemperature`),

  all_pairs AS (
  SELECT GridID, DATETIME(MeasureDateTime) AS MeasureDateTime FROM all_stations CROSS JOIN all_dates),


  local_table_data AS(
  SELECT tbl.GridID, DATETIME_TRUNC((DATETIME( TIMESTAMP(tbl.MeasureDateTime),grid.timezone)),HOUR) AS MeasureDateTime, MeasureValue,country FROM `urd_stage.tblHourlyTemperature` tbl
  INNER JOIN
    `urd_stage.grids` grid ON grid.grid_id = tbl.GridID 
  INNER JOIN
    `urd_datamart.grids` d_grid ON d_grid.grid_id = grid.grid_id and d_grid.type = grid.type
  WHERE grid.type = "era5"),

  filled_data AS (
  SELECT
    COALESCE(tbl.GridID, al.GridID) AS GridID,
    al.MeasureDateTime AS Date_Ranges,
    COALESCE(tbl.MeasureDateTime, al.MeasureDateTime) AS MeasureDateTime,
    CASE WHEN tbl.MeasureDateTime IS NULL THEN LAG(AVG(MeasureValue)) OVER (ORDER BY al.GridID,al.MeasureDateTime ASC) ELSE AVG(MeasureValue) END AS Measure, MeasureValue as missing_measure, country
  FROM all_pairs al
  LEFT JOIN
    local_table_data tbl
  ON
    tbl.GridID = al.GridID
    AND tbl.MeasureDateTime = al.MeasureDateTime
    GROUP BY
    GridID,al.GridID, al.MeasureDateTime, tbl.MeasureDateTime,MeasureValue, country
  ORDER BY
    MeasureDateTime 
    )

SELECT
  GridID, MeasureDateTime as Date,
  CASE 
      WHEN country = "United States" THEN ((AVG(Measure) - 273.15) * 1.8 + 32)
      ELSE (AVG(Measure)-273.15) END as Measure 
  FROM filled_data WHERE Measure IS NOT NULL GROUP BY GridID, Date, country
);
select GridID,date, measure from hourly_table;
'''
