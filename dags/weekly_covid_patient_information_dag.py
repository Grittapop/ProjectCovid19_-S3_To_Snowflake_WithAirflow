from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import json
import boto3
import pytz



# URL
COVID19TH_API_URL = "https://covid19.ddc.moph.go.th/api/Cases/today-cases-line-lists"



s3_client = boto3.client("s3",
                        aws_access_key_id= '**************************',
                        aws_secret_access_key= '**********************************')


# Datetime now thailand
thai_timezone = pytz.timezone('Asia/Bangkok')
now = datetime.now(tz=thai_timezone)
date_time_now  = now.strftime("%Y-%m-%d %H:%M:%S")



def transform_load_data():
    r = requests.get(COVID19TH_API_URL)
    data_covid = r.json()

    df = pd.DataFrame(data_covid)
    
    columns = ["year","weeknum","gender","age_number","age_range","job","risk","patient_type","province","reporting_group","region_odpc","region","update_date"]
    new_columns = ["Year","Weeknum","Gender","Age (Years)","Age_Range (Years)","Job","Risk","Patient_Type","Province","Reporting_Group","Region_Odpc","Region","Update_Date"]

    # Change column name
    for new_column,column in zip(new_columns,columns):
        df[new_column] = df[column]      

    # Delete old columns
    for column in columns:
        df.drop(columns=[column], inplace=True ,axis=1)

    # Remove the word "ปี"
    df["Age_Range (Years)"] = df["Age_Range (Years)"].str.replace('ปี', '')

    # Change type of age
    df["Age (Years)"] = df["Age (Years)"].replace({'0.0': 0})  # Replace the value '0.0' with 0.
    df["Age (Years)"] = df["Age (Years)"].astype(int)

    # Change type of Update_Date
    df['Update_Date'] = pd.to_datetime(df['Update_Date'])
    
    csv_data = df.to_csv(index=False)
    
    # Upload CSV to S3
    bucket_name = "bucket-weekly-covid-patient-information"
    object_key = "weekly_covid_patient_information.csv"
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_data)
     
    s3_data_url = "s3://bucket-weekly-covid-patient-information/weekly_covid_patient_information.csv"
    
    return s3_data_url 



default_args = {
    "owner": "stellar",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)

}
    



with DAG("weekly_covid_patient_information_dag",
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval="@once",
        catchup=False) as dag:



        t1 = HttpSensor(
            task_id ="weekly_covid_patient_information_api_ready",
            http_conn_id="weekly_covid_conn_id",
            endpoint="/api/Cases/today-cases-line-lists"
        
        )



        t2 = PythonOperator(
            task_id= "transform_load_data_to_s3",
            python_callable=transform_load_data
        
        )



        t3 = SnowflakeOperator(
            task_id = "create_snowflake_database",
            snowflake_conn_id = "snowflake_conn_id",           
            sql = """
                    CREATE DATABASE IF NOT EXISTS Project ;
            """
        )



        t4 = SnowflakeOperator(
            task_id = "create_snowflake_schema",
            snowflake_conn_id = "snowflake_conn_id",           
            sql = """
                    CREATE SCHEMA IF NOT EXISTS CovidTH ;
            """
        )



        t5 = SnowflakeOperator(
            task_id = "create_snowflake_table",
            snowflake_conn_id = "snowflake_conn_id",           
            sql = """
                    CREATE TABLE IF NOT EXISTS weekly_covid_patient_information (
                        Year int,
                        Weeknum int,
                        Gender varchar,
                        "Age (Years)" varchar,
                        "Age_Range (Years)" varchar,
                        Job varchar,
                        Risk varchar,
                        "Patient_Type" varchar,
                        Province varchar,
                        "Reporting_Group" varchar,
                        "Region_Odpc" varchar,
                        Region varchar,
                        "Update_Date" datetime

                    );
            """
        )



        t6 = SnowflakeOperator(
            task_id = "create_snowflake_external_stage",
            snowflake_conn_id = "snowflake_conn_id",           
            sql = """
                    CREATE OR REPLACE STAGE s3_stage
                        URL ='{{ti.xcom_pull("transform_load_data_to_s3")}}'
                        credentials=(aws_key_id='**************' aws_secret_key='**************************');
                
            """
        )



        t7 = SnowflakeOperator(
            task_id = "create_snowflake_file_format",
            snowflake_conn_id = "snowflake_conn_id",           
            sql = """
                    CREATE OR REPLACE file format csv_format type = 'csv' compression = 'auto' 
                        field_delimiter = ',' record_delimiter = '\n'
                        skip_header = 1 trim_space = false;
                                    
            """
        )



        t8 = SnowflakeOperator(
            task_id = "load_data_into_the_table",
            snowflake_conn_id = "snowflake_conn_id",           
            sql = """
                    COPY INTO weekly_covid_patient_information from @s3_stage file_format=csv_format;

            """
        )



        t9 = SlackWebhookOperator(
            task_id="slack_notification",
            slack_webhook_conn_id="slack_webhook_conn_id",
            message="Loaded data into snowflake successfully on " + date_time_now,
            channel="#weekly-covid-patient-information",
        )
            


t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9


   