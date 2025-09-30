from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import json

# Constants
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'
BIGQUERY_CONN_ID = 'bigquery_default'
MINIO_CONN_ID = 'minio_s3'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    # 'depends_on_past': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='weather_dag',
    default_args=default_args,
    schedule='@daily',
    tags=['weather'],
    catchup=False
) as dag:

    @task()
    def test_minio_connection():
        """Test MinIO connection using boto3 with Airflow connection"""
        try:
            # Get MinIO connection from Airflow
            conn = BaseHook.get_connection(MINIO_CONN_ID)
            
            # Initialize boto3 client for MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url=conn.extra_dejson.get('endpoint_url', conn.host),
                aws_access_key_id=conn.extra_dejson.get('aws_access_key_id', conn.login),
                aws_secret_access_key=conn.extra_dejson.get('aws_secret_access_key', conn.password),
                region_name=conn.extra_dejson.get('region_name', 'us-east-1')
            )
            
            # Test connection by listing buckets
            response = s3_client.list_buckets()
            buckets = response['Buckets']
            print(f"✅ MinIO connection successful! Found {len(buckets)} buckets: {[bucket['Name'] for bucket in buckets]}")
            
            # Create a test file in the airflow bucket
            test_content = "MinIO connection test - Weather DAG validation"
            s3_client.put_object(
                Bucket='airflow',
                Key='test/connection_test.txt',
                Body=test_content
            )
            print("✅ Test file created successfully in MinIO bucket 'airflow'")
            
            # Verify the file was created
            response = s3_client.list_objects_v2(Bucket='airflow', Prefix='test/')
            files = [obj['Key'] for obj in response.get('Contents', [])]
            print(f"✅ Files in test folder: {files}")
            
            return {
                "status": "success",
                "buckets": len(buckets),
                "test_file_created": True,
                "message": "MinIO connection validated successfully"
            }
            
        except Exception as e:
            print(f"❌ MinIO connection failed: {str(e)}")
            raise Exception(f"MinIO connection test failed: {str(e)}")

    @task()
    def extract_weather_data(minio_test_result): 
        """extract weather data from the open-meteo API - only runs if MinIO test passes"""
        print(f"MinIO test result: {minio_test_result}")
        print("✅ MinIO test passed, proceeding with weather data extraction...")
        
        # http hook to extract weather data from the open-meteo API
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        #api endpoint
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true' 
        # request to the api with timeout
        response = http_hook.run(endpoint=endpoint, extra_options={'timeout': 30})
        
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"Failed to extract weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data): 
        """transform weather data """
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """load weather data into the postgres database"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT
            )
        """)

        # insert data into the table
        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (transformed_data['latitude'], transformed_data['longitude'], transformed_data['temperature'], transformed_data['windspeed'], transformed_data['winddirection'], transformed_data['weathercode']))

        conn.commit()
        cursor.close()
        conn.close()

    @task()
    def load_to_bigquery(transformed_data):
        """load weather data to BigQuery"""
        bq_hook = BigQueryHook(gcp_conn_id=BIGQUERY_CONN_ID)
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS `bigquery-course-464012.airflow.weather_data` (
            latitude FLOAT64,
            longitude FLOAT64,
            temperature FLOAT64,
            windspeed FLOAT64,
            winddirection FLOAT64,
            weathercode INT64,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        # Insert data
        insert_sql = """
        INSERT INTO `bigquery-course-464012.airflow.weather_data` 
        (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES ({}, {}, {}, {}, {}, {})
        """.format(
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        )
        
        # Execute queries
        bq_hook.run_query(sql=create_table_sql, use_legacy_sql=False)
        bq_hook.run_query(sql=insert_sql, use_legacy_sql=False)
        print("Weather data successfully loaded to BigQuery")

    minio_test = test_minio_connection()

    weather_data = extract_weather_data(minio_test)
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
    # load_to_bigquery(transformed_data)

