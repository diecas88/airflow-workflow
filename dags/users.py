from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import json
import boto3
import csv
import io
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from credentials import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, REGION_NAME, OBJECT_KEY

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# Define the DAG
with DAG(
    dag_id='users_dag',
    default_args=default_args,
    schedule='@daily',
    tags=['users'],
    catchup=False
) as dag:     

    @task()
    def load_users_api():
        """load users data from the api"""
        http_hook = HttpHook(method='GET', http_conn_id='users_api')
        response = http_hook.run(endpoint='/api/?results=100')
        return response.json()
    
    @task()
    def load_users_to_s3(users_data):
        try:
            """load users data to the s3 bucket"""
            s3_client = boto3.client('s3', 
                region_name=REGION_NAME, 
                aws_access_key_id=AWS_ACCESS_KEY_ID, 
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=OBJECT_KEY,
                Body=json.dumps(users_data),
                ContentType='application/json'
            )

            print(f"Users data loaded to the s3 bucket: {BUCKET_NAME}/{OBJECT_KEY}")    
        except Exception as e:
            print(f"Error loading users data to the s3 bucket: {e}")
            raise e

    # Get data from BigQuery (use operator directly, not inside task)
    get_users_bq = BigQueryGetDataOperator(
        task_id='get_users_bq',
        project_id='bigquery-course-464012',
        dataset_id='airflow',
        table_id='users01',
        max_results=1000,
        gcp_conn_id='bigquery_default'  # Use your existing connection
    ) 
        
    @task()
    def load_users_bq_to_s3():        
        try:
            """load users data from the bigquery table to the s3 bucket"""
            # This task runs after BigQuery operator completes
            # We'll fetch the data directly from BigQuery here
            
            
            
            # Create BigQuery hook
            bq_hook = BigQueryHook(
                gcp_conn_id='bigquery_default',
                use_legacy_sql=False
            )
            
            # Query BigQuery table
            query = """
            SELECT id, first_name, last_name, email, gender, 
                   address, longitude, latitude, country, city, birthday
            FROM `bigquery-course-464012.airflow.users01`
            LIMIT 1000
            """
            
            # Execute query and get data
            results = bq_hook.get_pandas_df(sql=query)
            
            # Define CSV headers
            headers = [
                'id', 'first_name', 'last_name', 'email', 'gender', 
                'address', 'longitude', 'latitude', 'country', 'city', 'birthday'
            ]
            
            # Create CSV content using built-in csv module
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            
            # Write header row
            writer.writerow(headers)
            
            # Write data rows
            for _, row in results.iterrows():
                writer.writerow(row.values)
            
            csv_content = csv_buffer.getvalue()
            
            s3_client = boto3.client('s3', 
                region_name=REGION_NAME, 
                aws_access_key_id=AWS_ACCESS_KEY_ID, 
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            
            # Upload CSV to S3 with timestamp
            s3_key = f"bigquery_export/users_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=csv_content,
                ContentType='text/csv'
            )
            
            print(f"BigQuery data exported to S3: {BUCKET_NAME}/{s3_key}")
            print(f"Total rows exported: {len(results)}")
            
        except Exception as e:
            print(f"Error loading users data from the bigquery table to the s3 bucket: {e}")
            raise e
        

    # Task dependencies - Sequential workflow
    # Step 1: API workflow
    users_data = load_users_api()
    load_users_to_s3(users_data)

    # Step 2: BigQuery workflow (runs after API workflow completes)
    # Note: BigQuery operator runs independently
    load_users_bq_to_s3()
    # Set explicit dependencies for sequential execution
    # API workflow must complete before BigQuery workflow starts
    # Note: Tasks will run in the order they are defined
    # load_users_to_s3 >> get_users_bq >> load_users_bq_to_s3  # This causes the error

    
