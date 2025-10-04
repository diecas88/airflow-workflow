from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
import csv
import io
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import boto3

from credentials import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, REGION_NAME, OBJECT_KEY, GCP_PROJECT_ID, BQ_DATASET_TABLE, GCS_BUCKET_NAME, GCS_FILE_PATH, S3_BUCKET_NAME, S3_KEY

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id="users_bq_to_s3",
    default_args=default_args,
    schedule=None,  
    catchup=False,
    tags=["bigquery", "s3", "transfer"],
) as dag:

    start_task = EmptyOperator(
        task_id="start",
        doc_md="this starts the task export_bq_to_gcs"
    )

    def failed_task():
        return print("Failed task")
    
    def success_task():
        return print("Success task")
    
    @task()
    def export_bq_to_gcs():
        """Export BigQuery data to GCS bucket"""
        try:
            # Get data from BigQuery
            bq_hook = BigQueryHook(gcp_conn_id="bigquery_default", use_legacy_sql=False, location="US")
            query = f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET_TABLE}`"
            results = bq_hook.get_records(sql=query)
            
            # Convert to CSV using built-in csv module
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            
            # Write header
            if results:
                # Get column names from BigQuery
                columns = ['id', 'first_name', 'last_name', 'email', 'gender', 'address', 'longitude', 'latitude', 'country', 'city', 'birthday']
                writer.writerow(columns)
                # Write data rows
                for row in results:
                    writer.writerow(row)
            
            csv_data = csv_buffer.getvalue()
            
            # Upload to GCS
            gcs_hook = GCSHook(gcp_conn_id="bigquery_default")
            gcs_hook.upload(
                bucket_name=GCS_BUCKET_NAME,
                object_name=GCS_FILE_PATH,
                data=csv_data,
                mime_type='text/csv'
            )
            
            print(f"SUCCESS: BigQuery data exported to GCS: gs://{GCS_BUCKET_NAME}/{GCS_FILE_PATH}")
            print(f"Rows exported: {len(results)}")
            
        except Exception as e:
            print(f"ERROR: {e}")
            raise e
    
    fail_copy = PythonOperator(
        task_id="failed_task",
        python_callable=failed_task,
        doc_md="This is a Python operator with an empty function",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    success_copy = PythonOperator(
        task_id="success_task",
        python_callable=success_task,
        doc_md="This is a Python operator with an empty function",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    start_s3_task = EmptyOperator(
        task_id="start_s3_task",
        doc_md="this starts the task load_users_to_s3"
    )

    @task()
    def transfer_gcs_to_s3():
        """Transfer file from GCS to S3"""
        try:
            # Download from GCS
            gcs_hook = GCSHook(gcp_conn_id="bigquery_default")
            file_data = gcs_hook.download(
                bucket_name=GCS_BUCKET_NAME,
                object_name=GCS_FILE_PATH
            )
            
            # Upload to S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME
            )
            
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=S3_KEY,
                Body=file_data
            )
            
            print(f"SUCCESS: File transferred from GCS to S3: s3://{S3_BUCKET_NAME}/{S3_KEY}")
            
        except Exception as e:
            print(f"ERROR: {e}")
            raise e
    # Call the task function with conditional logic
    start_task >> export_bq_to_gcs() >> [fail_copy, success_copy]
    success_copy >> start_s3_task >> transfer_gcs_to_s3()