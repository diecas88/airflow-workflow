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

from credentials import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, REGION_NAME, OBJECT_KEY

# Replace with your specific details
GCP_PROJECT_ID = "bigquery-course-464012"
BQ_DATASET_TABLE = "airflow.users01"
GCS_BUCKET_NAME = "users01bucket"
GCS_FILE_PATH = "bq_export_data.csv"
S3_BUCKET_NAME = BUCKET_NAME
S3_KEY = "bq_data/bq_export_data.csv"

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
        doc_md="This is an empty operator that does nothing"
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
    
    # Call the task function
    start_task >> export_bq_to_gcs() >> [fail_copy, success_copy]

