from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
import csv
import io
import boto3
import time

from credentials import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, REGION_NAME, OBJECT_KEY, GLUE_JOB_NAME


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="glue_dag",
    default_args=default_args,
    schedule=None,  
    catchup=False,
    tags=["glue","aws"],
) as dag:

    start_task = EmptyOperator(
        task_id="start",
        doc_md="This starts the Glue job workflow"
    )

   
    def run_glue_job():

        # Create Glue client
        glue_client = boto3.client(
            'glue',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=REGION_NAME
        )
        
        try:
            # Start the Glue job
            response = glue_client.start_job_run(
                JobName= GLUE_JOB_NAME,
                
            )
            
            job_run_id = response['JobRunId']
            print(f"Started Glue job run: {job_run_id}")
            
            # Wait for job completion
            while True:
                job_run = glue_client.get_job_run(
                    JobName= GLUE_JOB_NAME,
                    RunId=job_run_id
                )
                
                status = job_run['JobRun']['JobRunState']
                print(f"Job status: {status}")
                
                if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                    break
                    
                time.sleep(30) 
            
            if status == 'SUCCEEDED':
                print("Glue job completed successfully!")
                return "SUCCESS"
            else:
                print(f"Glue job failed with status: {status}")
                raise Exception(f"Glue job failed with status: {status}")
                
        except Exception as e:
            print(f"Error running Glue job: {str(e)}")
            raise

    glue_job_task = PythonOperator(
        task_id="run_glue_job",
        python_callable=run_glue_job,
        doc_md="Execute AWS Glue job using boto3"
    )

    def failed_task():
        print("Glue job failed")
        return "Failed task"
    
    def success_task():
        print("Glue job completed successfully")
        return "Success task"
    
    
    fail_handler = PythonOperator(
        task_id="failed_task",
        python_callable=failed_task,
        doc_md="Handle Glue job failure",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    
    success_handler = PythonOperator(
        task_id="success_task",
        python_callable=success_task,
        doc_md="Handle Glue job success",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    start_task >> glue_job_task >> [fail_handler, success_handler]