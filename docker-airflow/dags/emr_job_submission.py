import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
import boto3

DEFAULT_ARGS={
    'owner':'joanna',
    'depend_on_past':False,
    'start_date':airflow.utils.dates.days_ago(0),
    'email':['joannawu0624@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False
}

SPARK_STEPS = [
    {
        'Name': 'midterm',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--class', 'Driver.MainApp',
                '--master','yarn',
                '--deploy-mode','cluster',
                '--num-executors','2',
                '--driver-memory','512m',
                '--executor-memory','3g',
                '--executor-cores','2',
                's3://joanna-wcd-midterm/spark-engine_2.12-0.0.1.jar',
                '-p','joannaMidterm',
                '-i', "{{task_instance.xcom_pull('source_file_type',key='fileType')}}",
                '-o','parquet',
                '-s',"{{task_instance.xcom_pull('parse_request',key='s3location')}}",
                '-c',"{{task_instance.xcom_pull('source_file_type',key='partitionColumn')}}",
                '-d',"{{task_instance.xcom_pull('source_file_type',key='outputBucket')}}",
                '-m','append',
                '--input-options','header=true'


                ]
        }
    }
]


CLUSTER_ID='j-IEQJCUW5YZU9'

def retrieve_s3_files(**kwargs):
    s3_location=kwargs['dag_run'].conf['s3_location']
    kwargs['ti'].xcom_push(key='s3location',value=s3_location)


dag=DAG(
    'emr_job_flow_manual_step_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None
    )

def get_file_type(**kwargs):
    s3_location=kwargs['dag_run'].conf['s3_location']
    if s3_location.endswith('.csv'):
        kwargs['ti'].xcom_push(key='fileType',value='Csv')
        kwargs['ti'].xcom_push(key='partitionColumn',value='job')
        kwargs['ti'].xcom_push(key='outputBucket',value='s3://midterm-output/banking')
    elif s3_location.endswith('.json'):
        kwargs['ti'].xcom_push(key='fileType',value='Json')
        kwargs['ti'].xcom_push(key='partitionColumn',value='name')
        kwargs['ti'].xcom_push(key='outputBucket',value='s3://midterm-output/people')
    else:
        print("File type is not supported")

source_file_type=PythonOperator(task_id="source_file_type",
                            provide_context=True,
                            python_callable=get_file_type,
                            dag=dag
                            )


parse_request=PythonOperator(task_id="parse_request",
                            provide_context=True,
                            python_callable=retrieve_s3_files,
                            dag=dag
                            )

step_adder=EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id=CLUSTER_ID,
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag

)

step_checker=EmrStepSensor(
    task_id='watch_step',
    job_flow_id=CLUSTER_ID,
    step_id="{{task_instance.xcom_pull('add_steps',key='return_value')[0]}}",
    aws_conn_id="aws_default",
    dag=dag
     )

step_adder.set_upstream(parse_request)
step_checker.set_upstream(step_adder)