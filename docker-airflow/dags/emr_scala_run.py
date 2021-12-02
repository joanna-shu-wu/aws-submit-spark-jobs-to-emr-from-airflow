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
[ec2-user@ip-172-31-80-26 dags]$ ls
emr_job_submission.py  emr_scala_run.py  __pycache__  tuto.py
[ec2-user@ip-172-31-80-26 dags]$ cat emr_scala_run.py
import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)


from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults

DEFAULT_ARGS={
    'owner':'joanna',
    'depend_on_past':False,
    'start_date':airflow.utils.dates.days_ago(0),
    'email':['joannawu0624@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False
}

class TimeSleepSensor(BaseSensorOperator):
    """
    Waits for specified time interval relative to task instance start

    :param sleep_duration: time after which the job succeeds
    :type sleep_duration: datetime.timedelta
    """

    @apply_defaults
    def __init__(self, sleep_duration, *args, **kwargs):
        super(TimeSleepSensor, self).__init__(*args, **kwargs)
        self.sleep_duration = sleep_duration
        self.poke_interval = kwargs.get('poke_interval',int(sleep_duration.total_seconds()))
        self.timeout = kwargs.get('timeout',int(sleep_duration.total_seconds()) + 30)


    def poke(self, context):
        ti = context["ti"]

        sensor_task_start_date = ti.start_date          
        target_time = sensor_task_start_date + self.sleep_duration

        self.log.info("Checking if the target time ({} - check:{}) has come - time to go: {}, start: {}, initial sleep_duration: {}"
                    .format(target_time, (timezone.utcnow() > target_time), (target_time-timezone.utcnow()), sensor_task_start_date, self.sleep_duration)
        )

        return timezone.utcnow() > target_time

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



dag=DAG(
    'emr_scala_run_dag',
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







JOB_FLOW_OVERRIDES = {
    "Name": "EMR_CLUSTER_Scala",
    "ReleaseLabel": "emr-6.4.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"},{"Name": "Hive"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export"
                    
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2KeyName": "midterm",
        "Ec2SubnetId":"subnet-40f9dd61",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)


sleep_task = TimeSleepSensor(
                    task_id="sleep_task",
                    sleep_duration=timedelta(minutes=8),  
                    mode='reschedule',
                    dag=dag
)
            
def retrieve_s3_files(**kwargs):
    s3_location=kwargs['dag_run'].conf['s3_location']
    kwargs['ti'].xcom_push(key='s3location',value=s3_location)




parse_request=PythonOperator(task_id="parse_request",
                            provide_context=True,
                            python_callable=retrieve_s3_files,
                            dag=dag
                            )

step_adder=EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    step=SPARK_STEPS,
    dag=dag

)

step_checker=EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{task_instance.xcom_pull('add_steps',key='return_value')[0]}}",
    aws_conn_id="aws_default",
    dag=dag
)


terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

create_emr_cluster.set_upstream(parse_request)
sleep_task.set_upstream(create_emr_cluster)
step_adder.set_upstream(sleep_task)
step_checker.set_upstream(step_adder)
terminate_emr_cluster.set_upstream(step_checker)
