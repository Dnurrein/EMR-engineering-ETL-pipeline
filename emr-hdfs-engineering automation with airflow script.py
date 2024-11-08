from airflow import DAG  # Importing DAG to define workflow structure in Airflow
from datetime import timedelta, datetime  # Importing timedelta and datetime for timing and scheduling
from airflow.operators.dummy_operator import DummyOperator  # Importing DummyOperator to represent no-op tasks
import boto3  # Importing boto3 for interacting with AWS services
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,  # Operator to create an EMR cluster
    EmrAddStepsOperator,  # Operator to add steps to an existing EMR cluster
    EmrTerminateJobFlowOperator  # Operator to terminate an EMR cluster
)
from airflow.providers.amazon.aws.sensors.emr import (
    EmrJobFlowSensor,  # Sensor to monitor EMR cluster state
    EmrStepSensor  # Sensor to monitor the state of steps in an EMR cluster
)

# EMR cluster configuration
job_flow_overrides = {
    "Name": "emr_sparkjob_cluster",  # Name of the EMR cluster
    "ReleaseLabel": "emr-6.13.0",  # EMR release version
    "LogUri": "s3://store-emrsparkjob-logs",  # S3 bucket for storing logs
    "VisibleToAllUsers": False,  # Set visibility to false
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],  # Applications to install on the cluster
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",  # Master node configuration
                "Market": "ON_DEMAND",  # Instance pricing model
                "InstanceRole": "MASTER",  # Role for the node (Master)
                "InstanceType": "m5.xlarge",  # Instance type for the master node
                "InstanceCount": 1,  # Number of master nodes
            },
            {
                "Name": "Core node",  # Core node configuration
                "Market": "ON_DEMAND",  # Instance pricing model
                "InstanceRole": "CORE",  # Role for the node (Core)
                "InstanceType": "m5.xlarge",  # Instance type for core nodes
                "InstanceCount": 2,  # Number of core nodes
            },
        ],
        "Ec2SubnetId": "subnet-02f1a19231b8840ec",  # Subnet ID for EC2 instances
        "Ec2KeyName": 'fbl_emr_keypair',  # EC2 key pair for SSH access
        "KeepJobFlowAliveWhenNoSteps": True,  # Keep the cluster alive even if there are no steps
        "TerminationProtected": False,  # Allow cluster termination when completed
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",  # IAM role for EC2 instances
    "ServiceRole": "EMR_DefaultRole",  # IAM role for EMR service
}

# Spark steps for data extraction and transformation
SPARK_STEPS_EXTRCATION = [
    {
        "Name": "Extract data",  # Step name for data extraction
        "ActionOnFailure": "CANCEL_AND_WAIT",  # Action if the step fails
        "HadoopJarStep": {
            "Jar": "s3://us-west-2.elasticmapreduce/lib/script-runner/script-runner.jar",  # Jar to run custom scripts
            "Args": ["s3://redfin-emr-engineering-data-project-/script/ingest.sh"]  # Script for data extraction
        },
    }
]

SPARK_STEPS_TRANSFORMATION = [
    {
        "Name": "Transform ingested data",  # Step name for data transformation
        "ActionOnFailure": "CONTINUE",  # Action if the step fails
        "HadoopJarStep": {
            "Jar": "command-runner.jar",  # Jar to run Spark commands
            "Args": ["spark-submit", "s3://redfin-emr-engineering-data-project-/script/Analysis script.py"]  # Command for Spark job
        },
    }
]

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # DAG owner name
    'depends_on_past': False,  # Task does not depend on previous runs
    'start_date': datetime(2023, 11, 7),  # DAG start date
    'email': ['dukenurrein@gmail.com'],  # Email for notifications
    'email_on_failure': False,  # Disable email on task failure
    'email_on_retry': False,  # Disable email on task retry
    'retries': 2,  # Number of retries on failure
    'retry_delay': timedelta(seconds=15)  # Delay between retries
}

# Defining the DAG structure
with DAG('redfin_emr_engineering_spark_job_dag',
            default_args=default_args,
            catchup=False) as dag:  # catchup=False to skip missed runs

        # Start pipeline task (no-op task)
        start_pipeline = DummyOperator(task_id="tsk_start_pipeline") 

        # Create EMR cluster task
        create_emr_cluster = EmrCreateJobFlowOperator(
            task_id="tsk_create_emr_cluster",  # Task ID for cluster creation
            job_flow_overrides=job_flow_overrides,  # EMR cluster configuration
            aws_conn_id="aws_default",  # AWS connection ID
            emr_conn_id="emr_default"  # EMR connection ID
        )

        # Check if the EMR cluster has been created
        is_emr_cluster_created = EmrJobFlowSensor(
            task_id="tsk_is_emr_cluster_ready",  # Task ID for checking cluster state
            job_flow_id="{{ti.xcom.pull(task_id='tsk_create_emr_cluster', keys='return_value')}}",  # Get job flow ID from previous task
            target_states={"WAITING"},  # Desired state for the cluster
            timeout=3600,  # Timeout for waiting
            poke_interval=720,  # Interval between checks
            mode='poke',  # Polling mode for sensor
        )

        # Start data extraction after cluster is ready
        extraction_steps = EmrAddStepsOperator(
            task_id="tsk_extraction_step",  # Task ID for extraction step
            job_flow_id="{{ti.xcom.pull(task_id='tsk_create_emr_cluster', keys='return_value')}}",  # Get job flow ID
            aws_conn_id="aws_default",  # AWS connection ID
            steps=SPARK_STEPS_EXTRCATION,  # Steps for extraction
        )

        # Check if data extraction is completed
        is_extraction_completed = EmrStepSensor(
            task_id="tsk_is_extraction_completed",  # Task ID for checking extraction status
            job_flow_id="{{ti.xcom.pull(task_id='tsk_create_emr_cluster', keys='return_value')}}",  # Get job flow ID
            step_id="{{ti.xcom.pull(task_id='tsk_extraction_step')[0]}}",  # Get step ID for the extraction step
            target_states={"COMPLETED"},  # Desired state for extraction
            timeout=3600,  # Timeout for waiting
            poke_interval=10,  # Interval between checks
        )

        # Start data transformation after extraction is complete
        transformation_step = EmrAddStepsOperator(
            task_id="tsk_transformation_step",  # Task ID for transformation step
            job_flow_id="{{ti.xcom.pull(task_id='tsk_create_emr_cluster', keys='return_value')}}",  # Get job flow ID
            steps=SPARK_STEPS_TRANSFORMATION,  # Steps for transformation
        )

        # Check if data transformation is completed
        is_transformation_completed = EmrStepSensor(
            task_id="tsk_is_transformation_completed",  # Task ID for checking transformation status
            job_flow_id="{{ti.xcom.pull(task_id='tsk_create_emr_cluster', keys='return_value')}}",  # Get job flow ID
            step_id="{{ti.xcom.pull(task_id='tsk_transformation_step')[0]}}",  # Get step ID for the transformation step
            target_states={"COMPLETED"},  # Desired state for transformation
            timeout=3600,  # Timeout for waiting
            poke_interval=10,  # Interval between checks
        )

        # Terminate the EMR cluster after all tasks are completed
        terminate_cluster = EmrTerminateJobFlowOperator(
            task_id='tsk_terminate_emr_cluster',  # Task ID for terminating cluster
            job_flow_id="{{ti.xcom.pull(task_id='tsk_create_emr_cluster', keys='return_value')}}",  # Get job flow ID
        )

        # Check if the EMR cluster has been terminated
        is_emr_cluster_terminated = EmrJobFlowSensor(
            task_id="tsk_is_emr_cluster_terminated",  # Task ID for checking cluster termination status
            job_flow_id="{{ti.xcom.pull(task_id='tsk_create_emr_cluster', keys='return_value')}}",  # Get job flow ID
            target_states={"TERMINATED"},  # Desired state for termination
            timeout=3600,  # Timeout for waiting
            poke_interval=720,  # Interval between checks
            mode='poke',  # Polling mode for sensor
        )

        # End pipeline task (no-op task)
        end_pipeline = DummyOperator(task_id="tsk_end_pipeline")  

        # Defining the sequence of task dependencies
        start_pipeline >> create_emr_cluster >> is_emr_cluster_created >> extraction_steps >> is_extraction_completed >> transformation_step >> is_transformation_completed >> terminate_cluster >> is_emr_cluster_terminated >> end_pipeline
