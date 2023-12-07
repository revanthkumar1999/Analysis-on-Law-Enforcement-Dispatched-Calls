from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Define default_args and DAG
default_args = {
    'owner': 'rk99',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'etl_flow_dag_real_time',
    default_args=default_args,
    schedule_interval=None,  # You can set the schedule as per your needs
)

# Define the AWS Lambda function to invoke
load_lambda_function_name = 'load-api'
delete_lambda_function_name = 'del-obj-s3'


# Create a task to invoke the Lambda function
load_lambda = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda_to_load',
    function_name=load_lambda_function_name,
    aws_conn_id='aws_lambda_func',  # Use the AWS Connection Conn Id you provided
    dag=dag,
)


# SQL command to load data from Snowflake stage into a table
sql_command = """
COPY INTO LAW_ENF_DB_DEV.RAW.LE_CURR_STAGE
FROM @LAW_S3_STAGE
FILE_FORMAT = (FORMAT_NAME = LAW_FILE_FORMAT) ON_ERROR = 'CONTINUE';
"""

# Create a task to load data into Snowflake
load_into_snowflake = SnowflakeOperator(
    task_id='load_into_snowflake',
    snowflake_conn_id='snowflake-conn',  # Use your Snowflake connection ID
    sql=sql_command,
    dag=dag,
)


del_lambda = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda_to_delete',
    function_name=delete_lambda_function_name,
    aws_conn_id='aws_lambda_func',  # Use the AWS Connection Conn Id you provided
    dag=dag,
)


run_dbt_job_raw = DbtCloudRunJobOperator(
    task_id='run_dbt_job_raw',
    dbt_cloud_conn_id='dbt_cloud_default', 
    job_id=468332, 
    dag=dag,
)

# SQL command to load data from Snowflake stage into a table
sql_command_del = """
TRUNCATE TABLE LAW_ENF_DB_DEV.RAW.LE_CURR_STAGE;
"""

# Delete Stage Table data from Snowflake
del_from_snowflake = SnowflakeOperator(
    task_id='del_from_snowflake',
    snowflake_conn_id='snowflake-conn',  # Use your Snowflake connection ID
    sql=sql_command_del,
    dag=dag,
)


run_dbt_job_curation = DbtCloudRunJobOperator(
    task_id='run_dbt_job_curation',
    dbt_cloud_conn_id='dbt_cloud_default', 
    job_id=468343, 
    dag=dag,
)


run_dbt_job_data_models = DbtCloudRunJobOperator(
    task_id='run_dbt_job_data_models',
    dbt_cloud_conn_id='dbt_cloud_default', 
    job_id=468344, 
    dag=dag,
)


# Set task dependencies
# (This line is actually not setting any dependencies and can be omitted if no dependencies are there)
 
load_lambda >> load_into_snowflake >> run_dbt_job_raw >> [del_lambda,del_from_snowflake] >> run_dbt_job_curation >> run_dbt_job_data_models


