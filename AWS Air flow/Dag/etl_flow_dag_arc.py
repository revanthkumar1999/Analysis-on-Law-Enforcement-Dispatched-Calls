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
    'etl_flow_dag_archive',
    default_args=default_args,
    schedule_interval=None,  # You can set the schedule as per your needs
)

# SQL command to load data from Snowflake stage into a table
sql_command_del = """
Truncate TABLE LAW_ENF_DB_DEV.RAW_RAW.LE_ARC;
"""

# Delete Stage Table data from Snowflake
del_from_snowflake_arc = SnowflakeOperator(
    task_id='del_from_snowflake_arc',
    snowflake_conn_id='snowflake-conn',  # Use your Snowflake connection ID
    sql=sql_command_del,
    dag=dag,
)


# SQL command to load data from Snowflake stage into a table
sql_command_load_arc = """
COPY INTO LAW_ENF_DB_DEV.RAW_RAW.LE_ARC
FROM @LAW_S3_ARC
FILE_FORMAT = (FORMAT_NAME = LAW_FILE_FORMAT) ON_ERROR = 'CONTINUE';
"""

# Create a task to load data into Snowflake
load_into_snowflake_arc = SnowflakeOperator(
    task_id='load_into_snowflake_arc',
    snowflake_conn_id='snowflake-conn',  # Use your Snowflake connection ID
    sql=sql_command_load_arc,
    dag=dag,
)


run_dbt_job_curation_arc = DbtCloudRunJobOperator(
    task_id='run_dbt_job_curation_arc',
    dbt_cloud_conn_id='dbt_cloud_default', 
    job_id=468579, 
    dag=dag,
)


run_dbt_job_data_model_arc = DbtCloudRunJobOperator(
    task_id='run_dbt_job_data_model_arc',
    dbt_cloud_conn_id='dbt_cloud_default', 
    job_id=468589, 
    dag=dag,
)


# Set task dependencies
 
del_from_snowflake_arc >> load_into_snowflake_arc >> run_dbt_job_curation_arc >> run_dbt_job_data_model_arc 


