from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="excel_to_postgres_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['ETL', 'PostgreSQL', 'Python'],
    doc_md="""
    ### Excel to PostgreSQL ETL Pipeline
    
    This DAG orchestrates the process of extracting data from Excel files,
    transforming it, and loading it into a PostgreSQL database.
    It runs a Python script located in the 'scripts' folder.
    """
) as dag:

    # Task to execute the main ETL Python script.
    # Assumes the script is accessible from the project's root directory.
    run_etl_script = BashOperator(
        task_id="run_excel_to_postgres_etl",
        bash_command="python scripts/excel_to_postgres_etl.py"
    )

    # A simple notification task to confirm successful completion of the ETL process.
    # In a real-world scenario, this could be a Slack or Email notification.
    etl_finished = BashOperator(
        task_id="etl_finished_successfully",
        bash_command="echo 'ETL pipeline finished successfully!'"
    )

    run_etl_script >> etl_finished