from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_hello():
    """Prints 'Hello, World!'"""
    print("Hello, World!")


with DAG(
    dag_id="hello_world_dag",
    schedule=None,  # Set to None for manual triggering, or a cron schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,  #  Don't run past runs when the DAG is unpaused
    tags=["example"],
    doc_md="A simple DAG to print 'Hello, World!'",
) as dag:
    # Define a BashOperator to execute the 'echo' command
    hello_bash_task = BashOperator(
        task_id="hello_bash",
        bash_command="echo 'Hello, World from Bash!'",
        doc_md="Prints Hello World from bash",
    )

    # Define a PythonOperator to execute the 'print_hello' function
    hello_python_task = PythonOperator(
        task_id="hello_python",
        python_callable=print_hello,
        doc_md="Prints Hello World from python",
    )

    # Set task dependencies: hello_bash_task runs first, then hello_python_task
    hello_bash_task >> hello_python_task

