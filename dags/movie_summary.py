from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator


def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op


with DAG(
        'Movie_summary',
        default_args={
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(seconds=3)
        },
        max_active_runs=1,
        max_active_tasks=3,
        description='About movie',
        schedule="10 2 * * *",
        start_date=datetime(2024, 7, 24),
        catchup=True,
        tags=['movie', 'api', 'amt'],
) as dag:
    # FUNCTION

    # OPERATOR

    task_apply = EmptyOperator(
        task_id='apply.type',

    )

    task_merge = EmptyOperator(
        task_id='merge',

    )

    task_de_dup = EmptyOperator(
        task_id='de_dup',

    )

    task_summary = EmptyOperator(
        task_id='make.summary',

    )

    start = gen_emp('start')
    end = gen_emp('end', rule="all_done")

    start >> task_apply >> task_merge >> task_de_dup >> task_summary >> end
