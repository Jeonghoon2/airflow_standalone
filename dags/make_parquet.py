from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable


def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op


with DAG(
        'make_parquet',
        default_args={
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(seconds=3)
        },
        description='hello world DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2024, 7, 10),
        catchup=True,
        tags=['parquet'],
) as dag:
    # 검증 완
    task_check_done = BashOperator(
        task_id='check.done',
        bash_command="""
        DONE_FILE=~/data/done/_import/{{ds_nodash}}/_DONE
        bash {{var.value.SH_HOME}}/simple_done_check.sh $DONE_FILE
        """
    )

    task_parquet = BashOperator(
        task_id='to.parquet',
        bash_command="""
        echo "to.parquet"

        READ_PATH=~/data/csv/{{ds_nodash}}/csv.csv
        SAVE_PATH=~/data/parquet/

        mkdir -p $SAVE_PATH

        python {{var.value.OP_HOME}}/csv2parquet.py $READ_PATH $SAVE_PATH
        """
    )
    task_make_done = BashOperator(
        task_id='make.done',
        bash_command="""
        PARQUET_DONE_PATH=~/data/done/_parquet/{{ds_nodash}}
        mkdir -p $PARQUET_DONE_PATH
        echo "PATH=$PARQUET_DONE_PATH"
        touch $PARQUET_DONE_PATH/_DONE
        """
    )
    task_err = BashOperator(
        task_id='err.report',
        bash_command="""
        """,
        trigger_rule="one_failed"
    )

    task_end = gen_emp('end', 'all_done')
    task_start = gen_emp('start')

    task_start >> task_check_done >> task_parquet >> task_make_done >> task_end
    task_check_done >> task_err >> task_end
