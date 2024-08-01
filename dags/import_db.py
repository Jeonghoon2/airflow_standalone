from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
        'import_db',
        default_args={
            'depends_on_past': False,
            'retries': 0,
            'retry_delay': timedelta(seconds=3)
        },
        description='hello world DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2024, 7, 12),
        catchup=True,
        tags=['simple', 'bash', 'etl', 'shop'],
) as dag:
    task_check = BashOperator(
        task_id="Check",
        bash_command="bash {{var.value.SH_HOME}}/simple_done_check.sh ~/data/done/{{ds_nodash}}/_DONE"
    )

    task_csv = BashOperator(
        task_id="to_csv",
        bash_command="""
            echo "to.csv"

            U_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}
            CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv

            mkdir -p $CSV_PATH
            cat $U_PATH | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_FILE}
            echo "transfer done"
            """
    )

    task_create_table = BashOperator(
        task_id="create.table",
        bash_command="""
            echo "create.table"
            echo "{{var.value.SQL_HOME}}/create_to_talbe.sql"
            sh {{var.value.SH_HOME}}/create_table.sh {{var.value.SQL_HOME}}/create_to_table
            echo "create done"
            """
    )

    task_tmp = BashOperator(
        task_id="to_tmp",
        bash_command="""
                echo "tmp"
                CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv
                echo $CSV_FILE
                bash {{var.value.SH_HOME}}/to_tmp.sh $CSV_FILE {ds}
            """
    )

    task_base = BashOperator(
        task_id="to_base",
        bash_command="""
                echo "base"
                bash {{var.value.SH_HOME}}/to_bash.sh {{ ds }}
            """
    )

    task_done = BashOperator(
        task_id="make_done",
        bash_command="""
                figlet "DONE"
                DONE_PATH=~/data/done/_import/{{ds_nodash}}
                mkdir -p $DONE_PATH
                echo "IMPORT_DONE_PATH=$DONE_PATH"
                touch $DONE_PATH/_DONE
            """
    )

    task_err = BashOperator(
        task_id="err_report",
        bash_command="""
            echo "err report"
           """,
        trigger_rule="one_failed"
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> task_check

    task_check >> task_csv >> task_create_table >> task_tmp >> task_base >> task_done >> task_end
    task_check >> task_err >> task_end
