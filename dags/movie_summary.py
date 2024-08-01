import sys
from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,

)

with DAG(
        'movie_summary',
        default_args={
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(seconds=3)
        },
        max_active_runs=1,
        max_active_tasks=3,
        description='movie',
        schedule="10 2 * * *",
        start_date=datetime(2024, 7, 29),
        catchup=True,
        tags=['api', 'movie', 'summary'],
) as dag:
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))


    def save_data(ds_nodash):
        from mov.api.call import apply_type2df

        df = apply_type2df(load_dt=ds_nodash)

        print("*" * 33)
        print(df.head(10))
        print("*" * 33)
        print(df.dtypes)

        # 개봉일 기준 그룹핑 누적 관객수 합
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)


    def branch_fun(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return rm_dir.task_id
        else:
            return "get.data", "echo.task"


    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )

    # get_data = PythonVirtualenvOperator(
    #     task_id='get.data',
    #     python_callable=get_data,
    #     requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
    #     system_site_packages=False,
    #     trigger_rule="all_done",
    #     venv_cache_path="/home/diginori/tmp2/air_venv/get_data"
    # )

    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )


    def get_data_with_params(**kwargs):

        url_params = dict(kwargs.get("url_params"))
        date = kwargs.get("ds")

        if len(url_params) >= 1:
            from mov.api.call import save2df
            df = save2df(load_dt=date, url_params=url_params)
            print(df)
        else:
            print("params안에 값이 없음")
            sys.exit(1)


    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        system_site_packages=False,
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
        op_kwargs={
            "url_params": {"repNationCd": "K"},
            "ds": "{{ds_nodash}}"
        },
        python_callable=get_data_with_params,
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        system_site_packages=False,
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
        op_kwargs={
            "url_params": {"repNationCd": "F"},
            "ds": "{{ds_nodash}}"
        },
        python_callable=get_data_with_params,
    )

    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        system_site_packages=False,
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
        op_kwargs={
            "url_params": {"multiMovieYn": "Y"},
            "ds": "{{ds_nodash}}"
        },
        python_callable=get_data_with_params,
    )
    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        system_site_packages=False,
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
        op_kwargs={
            "url_params": {"multiMovieYn": "N"},
            "ds": "{{ds_nodash}}"
        },
        python_callable=get_data_with_params,
    )


    # DataFrame 합치기 [ nation + multi ]
    def merge_dataframe():
        pass


    merge_df = PythonVirtualenvOperator(
        task_id='merge.df',
        python_callable=merge_dataframe,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
    )


    def summary_df():
        pass


    summary_df = PythonVirtualenvOperator(
        task_id='summary.df',
        python_callable=summary_df,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
    )


    def de_dup():
        pass


    de_dup = PythonVirtualenvOperator(
        task_id='de.dup',
        python_callable=de_dup,
        system_site_packages=False,
        trigger_rule="one_success",
        requirements=["git+https://github.com/Jeonghoon2/movie.git@0.2/api"],
    )

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    get_start = EmptyOperator(
        task_id='get.start',
        trigger_rule="all_done"
    )
    get_end = EmptyOperator(task_id='get.end')

    throw_err = BashOperator(
        task_id='throw.err',
        bash_command="exit 1",
        trigger_rule="all_done"
    )

    start >> branch_op
    start >> throw_err >> save_data

    branch_op >> rm_dir >> get_start
    branch_op >> echo_task >> get_start
    branch_op >> get_start
    get_start >> [nation_k, nation_f, multi_n, multi_y] >> merge_df >> de_dup >> summary_df >> get_end

    get_end >> save_data >> end
