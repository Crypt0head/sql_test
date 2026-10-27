import os
import sys

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonVirtualenvOperator
from rail_operator import (
    ENV,
    PROJECT_PATH,
    RailPYOperator,
    RailSQLOperator,
    get_config,
    parse_requirements,
    run_func_from_module,
)

sys.path.append(os.path.dirname(__file__))


# Тестовый прогон без записи в DRP - ставь 'False'
LOAD_TO_DRP = True

PY_COMMON_OVERRIDES = [
    f'env={ENV}',
    f'project_path={PROJECT_PATH}',
    'env.exec_dt={{ ds }}',
    'env.exec_ts={{ data_interval_end }}',
    'env.dag_id={{ dag.dag_id }}',
    f'env.session_config.impala.user_params.user_name={Variable.get("datalake_tech_user", default_var="")}',
    f'env.session_config.impala.user_params.password="{Variable.get("datalake_tech_password", default_var="")}"',
    'env.session_config.impala.kerberos.use_credentials=True',
    f'env.session_config.hive.user_params.user_name={Variable.get("datalake_tech_user", default_var="")}',
    f'env.session_config.hive.user_params.password="{Variable.get("datalake_tech_password", default_var="")}"',
    f'env.session_config.drp.user_params.user_name={Variable.get("drp_user", default_var="")}',
    f'env.session_config.drp.user_params.password="{Variable.get("drp_password", default_var="")}"',
]

SQL_COMMON_OVERRIDES = [
    f'env={ENV}',
    f'env.session_config.impala.user_params.user_name={Variable.get("datalake_tech_user", default_var="")}',
    f'env.session_config.impala.user_params.password="{Variable.get("datalake_tech_password", default_var="")}"',
    'env.session_config.impala.kerberos.use_credentials=True',
    f'env.session_config.hive.user_params.user_name={Variable.get("datalake_tech_user", default_var="")}',
    f'env.session_config.hive.user_params.password="{Variable.get("datalake_tech_password", default_var="")}"',
    f'env.session_config.drp.user_params.user_name={Variable.get("drp_user", default_var="")}',
    f'env.session_config.drp.user_params.password="{Variable.get("drp_password", default_var="")}"',
]


cfg = get_config()

dl_tables = cfg.env.tables.impala.tables
drp_tables = cfg.env.tables.drp.tables

# Окно расчёта считают сами SQL: месяцы последних acq_prev_month_days дней до exec_dt.
WINDOW_PARAMS = {
    'exec_dt': '{{ ds }}',
    'acq_prev_month_days': cfg.env.acq_prev_month_days,
}


with DAG(
    f'etl__acq_final_df__{ENV}',
    default_args={
        'owner': 'ShestopalovVYur',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    },
    description='Витрина эквайринга final_df: расчёт месяцев окна в Impala, история в озере, замена месяцев в DRP',
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    concurrency=3,
    max_active_runs=1,
    tags=[ENV, 'dashboard', 'acquiring'],
) as dag:
    start = DummyOperator(task_id='start')
    point1 = DummyOperator(task_id='point1')
    end = DummyOperator(task_id='end')

    refresh_task_list = []
    for table in cfg.env.acq_source_tables:
        refresh_task_list.append(RailSQLOperator(
            task_id=f'refresh_task_{table.split(".")[-1].lower()}',
            sql_file='refresh.sql',
            connect_to='impala',
            sql_params={'table_name': table},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        ))

    part_task_list = []
    for part in cfg.env.acq_parts:
        part_task_list.append(RailSQLOperator(
            task_id=f'drop_{part}',
            sql_file='drop_table.sql',
            connect_to='impala',
            sql_params={'table_name': dl_tables[part]},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        ))
        part_task_list.append(RailSQLOperator(
            task_id=f'build_{part}',
            sql_file=f'{part}.sql',
            connect_to='impala',
            sql_params={**dl_tables, **WINDOW_PARAMS},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        ))

    check_acq_slice = RailSQLOperator(
        task_id='check_acq_slice',
        sql_file='check_acq_slice.sql',
        connect_to='hive',
        sql_params={**dl_tables},
        overrides=SQL_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        dag=dag
    )

    invalidate_acq_slice = RailSQLOperator(
        task_id='invalidate_acq_slice',
        sql_file='refresh.sql',
        connect_to='impala',
        sql_params={'table_name': dl_tables.acq_slice_load},
        overrides=SQL_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        dag=dag
    )

    create_acq_slice_hist = RailSQLOperator(
        task_id='create_acq_slice_hist',
        sql_file='create_acq_slice_hist.sql',
        connect_to='impala',
        sql_params={**dl_tables},
        overrides=SQL_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        dag=dag
    )

    load_acq_slice_2_dl_hist = RailSQLOperator(
        task_id='load_acq_slice_2_dl_hist',
        sql_file='load_acq_slice_2_dl_hist.sql',
        connect_to='impala',
        sql_params={**dl_tables},
        overrides=SQL_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        dag=dag
    )

    notification_on_fail = RailPYOperator(
        task_id='notification_on_fail',
        py_file='notification_on_fail',
        overrides=PY_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        trigger_rule='one_failed',
        dag=dag,
    )

    start >> refresh_task_list >> point1
    chain(
        point1,
        *part_task_list,
        check_acq_slice,
        invalidate_acq_slice,
        create_acq_slice_hist,
        load_acq_slice_2_dl_hist,
    )

    main_task_list = refresh_task_list + part_task_list + [
        check_acq_slice,
        invalidate_acq_slice,
        create_acq_slice_hist,
        load_acq_slice_2_dl_hist,
    ]

    if LOAD_TO_DRP:
        # Около 7 000 строк в месяц передаются через pandas; вариант через Spark:
        # HIVE2JDBCOperator(task_id='hive2drp__acq_final_df', spark_conn_id='spark_kuz_lle',
        #     hive_table=dl_tables.acq_slice_load, tgt_conn_id='spark_drp',
        #     tgt_table=drp_tables.target_acq_fin_temp,
        #     mode='overwrite', enable_openlineage=False)
        load_acq_slice_2_drp_temp = PythonVirtualenvOperator(
            task_id='load_acq_slice_2_drp_temp',
            python_callable=run_func_from_module,
            op_kwargs={
                'module_name': 'load_acq_slice_2_drp_temp',
                'project_path': PROJECT_PATH,
                'config': PY_COMMON_OVERRIDES,
            },
            requirements=parse_requirements(),
        )

        check_acq_drp_temp = RailSQLOperator(
            task_id='check_acq_drp_temp',
            sql_file='check_acq_drp_temp.sql',
            connect_to='drp',
            sql_params={**drp_tables, **WINDOW_PARAMS},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        )

        delete_acq_months_from_target = RailSQLOperator(
            task_id='delete_acq_months_from_target',
            sql_file='delete_acq_months_from_target.sql',
            connect_to='drp',
            sql_params={**drp_tables, **WINDOW_PARAMS},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        )

        load_acq_temp2target = RailSQLOperator(
            task_id='load_acq_temp2target',
            sql_file='load_acq_temp2target.sql',
            connect_to='drp',
            sql_params={**drp_tables},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        )

        check_acq_drp_target = RailSQLOperator(
            task_id='check_acq_drp_target',
            sql_file='check_acq_drp_target.sql',
            connect_to='drp',
            sql_params={**drp_tables, **WINDOW_PARAMS},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        )

        load_acq_slice_2_dl_hist >> load_acq_slice_2_drp_temp >> check_acq_drp_temp \
            >> delete_acq_months_from_target >> load_acq_temp2target >> check_acq_drp_target >> end

        main_task_list += [
            load_acq_slice_2_drp_temp,
            check_acq_drp_temp,
            delete_acq_months_from_target,
            load_acq_temp2target,
            check_acq_drp_target,
        ]
    else:
        load_acq_slice_2_dl_hist >> end

    main_task_list >> notification_on_fail
