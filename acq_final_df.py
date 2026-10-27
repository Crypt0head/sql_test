import os
import sys

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.python import PythonSensor
from omegaconf import OmegaConf
from raisa_transport import HIVE2JDBCOperator
from rail_operator import (
    ENV,
    PROJECT_PATH,
    RailPYOperator,
    RailSQLOperator,
    get_config,
    parse_requirements,
)

sys.path.append(os.path.dirname(__file__))


cfg = get_config()

dl_conn = BaseHook.get_connection(cfg.env.acq_datalake_conn_id)
drp_conn = BaseHook.get_connection(cfg.env.acq_drp_conn_id)

CONN_OVERRIDES = [
    f'env.session_config.impala.user_params.user_name={dl_conn.login}',
    f'env.session_config.impala.user_params.password="{dl_conn.password}"',
    'env.session_config.impala.kerberos.use_credentials=True',
    f'env.session_config.hive.user_params.user_name={dl_conn.login}',
    f'env.session_config.hive.user_params.password="{dl_conn.password}"',
    f'env.session_config.drp.user_params.user_name={drp_conn.login}',
    f'env.session_config.drp.user_params.password="{drp_conn.password}"',
]

PY_COMMON_OVERRIDES = [
    f'env={ENV}',
    f'project_path={PROJECT_PATH}',
    'env.exec_dt={{ ds }}',
    'env.exec_ts={{ data_interval_end }}',
    'env.dag_id={{ dag.dag_id }}',
    *CONN_OVERRIDES,
]

SQL_COMMON_OVERRIDES = [
    f'env={ENV}',
    *CONN_OVERRIDES,
]

dl_tables = cfg.env.tables.impala.tables
drp_tables = cfg.env.tables.drp.tables

# Окно расчёта считают сами SQL: месяцы последних acq_prev_month_days дней до exec_dt.
WINDOW_PARAMS = {
    'exec_dt': '{{ ds }}',
    'acq_prev_month_days': cfg.env.acq_prev_month_days,
}


def run_func_from_module_for_sensor(project_path, module_name=None, overrides=[]):
    import importlib
    import sys

    sys.path.append(project_path)
    module = importlib.import_module(f"include.python.{module_name}")
    return module.main(overrides=overrides)


def branch_drp():
    # Variable читается при запуске задачи, поэтому переключение DRP не требует правки кода.
    load_to_drp = Variable.get(cfg.env.acq_load_to_drp_variable, default_var='true')
    return 'hive2drp__acq_slice' if load_to_drp.strip().lower() == 'true' else 'skip_drp'


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
    schedule_interval="0 12 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    concurrency=3,
    max_active_runs=1,
    tags=[ENV, 'dashboard', 'acquiring'],
) as dag:
    start = DummyOperator(task_id='start')
    point1 = DummyOperator(task_id='point1')
    point2 = DummyOperator(task_id='point2')
    skip_drp = DummyOperator(task_id='skip_drp')
    end = DummyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    check_new_data = PythonSensor(
        task_id='check_new_data',
        python_callable=run_func_from_module_for_sensor,
        op_args=[PROJECT_PATH],
        op_kwargs={
            'module_name': 'check_new_data',
            'overrides': PY_COMMON_OVERRIDES,
        },
        mode='reschedule',
        poke_interval=cfg.env.acq_sensor_poke_seconds,
        timeout=cfg.env.acq_sensor_timeout_seconds,
        retries=0,
        dag=dag,
    )

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

    drop_task_list = []
    build_task_list = []
    for part in cfg.env.acq_parts:
        drop_task_list.append(RailSQLOperator(
            task_id=f'drop_{part}',
            sql_file='drop_table.sql',
            connect_to='impala',
            sql_params={'table_name': dl_tables[part]},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        ))
        build_task_list.append(RailSQLOperator(
            task_id=f'build_{part}',
            sql_file=f'{part}.sql',
            connect_to='impala',
            sql_params={**dl_tables, **WINDOW_PARAMS},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        ))

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

    branch_load_to_drp = BranchPythonOperator(
        task_id='branch_load_to_drp',
        python_callable=branch_drp,
        dag=dag,
    )

    hive2drp__acq_slice = HIVE2JDBCOperator(
        task_id='hive2drp__acq_slice',
        spark_conn_id=cfg.env.acq_spark_conn_id,
        hive_table=dl_tables.acq_slice_load,
        tgt_conn_id=cfg.env.acq_drp_conn_id,
        tgt_table=drp_tables.target_acq_fin_temp,
        spark_conf=OmegaConf.to_container(cfg.env.acq_spark_conf, resolve=True),
        mode='overwrite',
        enable_openlineage=False,
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

    start >> check_new_data >> refresh_task_list >> point1
    # Удаления независимы и идут параллельно; сборки - по порядку acq_parts, части используют результаты предыдущих.
    point1 >> drop_task_list >> point2
    chain(
        point2,
        *build_task_list,
        invalidate_acq_slice,
        create_acq_slice_hist,
        load_acq_slice_2_dl_hist,
        branch_load_to_drp,
    )
    branch_load_to_drp >> hive2drp__acq_slice >> check_acq_drp_temp >> delete_acq_months_from_target \
        >> load_acq_temp2target >> check_acq_drp_target >> end
    branch_load_to_drp >> skip_drp >> end

    main_task_list = [check_new_data] + refresh_task_list + drop_task_list + build_task_list + [
        invalidate_acq_slice,
        create_acq_slice_hist,
        load_acq_slice_2_dl_hist,
        branch_load_to_drp,
        hive2drp__acq_slice,
        check_acq_drp_temp,
        delete_acq_months_from_target,
        load_acq_temp2target,
        check_acq_drp_target,
    ]
    main_task_list >> notification_on_fail
