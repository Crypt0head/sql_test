import os
import sys

from datetime import datetime, timedelta

import jinja2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from rail_operator import (
    ENV,
    PROJECT_PATH,
    DataQualityOperator,
    RailPYOperator,
    get_config,
    parse_requirements,
)

sys.path.append(os.path.dirname(__file__))


DQ_EMAILS = ['shestopalovvyur@rshb.ru']
DQ_CHECKS = ['structure', 'completeness', 'values', 'dynamics', 'reconcile_trx']
# Чтобы увидеть упавшую проверку и письмо-алерт, добавьте 'alert_test' в DQ_CHECKS на один запуск.

base_cfg = get_config()
if 'datalake_dq' not in base_cfg.env.session_config:
    raise KeyError(
        'session_config.datalake_dq is missing: copy the datalake_dq section from '
        'airflow_od_dq_checks conf/env/session_config/session_conf.yaml'
    )

# То же окно, что у DAG витрины, чтобы DQ проверял ровно пересчитанные месяцы.
PREV_MONTH_DAYS = int(base_cfg.env.acq_prev_month_days)

dl_conn = BaseHook.get_connection(base_cfg.env.acq_datalake_conn_id)

PY_COMMON_OVERRIDES = [
    f'env={ENV}',
    f'project_path={PROJECT_PATH}',
    'env.exec_dt={{ ds }}',
    'env.exec_ts={{ data_interval_end }}',
    'env.dag_id={{ dag.dag_id }}',
    f'env.session_config.impala.user_params.user_name={dl_conn.login}',
    f'env.session_config.impala.user_params.password="{dl_conn.password}"',
    'env.session_config.impala.kerberos.use_credentials=True',
]

DQ_COMMON_OVERRIDES = [
    f'env.session_config.datalake_dq.conn_params.username={dl_conn.login}',
    f'env.session_config.datalake_dq.conn_params.password="{dl_conn.password}"',
]

cfg = get_config(overrides=DQ_COMMON_OVERRIDES)
source_config = cfg['env']['session_config']['datalake_dq']
hist_table = cfg.env.tables.impala.tables.acq_slice_hist

checks_dir = os.path.join(PROJECT_PATH, 'dq')
tmp_dir = '/tmp'


def month_shift(year, month, delta):
    index = year * 12 + month - 1 + delta
    return index // 12, index % 12 + 1


def acq_dq_params(ds):
    day = datetime.strptime(ds, '%Y-%m-%d')
    months = [(day.year, day.month)]
    if day.day <= PREV_MONTH_DAYS:
        months.insert(0, month_shift(day.year, day.month, -1))
    labels = [f'{y}-{m:02d}' for y, m in months]
    period_end = month_shift(*months[-1], 1)
    closed = month_shift(day.year, day.month, -1)
    return {
        'hist': hist_table,
        'acq_months': ', '.join(f"'{label}'" for label in labels),
        'acq_months_select': ' UNION ALL '.join(f"SELECT '{label}' AS report_month" for label in labels),
        'closed_month': f'{closed[0]}-{closed[1]:02d}',
        'period_start': f'{months[0][0]}-{months[0][1]:02d}-01',
        'period_end': f'{period_end[0]}-{period_end[1]:02d}-01',
    }


def render_yaml(config_path_raw=None, config_path_rendered=None, **context):
    with open(os.path.join(checks_dir, config_path_raw), 'r') as fr:
        template = jinja2.Template(fr.read())
    rendered_text = template.render(**context, **acq_dq_params(context['ds']))
    with open(os.path.join(tmp_dir, config_path_rendered), 'w') as fw:
        fw.write(rendered_text)


with DAG(
    f'dq__acq_final_df__{ENV}',
    default_args={
        'owner': 'ShestopalovVYur',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    description='DQ-проверки витрины эквайринга final_df в истории озера',
    schedule_interval="0 12 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=[ENV, 'dashboard', 'acquiring', 'dq'],
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end', trigger_rule='all_done')

    wait_acq_final_df = ExternalTaskSensor(
        task_id='wait_acq_final_df',
        external_dag_id=f'etl__acq_final_df__{ENV}',
        external_task_id='load_acq_slice_2_dl_hist',
        allowed_states=['success'],
        failed_states=['failed', 'upstream_failed', 'skipped'],
        mode='reschedule',
        poke_interval=600,
        timeout=8 * 60 * 60,
    )

    check_tasks = []
    for name in DQ_CHECKS:
        render = PythonOperator(
            task_id=f'render_{name}',
            python_callable=render_yaml,
            op_kwargs={
                'config_path_raw': f'acq_dq_{name}.yaml',
                'config_path_rendered': f'acq_dq_{name}_rendered.yaml',
            },
        )
        check = DataQualityOperator(
            task_id=f'dq_{name}',
            project_name='acq_final_df_dq',
            source_config=source_config,
            checks_config_path=[os.path.join(tmp_dir, f'acq_dq_{name}_rendered.yaml')],
            email=DQ_EMAILS,
            pipeline_name=name,
            dag=dag,
        )
        wait_acq_final_df >> render >> check >> end
        check_tasks += [render, check]

    notification_on_fail = RailPYOperator(
        task_id='notification_on_fail',
        py_file='notification_on_fail',
        overrides=PY_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        trigger_rule='one_failed',
        dag=dag,
    )

    start >> wait_acq_final_df
    [wait_acq_final_df] + check_tasks >> notification_on_fail
