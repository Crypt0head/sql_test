import os
import sys

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonVirtualenvOperator
from raisa_transport import HIVE2JDBCOperator
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


# The previous month is recomputed while DOC_OPER docs for tariff 0 and late transactions still arrive.
PREV_MONTH_DAYS = 10

PY_COMMON_OVERRIDES = [
    f'env={ENV}',
    f'project_path={PROJECT_PATH}',
    'env.exec_dt={{ (data_interval_end - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}',
    'env.exec_ts={{ data_interval_end }}',
    'env.dag_id={{ dag.dag_id }}',
    f'++env.acq_prev_month_days={PREV_MONTH_DAYS}',
    f'env.session_config.impala.user_params.user_name={Variable.get("datalake_tech_user")}',
    f'env.session_config.impala.user_params.password="{Variable.get("datalake_tech_password")}"',
    'env.session_config.impala.kerberos.use_credentials=True',
    f'env.session_config.hive.user_params.user_name={Variable.get("datalake_tech_user")}',
    f'env.session_config.hive.user_params.password="{Variable.get("datalake_tech_password")}"',
    f'env.session_config.drp.user_params.user_name={Variable.get("drp_user")}',
    f'env.session_config.drp.user_params.password="{Variable.get("drp_password")}"',
]

SQL_COMMON_OVERRIDES = [
    f'env={ENV}',
    f'env.session_config.impala.user_params.user_name={Variable.get("datalake_tech_user")}',
    f'env.session_config.impala.user_params.password="{Variable.get("datalake_tech_password")}"',
    'env.session_config.impala.kerberos.use_credentials=True',
    f'env.session_config.drp.user_params.user_name={Variable.get("drp_user")}',
    f'env.session_config.drp.user_params.password="{Variable.get("drp_password")}"',
]

SPARK_COMMON_CONF = {
    "spark.executor.instances": "1",
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.driver.memory": "1g",
    "spark.driver.cores": "1",
    "spark.yarn.stagingDir": "hdfs:///tmp",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension",
    "spark.executor.extraClassPath": "hdfs:///apps/spark-acid-lib/spark-acid-3.5.2-assembly-0.6.0.jar,hdfs:////apps/spark-acid-lib/ojdbc8.jar,hdfs:////apps/spark-acid-lib/postgresql-42.2.5.jar",
    "spark.jars": "hdfs:///apps/spark-acid-lib/spark-acid-3.5.2-assembly-0.6.0.jar,hdfs:////apps/spark-acid-lib/ojdbc8.jar,hdfs:////apps/spark-acid-lib/postgresql-42.2.5.jar",
    'spark.yarn.queue': 'ai',
    'spark.ui.view.acls.groups': 'spark,hdfs,dr.who,go-sg-as-hdp-dlk_admdbd,go-sg-as-hdp-dlk_ds_ai',
    'spark.yarn.maxAppAttempts': '1',
    'spark.dynamicAllocation.enabled': 'false',
}

SOURCE_TABLES = [
    "ods_alpha.scd1_agreements",
    "ods_alpha.scd1_companies",
    "ods_alpha.scd1_agr_terms",
    "ods_alpha.scd1_trx",
    "ods_alpha.scd1_trx_acq",
    "ods_alpha.scd1_trx_int",
    "ods_alpha.scd1_base24_fiids",
    "ods_alpha.scd1_merchants",
    "ods_alpha.scd1_pos_terminals",
    "ods_alpha.scd1_mrc_pos_rent",
    "ods.scd1_z_r2_ip_merchants",
    "ods.scd1_z_r2_tariff_plan",
    "ods.scd1_z_r2_tariff_tune",
    "ods.scd1_z_r2_tariff_fix",
    "ods.scd1_z_R2_IP_DOG_OPER",
    "ods.scd1_z_client",
    "ods.scd1_z_cl_corp",
    "ods.scd1_z_depart",
    "ods.scd1_z_branch",
    "ocrm_ul.s_org_ext",
    "cdiul.ext_id_org",
    "sandbox_ai.shestopalov_terminal_amortization_model_jan_aug",
    "sandbox_ai.shestopalov_kedr_obshiy_chod_inn_month",
    "sandbox_ai.prod__kuznetsov_lle__acc_ul_trx_features_impala",
    "sandbox_ai.le_nbo_scoring__targets_product_agreems",
]

# Columns of include/sql/acq_month_slice.sql in the DRP target, all target columns are TEXT.
SLICE_COLUMNS = [
    "snapshot_month_start", "n_agr", "agr_id", "inn", "contract_number", "company_name",
    "cdi_id", "ssp_ocrm", "cft_id", "ogrn", "vsp_name", "vsp_code", "filial_rf",
    "retl_cnt", "retl_with_term_cnt", "term_cnt", "amortization", "term_calc_source",
    "trx_cnt", "trx_sum", "commission_from_ops", "int_component", "active_term_cnt",
    "active_terms", "active_retl_cnt", "tariff_name", "tariff_source", "c_tariff_plan",
    "tariff_name_actual", "commission_monthly_fix", "commission_monthly",
    "commission_monthly_oper_raw", "commission_monthly_mpos", "mcc", "mcc_profile",
    "mcc_cnt", "product_names", "kedr_obshiy_chod", "tariff_short", "aur",
    "commission_total", "chod", "fin_result", "tsp_effective", "acq_pct", "chod_pct",
    "report_month",
]
# Fill with slice columns that do not exist in the DRP target.
SLICE_COLUMNS_NOT_IN_TARGET = []
COMPAT_ALIASES = {
    "branch_rf": "filial_rf",
    "tarif_name": "tariff_name",
    "month": "snapshot_month_start",
}
def build_insert_columns():
    pairs = [(c, c) for c in SLICE_COLUMNS if c not in SLICE_COLUMNS_NOT_IN_TARGET]
    pairs += list(COMPAT_ALIASES.items())
    insert_cols = ", ".join(target for target, _ in pairs)
    select_cols = ", ".join(f"cast({source} AS text)" for _, source in pairs)
    return insert_cols, select_cols


INSERT_COLS, SELECT_COLS = build_insert_columns()


def acq_expected_months(data_interval_end):
    day = data_interval_end - timedelta(days=1)
    months = [f"{day.year}-{day.month:02d}"]
    if day.day <= PREV_MONTH_DAYS:
        prev_year, prev_month = (day.year - 1, 12) if day.month == 1 else (day.year, day.month - 1)
        months.insert(0, f"{prev_year}-{prev_month:02d}")
    return ", ".join(f"'{m}'" for m in months)


cfg = get_config()

dl_tables = cfg.env.tables.impala.tables
drp_tables = cfg.env.tables.drp.tables

EXPECTED_MONTHS = '{{ acq_expected_months(data_interval_end) }}'


with DAG(
    f'etl__acq_final_df__{ENV}',
    default_args={
        'owner': 'ShestopalovVYur',
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    },
    description='Витрина эквайринга final_df: расчёт месяца в Impala, история в озере, замена месяца в DRP',
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    concurrency=3,
    max_active_runs=1,
    user_defined_macros={'acq_expected_months': acq_expected_months},
    tags=[ENV, 'dashboard', 'acquiring'],
) as dag:
    start = DummyOperator(task_id='start')
    point1 = DummyOperator(task_id='point1')
    end = DummyOperator(task_id='end')

    refresh_task_list = []

    for table in SOURCE_TABLES:
        refresh_task = RailSQLOperator(
            task_id=f'refresh_task_{table.split(".")[-1].lower()}',
            sql_file='refresh.sql',
            connect_to='impala',
            sql_params={'table_name': table},
            overrides=SQL_COMMON_OVERRIDES,
            requirements=parse_requirements(),
            dag=dag
        )
        refresh_task_list.append(refresh_task)

    load_acq_final_df = PythonVirtualenvOperator(
        task_id='load_acq_final_df',
        python_callable=run_func_from_module,
        op_kwargs={
            'module_name': 'load_acq_final_df',
            'project_path': PROJECT_PATH,
            'config': PY_COMMON_OVERRIDES,
        },
        requirements=parse_requirements(),
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

    hive2drp__acq_final_df = HIVE2JDBCOperator(
        task_id='hive2drp__acq_final_df',
        spark_conn_id='spark_kuz_lle',
        hive_table=dl_tables.acq_slice_load,
        tgt_conn_id='spark_drp',
        tgt_table=drp_tables.target_acq_fin_temp,
        spark_conf=SPARK_COMMON_CONF,
        mode='overwrite',
        enable_openlineage=False
    )

    check_acq_drp_temp = RailSQLOperator(
        task_id='check_acq_drp_temp',
        sql_file='check_acq_drp_temp.sql',
        connect_to='drp',
        sql_params={**drp_tables, 'expected_months': EXPECTED_MONTHS},
        overrides=SQL_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        dag=dag
    )

    delete_acq_months_from_target = RailSQLOperator(
        task_id='delete_acq_months_from_target',
        sql_file='delete_acq_months_from_target.sql',
        connect_to='drp',
        sql_params={**drp_tables, 'expected_months': EXPECTED_MONTHS},
        overrides=SQL_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        dag=dag
    )

    load_acq_temp2target = RailSQLOperator(
        task_id='load_acq_temp2target',
        sql_file='load_acq_temp2target.sql',
        connect_to='drp',
        sql_params={**drp_tables, 'insert_cols': INSERT_COLS, 'select_cols': SELECT_COLS},
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

    load_chain = [
        load_acq_final_df,
        invalidate_acq_slice,
        create_acq_slice_hist,
        load_acq_slice_2_dl_hist,
        hive2drp__acq_final_df,
        check_acq_drp_temp,
        delete_acq_months_from_target,
        load_acq_temp2target,
    ]

    start >> refresh_task_list >> point1 >> load_chain[0]
    for upstream, downstream in zip(load_chain, load_chain[1:]):
        upstream >> downstream
    load_chain[-1] >> end
    load_chain >> notification_on_fail
