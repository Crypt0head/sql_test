import calendar
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from raisa_transport import HIVE2JDBCOperator
from rail_operator import (
    ENV,
    RailSQLOperator,
    parse_requirements,
)

sys.path.append(os.path.dirname(__file__))


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
    "spark.sql.parquet.binaryAsString": "true",
    "spark.yarn.queue": "ai",
    "spark.yarn.maxAppAttempts": "1",
    "spark.dynamicAllocation.enabled": "false",
}

SPARK_CONN_ID = "spark_kuz_lle"
DRP_CONN_ID = "spark_drp"

# The previous month is recomputed while DOC_OPER docs for tariff 0 and late transactions still arrive.
PREV_MONTH_DAYS = 10

LAKE_SCHEMA = "sandbox_ai"
LAKE_PREFIX = "tmp_shestopalov_acq_"
# Spark (HIVE2JDBCOperator) and RENAME do not work with insert-only ACID tables from Impala CTAS.
TABLE_PROPS = "TBLPROPERTIES ('transactional'='false')"

PARTS = [
    "01_sa_perimeter",
    "02_cdi_map",
    "03_cft_map",
    "05a_trx_agr_term",
    "05_trx_agr",
    "05b_active_retl",
    "mcc_by_agr",
    "04_terminals",
    "06_cft_attrs",
    "09_tariff",
    "10o_doc_oper",
    "10m_mpos",
    "10b_products",
    "kedr_chod",
    "month_slice",
]
LAKE_TABLES = {f"t_{part}": f"{LAKE_SCHEMA}.{LAKE_PREFIX}{part}" for part in PARTS}

REFRESH_TABLES = [
    "sandbox_ai.shestopalov_terminal_amortization_model_jan_aug",
    "sandbox_ai.shestopalov_kedr_obshiy_chod_inn_month",
    "sandbox_ai.prod__kuznetsov_lle__acc_ul_trx_features_impala",
    "sandbox_ai.le_nbo_scoring__targets_product_agreems",
]

DRP_TARGET = "sbx_da.tmp_shestopalov_acq_fin_version"
DRP_TEMP = "sbx_da.tmp_shestopalov_acq_month_slice_temp"

PRODUCT_FLAG_COLUMNS = [
    "rko", "accounting", "business_cards", "credit", "dbo", "deposit", "insurance",
    "loyalty_program", "nmo", "nso", "pravocard", "salary_project", "self_inkass",
    "service_package", "sms_info",
]
SLICE_COLUMNS = [
    "report_month", "snapshot_month_start", "inn", "company_name", "agr_id", "n_agr",
    "contract_number", "d_valid_from", "d_valid_to", "n_agr_actual", "contract_number_acq",
    "d_valid_from_actual", "d_valid_to_actual", "cdi_id", "ssp_ocrm", "cft_id", "ogrn",
    "filial_rf", "vsp_name", "vsp_code", "tariff_name", "tariff_source", "c_tariff_plan",
    "retl_cnt", "retl_with_term_cnt", "active_retl_cnt", "term_cnt", "active_terms",
    "active_term_cnt", "trx_cnt", "trx_sum", "commission_from_ops", "commission_monthly",
    "commission_monthly_source", "commission_monthly_mpos", "commission_monthly_oper_raw",
    "commission_monthly_oper_net", "int_component", "amortization", "term_calc_source",
    "mcc", "mcc_profile", "mcc_cnt",
    *PRODUCT_FLAG_COLUMNS,
    "kedr_obshiy_chod", "kedr_src_cnt", "kedr_obshiy_chod_contrib",
    "tariff_short", "aur", "commission_total", "chod", "recommended_products",
    "fin_result", "tsp_effective", "acq_pct", "chod_pct",
]
# Fill from the "нет в таблице дашборда" output of drp_month_replace_test.ipynb.
SLICE_COLUMNS_NOT_IN_TARGET = []
COMPAT_ALIASES = {
    "branch_rf": "filial_rf",
    "tarif_name": "tariff_name",
    "month": "snapshot_month_start",
}
MONEY_COLUMNS = {
    "trx_sum", "commission_from_ops", "commission_monthly", "commission_monthly_mpos",
    "commission_monthly_oper_raw", "commission_monthly_oper_net", "int_component",
    "commission_total", "aur", "amortization", "chod", "fin_result", "acq_pct", "chod_pct",
}


def text_expr(column):
    if column in MONEY_COLUMNS:
        return f"cast(round(cast({column} AS numeric), 2) AS text)"
    return f"cast({column} AS text)"


def build_insert_columns():
    pairs = [(c, c) for c in SLICE_COLUMNS if c not in SLICE_COLUMNS_NOT_IN_TARGET]
    pairs += list(COMPAT_ALIASES.items())
    insert_cols = ", ".join(target for target, _ in pairs)
    select_cols = ", ".join(text_expr(source) for _, source in pairs)
    return insert_cols, select_cols


INSERT_COLS, SELECT_COLS = build_insert_columns()


def acq_month(data_interval_end, shift=0):
    day = data_interval_end - timedelta(days=1)
    index = day.year * 12 + day.month - 1 + shift
    year, month = divmod(index, 12)
    month += 1
    next_year, next_month = divmod(index + 1, 12)
    return {
        "report_month": f"{year}-{month:02d}",
        "month_start": f"{year}-{month:02d}-01",
        "month_end": f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}",
        "month_next_start": f"{next_year}-{next_month + 1:02d}-01",
        "yearmm": f"{year}{month:02d}",
    }


def month_params(shift):
    keys = ["report_month", "month_start", "month_end", "month_next_start", "yearmm"]
    return {
        key: "{{ acq_month(data_interval_end, %d)['%s'] }}" % (shift, key)
        for key in keys
    }


def choose_prev_month(data_interval_end, **_):
    day = (data_interval_end - timedelta(days=1)).day
    return "prev_month__start" if day <= PREV_MONTH_DAYS else "prev_month__skip"


def sql_task(task_id, sql_file, connect_to, sql_params, **kwargs):
    return RailSQLOperator(
        task_id=task_id,
        sql_file=sql_file,
        connect_to=connect_to,
        sql_params=sql_params,
        overrides=SQL_COMMON_OVERRIDES,
        requirements=parse_requirements(),
        **kwargs,
    )


def build_month_chain(scope, shift, start_trigger_rule="all_success"):
    params = {
        **LAKE_TABLES,
        **month_params(shift),
        "table_props": TABLE_PROPS,
        "drp_target": DRP_TARGET,
        "drp_temp": DRP_TEMP,
        "insert_cols": INSERT_COLS,
        "select_cols": SELECT_COLS,
    }
    start = DummyOperator(task_id=f"{scope}__start", trigger_rule=start_trigger_rule)
    previous = start
    for part in PARTS:
        drop = sql_task(
            f"{scope}__drop_{part}",
            "drop_table.sql",
            "impala",
            {"table_name": LAKE_TABLES[f"t_{part}"]},
        )
        build = sql_task(f"{scope}__build_{part}", f"acq_{part}.sql", "impala", params)
        previous >> drop >> build
        previous = build

    slice_to_drp_temp = HIVE2JDBCOperator(
        task_id=f"{scope}__slice_to_drp_temp",
        spark_conn_id=SPARK_CONN_ID,
        hive_table=LAKE_TABLES["t_month_slice"],
        tgt_conn_id=DRP_CONN_ID,
        tgt_table=DRP_TEMP,
        spark_conf=SPARK_COMMON_CONF,
        mode="overwrite",
        enable_openlineage=False,
    )
    check_temp = sql_task(f"{scope}__check_drp_temp", "drp_check_temp.sql", "drp", params)
    replace_month = sql_task(f"{scope}__replace_month", "drp_replace_month.sql", "drp", params)
    finish = DummyOperator(task_id=f"{scope}__finish")
    previous >> slice_to_drp_temp >> check_temp >> replace_month >> finish
    return start, finish


with DAG(
    f"etl__acq_final_df__{ENV}",
    default_args={
        "owner": "ShestopalovVYur",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    description="Витрина эквайринга final_df: срез месяца в озере и замена месяца в DRP",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    user_defined_macros={"acq_month": acq_month},
    tags=[ENV, "dashboard", "acquiring"],
) as dag:
    start = DummyOperator(task_id="start")
    sources_ready = DummyOperator(task_id="sources_ready")
    end = DummyOperator(task_id="end")

    refresh_tasks = [
        sql_task(
            f"refresh__{table.split('.')[-1]}",
            "refresh.sql",
            "impala",
            {"table_name": table},
        )
        for table in REFRESH_TABLES
    ]

    branch_prev_month = BranchPythonOperator(
        task_id="branch_prev_month",
        python_callable=choose_prev_month,
    )
    prev_skip = DummyOperator(task_id="prev_month__skip")
    prev_start, prev_finish = build_month_chain("prev_month", shift=-1)
    cur_start, cur_finish = build_month_chain(
        "cur_month",
        shift=0,
        start_trigger_rule="none_failed_min_one_success",
    )

    start >> refresh_tasks >> sources_ready >> branch_prev_month
    branch_prev_month >> [prev_start, prev_skip]
    prev_finish >> cur_start
    prev_skip >> cur_start
    cur_finish >> end
