"""Проверка SQL-частей витрины с окном месяцев до выкладки DAG.

Запуск в ячейке Jupyter: %run _check_parts_window.py
Запускать, только когда DAG etl__acq_final_df__test не работает: таблицы частей общие.
Пишет только во временные таблицы sandbox_ai.tmp_shestopalov_acq_*; история и DRP не меняются.
"""
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from rail_connectors.connection import connect

REPO = Path.home() / "documents/Equaring/Фин.скрипт/Регламент/airflow_shestopalov_acq_dashbord"
SQL_DIR = REPO / "include/sql"

EXEC_DATES = ["2026-09-05"]
PREV_MONTH_DAYS = 10
# Продолжить с этой части, не пересчитывая готовые (None - с начала). Готовые таблицы должны быть на ту же exec_dt.
START_FROM = "acq_09_tariff"
# Повтор при "Failed to acquire lock": исходную таблицу ODS в этот момент кто-то пишет.
LOCK_RETRIES = 3
LOCK_WAIT_SECONDS = 300
MEM_LIMIT_DEFAULT = "8g"
MEM_LIMIT_BY_PART = {
    "acq_00_months": "1g",
    "acq_mcc_by_agr": "4g",
    "acq_kedr_chod": "4g",
    "acq_05_trx_agr": "12g",
    "acq_04_terminals": "12g",
}
HIST = "sandbox_ai.shestopalov_acq_final_df_hist"

TABLES = {
    "acq_00_months": "sandbox_ai.tmp_shestopalov_acq_00_months",
    "acq_01_sa_perimeter": "sandbox_ai.tmp_shestopalov_acq_01_sa_perimeter",
    "acq_02_cdi_map": "sandbox_ai.tmp_shestopalov_acq_02_cdi_map",
    "acq_03_cft_map": "sandbox_ai.tmp_shestopalov_acq_03_cft_map",
    "acq_05_trx_agr": "sandbox_ai.tmp_shestopalov_acq_05_trx_agr",
    "acq_mcc_by_agr": "sandbox_ai.tmp_shestopalov_acq_mcc_by_agr",
    "acq_04_terminals": "sandbox_ai.tmp_shestopalov_acq_04_terminals",
    "acq_05b_active_retl": "sandbox_ai.tmp_shestopalov_acq_05b_active_retl",
    "acq_06_cft_attrs": "sandbox_ai.tmp_shestopalov_acq_06_cft_attrs",
    "acq_09_tariff": "sandbox_ai.tmp_shestopalov_acq_09_tariff",
    "acq_10o_doc_oper": "sandbox_ai.tmp_shestopalov_acq_10o_doc_oper",
    "acq_10m_mpos": "sandbox_ai.tmp_shestopalov_acq_10m_mpos",
    "acq_10b_products": "sandbox_ai.tmp_shestopalov_acq_10b_products",
    "acq_kedr_chod": "sandbox_ai.tmp_shestopalov_acq_kedr_chod",
    "acq_slice_load": "sandbox_ai.tmp_shestopalov_acq_month_slice_load",
}
PARTS = list(TABLES)
MONTHLY_PARTS = {
    "acq_00_months", "acq_01_sa_perimeter", "acq_05_trx_agr", "acq_04_terminals",
    "acq_05b_active_retl", "acq_09_tariff", "acq_10o_doc_oper", "acq_10m_mpos",
    "acq_10b_products", "acq_kedr_chod", "acq_slice_load",
}
KERBEROS = {
    "keytab_path": "/home/jovyan/test_requests/tech.keytab",
    "use_credentials": True,
    "update_keytab": True,
}
USER = {"user_name": "Shestopalov-VYur"}

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", None)

imp = connect(
    to="IMPALA",
    extra_options={"db": "sandbox_ai"},
    driver_args={"tez.queue.name": "ai"},
    kerberos=KERBEROS,
    user_params=USER,
)


def log(message):
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def build_parts(exec_dt):
    params = {**TABLES, "exec_dt": exec_dt, "acq_prev_month_days": PREV_MONTH_DAYS}
    parts = PARTS[PARTS.index(START_FROM):] if START_FROM else PARTS
    for part in parts:
        table = TABLES[part]
        sql = (SQL_DIR / f"{part}.sql").read_text(encoding="utf-8").format(**params)
        mem_limit = MEM_LIMIT_BY_PART.get(part, MEM_LIMIT_DEFAULT)
        for attempt in range(1, LOCK_RETRIES + 1):
            log(f"{part}: старт, MEM_LIMIT={mem_limit}, попытка {attempt}")
            try:
                with imp:
                    imp.execute(f"SET MEM_LIMIT={mem_limit}")
                    imp.execute(f"DROP TABLE IF EXISTS {table}")
                    log(f"{part}: таблица удалена, строю")
                    imp.execute(sql)
                break
            except Exception as error:
                if "Failed to acquire lock" not in str(error) or attempt == LOCK_RETRIES:
                    raise
                log(f"{part}: исходная таблица заблокирована, жду {LOCK_WAIT_SECONDS // 60} мин")
                time.sleep(LOCK_WAIT_SECONDS)
        if part in MONTHLY_PARTS:
            sql = f"SELECT report_month, count(*) AS n FROM {table} GROUP BY report_month ORDER BY report_month"
        else:
            sql = f"SELECT 'все' AS report_month, count(*) AS n FROM {table}"
        with imp:
            imp.execute("SET MEM_LIMIT=1g")
            df = imp.fetch(sql)
        log(f"{part}: готово, " + ", ".join(f"{r.report_month}={r.n}" for r in df.itertuples()))


def check_slice():
    sql = f"""
        SELECT m.report_month, s.n_rows, s.n_agrs, p.n_rows AS n_perimeter,
               s.n_with_trx, s.n_with_cdi, s.n_with_cft, s.n_with_mcc
        FROM {TABLES['acq_00_months']} m
        LEFT JOIN (SELECT report_month, count(*) AS n_rows, count(DISTINCT n_agr) AS n_agrs,
                          count(trx_cnt) AS n_with_trx, count(cdi_id) AS n_with_cdi,
                          count(cft_id) AS n_with_cft, count(mcc) AS n_with_mcc
                   FROM {TABLES['acq_slice_load']} GROUP BY report_month) s
          ON s.report_month = m.report_month
        LEFT JOIN (SELECT report_month, count(*) AS n_rows
                   FROM {TABLES['acq_01_sa_perimeter']} GROUP BY report_month) p
          ON p.report_month = m.report_month
        ORDER BY m.report_month
    """
    with imp:
        df = imp.fetch(sql)
    filled = df[["n_with_trx", "n_with_cdi", "n_with_cft", "n_with_mcc"]].fillna(0).gt(0).all(axis=1)
    df["ok"] = (df.n_rows.fillna(0) > 0) & (df.n_rows == df.n_agrs) & (df.n_rows == df.n_perimeter) & filled
    print(df.to_string(index=False))
    return bool(df.ok.all())


def compare_with_hist():
    sums = """
        report_month,
        count(*) AS n_rows,
        sum(cast(trx_sum AS double)) AS trx_sum,
        sum(cast(commission_from_ops AS double)) AS commission_from_ops,
        sum(cast(commission_monthly AS double)) AS commission_monthly,
        sum(cast(amortization AS double)) AS amortization,
        sum(cast(fin_result AS double)) AS fin_result
    """
    with imp:
        new = imp.fetch(f"SELECT {sums} FROM {TABLES['acq_slice_load']} GROUP BY report_month")
        months = ", ".join(f"'{m}'" for m in new.report_month)
        old = imp.fetch(f"SELECT {sums} FROM {HIST} WHERE report_month IN ({months}) GROUP BY report_month")
    df = new.merge(old, on="report_month", how="left", suffixes=("_new", "_hist")).sort_values("report_month")
    print(df.T.to_string(header=False))


for exec_dt in EXEC_DATES:
    print(f"\n===== exec_dt = {exec_dt} =====")
    build_parts(exec_dt)
    print("\nПроверка среза (как check_acq_slice):")
    print("OK" if check_slice() else "ОШИБКА: срез пустой, есть дубли, строк не столько, сколько в периметре, или пустая обязательная часть")
    print("\nСрез против истории в озере (история посчитана раньше, поэтому за текущий месяц разница из-за новых дней ожидаема):")
    compare_with_hist()

print("\nПроверка assert_true в Hive (так будет работать задача check_acq_slice):")
try:
    hive = connect(to="DATALAKE", driver_args={"tez.queue.name": "ai"}, kerberos=KERBEROS, user_params=USER)
    with hive:
        hive.fetch((SQL_DIR / "check_acq_slice.sql").read_text(encoding="utf-8").format(**TABLES))
    print("assert_true в Hive отработал без ошибки")
except Exception as exc:
    print(f"Hive-проверка не выполнилась: {exc}")
