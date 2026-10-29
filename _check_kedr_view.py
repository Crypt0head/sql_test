"""Актуальность view КЕДР коллег в DRP и сверка с нашей таблицей ЧОД КЕДР в озере.

Запуск в Jupyter: %run _check_kedr_view.py
Только чтение: ничего не создаёт и не меняет ни в DRP, ни в озере.
"""
from getpass import getpass

import pandas as pd
from IPython.display import display
from rail_connectors.connection import connect

DRP_VIEW = "sbx_edm_dfip.v_detail_dmkb_dkb_dmdsmark"
OUR_TABLE = "sandbox_ai.shestopalov_kedr_obshiy_chod_inn_month"
LAKE_VIEWS = {"dmkb": "Kedr.v_detail_dmkb", "dmsb": "Kedr.v_detail_dmsb", "dkb": "Kedr.v_detail_dkb"}
TECH_DRP_USER = "drp_raisa"

MONTH_COLS = ("yearmm", "year_mm", "yyyymm", "report_month", "period", "month", "dt", "report_dt")
INN_COLS = ("inn", "client_inn", "inn_org", "c_inn", "ul_inn")
VALUE_COLS = ("nbi_ssp", "obshiy_chod", "chod", "nbi", "kedr_obshiy_chod", "nbi_total")

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)

drp = connect(
    to="DRP",
    user_params={"user_name": input("DRP login: ").strip(), "password": getpass("DRP password: ")},
)
imp = connect(
    to="IMPALA",
    extra_options={"db": "sandbox_ai"},
    driver_args={"tez.queue.name": "ai"},
    kerberos={
        "keytab_path": "/home/jovyan/test_requests/tech.keytab",
        "use_credentials": True,
        "update_keytab": True,
    },
    user_params={"user_name": "Shestopalov-VYur"},
)


def drp_fetch(sql):
    with drp:
        return drp.fetch(sql)


def imp_fetch(sql):
    with imp:
        imp.execute("SET MEM_LIMIT=4g")
        return imp.fetch(sql)


def pick(columns, candidates):
    return next((c for c in candidates if c in columns), None)


schema, name = DRP_VIEW.split(".")

print("\n=== 1. Колонки view ===")
cols = drp_fetch(f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{name}'
    ORDER BY ordinal_position
""")
display(cols)
columns = set(cols.column_name.str.lower())
month_col, inn_col, value_col = pick(columns, MONTH_COLS), pick(columns, INN_COLS), pick(columns, VALUE_COLS)
print(f"месяц={month_col} инн={inn_col} значение={value_col}")

print("\n=== 2. 5 строк ===")
display(drp_fetch(f"SELECT * FROM {DRP_VIEW} LIMIT 5"))

print("\n=== 3. Из чего собрана view ===")
try:
    print(drp_fetch(f"SELECT pg_get_viewdef('{DRP_VIEW}'::regclass, true) AS def").iloc[0, 0])
except Exception as exc:
    print("Определение не видно:", exc)

print(f"\n=== 4. Есть ли SELECT у технической учётки {TECH_DRP_USER} (нужно для DAG) ===")
try:
    display(drp_fetch(f"""
        SELECT has_schema_privilege('{TECH_DRP_USER}', '{schema}', 'USAGE') AS usage_ok,
               has_table_privilege('{TECH_DRP_USER}', '{DRP_VIEW}', 'SELECT') AS select_ok
    """))
except Exception as exc:
    print("Проверить не удалось:", exc)

if not (month_col and inn_col and value_col):
    raise SystemExit("Не нашёл колонки месяца, ИНН или ЧОД. Пришлите вывод раздела 1 — допишу имена.")

print("\n=== 5. Актуальность view: месяцы, строки, ИНН, сумма ===")
view_months = drp_fetch(f"""
    SELECT {month_col} AS month, count(*) AS n_rows, count(DISTINCT {inn_col}) AS n_inn,
           sum(cast({value_col} AS numeric)) AS value_sum
    FROM {DRP_VIEW}
    GROUP BY {month_col}
    ORDER BY {month_col} DESC
    LIMIT 24
""")
display(view_months)

print("\n=== 6. Наша таблица в озере ===")
our_months = imp_fetch(f"""
    SELECT cast(yearmm AS bigint) AS yearmm, count(*) AS n_rows,
           count(kedr_obshiy_chod) AS n_filled, sum(kedr_obshiy_chod) AS value_sum
    FROM {OUR_TABLE}
    GROUP BY 1
    ORDER BY 1 DESC
""")
display(our_months)

print("\n=== 7. Вьюхи КЕДР в озере (из них собрана наша таблица): последний месяц ===")
for src, view in LAKE_VIEWS.items():
    try:
        display(imp_fetch(f"SELECT '{src}' AS src, max(yearmm) AS max_yearmm FROM {view}"))
    except Exception as exc:
        print(src, "ошибка:", exc)

print("\n=== 8. Сверка по ИНН за последний общий месяц ===")
filled = our_months.loc[our_months.n_filled > 0, "yearmm"].astype(str).tolist()
common = [m for m in view_months.month.astype(str).str.replace("-", "").str[:6] if m in filled]
if not common:
    print("Общих месяцев нет — сравнить не с чем.")
else:
    ym = common[0]
    view_month_value = view_months.month.astype(str)[
        view_months.month.astype(str).str.replace("-", "").str[:6] == ym
    ].iloc[0]
    v = drp_fetch(f"""
        SELECT regexp_replace(cast({inn_col} AS text), '[^0-9]', '', 'g') AS inn,
               sum(cast({value_col} AS numeric)) AS view_value
        FROM {DRP_VIEW}
        WHERE cast({month_col} AS text) = '{view_month_value}'
        GROUP BY 1
    """)
    o = imp_fetch(f"""
        SELECT regexp_replace(cast(inn AS string), '[^0-9]', '') AS inn, kedr_obshiy_chod AS our_value
        FROM {OUR_TABLE}
        WHERE cast(yearmm AS bigint) = {ym} AND kedr_obshiy_chod IS NOT NULL
    """)
    m = o.merge(v, on="inn", how="outer", indicator=True)
    m["view_value"] = pd.to_numeric(m.view_value, errors="coerce")
    m["our_value"] = pd.to_numeric(m.our_value, errors="coerce")
    both = m[m._merge == "both"]
    same = (both.view_value - both.our_value).abs() < 1
    print(f"Месяц {ym}: ИНН у нас {len(o)}, во view {len(v)}, общих {len(both)}, совпало значение {int(same.sum())}")
    print(f"Сумма у нас {o.our_value.astype(float).sum():,.0f}, во view по тем же ИНН {both.view_value.sum():,.0f}")
    display(both.loc[~same].assign(diff=both.view_value - both.our_value).sort_values("diff", key=abs, ascending=False).head(10))
