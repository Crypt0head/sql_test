"""Сколько данных в вьюхах КЕДР и зачем фильтр по списку ИНН.

Запуск в Jupyter: %run _check_kedr_volume.py
Только чтение: ничего не создаёт и не меняет в озере.

Шаги (от лёгкого к тяжёлому):
  1. Из каких таблиц собраны вьюхи.
  2. Размер партиций по метаданным (данные не читаются).
  3. Месяц MONTH только по нашим ИНН: строки, ИНН, время.
  4. Месяц MONTH целиком: строки, ИНН, время (если RUN_FULL_COUNT = True).
"""
import re
import time

import pandas as pd
from IPython.display import display
from rail_connectors.connection import connect

MONTH = 202607                # месяц для замеров (YYYYMM)
RUN_FULL_COUNT = True         # шаг 4: полный подсчёт месяца, может идти долго
VIEWS = {"dmkb": "Kedr.v_detail_dmkb", "dmsb": "Kedr.v_detail_dmsb", "dkb": "Kedr.v_detail_dkb"}
OUR_TABLE = "sandbox_ai.shestopalov_kedr_obshiy_chod_inn_month"

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)
pd.set_option("display.max_colwidth", 200)

imp = connect(
    to="IMPALA",
    extra_options={"db": "sandbox_ai"},
    driver_args={"tez.queue.name": "ai"},
    kerberos={
        "keytab_path": "/home/jovyan/test_requests/tech.keytab",
        "use_credentials": True,
        "update_keytab": True,
    },
    user_params={"user_name": input("Login (keytab): ").strip()},
)


def imp_fetch(sql):
    with imp:
        imp.execute("SET MEM_LIMIT=4g")
        return imp.fetch(sql)


def timed(sql):
    start = time.time()
    try:
        df = imp_fetch(sql)
        return df, round(time.time() - start, 1), None
    except Exception as exc:
        return None, round(time.time() - start, 1), str(exc)[:600]


def size_gb(value):
    m = re.match(r"\s*([\d.]+)\s*([KMGT]?B)", str(value))
    if not m:
        return None
    k = {"B": 0, "KB": 1, "MB": 2, "GB": 3, "TB": 4}[m.group(2)]
    return round(float(m.group(1)) * 1024 ** k / 1024 ** 3, 2)


print("=== 1. Из чего собраны вьюхи ===")
base_tables = {}
for src, view in VIEWS.items():
    df, sec, err = timed(f"SHOW CREATE VIEW {view}")
    if err:
        print(src, "ошибка:", err)
        continue
    ddl = " ".join(df.iloc[:, 0].astype(str))
    found = sorted(set(re.findall(r"(?:FROM|JOIN)\s+`?([\w]+)`?\.`?([\w]+)`?", ddl, flags=re.I)))
    base_tables[src] = [f"{s}.{t}" for s, t in found]
    print(f"\n{src}: таблицы {base_tables[src]}")
    print(ddl[:1500] + (" ..." if len(ddl) > 1500 else ""))

print("\n=== 2. Размер партиций по метаданным (последние 12) ===")
summary = []
for src, tables in base_tables.items():
    for table in tables:
        df, sec, err = timed(f"SHOW TABLE STATS {table}")
        if err:
            print(table, "ошибка:", err)
            continue
        df.columns = [str(c) for c in df.columns]
        size_col = next((c for c in df.columns if c.lower() == "size"), None)
        rows_col = next((c for c in df.columns if c.lower() == "#rows"), None)
        files_col = next((c for c in df.columns if c.lower() == "#files"), None)
        if size_col:
            df["size_gb"] = df[size_col].map(size_gb)
        total = df[df.iloc[:, 0].astype(str).str.lower() == "total"]
        parts = df.drop(total.index)
        print(f"\n{src} / {table}: партиций {len(parts)}")
        display(parts.tail(12))
        if len(total):
            display(total)
        month_row = parts[parts.apply(lambda r: str(MONTH) in " ".join(map(str, r.values)), axis=1)]
        if len(month_row):
            r = month_row.iloc[0]
            summary.append({
                "src": src, "table": table, "month": MONTH,
                "rows_by_stats": r.get(rows_col) if rows_col else None,
                "files": r.get(files_col) if files_col else None,
                "size_gb": r.get("size_gb"),
                "total_size_gb": total["size_gb"].iloc[0] if len(total) and size_col else None,
            })
if summary:
    print(f"\nИтог по месяцу {MONTH} (rows_by_stats = -1 значит статистика не собрана):")
    display(pd.DataFrame(summary))

print(f"\n=== 3. Месяц {MONTH} только по нашим ИНН ===")
our = imp_fetch(f"""
    SELECT count(DISTINCT inn) AS n_inn
    FROM {OUR_TABLE}
    WHERE cast(yearmm AS bigint) = {MONTH}
""")
print("ИНН в нашем списке:", int(our.n_inn.iloc[0]))
results = []
for src, view in VIEWS.items():
    df, sec, err = timed(f"""
        SELECT count(*) AS n_rows, count(DISTINCT v.inn) AS n_inn
        FROM {view} v
        WHERE v.yearmm = {MONTH}
          AND regexp_replace(cast(v.inn AS string), '[^0-9]', '') IN (
              SELECT DISTINCT inn FROM {OUR_TABLE} WHERE cast(yearmm AS bigint) = {MONTH}
          )
    """)
    results.append({"src": src, "mode": "наши ИНН", "sec": sec,
                    "n_rows": None if err else int(df.n_rows.iloc[0]),
                    "n_inn": None if err else int(df.n_inn.iloc[0]), "error": err})
    print(src, "готово за", sec, "с", "ошибка" if err else "")

if RUN_FULL_COUNT:
    print(f"\n=== 4. Месяц {MONTH} целиком (может идти долго) ===")
    for src, view in VIEWS.items():
        df, sec, err = timed(f"""
            SELECT count(*) AS n_rows, count(DISTINCT inn) AS n_inn
            FROM {view}
            WHERE yearmm = {MONTH}
        """)
        results.append({"src": src, "mode": "весь месяц", "sec": sec,
                        "n_rows": None if err else int(df.n_rows.iloc[0]),
                        "n_inn": None if err else int(df.n_inn.iloc[0]), "error": err})
        print(src, "готово за", sec, "с", "ошибка" if err else "")

print("\n=== Итог ===")
res = pd.DataFrame(results)
display(res.drop(columns="error"))
res[["n_rows", "n_inn"]] = res[["n_rows", "n_inn"]].apply(pd.to_numeric, errors="coerce")
full = res[res["mode"] == "весь месяц"].set_index("src")
part = res[res["mode"] == "наши ИНН"].set_index("src")
if len(full):
    share = pd.DataFrame({
        "строк_наши_%": (part.n_rows / full.n_rows * 100).round(2),
        "ИНН_наши_%": (part.n_inn / full.n_inn * 100).round(2),
    })
    print("Доля наших клиентов в КЕДР:")
    display(share)
errors = res[res.error.notna()]
if len(errors):
    print("\nТексты ошибок (по ним видно: объём/память или битые файлы HDFS):")
    for _, r in errors.iterrows():
        print(f"- {r.src} / {r['mode']}: {r.error}")
