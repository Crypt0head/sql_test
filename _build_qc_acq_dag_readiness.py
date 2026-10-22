# -*- coding: utf-8 -*-
"""QC before prod: DAG history in the lake vs dashboard table, DQ fail queries, DRP _test checks."""
from __future__ import annotations

import json
from pathlib import Path


def md(text: str) -> dict:
    if not text.endswith("\n"):
        text += "\n"
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)}


def code(text: str) -> dict:
    if not text.endswith("\n"):
        text += "\n"
    return {
        "cell_type": "code",
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": text.splitlines(keepends=True),
    }


cells = []

cells.append(
    md(
        """# Проверка DAG витрины final_df перед встречей с Леонидом

Тетрадка только читает данные. Запись есть в одном месте — блок 5 (копия в `_test`), и он выключен флагом.

| Блок | Что проверяем | Когда запускать |
|---|---|---|
| 1 | Что лежит в истории DAG в озере по месяцам, нет ли дублей | Сейчас |
| 2 | История DAG = боевая таблица дашборда (её заливал ноутбук `final_df_script_3_1`) | Сейчас |
| 3 | Расхождения по договорам за выбранный месяц | Сейчас, если в блоке 2 есть разница |
| 4 | Запросы DQ-проверок из `dq/*.yaml`: каждая должна вернуть 0 строк | Сейчас (папка `dq/` должна лежать в репозитории) |
| 5 | Подготовка тестовой таблицы DRP `_test` | Перед включением `LOAD_TO_DRP` |
| 6 | DRP `_test` = история в озере, повторный запуск ничего не меняет | После 1-го и 2-го запуска DAG с DRP |
| 7 | Сохранить итог в Excel для встречи | В конце |

Пароли не сохраняются: DRP спрашивает логин и пароль через `getpass`, Impala идёт через keytab.
"""
    )
)

cells.append(
    code(
        r"""from datetime import datetime
from getpass import getpass
from pathlib import Path

import jinja2
import pandas as pd
import yaml
from IPython.display import display
from rail_connectors.connection import connect

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 220)
pd.set_option('display.max_rows', 100)

REPO_DIR = Path('/home/jovyan/documents/Equaring/Фин.скрипт/Регламент/airflow_shestopalov_acq_dashbord')
DQ_DIR = REPO_DIR / 'dq'
DATA_DIR = Path('/home/jovyan/documents/Equaring/Data')
OUT_DIR = DATA_DIR / 'qc_acq_dag_readiness'
OUT_DIR.mkdir(parents=True, exist_ok=True)

HIST = 'sandbox_ai.shestopalov_acq_final_df_hist'
DASHBOARD = 'sbx_da.tmp_shestopalov_acq_fin_version'
DRP_TEST = 'sbx_da.tmp_shestopalov_acq_fin_version_test'
DRP_USER_DEFAULT = 'Shestopalov-VYur'
MEM_LIMIT = '8g'

METRICS = [
    'trx_cnt', 'trx_sum', 'commission_from_ops', 'commission_monthly', 'commission_total',
    'amortization', 'chod', 'fin_result', 'term_cnt', 'active_term_cnt', 'retl_cnt',
]
# Сверка с ODS читает scd1_trx за месяцы проверки: 15+ минут.
RUN_RECONCILE_TRX = True
# Блок 5 создаёт и заполняет DRP_TEST. Включить один раз перед первым запуском DAG с DRP.
PREPARE_DRP_TEST = False

print('hist      ', HIST)
print('dashboard ', DASHBOARD)
print('drp test  ', DRP_TEST)
print('dq dir    ', DQ_DIR, 'exists=', DQ_DIR.exists())
"""
    )
)

cells.append(md("## 0) Подключения"))

cells.append(
    code(
        r"""if 'imp' not in globals() or imp is None:
    imp = connect(
        to='IMPALA',
        extra_options={'db': 'sandbox_ai'},
        driver_args={'tez.queue.name': 'ai'},
        kerberos={
            'keytab_path': '/home/jovyan/test_requests/tech.keytab',
            'use_credentials': True,
            'update_keytab': True,
        },
        user_params={'user_name': 'Shestopalov-VYur'},
    )
    imp._init_connection()
print('Impala ok')

if 'drp' not in globals() or drp is None:
    drp_user = input('DRP user: ').strip() or DRP_USER_DEFAULT
    drp = connect(to='DRP', user_params={'user_name': drp_user, 'password': getpass('DRP password: ')})
print('DRP ok')


def fetch_imp(sql, label):
    t0 = datetime.now()
    with imp:
        imp.execute(f'SET MEM_LIMIT={MEM_LIMIT}')
        df = imp.fetch(sql)
    df = pd.DataFrame() if df is None else df
    print(f'IMP {label}: rows={len(df):,} {(datetime.now() - t0).total_seconds():.0f}s')
    return df


def fetch_drp(sql, label):
    t0 = datetime.now()
    with drp:
        df = drp.fetch(sql)
    df = pd.DataFrame() if df is None else df
    print(f'DRP {label}: rows={len(df):,} {(datetime.now() - t0).total_seconds():.0f}s')
    return df


def drp_num(col):
    # В DRP все колонки текстовые: пустые строки и 'nan' считаем NULL.
    return (
        f"CASE WHEN btrim(cast({col} AS text)) ~ '^-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?$' "
        f"THEN btrim(cast({col} AS text))::numeric END"
    )


def month_totals_imp(table):
    sums = ',\n'.join(f'sum(cast({m} AS double)) AS {m}' for m in METRICS)
    return fetch_imp(
        f'''
        SELECT report_month, count(*) AS n_rows, count(DISTINCT n_agr) AS n_agr, {sums}
        FROM {table}
        GROUP BY report_month
        ORDER BY report_month
        ''',
        f'totals {table}',
    )


def month_totals_drp(table):
    sums = ',\n'.join(f'sum({drp_num(m)}) AS {m}' for m in METRICS)
    return fetch_drp(
        f'''
        SELECT substring(btrim(cast(report_month AS text)) FROM 1 FOR 7) AS report_month,
               count(*) AS n_rows,
               count(DISTINCT cast(n_agr AS text)) AS n_agr,
               {sums}
        FROM {table}
        GROUP BY 1
        ORDER BY 1
        ''',
        f'totals {table}',
    )


def compare_totals(left, right, left_name, right_name):
    cols = ['n_rows', 'n_agr'] + METRICS
    l = left.copy()
    r = right.copy()
    for df in (l, r):
        df['report_month'] = df['report_month'].astype(str).str[:7]
        for c in cols:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    m = l.merge(r, on='report_month', how='outer', suffixes=(f'_{left_name}', f'_{right_name}'))
    out = m[['report_month']].copy()
    for c in cols:
        a, b = m[f'{c}_{left_name}'], m[f'{c}_{right_name}']
        out[f'{c}_{left_name}'] = a
        out[f'{c}_{right_name}'] = b
        out[f'{c}_diff'] = (a - b).round(2)
    diff_cols = [f'{c}_diff' for c in cols]
    out['all_equal'] = out[diff_cols].fillna(1e9).abs().le(0.01).all(axis=1)
    return out.sort_values('report_month')


print('helpers ready')
"""
    )
)

cells.append(
    md(
        """## 1) История DAG в озере

Ожидаем: одна строка на договор (`n_rows = n_agr`), июль около 6 790 строк.
Какие месяцы есть, зависит от того, когда DAG начал работать: он пересчитывает только текущий месяц и прошлый в первые 10 дней.
"""
    )
)

cells.append(
    code(
        r"""hist_totals = month_totals_imp(HIST)
hist_totals['dups'] = hist_totals['n_rows'] - hist_totals['n_agr']
display(hist_totals)

hist_months = sorted(hist_totals['report_month'].astype(str))
print('Месяцы в истории:', hist_months)
if (hist_totals['dups'] != 0).any():
    print('ВНИМАНИЕ: есть дубли договоров в месяце')
else:
    print('Дублей нет')
"""
    )
)

cells.append(
    md(
        """## 2) История DAG = боевая таблица дашборда?

Сравниваем по месяцам, которые есть в обеих таблицах. Для закрытых месяцев разница должна быть 0 или объяснимой:
например, дашборд залит раньше, а поздние транзакции и документы DOC_OPER пришли потом.
"""
    )
)

cells.append(
    code(
        r"""dash_totals = month_totals_drp(DASHBOARD)
cmp_dash = compare_totals(hist_totals.drop(columns='dups'), dash_totals, 'dag', 'dash')
cmp_common = cmp_dash[cmp_dash['n_rows_dag'].notna() & cmp_dash['n_rows_dash'].notna()]
display(cmp_common[['report_month', 'n_rows_dag', 'n_rows_dash', 'trx_sum_dag', 'trx_sum_dash',
                    'trx_sum_diff', 'commission_total_diff', 'fin_result_diff', 'all_equal']])
print('Только в DAG:', cmp_dash.loc[cmp_dash['n_rows_dash'].isna(), 'report_month'].tolist())
print('Только в дашборде:', cmp_dash.loc[cmp_dash['n_rows_dag'].isna(), 'report_month'].tolist())
"""
    )
)

cells.append(
    md(
        """## 3) Расхождения по договорам за один месяц

По умолчанию — последний общий месяц. Если в блоке 2 всё совпало, блок можно пропустить.
"""
    )
)

cells.append(
    code(
        r"""common_months = cmp_common['report_month'].tolist()
AGR_MONTH = common_months[-1] if common_months else None
print('Месяц:', AGR_MONTH)

agr_diff = pd.DataFrame()
if AGR_MONTH:
    cols_imp = ', '.join(f'cast({m} AS double) AS {m}' for m in METRICS)
    dag_agr = fetch_imp(
        f"SELECT cast(n_agr AS string) AS n_agr, cast(filial_rf AS string) AS filial_rf, {cols_imp} "
        f"FROM {HIST} WHERE report_month = '{AGR_MONTH}'",
        'dag agreements',
    )
    cols_drp = ', '.join(f'{drp_num(m)} AS {m}' for m in METRICS)
    dash_agr = fetch_drp(
        f"SELECT cast(n_agr AS text) AS n_agr, cast(filial_rf AS text) AS filial_rf, {cols_drp} "
        f"FROM {DASHBOARD} WHERE substring(btrim(cast(report_month AS text)) FROM 1 FOR 7) = '{AGR_MONTH}'",
        'dashboard agreements',
    )
    for df in (dag_agr, dash_agr):
        df['n_agr'] = df['n_agr'].astype(str).str.strip()
        for m in METRICS:
            df[m] = pd.to_numeric(df[m], errors='coerce').fillna(0)
    m = dag_agr.merge(dash_agr, on='n_agr', how='outer', suffixes=('_dag', '_dash'), indicator=True)
    m['where'] = m['_merge'].map({'left_only': 'only_dag', 'right_only': 'only_dash', 'both': 'both'})
    for c in METRICS:
        m[f'{c}_diff'] = (m[f'{c}_dag'].fillna(0) - m[f'{c}_dash'].fillna(0)).round(2)
    diff_cols = [f'{c}_diff' for c in METRICS]
    agr_diff = m[(m['where'] != 'both') | m[diff_cols].abs().gt(0.01).any(axis=1)].copy()
    agr_diff['abs_trx_sum_diff'] = agr_diff['trx_sum_diff'].abs()
    agr_diff = agr_diff.sort_values('abs_trx_sum_diff', ascending=False)

    print(m['where'].value_counts().to_dict())
    print('Договоров с разницей хотя бы по одной метрике:', len(agr_diff))
    print('В каких метриках разница (число договоров):')
    display(agr_diff[diff_cols].abs().gt(0.01).sum().to_frame('n_agr'))
    display(agr_diff[['n_agr', 'where', 'filial_rf_dag', 'filial_rf_dash'] + diff_cols].head(30))
"""
    )
)

cells.append(
    md(
        """## 4) DQ-проверки вручную

Берём файлы `dq/acq_dq_*.yaml` из репозитория, подставляем параметры так же, как DQ DAG, и выполняем каждый `fail query` в Impala.
Проверка пройдена, если запрос вернул 0 строк. Если строки есть — смотрим их: это ошибка в данных или порог надо поправить.

Месяцы проверки по умолчанию — два последних месяца истории (как DAG в первые 10 дней месяца). «Закрытый» месяц — предпоследний.
"""
    )
)

cells.append(
    code(
        r"""def shift(label, delta):
    y, m = map(int, label.split('-'))
    i = y * 12 + m - 1 + delta
    return f'{i // 12}-{i % 12 + 1:02d}'


CHECK_MONTHS = hist_months[-2:]
assert len(CHECK_MONTHS) == 1 or shift(CHECK_MONTHS[0], 1) == CHECK_MONTHS[1], 'месяцы должны идти подряд'
dq_params = {
    'hist': HIST,
    'acq_months': ', '.join(f"'{x}'" for x in CHECK_MONTHS),
    'acq_months_select': ' UNION ALL '.join(f"SELECT '{x}' AS report_month" for x in CHECK_MONTHS),
    'closed_month': CHECK_MONTHS[-2] if len(CHECK_MONTHS) > 1 else shift(CHECK_MONTHS[0], -1),
    'period_start': f'{CHECK_MONTHS[0]}-01',
    'period_end': f'{shift(CHECK_MONTHS[-1], 1)}-01',
}
display(pd.Series(dq_params).to_frame('value'))

dq_rows = []
dq_failed = {}
for path in sorted(DQ_DIR.glob('acq_dq_*.yaml')):
    if path.stem == 'acq_dq_alert_test':
        continue
    if path.stem == 'acq_dq_reconcile_trx' and not RUN_RECONCILE_TRX:
        print('SKIP', path.name)
        continue
    rendered = yaml.safe_load(jinja2.Template(path.read_text(encoding='utf-8')).render(**dq_params))
    for checks in rendered.values():
        for item in checks:
            check = item['failed rows']
            try:
                res = fetch_imp(check['fail query'], check['name'])
                status = 'OK' if res.empty else 'FAIL'
                if not res.empty:
                    dq_failed[check['name']] = res
                dq_rows.append({'file': path.name, 'check': check['name'], 'status': status, 'rows': len(res)})
            except Exception as exc:
                dq_rows.append({'file': path.name, 'check': check['name'], 'status': 'ERROR', 'rows': None,
                                'error': f'{type(exc).__name__}: {exc}'[:300]})

dq_summary = pd.DataFrame(dq_rows)
display(dq_summary)
for name, res in dq_failed.items():
    print('===', name)
    display(res.head(20))
"""
    )
)

cells.append(
    md(
        """## 5) Подготовка тестовой таблицы DRP

Один раз перед первым запуском DAG с `LOAD_TO_DRP = True`. `DRP_TEST` — копия боевой таблицы дашборда со структурой и данными:
так проверка покажет и замену месяцев, и то, что другие месяцы не тронуты. Боевую таблицу блок только читает.

Если `DRP_TEST` уже есть (её создавала тетрадка `drp_month_replace_test.ipynb`), блок ничего не делает.
"""
    )
)

cells.append(
    code(
        r"""schema, table = DRP_TEST.split('.', 1)
exists = fetch_drp(
    f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'",
    'test table exists',
)
print(DRP_TEST, 'exists=', not exists.empty)

if PREPARE_DRP_TEST and exists.empty:
    with drp:
        drp.execute(f'CREATE TABLE {DRP_TEST} (LIKE {DASHBOARD} INCLUDING ALL)')
        drp.execute(f'INSERT INTO {DRP_TEST} SELECT * FROM {DASHBOARD}')
    print('Создана и заполнена', DRP_TEST)
elif not PREPARE_DRP_TEST:
    print('PREPARE_DRP_TEST = False, ничего не создаём')
"""
    )
)

cells.append(
    md(
        """## 6) DRP `_test` после запуска DAG с загрузкой в DRP

Запускать два раза: после 1-го и после 2-го запуска DAG в один и тот же день.

- Месяцы, которые пересчитал DAG, в `_test` должны совпасть с историей в озере.
- Остальные месяцы должны совпасть с боевой таблицей дашборда (их DAG трогать не должен).
- Снимок сохраняется в CSV; при повторном запуске он сравнивается с предыдущим. Повторный запуск DAG не должен ничего менять.
"""
    )
)

cells.append(
    code(
        r"""# Как acq_expected_months в DAG: месяц вчерашнего дня, и прошлый, если сегодня не позже 10-го.
PREV_MONTH_DAYS = 10
run_day = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
DRP_MONTHS = [run_day.strftime('%Y-%m')]
if run_day.day <= PREV_MONTH_DAYS:
    DRP_MONTHS.insert(0, shift(DRP_MONTHS[0], -1))
print('Месяцы, которые DAG сегодня заменил в DRP:', DRP_MONTHS)

test_totals = month_totals_drp(DRP_TEST)
cmp_test_hist = compare_totals(test_totals, hist_totals.drop(columns='dups'), 'test', 'dag')
cmp_test_hist = cmp_test_hist[cmp_test_hist['report_month'].isin(DRP_MONTHS)]
print('Месяцы DAG: _test = история в озере')
display(cmp_test_hist[['report_month', 'n_rows_test', 'n_rows_dag', 'trx_sum_diff', 'fin_result_diff', 'all_equal']])

cmp_test_dash = compare_totals(test_totals, dash_totals, 'test', 'dash')
cmp_test_dash = cmp_test_dash[~cmp_test_dash['report_month'].isin(DRP_MONTHS)]
print('Остальные месяцы: _test = дашборд (не тронуты)')
display(cmp_test_dash[['report_month', 'n_rows_test', 'n_rows_dash', 'trx_sum_diff', 'all_equal']])

snapshots = sorted(OUT_DIR.glob('drp_test_snapshot_*.csv'))
snap_path = OUT_DIR / f"drp_test_snapshot_{datetime.now():%Y%m%d_%H%M%S}.csv"
test_totals.to_csv(snap_path, index=False)
print('Снимок:', snap_path.name)
if snapshots:
    prev = pd.read_csv(snapshots[-1], dtype={'report_month': str})
    cmp_rerun = compare_totals(test_totals, prev, 'now', 'prev')
    print('Сравнение с предыдущим снимком', snapshots[-1].name)
    display(cmp_rerun[['report_month', 'n_rows_now', 'n_rows_prev', 'trx_sum_diff', 'all_equal']])
    print('Повторный запуск ничего не изменил:', bool(cmp_rerun['all_equal'].all()))
"""
    )
)

cells.append(md("## 7) Итог в Excel для встречи"))

cells.append(
    code(
        r"""out_xlsx = OUT_DIR / f"qc_acq_dag_readiness_{datetime.now():%Y%m%d}.xlsx"
with pd.ExcelWriter(out_xlsx, engine='openpyxl') as w:
    hist_totals.to_excel(w, sheet_name='hist_by_month', index=False)
    cmp_dash.to_excel(w, sheet_name='dag_vs_dashboard', index=False)
    if len(agr_diff):
        agr_diff.drop(columns=['_merge']).head(5000).to_excel(w, sheet_name=f'agr_diff_{AGR_MONTH}', index=False)
    if 'dq_summary' in globals():
        dq_summary.to_excel(w, sheet_name='dq_manual', index=False)
    if 'cmp_test_hist' in globals():
        cmp_test_hist.to_excel(w, sheet_name='drp_test_vs_hist', index=False)
        cmp_test_dash.to_excel(w, sheet_name='drp_test_vs_dash', index=False)
print('Saved', out_xlsx)
"""
    )
)

nb = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    },
    "cells": cells,
}

out = Path(__file__).with_name("qc_acq_dag_readiness.ipynb")
out.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print("wrote", out)
