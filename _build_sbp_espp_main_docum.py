# -*- coding: utf-8 -*-
"""Build notebook: разбор скрипта коллеги по СБП (ESPP-платёжки в scd1_z_main_docum)."""
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
        """# СБП в `ods.scd1_z_main_docum`: разбор скрипта коллеги (для Алины)

Исходный скрипт:

```sql
select * from ods.scd1_z_main_docum md
join ods.scd1_z_name_paydoc np on np.id = md.c_vid_doc and np.c_code = 'БЕЗН_ПЛ_ПОРУЧ'
join eq_account ea on ea.id = md.c_acc_kt
where 1=1
  and md.c_document_uno like 'ESPP%'
```

Что он делает: берёт **безналичные платёжные поручения** ЦФТ, у которых уникальный номер документа (`c_document_uno`)
начинается с `ESPP`, и оставляет только те, где счёт по кредиту (`c_acc_kt`) есть в `eq_account`.

Гипотеза: `ESPP` — префикс системы, через которую в ЦФТ приходят операции СБП. Тетрадка её проверяет.

Шаги:
1. Найти таблицу `eq_account` (в скрипте она без схемы).
2. Структура трёх таблиц.
3. Справочник видов документов: есть ли `БЕЗН_ПЛ_ПОРУЧ`.
4. Префиксы `c_document_uno` у платёжек за период — какие системы вообще бывают.
5. Скрипт коллеги как есть (с фильтром по дате) — выборка строк.
6. Сколько ESPP-документов теряет `join eq_account`.
7. Назначения платежа у ESPP: есть ли признаки СБП (`СБП`, `быстрых платежей`, `НСПК`).
8. ESPP в других видах документов (не только платёжки).
9. Выгрузка в Excel.

Период задаётся в ячейке параметров по `c_date_prov` (дата проводки, varchar).
"""
    )
)

cells.append(
    code(
        r"""import re
import sys
from pathlib import Path

import pandas as pd
from rail_connectors.connection import connect

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 200)
pd.set_option('display.max_rows', 300)

LAKE_USER = 'Shestopalov-VYur'
LAKE_PASSWORD = None
for cand in [
    Path.cwd(),
    Path.cwd() / 'sources' / 'sql',
    Path('/home/jovyan/documents/Equaring'),
    Path('/home/jovyan/documents/Equaring/sources/sql'),
]:
    if (cand / 'connection_secrets.py').exists():
        if str(cand) not in sys.path:
            sys.path.insert(0, str(cand))
        from connection_secrets import LAKE_USER, LAKE_PASSWORD
        break

user_params = {'user_name': LAKE_USER}
if LAKE_PASSWORD:
    user_params['password'] = LAKE_PASSWORD

imp = connect(
    to='IMPALA',
    extra_options={'db': 'sandbox_ai'},
    driver_args={'tez.queue.name': 'ai'},
    kerberos={
        'keytab_path': '/home/jovyan/test_requests/tech.keytab',
        'use_credentials': True,
        'update_keytab': True,
    },
    user_params=user_params,
)
imp._init_connection()
print('imp ready, user =', LAKE_USER)"""
    )
)

cells.append(
    code(
        r"""# Период по c_date_prov: [PERIOD_START, PERIOD_END_EXCL)
PERIOD_START = '2026-07-01'
PERIOD_END_EXCL = '2026-08-01'

# Короткое окно для выборок строк (шаги 5 и 7), чтобы не тащить весь месяц
SAMPLE_START = '2026-07-01'
SAMPLE_END_EXCL = '2026-07-08'
SAMPLE_LIMIT = 500

MEM_LIMIT = '16g'

MAIN_DOCUM = 'ods.scd1_z_main_docum'
NAME_PAYDOC = 'ods.scd1_z_name_paydoc'
PAYDOC_CODE = 'БЕЗН_ПЛ_ПОРУЧ'
UNO_PREFIX = 'ESPP'

# Если шаг 1 найдёт таблицу — подставится автоматически; можно задать руками
EQ_ACCOUNT = None

OUT_XLSX = './sbp_espp_main_docum.xlsx'

PERIOD_WHERE = (
    f"cast(md.c_date_prov as date) >= cast('{PERIOD_START}' as date) "
    f"and cast(md.c_date_prov as date) < cast('{PERIOD_END_EXCL}' as date)"
)
SAMPLE_WHERE = (
    f"cast(md.c_date_prov as date) >= cast('{SAMPLE_START}' as date) "
    f"and cast(md.c_date_prov as date) < cast('{SAMPLE_END_EXCL}' as date)"
)

# Impala lower() не работает с кириллицей, поэтому варианты регистра перечислены явно
SBP_NAZN_RLIKE = '(СБП|сбп|SBP|sbp|[Бб]ыстрых платеж|БЫСТРЫХ ПЛАТЕЖ|НСПК|NSPK)'


def q(sql):
    with imp:
        imp.execute(f'set MEM_LIMIT={MEM_LIMIT}')
        df = imp.fetch(sql)
    if df is None:
        df = pd.DataFrame()
    df.columns = [str(c).lower() for c in df.columns]
    return df


def describe(table):
    df = q(f'describe {table}')
    return df[[c for c in df.columns if c in ('name', 'type', 'comment')]]


results = {}
print('period:', PERIOD_START, '..', PERIOD_END_EXCL, '| sample:', SAMPLE_START, '..', SAMPLE_END_EXCL)"""
    )
)

cells.append(
    md(
        """## 1) Где лежит `eq_account`

В скрипте таблица без схемы — значит, у коллеги она в схеме по умолчанию. Ищем по схемам `ods`, `ods_alpha` и текущей БД.
Если найдено несколько — берётся первая; при необходимости задайте `EQ_ACCOUNT` руками в ячейке параметров."""
    )
)

cells.append(
    code(
        r"""found = []
for schema in ['ods', 'ods_alpha', 'sandbox_ai', 'default']:
    try:
        t = q(f"show tables in {schema} like '*eq_account*'")
    except Exception as e:
        print(f'{schema}: skip ({type(e).__name__}: {str(e)[:120]})')
        continue
    for name in t.iloc[:, 0].tolist() if not t.empty else []:
        found.append(f'{schema}.{name}')

print('Найдено:', found or 'ничего')
if EQ_ACCOUNT is None and found:
    EQ_ACCOUNT = found[0]
print('EQ_ACCOUNT =', EQ_ACCOUNT)
if EQ_ACCOUNT is None:
    print('Таблица не найдена: уточните у коллеги полное имя и задайте EQ_ACCOUNT. Шаги 5–6 без неё не выполнятся.')"""
    )
)

cells.append(
    md(
        """## 2) Структура таблиц

Смотрим поля, которые использует скрипт (`c_vid_doc`, `c_acc_kt`, `c_document_uno`), и что ещё есть полезного:
суммы (`c_sum`, `c_sum_po`), номера счетов (`c_num_dt`, `c_num_kt`), назначение (`c_nazn`), даты."""
    )
)

cells.append(
    code(
        r"""md_schema = describe(MAIN_DOCUM)
print(MAIN_DOCUM, '— колонок:', len(md_schema))
display(md_schema)

np_schema = describe(NAME_PAYDOC)
print(NAME_PAYDOC, '— колонок:', len(np_schema))
display(np_schema)

if EQ_ACCOUNT:
    ea_schema = describe(EQ_ACCOUNT)
    print(EQ_ACCOUNT, '— колонок:', len(ea_schema))
    display(ea_schema)

md_cols = set(md_schema['name'].str.lower())
need = ['c_vid_doc', 'c_acc_kt', 'c_acc_dt', 'c_document_uno', 'c_num_dt', 'c_num_kt', 'c_sum', 'c_nazn', 'c_date_prov']
print('Нет в main_docum:', [c for c in need if c not in md_cols] or 'всё на месте')"""
    )
)

cells.append(
    md(
        """## 3) Справочник видов документов

`c_vid_doc` в `main_docum` ссылается на `z_name_paydoc.id`. Проверяем, что код `БЕЗН_ПЛ_ПОРУЧ` существует, и смотрим соседние коды."""
    )
)

cells.append(
    code(
        r"""paydoc = q(f'''
select np.id, np.c_code, np.c_name
from {NAME_PAYDOC} np
order by np.c_code
''')
print('Видов документов:', len(paydoc))
display(paydoc[paydoc['c_code'] == PAYDOC_CODE])
display(paydoc)
results['paydoc_ref'] = paydoc"""
    )
)

cells.append(
    md(
        """## 4) Префиксы `c_document_uno` у платёжных поручений

Буквенный префикс УНО — признак системы-источника документа. Смотрим, какие префиксы бывают, сколько документов и
сколько из них похожи на СБП по назначению платежа. Если СБП встречается и под другими префиксами — скрипт коллеги их пропускает."""
    )
)

cells.append(
    code(
        r"""uno_prefix = q(f'''
select
  regexp_extract(md.c_document_uno, '^[A-Za-z_]+', 0) as uno_prefix,
  count(*) as docs,
  sum(cast(md.c_sum as decimal(38,2))) as sum_total,
  sum(case when md.c_nazn rlike '{SBP_NAZN_RLIKE}' then 1 else 0 end) as docs_nazn_sbp,
  min(md.c_document_uno) as uno_example,
  min(md.c_nazn) as nazn_example
from {MAIN_DOCUM} md
join {NAME_PAYDOC} np on np.id = md.c_vid_doc and np.c_code = '{PAYDOC_CODE}'
where {PERIOD_WHERE}
group by 1
order by docs desc
''')
uno_prefix['share_nazn_sbp'] = (uno_prefix['docs_nazn_sbp'] / uno_prefix['docs']).round(4)
display(uno_prefix)
results['uno_prefix'] = uno_prefix"""
    )
)

cells.append(
    md(
        """## 5) Скрипт коллеги как есть

То же самое, что в исходнике, но с фильтром по дате и лимитом. Колонки разведены префиксами `md_`, `np_`, `ea_`,
чтобы не было дублей `id` / `c_code`."""
    )
)

cells.append(
    code(
        r"""if EQ_ACCOUNT is None:
    print('Пропуск: не задана EQ_ACCOUNT')
else:
    ea_cols = q(f'select * from {EQ_ACCOUNT} limit 0').columns.tolist()
    ea_select = ',\n  '.join(f'ea.{c} as ea_{c}' for c in ea_cols)
    colleague = q(f'''
select
  md.*,
  np.c_code as np_c_code,
  np.c_name as np_c_name,
  {ea_select}
from {MAIN_DOCUM} md
join {NAME_PAYDOC} np on np.id = md.c_vid_doc and np.c_code = '{PAYDOC_CODE}'
join {EQ_ACCOUNT} ea on ea.id = md.c_acc_kt
where {SAMPLE_WHERE}
  and md.c_document_uno like '{UNO_PREFIX}%'
limit {SAMPLE_LIMIT}
''')
    print('Строк:', len(colleague))
    # Колонки, где есть хоть одно значение, — их и разбирать
    non_empty = [c for c in colleague.columns if colleague[c].notna().any() and (colleague[c].astype(str).str.strip() != '').any()]
    print('Непустых колонок:', len(non_empty), 'из', len(colleague.columns))
    display(colleague[non_empty].head(30))
    results['colleague_sample'] = colleague[non_empty]"""
    )
)

cells.append(
    md(
        """### 5a) Профиль непустых колонок выборки

Для каждой колонки — число уникальных значений и самые частые. Константы (1 уникальное значение) — это и есть
общие признаки ESPP-документов; их стоит сравнить с другими префиксами."""
    )
)

cells.append(
    code(
        r"""if 'colleague_sample' in results:
    s = results['colleague_sample']
    prof = []
    for c in s.columns:
        vc = s[c].astype(str).value_counts(dropna=False)
        prof.append({
            'column': c,
            'nunique': s[c].nunique(dropna=True),
            'top_values': '; '.join(f'{k} ({v})' for k, v in vc.head(5).items()),
        })
    col_profile = pd.DataFrame(prof).sort_values(['nunique', 'column'])
    display(col_profile)
    results['colleague_col_profile'] = col_profile"""
    )
)

cells.append(
    md(
        """## 6) Сколько теряет `join eq_account`

В скрипте внутренний join: ESPP-платёжки, чей `c_acc_kt` пуст или отсутствует в `eq_account`, выпадают.
Считаем за весь период."""
    )
)

cells.append(
    code(
        r"""if EQ_ACCOUNT is None:
    print('Пропуск: не задана EQ_ACCOUNT')
else:
    join_loss = q(f'''
select
  case
    when md.c_acc_kt is null then 'c_acc_kt пуст'
    when ea.id is null then 'нет в eq_account'
    else 'есть в eq_account'
  end as kt_status,
  count(*) as docs,
  sum(cast(md.c_sum as decimal(38,2))) as sum_total
from {MAIN_DOCUM} md
join {NAME_PAYDOC} np on np.id = md.c_vid_doc and np.c_code = '{PAYDOC_CODE}'
left join {EQ_ACCOUNT} ea on ea.id = md.c_acc_kt
where {PERIOD_WHERE}
  and md.c_document_uno like '{UNO_PREFIX}%'
group by 1
order by docs desc
''')
    join_loss['share'] = (join_loss['docs'] / join_loss['docs'].sum()).round(4)
    display(join_loss)
    results['join_loss'] = join_loss"""
    )
)

cells.append(
    md(
        """## 7) Назначения платежа у ESPP

Схлопываем цифры в `#`, чтобы одинаковые шаблоны назначения собрались вместе. Если в топе — «перевод по СБП»,
«операция НСПК» и т.п., гипотеза подтверждается. Заодно — балансовые счета (первые 5 цифр) по дебету и кредиту:
по ним видно, это переводы физлиц, расчёты с НСПК или что-то ещё."""
    )
)

cells.append(
    code(
        r"""nazn_patterns = q(f'''
select
  regexp_replace(md.c_nazn, '[0-9]+', '#') as nazn_pattern,
  count(*) as docs,
  sum(cast(md.c_sum as decimal(38,2))) as sum_total,
  max(case when md.c_nazn rlike '{SBP_NAZN_RLIKE}' then 1 else 0 end) as has_sbp_word,
  min(md.c_nazn) as nazn_example
from {MAIN_DOCUM} md
join {NAME_PAYDOC} np on np.id = md.c_vid_doc and np.c_code = '{PAYDOC_CODE}'
where {PERIOD_WHERE}
  and md.c_document_uno like '{UNO_PREFIX}%'
group by 1
order by docs desc
limit 100
''')
display(nazn_patterns)
results['espp_nazn_patterns'] = nazn_patterns

acc_pairs = q(f'''
select
  substr(md.c_num_dt, 1, 5) as bal_dt,
  substr(md.c_num_kt, 1, 5) as bal_kt,
  count(*) as docs,
  sum(cast(md.c_sum as decimal(38,2))) as sum_total
from {MAIN_DOCUM} md
join {NAME_PAYDOC} np on np.id = md.c_vid_doc and np.c_code = '{PAYDOC_CODE}'
where {PERIOD_WHERE}
  and md.c_document_uno like '{UNO_PREFIX}%'
group by 1, 2
order by docs desc
limit 50
''')
display(acc_pairs)
results['espp_bal_accounts'] = acc_pairs"""
    )
)

cells.append(
    md(
        """## 8) ESPP в других видах документов

Если ESPP-документы есть и среди мемордеров и прочих видов, фильтр `БЕЗН_ПЛ_ПОРУЧ` отрезает часть СБП-операций."""
    )
)

cells.append(
    code(
        r"""espp_by_vid = q(f'''
select
  np.c_code,
  np.c_name,
  count(*) as docs,
  sum(cast(md.c_sum as decimal(38,2))) as sum_total
from {MAIN_DOCUM} md
left join {NAME_PAYDOC} np on np.id = md.c_vid_doc
where {PERIOD_WHERE}
  and md.c_document_uno like '{UNO_PREFIX}%'
group by 1, 2
order by docs desc
''')
espp_by_vid['share'] = (espp_by_vid['docs'] / espp_by_vid['docs'].sum()).round(4)
display(espp_by_vid)
results['espp_by_vid_doc'] = espp_by_vid"""
    )
)

cells.append(
    md(
        """## 9) Выгрузка

Каждый результат — отдельный лист Excel."""
    )
)

cells.append(
    code(
        r"""with pd.ExcelWriter(OUT_XLSX) as xw:
    for name, df in results.items():
        df.to_excel(xw, sheet_name=name[:31], index=False)
print('Сохранено:', Path(OUT_XLSX).resolve())
print('Листы:', list(results))"""
    )
)

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

out = Path(__file__).with_name("sbp_espp_main_docum.ipynb")
out.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print("written", out)
