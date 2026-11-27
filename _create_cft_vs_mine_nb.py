# -*- coding: utf-8 -*-
"""Generate qc_te_cft_vs_mine.ipynb"""
import json
from pathlib import Path

nb = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "pygments_lexer": "ipython3"},
    },
    "cells": [],
}


def md(text: str) -> None:
    nb["cells"].append({"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)})


def code(text: str) -> None:
    nb["cells"].append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
                        "source": text.splitlines(keepends=True)})


md(
    """# Выгрузка договоров из ЦФТ и моя выгрузка из Альфы

- Какие договоры ЦФТ идут в сравнение, задаёт `CFT_ACTIVE_RULE`. По умолчанию `'term'` - как в Альфе: дата начала не позже даты среза, дата окончания пустая или не раньше неё. Дата закрытия не учитывается: в ЦФТ она обычно на полгода позже даты окончания. `'all'` - весь файл. Колонки ищутся по названиям: наименование клиента, номер договора, статус, код филиала, даты начала, окончания и закрытия, тарифный план, индивидуальный тарифный план, ID, комиссии. Необязательные колонки, если их нет, пропускаются.
- Моя выгрузка - лист `С ТЭ на <дата>` из `trade_acquiring_clients_<дата>.xlsx`. **Дата должна совпадать с датой среза ЦФТ**, иначе часть расхождений будет из-за разницы дат.
- Договоры сопоставляются по номеру. Если номер не совпал, но совпал ID ЦФТ с моим «ЦФТ ID», это тот же договор под другим номером.
- К озеру один запрос: `ods_alpha.scd1_agreements` + `scd1_companies` (около минуты). Из него ИНН и наименование для номеров ЦФТ и причины. `scd1_agr_terms` не читается: если договор ЦФТ в Альфе проходит все фильтры, кроме привязки P, а у меня его нет, причина - привязка терминала P.
- `PREV_FILE` - прошлая выгрузка Снигура, для сравнения «что изменилось». Если файла нет, блок пропускается.
"""
)

code(
    r"""import getpass
import re
import time
from pathlib import Path

import pandas as pd
from rail_connectors.connection import connect

AS_OF_DATE = '2026-09-01'
BASE_DIR = Path('/home/jovyan/documents/Equaring')
CFT_FILE = Path('CFT_clients_010926.xlsx').resolve()  # файл лежит рядом с тетрадкой
CFT_SHEET = 0
CFT_ACTIVE_RULE = 'term'  # 'term' - как в Альфе: начало <= даты, окончание пустое или >= даты (дата закрытия не учитывается); 'all' - весь файл
MINE_FILE = BASE_DIR / ('trade_acquiring_clients_%s.xlsx' % AS_OF_DATE)
PREV_FILE = BASE_DIR / 'Скрипт для Ани по сборке продукта эквайринг' / 'cdwh_acq_clients.xlsx'
OUT_FILE = BASE_DIR / ('te_cft_vs_mine_%s.xlsx' % AS_OF_DATE)

as_of = pd.Timestamp(AS_OF_DATE)
as_of_ru = as_of.strftime('%d.%m.%Y')
MINE_SHEET = 'С ТЭ на %s' % as_of_ru
print(CFT_FILE, CFT_FILE.exists(), MINE_FILE, MINE_FILE.exists(), MINE_SHEET, sep='\n')

LAKE_USER = input('Логин Impala: ').strip()
LAKE_PASSWORD = getpass.getpass('Пароль Impala: ')
imp = connect(
    to='IMPALA',
    extra_options={'db': 'sandbox_ai'},
    driver_args={'tez.queue.name': 'ai'},
    kerberos={
        'keytab_path': '/home/jovyan/test_requests/tech.keytab',
        'use_credentials': True,
        'update_keytab': True,
    },
    user_params={'user_name': LAKE_USER, 'password': LAKE_PASSWORD},
)
imp._init_connection()
print('imp ready')
"""
)

md("## 1. Файлы")

code(
    r"""def norm_agr(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if re.fullmatch(r'\d+\.0', s):
        s = s[:-2]
    s = re.sub(r'\s+', '', s).upper()
    return None if s in ('', 'NONE', 'NAN', '<NA>') else s

def norm_id(v):
    k = norm_agr(v)
    if k is None:
        return None
    return (k.lstrip('0') or '0') if k.isdigit() else k

def norm_code(v):
    k = norm_agr(v)
    return k.zfill(3) if k and k.isdigit() else k

def to_date(s):
    s = pd.Series(s, dtype=object)
    iso = s.map(lambda v: isinstance(v, str) and bool(re.match(r'\s*\d{4}-\d{2}-\d{2}', v)))
    out = pd.to_datetime(s.where(~iso), errors='coerce', dayfirst=True)
    out[iso] = pd.to_datetime(s[iso].str.strip().str[:10], errors='coerce', format='%Y-%m-%d')
    return out.dt.normalize()

def _low(v):
    return re.sub(r'\s+', ' ', str(v)).strip().lower()

def pick(df, must, exclude=(), required=True, regex=None):
    hits = [c for c in df.columns
            if all(k in _low(c) for k in must) and not any(x in _low(c) for x in exclude)
            and (regex is None or re.search(regex, _low(c)))]
    if len(hits) == 1:
        return hits[0]
    if not hits and not required:
        return None
    raise KeyError('Колонка %s: найдено %s. Все колонки: %s' % (must, hits, list(df.columns)))

def read_with_header(path, sheet, marker='договор', max_rows=20):
    raw = pd.read_excel(path, sheet_name=sheet, header=None, dtype=object)
    hdr = next(i for i in range(min(max_rows, len(raw)))
               if any(marker in _low(v) for v in raw.iloc[i] if pd.notna(v)))
    df = raw.iloc[hdr + 1:].copy()
    df.columns = [_low(v) if pd.notna(v) else 'col_%s' % j for j, v in enumerate(raw.iloc[hdr])]
    return df.dropna(how='all').reset_index(drop=True)

cft_raw = read_with_header(CFT_FILE, CFT_SHEET)
COLS = {
    'name': pick(cft_raw, ['наимен'], exclude=['тариф']),
    'agr': pick(cft_raw, ['номер', 'договор'], exclude=['тариф', 'индивид']),
    'status': pick(cft_raw, ['статус']),
    'start': pick(cft_raw, ['начал']),
    'end': pick(cft_raw, ['окончан'], exclude=['тариф']),
    'close': pick(cft_raw, ['закрыт'], exclude=['статус']),
    'branch': pick(cft_raw, ['филиал'], required=False),
    'tariff': pick(cft_raw, ['тариф'], exclude=['индивид', 'комисс', 'дата'], required=False),
    'ind_tariff': pick(cft_raw, ['индивид'], exclude=['комисс'], required=False),
    'id': pick(cft_raw, [], regex=r'(^|\W)id(\W|$)', required=False),
}
comm_cols = [c for c in cft_raw.columns if 'комисс' in _low(c)]
for k, v in COLS.items():
    print('%-10s -> %s' % (k, v))
print('комиссии   ->', comm_cols)

def col(key, fn=None):
    c = COLS[key]
    if c is None:
        return pd.Series(pd.NA, index=cft_raw.index, dtype=object)
    return fn(cft_raw[c]) if fn else cft_raw[c].astype('string').str.strip()

cft = pd.DataFrame({
    'agr_key': cft_raw[COLS['agr']].map(norm_agr),
    'Номер договора (ЦФТ)': col('agr'),
    'Клиент (ЦФТ)': col('name'),
    'Статус (ЦФТ)': col('status'),
    'Код филиала': col('branch', lambda s: s.map(norm_code)),
    'Дата начала (ЦФТ)': to_date(cft_raw[COLS['start']]),
    'Дата окончания (ЦФТ)': to_date(cft_raw[COLS['end']]),
    'Дата закрытия (ЦФТ)': to_date(cft_raw[COLS['close']]),
    'Тарифный план (ЦФТ)': col('tariff'),
    'Индивидуальный ТП (ЦФТ)': col('ind_tariff'),
    'ID (ЦФТ)': col('id', lambda s: s.map(norm_id)),
})
cft_rows = len(cft)
dup_numbers = int(cft['agr_key'].duplicated(keep=False).sum())
cft = cft[cft['agr_key'].notna()].drop_duplicates('agr_key').reset_index(drop=True)

cft['Действует на дату (ЦФТ)'] = (
    (cft['Дата начала (ЦФТ)'].isna() | (cft['Дата начала (ЦФТ)'] <= as_of))
    & (cft['Дата окончания (ЦФТ)'].isna() | (cft['Дата окончания (ЦФТ)'] >= as_of))
)
term_filter = pd.crosstab(
    cft['Статус (ЦФТ)'].fillna('(пусто)'),
    cft['Действует на дату (ЦФТ)'].map({True: 'Остаётся', False: 'Исключён'}),
    margins=True, margins_name='Всего',
).rename_axis('Статус в ЦФТ: фильтр по сроку действия на %s' % as_of_ru).reset_index()
cft_all = cft
if CFT_ACTIVE_RULE == 'term':
    cft = cft[cft['Действует на дату (ЦФТ)']].reset_index(drop=True)
elif CFT_ACTIVE_RULE != 'all':
    raise ValueError("CFT_ACTIVE_RULE: 'term' или 'all'")
print('Правило ЦФТ: %s | договоров в файле %s, в сравнении %s, исключено %s (начало позже даты: %s, окончание раньше даты: %s)' % (
    CFT_ACTIVE_RULE, len(cft_all), len(cft), len(cft_all) - len(cft),
    int((cft_all['Дата начала (ЦФТ)'] > as_of).sum()), int((cft_all['Дата окончания (ЦФТ)'] < as_of).sum())))
display(term_filter)

mine_raw = pd.read_excel(MINE_FILE, sheet_name=MINE_SHEET, dtype=object)
my = pd.DataFrame({
    'agr_key': mine_raw['Номер договора'].map(norm_agr),
    'Номер договора (мой)': mine_raw['Номер договора'].astype('string'),
    'ИНН (мой)': mine_raw['ИНН'].astype('string'),
    'ЦФТ ID': mine_raw['ЦФТ ID'].map(norm_id),
    'РФ': mine_raw['РФ, в котором обслуживается клиент'].astype('string').str.strip(),
})
my = my[my['agr_key'].notna()].drop_duplicates('agr_key').reset_index(drop=True)

fill = pd.DataFrame([
    (c, int(cft_raw[c].notna().sum()), round(cft_raw[c].notna().mean() * 100, 1)) for c in cft_raw.columns
], columns=['Колонка ЦФТ', 'Заполнено строк', 'Заполнено, %'])
print('ЦФТ: строк %s, уникальных номеров %s, строк с повтором номера %s | у меня: %s'
      % (cft_rows, len(cft), dup_numbers, len(my)))
display(fill)
"""
)

md("## 2. Один запрос к озеру: договоры и клиенты")

code(
    r"""t0 = time.time()
print('Запрос пошёл в', time.strftime('%H:%M:%S'), flush=True)
with imp:
    imp.execute('set MEM_LIMIT=16g')
    lake = imp.fetch('''
SELECT
  upper(regexp_replace(a.c_agr_number, '[[:space:]]', '')) AS agr_key,
  cast(a.n_agr AS string) AS n_agr,
  a.acq_class,
  a.d_valid_from,
  a.d_valid_to,
  coalesce(a.ods_deleted_flg, '0') AS agr_deleted,
  regexp_replace(c.c_inn, '[^0-9]', '') AS inn,
  c.c_cmp_name AS cmp_name,
  coalesce(c.ods_deleted_flg, '0') AS cmp_deleted
FROM ods_alpha.scd1_agreements a
LEFT JOIN ods_alpha.scd1_companies c ON c.n_cmp = a.n_cmp_client
WHERE a.c_agr_number IS NOT NULL
''')
lake.columns = [str(c).split('.')[-1].lower() for c in lake.columns]
lake['agr_deleted'] = lake['agr_deleted'].astype(str)
lake['cmp_deleted'] = lake['cmp_deleted'].astype(str)
lake['d_valid_from'] = pd.to_datetime(lake['d_valid_from'], errors='coerce').dt.normalize()
lake['d_valid_to'] = pd.to_datetime(lake['d_valid_to'], errors='coerce').dt.normalize()
print('Строк: %s за %.0f с' % (len(lake), time.time() - t0))
"""
)

md(
    """## 3. Сопоставление и причины

Порядок причин для «только в ЦФТ»: тот же договор у меня под другим номером (совпал ID) → что не так в Альфе (удалён, не SA, срок, клиент/ИНН, иначе привязка P) → номера нет в Альфе.

Для «только у меня»: тот же договор в ЦФТ под другим номером (совпал ID) → в ЦФТ нет ни одного договора этого филиала → тот же клиент в ЦФТ под другим номером (ИНН или наименование) → клиента в ЦФТ нет.
"""
)

code(
    r"""LEGAL_RE = re.compile(
    r'\b(ООО|ОАО|ЗАО|ПАО|АО|НАО|ИП|КФХ|ГКФХ|СПК|СХПК|ТОО|ПК|'
    r'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ|ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ|'
    r'АКЦИОНЕРНОЕ ОБЩЕСТВО|КРЕСТЬЯНСКОЕ \(?ФЕРМЕРСКОЕ\)? ХОЗЯЙСТВО|ГЛАВА)\b')

def norm_inn(v):
    if pd.isna(v):
        return None
    s = re.sub(r'\D', '', str(v))
    if len(s) in (9, 11):
        s = '0' + s
    return s or None

def norm_name(v):
    if pd.isna(v) or not str(v).strip():
        return None
    s = str(v).upper().replace('Ё', 'Е')
    s = re.sub(r'[^\w\s]', ' ', s)
    s = LEGAL_RE.sub(' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s or None

cmp_ok = lake[lake['cmp_deleted'].ne('1')]
inn_by_key = cmp_ok[cmp_ok['inn'].fillna('').ne('')].drop_duplicates('agr_key').set_index('agr_key')['inn']
name_by_key = cmp_ok.dropna(subset=['cmp_name']).drop_duplicates('agr_key').set_index('agr_key')['cmp_name']

cft['inn'] = cft['agr_key'].map(inn_by_key).map(norm_inn)
cft['name_n'] = cft['Клиент (ЦФТ)'].map(norm_name)
my['inn'] = my['ИНН (мой)'].map(norm_inn)
my['name_n'] = my['agr_key'].map(name_by_key).map(norm_name)
for df in (cft, my):
    df['client'] = df['inn'].where(df['inn'].notna(), 'Н:' + df['name_n'].fillna('?'))

both_keys = set(cft['agr_key']) & set(my['agr_key'])
cft['Есть у меня'] = cft['agr_key'].isin(both_keys)
my['Есть в ЦФТ'] = my['agr_key'].isin(both_keys)

pairs = cft[cft['Есть у меня']].merge(my[['agr_key', 'ЦФТ ID']], on='agr_key')
pairs = pairs[pairs['ID (ЦФТ)'].notna() & pairs['ЦФТ ID'].notna()]
id_same = float((pairs['ID (ЦФТ)'] == pairs['ЦФТ ID']).mean() * 100) if len(pairs) else 0.0
ID_IS_AGR = False
print('id в ЦФТ совпадает с моим «ЦФТ ID» (ID клиента) у %.1f%% общих договоров (%s пар). '
      'Что это за id, проверяет qc_te_cft_report_alpha.ipynb; здесь по id не сопоставляю.' % (id_same, len(pairs)))

my_by_id = my[~my['Есть в ЦФТ'] & my['ЦФТ ID'].notna()].drop_duplicates('ЦФТ ID').set_index('ЦФТ ID')['Номер договора (мой)']
cft_by_id = cft[~cft['Есть у меня'] & cft['ID (ЦФТ)'].notna()].drop_duplicates('ID (ЦФТ)').set_index('ID (ЦФТ)')['Номер договора (ЦФТ)']
if ID_IS_AGR:
    cft['Мой номер по ID'] = cft['ID (ЦФТ)'].map(my_by_id).where(~cft['Есть у меня'])
    my['Номер в ЦФТ по ID'] = my['ЦФТ ID'].map(cft_by_id).where(~my['Есть в ЦФТ'])
else:
    cft['Мой номер по ID'] = pd.NA
    my['Номер в ЦФТ по ID'] = pd.NA

common_rf = cft[cft['Есть у меня']].merge(my[['agr_key', 'РФ']].dropna(subset=['РФ']), on='agr_key')
code_to_rf = (common_rf.dropna(subset=['Код филиала']).groupby('Код филиала')['РФ']
              .agg(lambda s: s.value_counts().index[0]) if len(common_rf) else pd.Series(dtype=object))
rf_in_cft = set(common_rf['РФ'])

my_inns, my_names = set(my['inn'].dropna()), set(my['name_n'].dropna())
cft_inns, cft_names = set(cft['inn'].dropna()), set(cft['name_n'].dropna())

def found_how(inn, name, inns, names):
    if inn and inn in inns:
        return 'по ИНН'
    if name and name in names:
        return 'по наименованию'
    return None

cft['Клиент у меня'] = [found_how(i, n, my_inns, my_names) for i, n in zip(cft['inn'], cft['name_n'])]
my['Клиент в ЦФТ'] = [found_how(i, n, cft_inns, cft_names) for i, n in zip(my['inn'], my['name_n'])]

def stage(r):
    if r['agr_deleted'] == '1':
        return 1, 'В Альфе договор удалён'
    if str(r['acq_class'] if pd.notna(r['acq_class']) else '').strip().upper() != 'SA':
        return 2, 'В Альфе класс договора не SA'
    if pd.notna(r['d_valid_from']) and r['d_valid_from'] > as_of:
        return 3, 'В Альфе договор начинается позже %s' % as_of_ru
    if pd.notna(r['d_valid_to']) and r['d_valid_to'] < as_of:
        return 3, 'В Альфе срок закончился до %s' % as_of_ru
    if r['cmp_deleted'] == '1' or pd.isna(r['inn']) or not r['inn']:
        return 4, 'В Альфе нет клиента или ИНН'
    return 5, 'В Альфе остальное в порядке: нет действующей привязки терминала P'

only_cft = cft[~cft['Есть у меня']].copy()
only_my = my[~my['Есть в ЦФТ']].copy()

cand = lake[lake['agr_key'].isin(set(only_cft['agr_key']))].copy()
if len(cand):
    st = cand.apply(stage, axis=1, result_type='expand')
    cand['stage'], cand['reason'] = st[0], st[1]
    best = (cand.sort_values('stage', ascending=False).drop_duplicates('agr_key')
            [['agr_key', 'reason', 'd_valid_to']].rename(columns={'d_valid_to': 'Дата окончания (Альфа)'}))
else:
    best = pd.DataFrame(columns=['agr_key', 'reason', 'Дата окончания (Альфа)'])
only_cft = only_cft.merge(best, on='agr_key', how='left')

def cft_reason(r):
    if pd.notna(r['Мой номер по ID']):
        return 'Тот же договор у меня под другим номером (совпал ID)'
    if pd.isna(r['reason']):
        return ('Номера нет в Альфе, клиент есть у меня под другим номером' if r['Клиент у меня']
                else 'Номера нет в Альфе, клиента у меня нет')
    if r['reason'].startswith('В Альфе срок закончился'):
        return '%s, в ЦФТ статус «%s»' % (r['reason'], r['Статус (ЦФТ)'] if pd.notna(r['Статус (ЦФТ)']) else 'пусто')
    return r['reason']

cft_end_all = cft_all.set_index('agr_key')['Дата окончания (ЦФТ)']

def my_reason(r):
    if r['agr_key'] in cft_end_all.index and r['agr_key'] not in both_keys:
        return 'В ЦФТ есть, но срок окончания раньше %s (в Альфе действует)' % as_of_ru
    if pd.notna(r['Номер в ЦФТ по ID']):
        return 'Тот же договор в ЦФТ под другим номером (совпал ID)'
    if COLS['branch'] and pd.notna(r['РФ']) and r['РФ'] not in rf_in_cft:
        return 'В выгрузке ЦФТ нет договоров этого филиала'
    if r['Клиент в ЦФТ']:
        return 'Тот же клиент в ЦФТ под другим номером (%s)' % r['Клиент в ЦФТ']
    return 'Клиента в ЦФТ нет ни по ИНН, ни по наименованию'

only_cft['Причина'] = only_cft.apply(cft_reason, axis=1) if len(only_cft) else None
only_my['Причина'] = only_my.apply(my_reason, axis=1) if len(only_my) else None
print('Есть у обоих: %s | только в ЦФТ: %s | только у меня: %s' % (len(both_keys), len(only_cft), len(only_my)))
"""
)

md("## 4. Итог")

code(
    r"""def client_split(df, col_found):
    per = df.assign(m=df['agr_key'].isin(both_keys) | df[col_found].notna()).groupby('client')['m'].any()
    return len(per), int(per.sum()), int((~per).sum())

def pct(a, b):
    return round(a / b * 100, 1) if b else None

sc, mc = client_split(cft, 'Клиент у меня'), client_split(my, 'Клиент в ЦФТ')
counts = pd.DataFrame([
    ('Строк в файле', cft_rows, len(mine_raw)),
    ('Договоров в файле (уникальных номеров)', len(cft_all), len(my)),
    ('Договоров в сравнении', len(cft), len(my)),
    ('  из них с датой окончания раньше %s' % as_of_ru, int((cft['Дата окончания (ЦФТ)'] < as_of).sum()), None),
    ('Филиалов', cft['Код филиала'].nunique() if COLS['branch'] else None, my['РФ'].nunique()),
    ('Клиентов по ИНН', cft['inn'].nunique(), my['inn'].nunique()),
    ('  договоров без ИНН (номер не найден в Альфе)', int(cft['inn'].isna().sum()), int(my['inn'].isna().sum())),
    ('Клиентов по наименованию', cft['name_n'].nunique(), my['name_n'].nunique()),
    ('Клиентов (ИНН, иначе наименование)', sc[0], mc[0]),
    ('Договоров есть у обоих', len(both_keys), len(both_keys)),
    ('  % от договоров стороны', pct(len(both_keys), len(cft)), pct(len(both_keys), len(my))),
    ('Договоров только у этой стороны', len(only_cft), len(only_my)),
    ('  из них тот же договор под другим номером (ID)', int(only_cft['Мой номер по ID'].notna().sum()),
     int(only_my['Номер в ЦФТ по ID'].notna().sum())),
    ('Клиентов есть у другой стороны', sc[1], mc[1]),
    ('Клиентов нет у другой стороны', sc[2], mc[2]),
], columns=['Показатель',
            'ЦФТ (весь файл)' if CFT_ACTIVE_RULE == 'all' else 'ЦФТ (действуют по сроку на %s)' % as_of_ru,
            'Моя выгрузка на %s' % as_of_ru])

def reason_table(df, title):
    t = df['Причина'].value_counts().rename_axis(title).reset_index(name='Договоров')
    t['Доля, %'] = (t['Договоров'] / max(len(df), 1) * 100).round(1)
    return pd.concat([t, pd.DataFrame([{title: 'Всего', 'Договоров': len(df), 'Доля, %': 100.0}])], ignore_index=True)

reasons_cft = reason_table(only_cft, 'Только в ЦФТ: почему нет у меня')
reasons_my = reason_table(only_my, 'Только у меня: почему нет в ЦФТ')
calc = len(cft) - len(only_cft) + len(only_my)
math = pd.DataFrame([
    ('Договоров в ЦФТ', len(cft)),
    ('− только в ЦФТ', len(only_cft)),
    ('+ только у меня', len(only_my)),
    ('= договоров у меня (расчёт)', calc),
    ('Договоров у меня (факт)', len(my)),
    ('Сходится', 'да' if calc == len(my) else 'нет'),
], columns=['Сверка количества', 'Значение'])

by_status = pd.crosstab(cft['Статус (ЦФТ)'].fillna('(пусто)'),
                        cft['Есть у меня'].map({True: 'Есть у меня', False: 'Нет у меня'}))
for c in ('Есть у меня', 'Нет у меня'):
    if c not in by_status:
        by_status[c] = 0
by_status = by_status[['Есть у меня', 'Нет у меня']]
by_status['Всего'] = by_status.sum(axis=1)
by_status = by_status.rename_axis('Статус в ЦФТ').reset_index()

for t in (counts, math, reasons_cft, reasons_my, by_status):
    display(t)
"""
)

md(
    """## 5. Филиалы

Код филиала ЦФТ сопоставлен с моим РФ по общим договорам. «Нет общих договоров» - в ЦФТ нет ни одного договора, который есть у меня в этом РФ.
"""
)

code(
    r"""if COLS['branch']:
    cft_code_cnt = cft['Код филиала'].value_counts()
    rf_code = code_to_rf.rename('РФ').rename_axis('Код филиала').reset_index()
    branches = (my['РФ'].value_counts().rename('Договоров у меня').rename_axis('РФ').reset_index()
                .merge(common_rf.groupby('РФ').size().rename('Общих договоров').reset_index(), on='РФ', how='left')
                .merge(rf_code.groupby('РФ')['Код филиала'].agg(', '.join).reset_index(), on='РФ', how='left'))
    branches['Договоров в ЦФТ с этим кодом'] = branches['Код филиала'].map(
        lambda c: sum(int(cft_code_cnt.get(x, 0)) for x in c.split(', ')) if isinstance(c, str) else 0)
    branches['Общих договоров'] = branches['Общих договоров'].fillna(0).astype(int)
    branches['Код филиала'] = branches['Код филиала'].fillna('нет общих договоров')
    branches['Покрытие, %'] = (branches['Общих договоров'] / branches['Договоров у меня'] * 100).round(1)
    branches = branches.sort_values(['Общих договоров', 'Договоров у меня'], ascending=[True, False])
    codes_unmapped = (cft_code_cnt[~cft_code_cnt.index.isin(code_to_rf.index)]
                      .rename('Договоров в ЦФТ').rename_axis('Код филиала без общих договоров').reset_index())
    missing = branches[branches['Общих договоров'] == 0]
    print('Кодов филиалов в ЦФТ: %s | РФ у меня: %s | РФ без общих договоров: %s (моих договоров в них: %s)'
          % (cft['Код филиала'].nunique(), my['РФ'].nunique(), len(missing), int(missing['Договоров у меня'].sum())))
    display(branches)
    display(codes_unmapped)
else:
    branches = codes_unmapped = pd.DataFrame()
    print('Колонки с кодом филиала нет')
"""
)

md("## 6. Тарифы, ID и комиссии")

code(
    r"""def split_by_presence(colname, top=20):
    if cft[colname].isna().all():
        return pd.DataFrame()
    g = cft['Есть у меня'].map({True: 'Есть у меня', False: 'Нет у меня'})
    t = pd.crosstab(cft[colname].fillna('(пусто)'), g)
    for c in ('Есть у меня', 'Нет у меня'):
        if c not in t:
            t[c] = 0
    t['Всего'] = t.sum(axis=1)
    t = t.sort_values('Всего', ascending=False)
    if len(t) > top:
        t = pd.concat([t.head(top), t.iloc[top:].sum().to_frame('Прочие').T])
    return t.rename_axis(colname).reset_index()

tariffs = split_by_presence('Тарифный план (ЦФТ)')
ind_tariff = pd.DataFrame([
    ('Индивидуальный ТП заполнен', int(cft['Индивидуальный ТП (ЦФТ)'].notna().sum())),
    ('  из них есть у меня', int((cft['Индивидуальный ТП (ЦФТ)'].notna() & cft['Есть у меня']).sum())),
    ('ID заполнен', int(cft['ID (ЦФТ)'].notna().sum())),
    ('id совпадает с моим «ЦФТ ID» (ID клиента) у общих договоров, %', round(id_same, 1)),
] + [('Заполнена «%s»' % c, int(cft_raw[c].notna().sum())) for c in comm_cols],
    columns=['Показатель', 'Значение'])
display(tariffs)
display(ind_tariff)
"""
)

md("## 7. Что изменилось по сравнению с прошлой выгрузкой Снигура")

code(
    r"""if PREV_FILE.exists():
    prev_raw = read_with_header(PREV_FILE, 0)
    prev_keys = set(prev_raw[pick(prev_raw, ['номер', 'договор'], exclude=['тариф', 'индивид'])].map(norm_agr).dropna())
    prev_branch = pick(prev_raw, ['филиал'], required=False)
    prev_codes = set(prev_raw[prev_branch].map(norm_code).dropna()) if prev_branch else set()
    new_codes = sorted(set(cft_all['Код филиала'].dropna()) - prev_codes) if prev_codes else []
    cft_keys = set(cft_all['agr_key'])
    changes = pd.DataFrame([
        ('Договоров в прошлой выгрузке', len(prev_keys)),
        ('Договоров в новой выгрузке', len(cft_keys)),
        ('Есть в обеих', len(prev_keys & cft_keys)),
        ('Только в новой', len(cft_keys - prev_keys)),
        ('  из них есть у меня', len((cft_keys - prev_keys) & set(my['agr_key']))),
        ('Только в прошлой', len(prev_keys - cft_keys)),
        ('Кодов филиалов: было / стало', '%s / %s' % (len(prev_codes), cft_all['Код филиала'].nunique())),
        ('Новые коды филиалов', ', '.join(new_codes) or '-'),
        ('Новые коды = РФ', ', '.join('%s %s' % (c, code_to_rf.get(c, '?')) for c in new_codes) or '-'),
    ], columns=['Сравнение с прошлой выгрузкой', 'Значение'])
else:
    changes = pd.DataFrame([('Нет файла %s' % PREV_FILE.name, '')], columns=['Сравнение с прошлой выгрузкой', 'Значение'])
display(changes)
"""
)

md("## 8. Excel")

code(
    r"""def autofit(ws):
    for col_ in ws.columns:
        width = max(len(str(c.value)) if c.value is not None else 0 for c in col_)
        ws.column_dimensions[col_[0].column_letter].width = min(max(width + 2, 10), 70)

def dates(df):
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].dt.date
    return df

cft_cols = ['Номер договора (ЦФТ)', 'Клиент (ЦФТ)', 'Статус (ЦФТ)', 'Код филиала', 'Дата начала (ЦФТ)',
            'Дата окончания (ЦФТ)', 'Дата закрытия (ЦФТ)', 'Тарифный план (ЦФТ)', 'ID (ЦФТ)', 'inn']
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as w:
    row = 0
    for t in (counts, math, term_filter, reasons_cft, reasons_my, by_status, changes):
        t.to_excel(w, index=False, sheet_name='Итог', startrow=row)
        row += len(t) + 2
    row = 0
    for t in (branches, codes_unmapped):
        if len(t):
            t.to_excel(w, index=False, sheet_name='Филиалы', startrow=row)
            row += len(t) + 2
    row = 0
    for t in (tariffs, ind_tariff, fill):
        if len(t):
            t.to_excel(w, index=False, sheet_name='Тарифы и колонки', startrow=row)
            row += len(t) + 2
    dates(only_cft[['Причина'] + cft_cols + ['Клиент у меня', 'Мой номер по ID', 'Дата окончания (Альфа)']]
          ).rename(columns={'inn': 'ИНН (Альфа)'}).to_excel(w, index=False, sheet_name='Только в ЦФТ')
    dates(only_my.assign(**{'Наименование (Альфа)': only_my['agr_key'].map(name_by_key),
                            'Дата окончания (ЦФТ)': only_my['agr_key'].map(cft_end_all)})[
        ['Причина', 'Номер договора (мой)', 'ИНН (мой)', 'ЦФТ ID', 'РФ', 'Наименование (Альфа)',
         'Клиент в ЦФТ', 'Дата окончания (ЦФТ)']]).to_excel(w, index=False, sheet_name='Только у меня')
    dates(cft[cft['Есть у меня']][cft_cols]).rename(columns={'inn': 'ИНН (Альфа)'}).to_excel(
        w, index=False, sheet_name='Есть у обоих')
    for ws in w.book.worksheets:
        autofit(ws)
        if ws.title not in ('Итог', 'Филиалы', 'Тарифы и колонки'):
            ws.freeze_panes = 'A2'
print(OUT_FILE)
"""
)

out = Path(__file__).with_name("qc_te_cft_vs_mine.ipynb")
out.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print(out)
