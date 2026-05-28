# =================================================================
# Поиск значения 18 230 во всех таблицах R2-тарифов
# для agr_id = 316162049716, c_tariff_plan = 316253612687
# tariff_id (из tt.c_tariff): 42429264236, 53599758030
# =================================================================
# Каждый блок выводит ВСЕ колонки своей таблицы (select *)
# Запусти их по очереди и в каждом результате поищи глазами 18230.

target_agr_id      = '316162049716'
target_tariff_plan = '316253612687'
target_tariff_ids  = ['42429264236', '53599758030']
tids_in            = ', '.join([f"'{x}'" for x in target_tariff_ids])

# -----------------------------------------------------------------
# 1) ods.scd1_z_r2_ip_merchants — все поля по нашему agr_id
# -----------------------------------------------------------------
sql_1 = f"""
select *
from ods.scd1_z_r2_ip_merchants
where cast(id as string) = '{target_agr_id}'
"""

with imp:
    imp.execute('set MEM_LIMIT=8g')
    df_1 = imp.fetch(sql_1)

print('=' * 70)
print('1) ods.scd1_z_r2_ip_merchants')
print('=' * 70)
print(f'Строк: {len(df_1) if df_1 is not None else 0}')
display(df_1)


# -----------------------------------------------------------------
# 2) ods.scd1_z_r2_tariff_plan — все поля по нашему c_tariff_plan
# -----------------------------------------------------------------
sql_2 = f"""
select *
from ods.scd1_z_r2_tariff_plan
where cast(id as string) = '{target_tariff_plan}'
"""

with imp:
    imp.execute('set MEM_LIMIT=8g')
    df_2 = imp.fetch(sql_2)

print('=' * 70)
print('2) ods.scd1_z_r2_tariff_plan')
print('=' * 70)
print(f'Строк: {len(df_2) if df_2 is not None else 0}')
display(df_2)


# -----------------------------------------------------------------
# 3) ods.scd1_z_r2_tariff_tune — все поля по c_tariff_plan
# -----------------------------------------------------------------
sql_3 = f"""
select *
from ods.scd1_z_r2_tariff_tune
where cast(c_tariff_plan as string) = '{target_tariff_plan}'
"""

with imp:
    imp.execute('set MEM_LIMIT=8g')
    df_3 = imp.fetch(sql_3)

print('=' * 70)
print('3) ods.scd1_z_r2_tariff_tune')
print('=' * 70)
print(f'Строк: {len(df_3) if df_3 is not None else 0}')
display(df_3)


# -----------------------------------------------------------------
# 4) ods.scd1_z_r2_tariff_fix — все поля по нашим tariff_id
# -----------------------------------------------------------------
sql_4 = f"""
select *
from ods.scd1_z_r2_tariff_fix
where cast(id as string) in ({tids_in})
"""

with imp:
    imp.execute('set MEM_LIMIT=8g')
    df_4 = imp.fetch(sql_4)

print('=' * 70)
print('4) ods.scd1_z_r2_tariff_fix')
print('=' * 70)
print(f'Строк: {len(df_4) if df_4 is not None else 0}')
display(df_4)


# -----------------------------------------------------------------
# 5) ods.scd1_z_r2_tariff_calc — все поля по нашим tariff_id
# -----------------------------------------------------------------
sql_5 = f"""
select *
from ods.scd1_z_r2_tariff_calc
where cast(id as string) in ({tids_in})
"""

with imp:
    imp.execute('set MEM_LIMIT=8g')
    df_5 = imp.fetch(sql_5)

print('=' * 70)
print('5) ods.scd1_z_r2_tariff_calc')
print('=' * 70)
print(f'Строк: {len(df_5) if df_5 is not None else 0}')
display(df_5)


# -----------------------------------------------------------------
# 6) ods.scd1_z_r2_tariffs — общая таблица тарифов (закомм. у техлида)
# -----------------------------------------------------------------
sql_6 = f"""
select *
from ods.scd1_z_r2_tariffs
where cast(id as string) in ({tids_in})
"""

try:
    with imp:
        imp.execute('set MEM_LIMIT=8g')
        df_6 = imp.fetch(sql_6)
    print('=' * 70)
    print('6) ods.scd1_z_r2_tariffs')
    print('=' * 70)
    print(f'Строк: {len(df_6) if df_6 is not None else 0}')
    display(df_6)
except Exception as e:
    print('=' * 70)
    print('6) ods.scd1_z_r2_tariffs - ОШИБКА (возможно таблицы нет):')
    print('=' * 70)
    print(str(e))


# -----------------------------------------------------------------
# 7) ods.scd1_z_r2_tariff_comiss — комиссии (закомм. у техлида)
# -----------------------------------------------------------------
sql_7 = f"""
select *
from ods.scd1_z_r2_tariff_comiss
where cast(id as string) in ({tids_in})
   or cast(c_tariff_plan as string) = '{target_tariff_plan}'
"""

try:
    with imp:
        imp.execute('set MEM_LIMIT=8g')
        df_7 = imp.fetch(sql_7)
    print('=' * 70)
    print('7) ods.scd1_z_r2_tariff_comiss')
    print('=' * 70)
    print(f'Строк: {len(df_7) if df_7 is not None else 0}')
    display(df_7)
except Exception as e:
    print('=' * 70)
    print('7) ods.scd1_z_r2_tariff_comiss - ОШИБКА (возможно таблицы нет или другой ключ):')
    print('=' * 70)
    print(str(e))


# -----------------------------------------------------------------
# 8) Глобальный поиск 18 230 в tariff_fix.c_summa за весь словарь
# -----------------------------------------------------------------
sql_8 = """
select id, c_summa, count(*) as cnt
from ods.scd1_z_r2_tariff_fix
where c_summa between 18229 and 18231
group by id, c_summa
order by cnt desc
"""

with imp:
    imp.execute('set MEM_LIMIT=8g')
    df_8 = imp.fetch(sql_8)

print('=' * 70)
print('8) Все записи tariff_fix где c_summa = 18 230')
print('=' * 70)
print(f'Строк: {len(df_8) if df_8 is not None else 0}')
display(df_8)
