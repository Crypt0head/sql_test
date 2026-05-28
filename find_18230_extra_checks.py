# Доп. проверки: структура tariff_comiss + глобальный поиск 18230 в tariff_calc

# 1) Колонки tariff_comiss
sql_descr = "describe ods.scd1_z_r2_tariff_comiss"
with imp:
    imp.execute('set MEM_LIMIT=8g')
    descr_df = imp.fetch(sql_descr)
descr_df


# 2) Глобальный поиск 18230 в tariff_calc.c_summa (если такая колонка есть)
sql_calc = """
select id, c_summa
from ods.scd1_z_r2_tariff_calc
where c_summa between 18229 and 18231
"""
with imp:
    imp.execute('set MEM_LIMIT=8g')
    calc_df = imp.fetch(sql_calc)
print(len(calc_df) if calc_df is not None else 0)
calc_df
