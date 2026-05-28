# Поиск 18230 в tariff_comiss.c_value

target_tariff_ids = ['42429264236', '53599758030']
tids_in = ', '.join([f"'{x}'" for x in target_tariff_ids])

# 1) tariff_comiss для нашего договора
sql_comiss = f"""
select id, c_value, c_max_value, c_min_value, c_currency
from ods.scd1_z_r2_tariff_comiss
where cast(id as string) in ({tids_in})
"""
with imp:
    imp.execute('set MEM_LIMIT=8g')
    comiss_df = imp.fetch(sql_comiss)
print(len(comiss_df) if comiss_df is not None else 0)
comiss_df


# 2) Глобальный поиск 18230 в tariff_comiss.c_value
sql_comiss_search = """
select id, c_value
from ods.scd1_z_r2_tariff_comiss
where c_value like '%18230%' or c_value like '%18 230%'
limit 20
"""
with imp:
    imp.execute('set MEM_LIMIT=8g')
    search_df = imp.fetch(sql_comiss_search)
print(len(search_df) if search_df is not None else 0)
search_df
