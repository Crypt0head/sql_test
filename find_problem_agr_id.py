# =================================================================
# СЕКЦИЯ A — Найти ИНН для договоров с cm_excel ≈ 18 230
# Вставлять в datamart_month_lake_agr.ipynb (там уже есть diag, final_df)
# =================================================================

target_agr_ids = [
    '316162049716', '856439965914', '823300861561',
    '935930203328', '936021373400', '936040918730',
    '936056870605', '936184553451', '936199496576',
    '823295630961',
]

inn_lookup = (
    final_df[['agr_id', 'inn', 'n_cmp_client', 'cmp_name']]
    .drop_duplicates(subset=['agr_id'])
)
inn_lookup['agr_id'] = inn_lookup['agr_id'].astype(str)

problem_df = (
    diag[diag['agr_id'].isin(target_agr_ids)]
    [['agr_id', 'cm_lake', 'cm_excel', 'delta_cm', 'rc_lake', 'rc_excel']]
    .merge(inn_lookup, on='agr_id', how='left')
)

print('Договоры с cm_excel ≈ 18 230:')
display(problem_df)

print('\nДоговор для глубокого разбора (первый из списка):')
first = problem_df.iloc[0]
print(f"  agr_id        = {first['agr_id']}")
print(f"  inn           = {first['inn']}")
print(f"  n_cmp_client  = {first['n_cmp_client']}")
print(f"  cmp_name      = {first['cmp_name']}")
print(f"  cm_lake       = {first['cm_lake']}")
print(f"  cm_excel      = {first['cm_excel']}")
print(f"  rc_lake       = {first['rc_lake']}")
print(f"  rc_excel      = {first['rc_excel']}")


# =================================================================
# СЕКЦИЯ B — SQL-разведка R2-цепочки для конкретного agr_id
# Вставлять в любую тетрадку с активным imp-подключением.
# Замени значение target_agr_id ниже на нужный ID.
# =================================================================

target_agr_id = '316162049716'   # подставь нужный

sql_r2_debug = f"""
select
    cast(m.id as string)                  as agr_id,
    cast(m.c_tariff_plan as string)       as c_tariff_plan,
    cast(tp.c_name as string)             as tariff_plan_name,
    cast(tp.c_code as string)             as tariff_plan_code,
    cast(tt.c_tariff as string)           as tariff_id,
    cast(tt.c_rule as string)             as tariff_rule,
    cast(tt.c_vid_comiss as string)       as vid_comiss_id,
    cast(vc.c_name as string)             as vid_comiss_name,
    cast(tf.id as string)                 as tariff_fix_id,
    cast(tf.c_summa as decimal(18,2))     as fix_summa,
    cast(tc.id as string)                 as tariff_calc_id,
    cast(tc.c_name as string)             as tariff_calc_name
from ods.scd1_z_r2_ip_merchants m
left join ods.scd1_z_r2_tariff_plan  tp on cast(tp.id as string) = cast(m.c_tariff_plan as string)
left join ods.scd1_z_r2_tariff_tune  tt on cast(tt.c_tariff_plan as string) = cast(m.c_tariff_plan as string)
left join ods.scd1_z_r2_tariff_fix   tf on cast(tf.id as string) = cast(tt.c_tariff as string)
left join ods.scd1_z_r2_tariff_calc  tc on cast(tc.id as string) = cast(tt.c_tariff as string)
left join ods.scd1_z_r2_vid_comiss   vc on cast(vc.id as string) = cast(tt.c_vid_comiss as string)
where cast(m.id as string) = '{target_agr_id}'
order by tt.c_tariff
"""

with imp:
    imp.execute('set MEM_LIMIT=8g')
    debug_df = imp.fetch(sql_r2_debug)

if debug_df is None:
    debug_df = pd.DataFrame()

print(f'R2-цепочка для agr_id={target_agr_id}: {len(debug_df):,} строк')
display(debug_df)
