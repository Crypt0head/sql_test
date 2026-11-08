SET MEM_LIMIT=4g;
CREATE TABLE {t_kedr_chod} STORED AS PARQUET {table_props} AS
select
  inn,
  max(yearmm) as yearmm,
  max(kedr_obshiy_chod) as kedr_obshiy_chod,
  max(src_cnt) as src_cnt
from (
  select
    regexp_replace(trim(cast(inn as string)), '[^0-9]', '') as inn,
    cast(yearmm as bigint) as yearmm,
    cast(kedr_obshiy_chod as double) as kedr_obshiy_chod,
    cast(src_cnt as bigint) as src_cnt
  from sandbox_ai.shestopalov_kedr_obshiy_chod_inn_month
  where cast(yearmm as bigint) = {yearmm}
) k
where inn is not null and inn <> ''
group by inn
