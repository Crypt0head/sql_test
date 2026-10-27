CREATE TABLE {acq_kedr_chod}
AS
select
  report_month,
  inn,
  max(yearmm) as yearmm,
  max(kedr_obshiy_chod) as kedr_obshiy_chod,
  max(src_cnt) as src_cnt
from (
  select
    m.report_month,
    regexp_replace(trim(cast(k.inn as string)), '[^0-9]', '') as inn,
    cast(k.yearmm as bigint) as yearmm,
    cast(k.kedr_obshiy_chod as double) as kedr_obshiy_chod,
    cast(k.src_cnt as bigint) as src_cnt
  from sandbox_ai.shestopalov_kedr_obshiy_chod_inn_month k
  join {acq_00_months} m
    on cast(k.yearmm as bigint) = cast(m.yearmm as bigint)
) x
where inn is not null and inn <> ''
group by report_month, inn
