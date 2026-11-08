CREATE TABLE {acq_kedr_chod}
AS
-- Общий ЧОД клиента из КЕДР: сумма nbi_ssp по ИНН в каждой витрине КЕДР, затем максимум из трёх
-- (клиент может быть сразу в нескольких витринах). Так же считает вид DRP sbx_edm_dfip.v_detail_dmkb_dkb_dmsb.
with src as (
  select 'dmkb' as src, cast(k.yearmm as bigint) as yearmm,
         regexp_replace(trim(cast(k.inn as string)), '[^0-9]', '') as inn_digits,
         cast(k.nbi_ssp as double) as nbi
  from Kedr.v_detail_dmkb k
  where k.yearmm >= cast(from_timestamp(trunc(date_sub(cast('{exec_dt}' as timestamp), {acq_prev_month_days}), 'MM'), 'yyyyMM') as bigint)
  union all
  select 'dmsb', cast(k.yearmm as bigint),
         regexp_replace(trim(cast(k.inn as string)), '[^0-9]', ''),
         cast(k.nbi_ssp as double)
  from Kedr.v_detail_dmsb k
  where k.yearmm >= cast(from_timestamp(trunc(date_sub(cast('{exec_dt}' as timestamp), {acq_prev_month_days}), 'MM'), 'yyyyMM') as bigint)
  union all
  select 'dkb', cast(k.yearmm as bigint),
         regexp_replace(trim(cast(k.inn as string)), '[^0-9]', ''),
         cast(k.nbi_ssp as double)
  from Kedr.v_detail_dkb k
  where k.yearmm >= cast(from_timestamp(trunc(date_sub(cast('{exec_dt}' as timestamp), {acq_prev_month_days}), 'MM'), 'yyyyMM') as bigint)
),
by_src as (
  select
    m.report_month,
    s.yearmm,
    s.src,
    -- ИНН с потерянным ведущим нулём: 9 цифр -> 10 (юрлицо), 11 -> 12 (ИП).
    case length(s.inn_digits)
      when 9 then lpad(s.inn_digits, 10, '0')
      when 11 then lpad(s.inn_digits, 12, '0')
      else s.inn_digits
    end as inn,
    sum(s.nbi) as nbi
  from src s
  join {acq_00_months} m
    on s.yearmm = cast(m.yearmm as bigint)
  where s.inn_digits <> ''
  group by 1, 2, 3, 4
)
select
  report_month,
  inn,
  max(yearmm) as yearmm,
  max(nbi) as kedr_obshiy_chod,
  count(distinct src) as src_cnt
from by_src
group by report_month, inn
