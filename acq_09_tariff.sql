CREATE TABLE {acq_09_tariff}
AS
with agr_actual as (
  select
    m.report_month,
    cast(a.abs_agr_id as string) as agr_id,
    cast(a.n_agr as string) as n_agr_actual,
    cast(a.c_agr_number as string) as contract_number_acq,
    cast(a.d_valid_from as date) as d_valid_from_actual,
    cast(a.d_valid_to as date) as d_valid_to_actual,
    row_number() over (
      partition by m.report_month, cast(a.abs_agr_id as string)
      order by cast(a.d_valid_from as date) desc, cast(a.n_agr as string) desc
    ) as rn
  from ods_alpha.scd1_agreements a
  join {acq_00_months} m
    on cast(a.d_valid_from as date) <= m.month_end
   and (a.d_valid_to is null or cast(a.d_valid_to as date) >= m.month_end)
  where cast(a.abs_agr_id as string) in (select agr_id from {acq_01_sa_perimeter})
    and upper(trim(cast(a.acq_class as string))) = 'SA'
    and coalesce(a.ods_deleted_flg, '0') <> '1'
),
merchant_one as (
  select
    cast(m.id as string) as agr_id,
    cast(m.c_tariff_plan as string) as c_tariff_plan,
    row_number() over (
      partition by cast(m.id as string)
      order by cast(m.c_tariff_plan as string) desc
    ) as rn
  from ods.scd1_z_r2_ip_merchants m
  where cast(m.id as string) in (select agr_id from {acq_01_sa_perimeter})
    and coalesce(m.ods_deleted_flg, '0') <> '1'
)
select
  mm.report_month,
  mo.agr_id,
  aa.n_agr_actual,
  aa.contract_number_acq,
  aa.d_valid_from_actual,
  aa.d_valid_to_actual,
  mo.c_tariff_plan,
  cast(tp.c_name as string) as tariff_name_actual
from merchant_one mo
cross join {acq_00_months} mm
left join agr_actual aa
  on aa.agr_id = mo.agr_id
 and aa.report_month = mm.report_month
 and aa.rn = 1
left join ods.scd1_z_r2_tariff_plan tp
  on cast(tp.id as string) = mo.c_tariff_plan
where mo.rn = 1
