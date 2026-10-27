CREATE TABLE {acq_01_sa_perimeter}
AS
select distinct
  m.report_month,
  cast(m.month_start as string) as snapshot_month_start,
  cast(a.n_agr as string) as n_agr,
  cast(a.abs_agr_id as string) as agr_id,
  cast(a.n_cmp_client as string) as n_cmp_client,
  cast(a.c_agr_number as string) as contract_number,
  cast(a.d_valid_from as date) as d_valid_from,
  cast(a.d_valid_to as date) as d_valid_to,
  regexp_replace(trim(cast(c.c_inn as string)), '[^0-9]', '') as inn,
  cast(c.c_cmp_name as string) as company_name
from ods_alpha.scd1_agreements a
join ods_alpha.scd1_companies c
  on c.n_cmp = a.n_cmp_client
join {acq_00_months} m
  on cast(a.d_valid_from as date) <= m.month_end
 and (a.d_valid_to is null or cast(a.d_valid_to as date) >= m.month_start)
left semi join (
    select distinct
      cast(t.n_agr as string) as n_agr,
      tm.report_month
    from ods_alpha.scd1_agr_terms t
    join {acq_00_months} tm
      on cast(t.d_valid_from as date) <= tm.month_end
     and (t.d_valid_to is null or cast(t.d_valid_to as date) >= tm.month_start)
    where upper(trim(cast(t.cf_ter_type as string))) = 'P'
      and coalesce(t.ods_deleted_flg, '0') <> '1'
  ) pt
  on pt.n_agr = cast(a.n_agr as string)
 and pt.report_month = m.report_month
where upper(trim(cast(a.acq_class as string))) = 'SA'
  and coalesce(a.ods_deleted_flg, '0') <> '1'
  and coalesce(c.ods_deleted_flg, '0') <> '1'
  and c.c_inn is not null
