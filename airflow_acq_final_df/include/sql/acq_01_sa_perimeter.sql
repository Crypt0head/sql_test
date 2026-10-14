CREATE TABLE {acq_01_sa_perimeter}
AS
select distinct
  cast(a.n_agr as string) as n_agr,
  cast(a.abs_agr_id as string) as agr_id,
  cast(a.n_cmp_client as string) as n_cmp_client,
  cast(a.c_agr_number as string) as contract_number,
  regexp_replace(trim(cast(c.c_inn as string)), '[^0-9]', '') as inn,
  cast(c.c_cmp_name as string) as company_name
from ods_alpha.scd1_agreements a
join ods_alpha.scd1_companies c
  on c.n_cmp = a.n_cmp_client
where upper(trim(cast(a.acq_class as string))) = 'SA'
  and cast(a.d_valid_from as date) <= cast('{month_end}' as date)
  and (a.d_valid_to is null or cast(a.d_valid_to as date) >= cast('{month_start}' as date))
  and coalesce(a.ods_deleted_flg, '0') <> '1'
  and coalesce(c.ods_deleted_flg, '0') <> '1'
  and c.c_inn is not null
  and exists (
      select 1
      from ods_alpha.scd1_agr_terms t
      where cast(t.n_agr as string) = cast(a.n_agr as string)
        and cast(t.d_valid_from as date) <= cast('{month_end}' as date)
        and (t.d_valid_to is null or cast(t.d_valid_to as date) >= cast('{month_start}' as date))
        and upper(trim(cast(t.cf_ter_type as string))) = 'P'
        and coalesce(t.ods_deleted_flg, '0') <> '1'
  )
