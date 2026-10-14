CREATE TABLE {acq_02_cdi_map}
AS
with ocrm_current as (
  select
    regexp_replace(trim(cast(soe.x_inn as string)), '[^0-9]', '') as inn,
    cast(soe.row_id as string) as row_id,
    trim(cast(soe.x_area_resp as string)) as ssp_ocrm,
    row_number() over (
      partition by regexp_replace(trim(cast(soe.x_inn as string)), '[^0-9]', '')
      order by cast(soe.created as timestamp) desc, cast(soe.row_id as string) desc
    ) as rn
  from ocrm_ul.s_org_ext soe
  where regexp_replace(trim(cast(soe.x_inn as string)), '[^0-9]', '') in (
      select inn from {acq_01_sa_perimeter}
    )
    and coalesce(soe.x_removed_flg, 'N') = 'N'
    and coalesce(soe.x_duplicate_flg, 'N') = 'N'
),
ocrm_one as (
  select inn, row_id, ssp_ocrm
  from ocrm_current
  where rn = 1
)
select
  o.inn,
  o.ssp_ocrm,
  cast(e.party_id as string) as cdi_id
from ocrm_one o
left join cdiul.ext_id_org e
  on cast(e.cmo_ext_party_source_id as string) = o.row_id
 and upper(cast(e.cmo_ext_source_system as string)) like 'OCRM%'
