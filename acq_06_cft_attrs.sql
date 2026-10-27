CREATE TABLE {acq_06_cft_attrs}
AS
with merchant_ranked as (
  select
    m.id as r2_id_raw,
    m.c_cl_org as cft_id_raw,
    m.c_depart as c_depart_raw,
    m.c_tariff_plan as c_tariff_plan_raw,
    row_number() over (
      partition by m.c_cl_org
      order by
        case when m.c_tariff_plan is not null then 0 else 1 end,
        case when m.c_depart is not null then 0 else 1 end,
        m.id desc
    ) as rn
  from ods.scd1_z_r2_ip_merchants m
  where cast(m.c_cl_org as string) in (select cft_id from {acq_03_cft_map} where cft_id is not null)
    and m.c_cl_org is not null
    and coalesce(m.ods_deleted_flg, '0') <> '1'
),
r2_raw as (
  select
    r2_id_raw,
    cft_id_raw,
    c_depart_raw,
    c_tariff_plan_raw
  from merchant_ranked
  where rn = 1
),
r2_enriched as (
  select
    r2.r2_id_raw as r2_id,
    r2.cft_id_raw as cft_id,
    r2.c_tariff_plan_raw as c_tariff_plan,
    corp.c_register_gos_reg_num_rec as ogrn,
    dep.c_name as vsp_name,
    dep.c_num as vsp_code,
    br.c_shortlabel as filial_rf_raw,
    tp.c_name as tariff_name_legacy,
    cast(null as decimal(18,2)) as commission_monthly_legacy
  from r2_raw r2
  left join ods.scd1_z_cl_corp corp on corp.id = r2.cft_id_raw
  left join ods.scd1_z_depart dep on dep.id = r2.c_depart_raw
  left join ods.scd1_z_branch br on br.id = dep.c_filial
  left join ods.scd1_z_r2_tariff_plan tp on tp.id = r2.c_tariff_plan_raw
)
select
  r2_id,
  cft_id,
  c_tariff_plan,
  ogrn,
  vsp_name,
  vsp_code,
  filial_rf_raw,
  tariff_name_legacy,
  commission_monthly_legacy
from r2_enriched
