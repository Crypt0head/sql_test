CREATE TABLE {acq_10o_doc_oper}
AS
with docs as (
  select
    cast(m.id as string) as agr_id,
    cast(o.c_date_create as timestamp) as c_date_create,
    cast(o.c_calc_summ as double) as c_calc_summ
  from ods.scd1_z_R2_IP_DOG_OPER o
  join ods.scd1_z_r2_ip_merchants m
    on m.id = o.c_parent_id
  join ods.scd1_z_client cl
    on m.c_cl_org = cl.id
  where o.c_parent_class = 'R2_IP_MERCHANTS'
    and cl.class_id = 'CL_ORG'
    and cast(o.c_date_create as date) >= cast('{month_start}' as date)
    and cast(o.c_date_create as date) < add_months(cast('{month_start}' as date), 2)
),
tariff as (
  select
    cast(agr_id as string) as agr_id,
    trim(cast(c_tariff_plan as string)) as c_tariff_plan
  from {acq_09_tariff}
),
perimeter as (
  select distinct cast(agr_id as string) as agr_id
  from {acq_01_sa_perimeter}
  where agr_id is not null
)
select
  p.agr_id,
  round(coalesce(sum(d.c_calc_summ), 0) / 1.22, 2) as commission_monthly,
  sum(d.c_calc_summ) as commission_monthly_oper_raw
from perimeter p
left join tariff t
  on t.agr_id = p.agr_id
left join docs d
  on d.agr_id = p.agr_id
 and (
    (
      coalesce(t.c_tariff_plan, '') in ('0', '0.0', '0,0')
      and cast(d.c_date_create as date) >= add_months(cast('{month_start}' as date), 1)
      and cast(d.c_date_create as date) < add_months(cast('{month_start}' as date), 2)
    )
    or (
      coalesce(t.c_tariff_plan, '') not in ('0', '0.0', '0,0')
      and cast(d.c_date_create as date) >= cast('{month_start}' as date)
      and cast(d.c_date_create as date) < add_months(cast('{month_start}' as date), 1)
    )
  )
group by p.agr_id
