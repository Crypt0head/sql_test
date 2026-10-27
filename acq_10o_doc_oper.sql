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
    -- Документы от начала окна до конца месяца, следующего за последним месяцем окна (тариф 0).
    and cast(o.c_date_create as timestamp) >= trunc(date_sub(cast('{exec_dt}' as timestamp), {acq_prev_month_days}), 'MM')
    and cast(o.c_date_create as timestamp) < months_add(trunc(cast('{exec_dt}' as timestamp), 'MM'), 2)
),
tariff as (
  select
    report_month,
    cast(agr_id as string) as agr_id,
    trim(cast(c_tariff_plan as string)) as c_tariff_plan
  from {acq_09_tariff}
),
perimeter as (
  select distinct
    p.report_month,
    m.month_start,
    cast(p.agr_id as string) as agr_id
  from {acq_01_sa_perimeter} p
  join {acq_00_months} m on m.report_month = p.report_month
  where p.agr_id is not null
)
select
  p.report_month,
  p.agr_id,
  round(coalesce(sum(d.c_calc_summ), 0) / 1.22, 2) as commission_monthly,
  sum(d.c_calc_summ) as commission_monthly_oper_raw
from perimeter p
left join tariff t
  on t.agr_id = p.agr_id
 and t.report_month = p.report_month
left join docs d
  on d.agr_id = p.agr_id
 and (
    (
      coalesce(t.c_tariff_plan, '') in ('0', '0.0', '0,0')
      and cast(d.c_date_create as date) >= add_months(p.month_start, 1)
      and cast(d.c_date_create as date) < add_months(p.month_start, 2)
    )
    or (
      coalesce(t.c_tariff_plan, '') not in ('0', '0.0', '0,0')
      and cast(d.c_date_create as date) >= p.month_start
      and cast(d.c_date_create as date) < add_months(p.month_start, 1)
    )
  )
group by p.report_month, p.agr_id
