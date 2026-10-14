CREATE TABLE {acq_08b_tariff_fix}
AS
with tt_pairs as (
  select distinct
    tt.c_tariff_plan as c_tariff_plan_raw,
    tt.c_tariff as c_tariff_raw
  from ods.scd1_z_r2_tariff_tune tt
  where tt.c_tariff_plan is not null
),
tf_agg as (
  select
    tf.id as c_tariff_raw,
    max(cast(tf.c_summa as decimal(18,2))) as commission_monthly_fix
  from ods.scd1_z_r2_tariff_fix tf
  group by tf.id
)
select
  cast(p.c_tariff_plan_raw as string) as c_tariff_plan,
  max(a.commission_monthly_fix) as commission_monthly_fix
from tt_pairs p
left join tf_agg a
  on p.c_tariff_raw = a.c_tariff_raw
group by p.c_tariff_plan_raw
