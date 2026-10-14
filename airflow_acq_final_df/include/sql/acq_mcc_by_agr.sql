CREATE TABLE {acq_mcc_by_agr}
AS
with sa as (
  select
    cast(a.abs_agr_id as string) as agr_id,
    cast(a.n_agr as string) as n_agr,
    cast(a.n_cmp_client as string) as n_cmp_client
  from ods_alpha.scd1_agreements a
  where a.acq_class = 'SA'
    and a.abs_agr_id is not null
),
raw_mcc as (
  select
    s.n_agr,
    cast(m.n_mcc as string) as mcc,
    cast(m.c_mrc_profile as string) as mcc_profile
  from sa s
  left join ods_alpha.scd1_merchants m
    on cast(m.n_cmp as string) = s.n_cmp_client
  where m.n_mcc is not null or m.c_mrc_profile is not null
)
select
  n_agr,
  group_concat(distinct mcc, ',') as mcc,
  group_concat(distinct mcc_profile, ',') as mcc_profile,
  count(distinct mcc) as mcc_cnt
from raw_mcc
group by n_agr
