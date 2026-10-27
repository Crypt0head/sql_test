CREATE TABLE {acq_10m_mpos}
AS
-- Аренда привязывается к договору по дате аренды, поэтому условия и договоры берутся сразу на всё окно.
with win as (
  select min(month_start) as window_start, max(month_end) as window_end
  from {acq_00_months}
),
rent_base as (
  select
    cast(r.c_nmrc as string) as c_nmrc,
    cast(r.d_rent as date) as d_rent_dt,
    from_timestamp(cast(r.d_rent as timestamp), 'yyyy-MM') as report_month,
    cast(r.n_amt as double) as n_amt_num,
    cast(r.ods_commit_ts as timestamp) as ods_commit_ts,
    cast(r.ods_insert_ts as timestamp) as ods_insert_ts,
    cast(r.ods_op_csn as decimal(38, 0)) as ods_op_csn,
    coalesce(cast(r.ods_deleted_flg as string), '0') as ods_deleted_flg
  from ods_alpha.scd1_mrc_pos_rent r
  cross join win w
  where r.c_nmrc is not null
    and cast(r.d_rent as date) between w.window_start and w.window_end
),
rent_ranked as (
  select
    *,
    row_number() over (
      partition by c_nmrc, d_rent_dt
      order by coalesce(ods_commit_ts, ods_insert_ts) desc, ods_op_csn desc, ods_insert_ts desc
    ) as rn
  from rent_base
  where ods_deleted_flg not in ('1', 'Y', 'y')
),
rent_dedup as (
  select c_nmrc, d_rent_dt, report_month, n_amt_num
  from rent_ranked
  where rn = 1
),
terms_active as (
  select distinct
    cast(t.n_agr as string) as n_agr,
    cast(t.c_nmrc as string) as c_nmrc,
    cast(t.d_valid_from as date) as d_valid_from,
    cast(t.d_valid_to as date) as d_valid_to
  from ods_alpha.scd1_agr_terms t
  cross join win w
  where coalesce(cast(t.ods_deleted_flg as string), '0') not in ('1', 'Y', 'y')
    and t.c_nmrc is not null
    and cast(t.d_valid_from as date) <= w.window_end
    and (t.d_valid_to is null or cast(t.d_valid_to as date) >= w.window_start)
),
agreements_active as (
  select distinct
    cast(a.n_agr as string) as n_agr,
    cast(a.abs_agr_id as string) as agr_id,
    cast(a.n_cmp_client as string) as n_cmp_client,
    cast(a.d_valid_from as date) as d_valid_from,
    cast(a.d_valid_to as date) as d_valid_to
  from ods_alpha.scd1_agreements a
  cross join win w
  where coalesce(cast(a.ods_deleted_flg as string), '0') not in ('1', 'Y', 'y')
    and upper(trim(cast(a.acq_class as string))) = 'SA'
    and cast(a.d_valid_from as date) <= w.window_end
    and (a.d_valid_to is null or cast(a.d_valid_to as date) >= w.window_start)
),
companies_active as (
  select distinct
    cast(c.n_cmp as string) as n_cmp,
    regexp_replace(trim(cast(c.c_inn as string)), '[^0-9]', '') as inn_key
  from ods_alpha.scd1_companies c
  where coalesce(cast(c.ods_deleted_flg as string), '0') not in ('1', 'Y', 'y')
    and c.c_inn is not null
),
mapped_raw as (
  select
    r.c_nmrc,
    r.d_rent_dt,
    r.report_month,
    r.n_amt_num,
    c.inn_key,
    cast(a.agr_id as string) as agr_id_key
  from rent_dedup r
  left join terms_active t
    on t.c_nmrc = r.c_nmrc
   and r.d_rent_dt between t.d_valid_from and coalesce(t.d_valid_to, cast('2999-12-31' as date))
  left join agreements_active a
    on a.n_agr = t.n_agr
   and r.d_rent_dt between a.d_valid_from and coalesce(a.d_valid_to, cast('2999-12-31' as date))
  left join companies_active c
    on c.n_cmp = a.n_cmp_client
),
map_stats as (
  select
    c_nmrc,
    d_rent_dt,
    n_amt_num,
    count(distinct case when inn_key is not null and agr_id_key is not null
      then concat_ws('|', inn_key, agr_id_key) end) as valid_key_cnt
  from mapped_raw
  group by c_nmrc, d_rent_dt, n_amt_num
),
mapped_unique as (
  select
    mr.c_nmrc,
    mr.d_rent_dt,
    mr.report_month,
    mr.n_amt_num,
    max(mr.inn_key) as inn_key,
    max(mr.agr_id_key) as agr_id_key
  from mapped_raw mr
  join map_stats ms
    on ms.c_nmrc = mr.c_nmrc
   and ms.d_rent_dt = mr.d_rent_dt
   and ms.n_amt_num = mr.n_amt_num
  where ms.valid_key_cnt = 1
    and mr.inn_key is not null
    and mr.agr_id_key is not null
  group by mr.c_nmrc, mr.d_rent_dt, mr.report_month, mr.n_amt_num
)
select
  report_month,
  inn_key as inn,
  agr_id_key as agr_id,
  sum(n_amt_num) as commission_monthly_mpos
from mapped_unique
group by report_month, inn_key, agr_id_key
