CREATE TABLE {acq_05_trx_agr}
AS
with sa_agr as (
  select distinct
    m.report_month,
    cast(a.n_agr as string) as n_agr,
    cast(a.n_cmp_client as string) as n_cmp_client,
    regexp_replace(trim(cast(c.c_inn as string)), '[^0-9]', '') as inn
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
),
fiid_rshb as (
  select distinct cast(fa.c_fiid as string) as c_fiid
  from ods_alpha.scd1_base24_fiids fa
  where coalesce(cast(fa.c_fiid_grp as string), 'UNKNOWN') = 'RSHB'
),
trx_base_raw as (
  select
    cast(t.n_trx as string) as n_trx,
    cast(t.c_nter as string) as c_nter,
    cast(t.n_amt_src as double) as n_amt_src,
    from_timestamp(cast(t.d_trx_orig as timestamp), 'yyyy-MM') as report_month
  from ods_alpha.scd1_trx t
  join fiid_rshb fr
    on fr.c_fiid = cast(t.c_fiid_acq as string)
  -- Границы окна прямо в условии, чтобы Impala читала только нужные даты.
  where cast(t.d_trx_orig as timestamp) >= trunc(date_sub(cast('{exec_dt}' as timestamp), {acq_prev_month_days}), 'MM')
    and cast(t.d_trx_orig as timestamp) < months_add(trunc(cast('{exec_dt}' as timestamp), 'MM'), 1)
    and t.c_nter is not null
    and coalesce(t.ods_deleted_flg, '0') <> '1'
    and t.c_trx_class = 'SA'
    and t.c_trx_type = 'S01'
    and coalesce(t.cf_trx_stat, '') <> 'R'
),
trx_base as (
  select n_trx, max(report_month) as report_month, max(c_nter) as c_nter, max(n_amt_src) as n_amt_src
  from trx_base_raw
  group by n_trx
),
ta_raw as (
  select tb.report_month, cast(a.n_trx as string) as n_trx, cast(a.n_agr as string) as n_agr, coalesce(cast(a.n_amt_tax as double), 0.0) as n_amt_tax
  from ods_alpha.scd1_trx_acq a
  join trx_base tb on tb.n_trx = cast(a.n_trx as string)
  join sa_agr ss on ss.n_agr = cast(a.n_agr as string) and ss.report_month = tb.report_month
),
ta as (
  select report_month, n_trx, n_agr, max(n_amt_tax) as n_amt_tax
  from ta_raw
  group by report_month, n_trx, n_agr
),
trx_keys as (
  select distinct n_trx
  from ta
),
trx_int_agg as (
  select cast(i.n_trx as string) as n_trx, max(coalesce(cast(i.n_amt_fee as double), 0.0)) as n_amt_fee
  from ods_alpha.scd1_trx_int i
  join trx_keys k on k.n_trx = cast(i.n_trx as string)
  where coalesce(i.ods_deleted_flg, '0') <> '1'
  group by cast(i.n_trx as string)
),
tj as (
  select ta.report_month, ta.n_agr, sa.n_cmp_client, sa.inn, ta.n_trx, tb.c_nter, tb.n_amt_src, ta.n_amt_tax
  from ta
  join trx_base tb on tb.n_trx = ta.n_trx
  left join sa_agr sa on sa.n_agr = ta.n_agr and sa.report_month = ta.report_month
),
trx_agg as (
  select
    tj.report_month,
    tj.n_agr,
    count(distinct tj.n_trx) as trx_cnt,
    sum(tj.n_amt_src) as trx_sum,
    sum(tj.n_amt_tax) as commission_from_ops,
    sum(coalesce(i.n_amt_fee, 0.0)) as int_component
  from tj
  left join trx_int_agg i on i.n_trx = tj.n_trx
  group by tj.report_month, tj.n_agr
),
active_term_agg_ncmp as (
  select
    tj.report_month,
    tj.n_cmp_client,
    count(distinct case when coalesce(tj.n_amt_src, 0.0) > 1 then tj.c_nter else null end) as active_term_cnt
  from tj
  where tj.n_cmp_client is not null
  group by tj.report_month, tj.n_cmp_client
),
active_term_agg_agr as (
  select
    tj.report_month,
    tj.n_agr,
    count(distinct case when coalesce(tj.n_amt_src, 0.0) > 1 then tj.c_nter else null end) as active_terms
  from tj
  where tj.n_agr is not null
  group by tj.report_month, tj.n_agr
)
select
  t.report_month,
  t.n_agr,
  sa.inn,
  t.trx_cnt,
  t.trx_sum,
  t.commission_from_ops,
  t.int_component,
  sa.n_cmp_client,
  a.active_term_cnt,
  aa.active_terms
from trx_agg t
left join sa_agr sa on sa.n_agr = t.n_agr and sa.report_month = t.report_month
left join active_term_agg_ncmp a on a.n_cmp_client = sa.n_cmp_client and a.report_month = t.report_month
left join active_term_agg_agr aa on aa.n_agr = t.n_agr and aa.report_month = t.report_month
