CREATE TABLE {acq_05b_active_retl}
AS
with fiid_rshb as (
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
  where cast(t.d_trx_orig as timestamp) >= trunc(date_sub(cast('{exec_dt}' as timestamp), {acq_prev_month_days}), 'MM')
    and cast(t.d_trx_orig as timestamp) < months_add(trunc(cast('{exec_dt}' as timestamp), 'MM'), 1)
    and t.c_nter is not null
    and coalesce(t.ods_deleted_flg, '0') <> '1'
    and t.c_trx_class = 'SA'
    and t.c_trx_type = 'S01'
    and coalesce(t.cf_trx_stat, '') <> 'R'
    and coalesce(cast(t.n_amt_src as double), 0.0) > 1
),
trx_base as (
  select n_trx, max(report_month) as report_month, max(c_nter) as c_nter
  from trx_base_raw
  group by n_trx
),
active_nter as (
  select distinct
    tb.report_month,
    cast(a.n_agr as string) as n_agr,
    tb.c_nter
  from ods_alpha.scd1_trx_acq a
  join trx_base tb on tb.n_trx = cast(a.n_trx as string)
  where a.n_agr is not null
    and coalesce(a.ods_deleted_flg, '0') <> '1'
)
select
  an.report_month,
  an.n_agr,
  count(distinct cast(p.c_nmrc as string)) as active_retl_cnt
from active_nter an
join ods_alpha.scd1_pos_terminals p
  on cast(p.c_nter as string) = an.c_nter
where coalesce(p.ods_deleted_flg, '0') <> '1'
  and p.c_nmrc is not null
group by an.report_month, an.n_agr
