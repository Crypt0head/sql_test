CREATE TABLE {acq_05_trx_agr}
AS
with sa_agr as (
  select distinct
    cast(a.n_agr as string) as n_agr,
    cast(a.n_cmp_client as string) as n_cmp_client,
    regexp_replace(trim(cast(c.c_inn as string)), '[^0-9]', '') as inn
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
),
fiid_rshb as (
  select distinct cast(fa.c_fiid as string) as c_fiid
  from ods_alpha.scd1_base24_fiids fa
  where coalesce(cast(fa.c_fiid_grp as string), 'UNKNOWN') = 'RSHB'
),
trx_base_raw as (
  select cast(t.n_trx as string) as n_trx, cast(t.c_nter as string) as c_nter, cast(t.n_amt_src as double) as n_amt_src
  from ods_alpha.scd1_trx t
  join fiid_rshb fr
    on fr.c_fiid = cast(t.c_fiid_acq as string)
  where cast(t.d_trx_orig as timestamp) >= cast('{month_start}' as timestamp)
    and cast(t.d_trx_orig as timestamp) < cast(date_add(cast('{month_end}' as date), 1) as timestamp)
    and t.c_nter is not null
    and coalesce(t.ods_deleted_flg, '0') <> '1'
    and t.c_trx_class = 'SA'
    and t.c_trx_type = 'S01'
    and coalesce(t.cf_trx_stat, '') <> 'R'
),
trx_base as (
  select n_trx, max(c_nter) as c_nter, max(n_amt_src) as n_amt_src
  from trx_base_raw
  group by n_trx
),
ta_raw as (
  select cast(a.n_trx as string) as n_trx, cast(a.n_agr as string) as n_agr, coalesce(cast(a.n_amt_tax as double), 0.0) as n_amt_tax
  from ods_alpha.scd1_trx_acq a
  join trx_base tb on tb.n_trx = cast(a.n_trx as string)
  join sa_agr ss on ss.n_agr = cast(a.n_agr as string)
),
ta as (
  select n_trx, n_agr, max(n_amt_tax) as n_amt_tax
  from ta_raw
  group by n_trx, n_agr
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
  select ta.n_agr, sa.n_cmp_client, sa.inn, ta.n_trx, tb.c_nter, tb.n_amt_src, ta.n_amt_tax
  from ta
  join trx_base tb on tb.n_trx = ta.n_trx
  left join sa_agr sa on sa.n_agr = ta.n_agr
),
trx_agg as (
  select
    tj.n_agr,
    count(distinct tj.n_trx) as trx_cnt,
    sum(tj.n_amt_src) as trx_sum,
    sum(tj.n_amt_tax) as commission_from_ops,
    sum(coalesce(i.n_amt_fee, 0.0)) as int_component
  from tj
  left join trx_int_agg i on i.n_trx = tj.n_trx
  group by tj.n_agr
),
active_term_agg_ncmp as (
  select
    tj.n_cmp_client,
    count(distinct case when coalesce(tj.n_amt_src, 0.0) > 1 then tj.c_nter else null end) as active_term_cnt
  from tj
  where tj.n_cmp_client is not null
  group by tj.n_cmp_client
),
active_term_agg_agr as (
  select
    tj.n_agr,
    count(distinct case when coalesce(tj.n_amt_src, 0.0) > 1 then tj.c_nter else null end) as active_terms
  from tj
  where tj.n_agr is not null
  group by tj.n_agr
)
select
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
left join sa_agr sa on sa.n_agr = t.n_agr
left join active_term_agg_ncmp a on a.n_cmp_client = sa.n_cmp_client
left join active_term_agg_agr aa on aa.n_agr = t.n_agr
