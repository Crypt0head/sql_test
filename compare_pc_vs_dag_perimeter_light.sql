-- Lightweight A/B comparison: Processing Center perimeter vs Tech Lead DAG perimeter
--
-- How to use period filters:
-- 1) Month mode (default in this script):
--    set ${month_start} = 'YYYY-MM-01'
--    keep filters with trunc(..., 'MM') = cast('${month_start}' as date)
--
-- 2) Day mode (for low-memory test run):
--    set ${day_dt} = 'YYYY-MM-DD'
--    replace month filters with date equality:
--      cast(t.d_src_file_date as date) = cast('${day_dt}' as date)           -- PC block
--      to_date(cast(t.d_trx_orig as timestamp)) = cast('${day_dt}' as date)  -- DAG block
--
-- Recommended run order:
--   Step 1: summary metrics (first query block)
--   Step 2: overlap counters (second query block)

-- =========================
-- Step 1: Summary metrics
-- =========================
with
pc_perimeter as (
    select
        t.n_trx,
        t.n_amt_src,
        coalesce(ta.n_amt_tax, 0) as n_amt_tax,
        coalesce(tn.n_amt_fee, 0) as n_amt_fee
    from ods_alpha.scd1_trx t
    left join ods_alpha.scd1_trx_acq ta
        on ta.n_trx = t.n_trx
    left join ods_alpha.scd1_trx_int tn
        on tn.n_trx = t.n_trx
    left join ods_alpha.scd1_base24_fiids f
        on f.c_fiid = t.c_fiid_iss
    where
        (t.c_fiid_acq like 'P0__' or t.c_fiid_acq like 'S0__')
        and t.cf_trx_stat <> 'R'
        and t.c_trx_type in ('TC1','TD1','A01','S01','CP1')
        and trunc(cast(t.d_src_file_date as date), 'MM') = cast('${month_start}' as date)
        and (
            (tn.n_trx is null)
            or (tn.d_net_setl is null and f.c_fiid_grp = 'AMEX')
            or (tn.d_net_setl is not null and f.c_fiid_grp <> 'AMEX')
            or (f.c_fiid_grp = 'RSHB')
        )
),
dag_perimeter as (
    select
        t.n_trx,
        t.n_amt_src,
        coalesce(ta.n_amt_tax, 0) as n_amt_tax,
        coalesce(ti.n_amt_fee, 0) as n_amt_fee
    from ods_alpha.scd1_trx t
    left join ods_alpha.scd1_trx_acq ta
        on ta.n_trx = t.n_trx
    left join ods_alpha.scd1_trx_int ti
        on ti.n_trx = t.n_trx
    where
        t.c_trx_class = 'SA'
        and t.c_trx_type = 'S01'
        and t.c_nter is not null
        and trunc(to_date(cast(t.d_trx_orig as timestamp)), 'MM') = cast('${month_start}' as date)
        and t.ods_deleted_flg <> '1'
        and t.cf_trx_stat <> 'R'
)
select
    'PC' as perimeter,
    count(*) as trx_cnt,
    sum(n_amt_src) as amt_sum,
    sum(n_amt_tax) as tax_sum,
    sum(n_amt_fee) as fee_sum
from pc_perimeter
union all
select
    'DAG' as perimeter,
    count(*) as trx_cnt,
    sum(n_amt_src) as amt_sum,
    sum(n_amt_tax) as tax_sum,
    sum(n_amt_fee) as fee_sum
from dag_perimeter;

-- =========================
-- Step 2: Overlap counters
-- =========================
with
pc as (
    select t.n_trx
    from ods_alpha.scd1_trx t
    left join ods_alpha.scd1_trx_int tn
        on tn.n_trx = t.n_trx
    left join ods_alpha.scd1_base24_fiids f
        on f.c_fiid = t.c_fiid_iss
    where
        (t.c_fiid_acq like 'P0__' or t.c_fiid_acq like 'S0__')
        and t.cf_trx_stat <> 'R'
        and t.c_trx_type in ('TC1','TD1','A01','S01','CP1')
        and trunc(cast(t.d_src_file_date as date), 'MM') = cast('${month_start}' as date)
        and (
            (tn.n_trx is null)
            or (tn.d_net_setl is null and f.c_fiid_grp = 'AMEX')
            or (tn.d_net_setl is not null and f.c_fiid_grp <> 'AMEX')
            or (f.c_fiid_grp = 'RSHB')
        )
),
dag as (
    select t.n_trx
    from ods_alpha.scd1_trx t
    where
        t.c_trx_class = 'SA'
        and t.c_trx_type = 'S01'
        and t.c_nter is not null
        and trunc(to_date(cast(t.d_trx_orig as timestamp)), 'MM') = cast('${month_start}' as date)
        and t.ods_deleted_flg <> '1'
        and t.cf_trx_stat <> 'R'
)
select 'intersect_cnt' as metric, count(*) as val
from (
    select pc.n_trx
    from pc
    join dag on pc.n_trx = dag.n_trx
) x
union all
select 'only_pc_cnt' as metric, count(*) as val
from (
    select pc.n_trx
    from pc
    left anti join dag on pc.n_trx = dag.n_trx
) x
union all
select 'only_dag_cnt' as metric, count(*) as val
from (
    select dag.n_trx
    from dag
    left anti join pc on dag.n_trx = pc.n_trx
) x;
