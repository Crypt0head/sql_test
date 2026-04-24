-- Canonical transaction perimeter check (based on Tech Lead DAG logic)
--
-- Source logic (raw_trx):
--   c_trx_class = 'SA'
--   c_trx_type  = 'S01'
--   c_nter is not null
--   cf_trx_stat <> 'R'
--   ods_deleted_flg <> '1'
--   period is based on d_trx_orig
--
-- How to use:
-- 1) Daily check:
--    replace ${day_dt} with date like 2026-03-15
-- 2) Monthly check:
--    replace ${month_start} with first day of month like 2026-03-01

-- =========================
-- Query A: Daily check
-- =========================
with dag_perimeter_day as (
    select
        t.n_trx,
        t.n_amt_src,
        coalesce(ta.n_amt_tax, 0) as n_amt_tax,
        coalesce(ti.n_amt_fee, 0) as n_amt_fee,
        to_date(cast(t.d_trx_orig as timestamp)) as trx_dt
    from ods_alpha.scd1_trx t
    left join ods_alpha.scd1_trx_acq ta
        on ta.n_trx = t.n_trx
    left join ods_alpha.scd1_trx_int ti
        on ti.n_trx = t.n_trx
    where
        t.c_trx_class = 'SA'
        and t.c_trx_type = 'S01'
        and t.c_nter is not null
        and t.ods_deleted_flg <> '1'
        and t.cf_trx_stat <> 'R'
        and to_date(cast(t.d_trx_orig as timestamp)) = cast('${day_dt}' as date)
)
select
    cast('${day_dt}' as date) as check_day,
    count(*) as rows_cnt,
    count(distinct n_trx) as trx_cnt,
    sum(n_amt_src) as amt_sum,
    sum(n_amt_tax) as tax_sum,
    sum(n_amt_fee) as fee_sum
from dag_perimeter_day;

-- =========================
-- Query B: Monthly check
-- =========================
with dag_perimeter_month as (
    select
        t.n_trx,
        t.n_amt_src,
        coalesce(ta.n_amt_tax, 0) as n_amt_tax,
        coalesce(ti.n_amt_fee, 0) as n_amt_fee,
        to_date(cast(t.d_trx_orig as timestamp)) as trx_dt
    from ods_alpha.scd1_trx t
    left join ods_alpha.scd1_trx_acq ta
        on ta.n_trx = t.n_trx
    left join ods_alpha.scd1_trx_int ti
        on ti.n_trx = t.n_trx
    where
        t.c_trx_class = 'SA'
        and t.c_trx_type = 'S01'
        and t.c_nter is not null
        and t.ods_deleted_flg <> '1'
        and t.cf_trx_stat <> 'R'
        and trunc(to_date(cast(t.d_trx_orig as timestamp)), 'MM') = cast('${month_start}' as date)
)
select
    cast('${month_start}' as date) as month_start,
    count(*) as rows_cnt,
    count(distinct n_trx) as trx_cnt,
    sum(n_amt_src) as amt_sum,
    sum(n_amt_tax) as tax_sum,
    sum(n_amt_fee) as fee_sum
from dag_perimeter_month;
