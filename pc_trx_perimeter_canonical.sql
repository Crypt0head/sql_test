-- Canonical transaction perimeter check (based on Processing Center logic)
--
-- Source logic (PC script from your screenshot):
--   (c_fiid_acq like 'P0__' or c_fiid_acq like 'S0__')
--   cf_trx_stat <> 'R'
--   c_trx_type in ('TC1','TD1','A01','S01','CP1')
--   period is based on d_src_file_date
--   settlement/FIID group rule:
--     tn.n_trx is null
--     or (tn.d_net_setl is null and f.c_fiid_grp = 'AMEX')
--     or (tn.d_net_setl is not null and f.c_fiid_grp <> 'AMEX')
--     or (f.c_fiid_grp = 'RSHB')
--
-- How to use:
-- 1) Daily check:
--    replace ${day_dt} with date like 2026-03-15
-- 2) Monthly check:
--    replace ${month_start} with first day of month like 2026-03-01

-- =========================
-- Query A: Daily check
-- =========================
with pc_perimeter_day as (
    select
        t.n_trx,
        t.n_amt_src,
        coalesce(ta.n_amt_tax, 0) as n_amt_tax,
        coalesce(tn.n_amt_fee, 0) as n_amt_fee,
        cast(t.d_src_file_date as date) as src_dt
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
        and t.c_trx_type in ('TC1', 'TD1', 'A01', 'S01', 'CP1')
        and cast(t.d_src_file_date as date) = cast('${day_dt}' as date)
        and (
            (tn.n_trx is null)
            or (tn.d_net_setl is null and f.c_fiid_grp = 'AMEX')
            or (tn.d_net_setl is not null and f.c_fiid_grp <> 'AMEX')
            or (f.c_fiid_grp = 'RSHB')
        )
)
select
    cast('${day_dt}' as date) as check_day,
    count(*) as rows_cnt,
    count(distinct n_trx) as trx_cnt,
    sum(n_amt_src) as amt_sum,
    sum(n_amt_tax) as tax_sum,
    sum(n_amt_fee) as fee_sum
from pc_perimeter_day;

-- =========================
-- Query B: Monthly check
-- =========================
with pc_perimeter_month as (
    select
        t.n_trx,
        t.n_amt_src,
        coalesce(ta.n_amt_tax, 0) as n_amt_tax,
        coalesce(tn.n_amt_fee, 0) as n_amt_fee,
        cast(t.d_src_file_date as date) as src_dt
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
        and t.c_trx_type in ('TC1', 'TD1', 'A01', 'S01', 'CP1')
        and trunc(cast(t.d_src_file_date as date), 'MM') = cast('${month_start}' as date)
        and (
            (tn.n_trx is null)
            or (tn.d_net_setl is null and f.c_fiid_grp = 'AMEX')
            or (tn.d_net_setl is not null and f.c_fiid_grp <> 'AMEX')
            or (f.c_fiid_grp = 'RSHB')
        )
)
select
    cast('${month_start}' as date) as month_start,
    count(*) as rows_cnt,
    count(distinct n_trx) as trx_cnt,
    sum(n_amt_src) as amt_sum,
    sum(n_amt_tax) as tax_sum,
    sum(n_amt_fee) as fee_sum
from pc_perimeter_month;
