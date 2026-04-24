-- A/B comparison of transaction perimeter: Processing Center vs Tech Lead DAG
-- Replace ${month_start} with a month start date, e.g. 2024-02-01

with
pc_perimeter as (
    select
        t.n_trx,
        cast(t.d_trx_orig as timestamp) as trx_dttm,
        to_date(cast(t.d_trx_orig as timestamp)) as trx_dt,
        t.n_amt_src,
        ta.n_amt_tax,
        tn.n_amt_fee,
        t.c_trx_type
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
        cast(t.d_trx_orig as timestamp) as trx_dttm,
        to_date(cast(t.d_trx_orig as timestamp)) as trx_dt,
        t.n_amt_src,
        ta.n_amt_tax,
        ti.n_amt_fee,
        t.c_trx_type
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
),
summary as (
    select
        'PC' as perimeter,
        count(distinct n_trx) as trx_cnt,
        sum(n_amt_src) as amt_sum,
        sum(coalesce(n_amt_tax, 0)) as tax_sum,
        sum(coalesce(n_amt_fee, 0)) as fee_sum
    from pc_perimeter
    union all
    select
        'DAG' as perimeter,
        count(distinct n_trx) as trx_cnt,
        sum(n_amt_src) as amt_sum,
        sum(coalesce(n_amt_tax, 0)) as tax_sum,
        sum(coalesce(n_amt_fee, 0)) as fee_sum
    from dag_perimeter
),
overlap as (
    select
        count(distinct coalesce(p.n_trx, d.n_trx)) as union_trx_cnt,
        count(distinct case when p.n_trx is not null and d.n_trx is not null then p.n_trx end) as intersect_trx_cnt,
        count(distinct case when p.n_trx is not null and d.n_trx is null then p.n_trx end) as only_pc_trx_cnt,
        count(distinct case when p.n_trx is null and d.n_trx is not null then d.n_trx end) as only_dag_trx_cnt
    from pc_perimeter p
    full outer join dag_perimeter d
        on p.n_trx = d.n_trx
)
select * from summary
union all
select
    'OVERLAP' as perimeter,
    union_trx_cnt as trx_cnt,
    cast(intersect_trx_cnt as double) as amt_sum,
    cast(only_pc_trx_cnt as double) as tax_sum,
    cast(only_dag_trx_cnt as double) as fee_sum
from overlap;
