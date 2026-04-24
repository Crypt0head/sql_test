-- Optimized MVP check for one INN (Impala/Hive style)
-- Goal: minimal scan on very large trx table.
--
-- Usage:
--   1) Set params.target_inn
--   2) Set params.month_start (first day of month)
--   3) Run query as-is
--
-- Notes:
--   - Perimeter follows Tech Lead DAG logic:
--       c_trx_class = 'SA'
--       c_trx_type  = 'S01'
--       c_nter is not null
--       ods_deleted_flg <> '1'
--       cf_trx_stat <> 'R'
--   - If your cluster has partition on d_src_file_date, you can optionally
--     add that filter in trx_base for stronger partition pruning.

with
params as (
    select
        '2204035492' as target_inn,
        cast('2026-02-01' as date) as month_start,
        date_sub(add_months(cast('2026-02-01' as date), 1), 1) as month_end
),

client_cmp as (
    select
        c.n_cmp,
        c.c_inn as inn
    from ods_alpha.scd1_companies c
    join params p on c.c_inn = p.target_inn
),

agr_keys as (
    select
        a.abs_agr_id as agr_id,
        a.n_agr,
        a.c_agr_number,
        a.n_cmp_client as n_cmp,
        a.d_valid_from,
        a.d_valid_to,
        a.cf_agr_financial,
        a.cf_paym_group
    from ods_alpha.scd1_agreements a
    join client_cmp cc on cc.n_cmp = a.n_cmp_client
    where a.acq_class = 'SA'
      and a.abs_agr_id is not null
),

branch_map as (
    select
        m.id as agr_id,
        corp.c_register_nos_reg_num_rec as ogrn,
        br.c_code as branch_cd,
        br.c_shortlabel as branch_nm
    from ods.scd1_z_r2_ip_merchants m
    join agr_keys a on a.agr_id = m.id
    left join ods.scd1_z_client cl on cl.id = m.c_cl_org
    left join ods.scd1_z_cl_corp corp on corp.id = m.c_cl_org
    left join ods.scd1_z_branch br on br.n_id = cl.c_filial
),

-- Early filter on trx by DAG perimeter + month
trx_base as (
    select
        t.n_trx,
        to_date(cast(t.d_trx_orig as timestamp)) as trx_dt,
        t.n_amt_src,
        t.c_mrc_name,
        t.n_mcc
    from ods_alpha.scd1_trx t
    join params p on 1=1
    where t.c_trx_class = 'SA'
      and t.c_trx_type = 'S01'
      and t.c_nter is not null
      and t.ods_deleted_flg <> '1'
      and t.cf_trx_stat <> 'R'
      and trunc(to_date(cast(t.d_trx_orig as timestamp)), 'MM') = p.month_start
      -- Optional extra pruning if available in your environment:
      -- and trunc(cast(t.d_src_file_date as date), 'MM') = p.month_start
),

trx_acq_small as (
    select
        b.n_trx,
        b.trx_dt,
        b.n_amt_src,
        b.c_mrc_name,
        b.n_mcc,
        ta.n_agr,
        ta.acq_tar_id,
        coalesce(ta.n_amt_tax, 0) as n_amt_tax
    from trx_base b
    join ods_alpha.scd1_trx_acq ta
      on ta.n_trx = b.n_trx
),

trx_inn as (
    select
        t.*,
        a.agr_id,
        a.c_agr_number
    from trx_acq_small t
    join agr_keys a
      on a.n_agr = t.n_agr
),

trx_full as (
    select
        ti.*,
        coalesce(ints.n_amt_fee, 0) as n_amt_fee
    from trx_inn ti
    left join ods_alpha.scd1_trx_int ints
      on ints.n_trx = ti.n_trx
),

metrics_by_agr as (
    select
        agr_id,
        c_agr_number,
        count(distinct n_trx) as cnt_trx,
        sum(n_amt_src) as sum_trx_amt,
        sum(n_amt_tax) as sum_tax_acq,
        sum(n_amt_fee) as sum_tax_irf,
        count(distinct acq_tar_id) as tariff_variants_cnt,
        min(acq_tar_id) as tariff_id_min,
        max(acq_tar_id) as tariff_id_max
    from trx_full
    group by agr_id, c_agr_number
),

name_stat as (
    select
        agr_id,
        c_mrc_name as client_name_from_trx,
        count(*) as cnt_name,
        row_number() over (partition by agr_id order by count(*) desc, c_mrc_name) as rn
    from trx_full
    group by agr_id, c_mrc_name
),

mcc_stat as (
    select
        agr_id,
        n_mcc,
        count(*) as cnt_mcc,
        row_number() over (partition by agr_id order by count(*) desc, n_mcc) as rn
    from trx_full
    group by agr_id, n_mcc
)

select
    p.month_start,
    p.month_end,
    cc.inn,
    a.n_cmp,
    a.agr_id as cdi_id_candidate,
    a.c_agr_number as contract_number,
    a.d_valid_from as contract_start_dt,
    a.d_valid_to as contract_end_dt,
    bm.ogrn,
    bm.branch_nm as filial_rf,
    bm.branch_cd as vsp_code,
    ns.client_name_from_trx as client_name,
    ms.n_mcc as mcc_code,
    a.cf_agr_financial,
    a.cf_paym_group,
    m.cnt_trx as operations_cnt,
    m.sum_trx_amt as operations_sum,
    m.sum_tax_acq as commission_percent_component,
    m.sum_tax_irf as int_component,
    m.tariff_id_min,
    m.tariff_id_max,
    m.tariff_variants_cnt,
    case when m.tariff_variants_cnt > 1 then 1 else 0 end as tariff_is_ambiguous
from agr_keys a
join client_cmp cc on cc.n_cmp = a.n_cmp
join params p on 1=1
left join branch_map bm on bm.agr_id = a.agr_id
left join metrics_by_agr m on m.agr_id = a.agr_id
left join name_stat ns on ns.agr_id = a.agr_id and ns.rn = 1
left join mcc_stat ms on ms.agr_id = a.agr_id and ms.rn = 1
order by a.c_agr_number;
