CREATE TABLE {acq_slice_load}
STORED AS PARQUET
TBLPROPERTIES('transactional'='false')
AS
with cdi_one as (
  select inn, cdi_id, ssp_ocrm
  from (
    select
      inn,
      cdi_id,
      ssp_ocrm,
      row_number() over (partition by inn order by cdi_id) as rn
    from {acq_02_cdi_map}
  ) x
  where rn = 1
),
cft_one as (
  select cdi_id, cft_id
  from (
    select
      cdi_id,
      cft_id,
      row_number() over (partition by cdi_id order by cft_id) as rn
    from {acq_03_cft_map}
  ) x
  where rn = 1
),
mpos_one as (
  select
    report_month,
    cast(agr_id as string) as agr_id,
    inn,
    sum(commission_monthly_mpos) as commission_monthly_mpos
  from {acq_10m_mpos}
  group by report_month, cast(agr_id as string), inn
),
term_one as (
  select
    report_month,
    n_agr,
    max(retl_cnt) as retl_cnt,
    max(retl_with_term_cnt) as retl_with_term_cnt,
    max(term_cnt) as term_cnt,
    max(amortization) as amortization,
    max(term_calc_source) as term_calc_source
  from {acq_04_terminals}
  group by report_month, n_agr
),
trx_one as (
  select
    report_month,
    n_agr,
    max(trx_cnt) as trx_cnt,
    max(trx_sum) as trx_sum,
    max(commission_from_ops) as commission_from_ops,
    max(int_component) as int_component,
    max(active_term_cnt) as active_term_cnt,
    max(active_terms) as active_terms
  from {acq_05_trx_agr}
  group by report_month, n_agr
),
prod_one as (
  select
    report_month,
    inn,
    group_concat(distinct product_name, ',') as product_names
  from {acq_10b_products}
  where coalesce(has_product, 0) = 1
  group by report_month, inn
),
prod_flags as (
  select
    report_month,
    inn,
    max(case when product_name = 'rko' then 1 else 0 end) as rko,
    max(case when product_name = 'accounting' then 1 else 0 end) as accounting,
    max(case when product_name = 'business_cards' then 1 else 0 end) as business_cards,
    max(case when product_name = 'credit' then 1 else 0 end) as credit,
    max(case when product_name = 'dbo' then 1 else 0 end) as dbo,
    max(case when product_name = 'deposit' then 1 else 0 end) as deposit,
    max(case when product_name = 'insurance' then 1 else 0 end) as insurance,
    max(case when product_name = 'loyalty_program' then 1 else 0 end) as loyalty_program,
    max(case when product_name = 'nmo' then 1 else 0 end) as nmo,
    max(case when product_name = 'nso' then 1 else 0 end) as nso,
    max(case when product_name = 'pravocard' then 1 else 0 end) as pravocard,
    max(case when product_name = 'salary_project' then 1 else 0 end) as salary_project,
    max(case when product_name = 'self_inkass' then 1 else 0 end) as self_inkass,
    max(case when product_name = 'service_package' then 1 else 0 end) as service_package,
    max(case when product_name = 'sms_info' then 1 else 0 end) as sms_info,
    max(case when product_name = 'factoring' then 1 else 0 end) as factoring,
    max(case when product_name = 'lising' then 1 else 0 end) as lising
  from {acq_10b_products}
  where coalesce(has_product, 0) = 1
  group by report_month, inn
),
base as (
  select
    p.snapshot_month_start,
    p.n_agr,
    p.agr_id,
    p.inn,
    p.contract_number,
    p.company_name,
    p.d_valid_from,
    p.d_valid_to,
    tar.n_agr_actual,
    tar.contract_number_acq,
    tar.d_valid_from_actual,
    tar.d_valid_to_actual,
    cdi.cdi_id,
    cdi.ssp_ocrm,
    cft.cft_id,
    r2.ogrn,
    r2.vsp_name,
    r2.vsp_code,
    r2.filial_rf_raw as filial_rf,
    term.retl_cnt,
    least(
      greatest(coalesce(term.retl_with_term_cnt, 0), 0),
      coalesce(term.retl_cnt, 0)
    ) as retl_with_term_cnt,
    term.term_cnt,
    term.amortization,
    term.term_calc_source,
    trx.trx_cnt,
    trx.trx_sum,
    trx.commission_from_ops,
    trx.int_component,
    trx.active_term_cnt,
    trx.active_terms,
    retl.active_retl_cnt,
    coalesce(
      nullif(trim(cast(tar.tariff_name_actual as string)), ''),
      cast(r2.tariff_name_legacy as string)
    ) as tariff_name,
    case
      when nullif(trim(cast(tar.tariff_name_actual as string)), '') is not null then 'agr_id_actual'
      else 'legacy_cft'
    end as tariff_source,
    tar.c_tariff_plan,
    tar.tariff_name_actual,
    -- Справочная колонка оставлена пустой, чтобы не менять структуру истории; в расчётах не участвует.
    cast(null as decimal(18,2)) as commission_monthly_fix,
    coalesce(oper.commission_monthly, 0) as commission_monthly,
    coalesce(oper.commission_monthly, 0) as commission_monthly_oper_net,
    case
      when oper.commission_monthly_oper_raw is not null then 'oper_doc_net'
      else 'oper_doc_missing'
    end as commission_monthly_source,
    coalesce(oper.commission_monthly_oper_raw, 0) as commission_monthly_oper_raw,
    mpos.commission_monthly_mpos,
    mcc.mcc,
    mcc.mcc_profile,
    mcc.mcc_cnt,
    prod.product_names,
    coalesce(pf.rko, 0) as rko,
    coalesce(pf.accounting, 0) as accounting,
    coalesce(pf.business_cards, 0) as business_cards,
    coalesce(pf.credit, 0) as credit,
    coalesce(pf.dbo, 0) as dbo,
    coalesce(pf.deposit, 0) as deposit,
    coalesce(pf.insurance, 0) as insurance,
    coalesce(pf.loyalty_program, 0) as loyalty_program,
    coalesce(pf.nmo, 0) as nmo,
    coalesce(pf.nso, 0) as nso,
    coalesce(pf.pravocard, 0) as pravocard,
    coalesce(pf.salary_project, 0) as salary_project,
    coalesce(pf.self_inkass, 0) as self_inkass,
    coalesce(pf.service_package, 0) as service_package,
    coalesce(pf.sms_info, 0) as sms_info,
    coalesce(pf.factoring, 0) as factoring,
    coalesce(pf.lising, 0) as lising,
    kedr.kedr_obshiy_chod,
    kedr.src_cnt as kedr_src_cnt,
    -- ЧОД КЕДР считается на клиента: на дашборде его суммируют по одному договору клиента, остальным 0.
    case
      when row_number() over (partition by p.report_month, p.inn order by p.agr_id) = 1
        then coalesce(kedr.kedr_obshiy_chod, 0)
      else 0
    end as kedr_obshiy_chod_contrib
  from {acq_01_sa_perimeter} p
  left join cdi_one cdi on cdi.inn = p.inn
  left join cft_one cft on cft.cdi_id = cdi.cdi_id
  left join term_one term on term.n_agr = p.n_agr and term.report_month = p.report_month
  left join trx_one trx on trx.n_agr = p.n_agr and trx.report_month = p.report_month
  left join {acq_05b_active_retl} retl on retl.n_agr = p.n_agr and retl.report_month = p.report_month
  left join {acq_06_cft_attrs} r2
    on cast(r2.cft_id as string) = cft.cft_id
  left join {acq_09_tariff} tar on tar.agr_id = p.agr_id and tar.report_month = p.report_month
  left join {acq_10o_doc_oper} oper on oper.agr_id = p.agr_id and oper.report_month = p.report_month
  left join mpos_one mpos on mpos.agr_id = p.agr_id and mpos.inn = p.inn and mpos.report_month = p.report_month
  left join {acq_mcc_by_agr} mcc on mcc.n_agr = p.n_agr
  left join prod_one prod on prod.inn = p.inn and prod.report_month = p.report_month
  left join prod_flags pf on pf.inn = p.inn and pf.report_month = p.report_month
  left join {acq_kedr_chod} kedr on kedr.inn = p.inn and kedr.report_month = p.report_month
)
select
  base.*,
  case
    when lower(coalesce(tariff_name, '')) like '%акт%' then 'По актам'
    when lower(coalesce(tariff_name, '')) like '%акцион%'
      or lower(coalesce(tariff_name, '')) like '%меню%'
      or lower(coalesce(tariff_name, '')) like '%сезон%' then 'Акционный'
    when lower(coalesce(tariff_name, '')) like '%индив%' then 'Индивидуальный'
    when lower(coalesce(tariff_name, '')) like '%стандарт%' then 'Стандарт'
    else 'Не сегментирован'
  end as tariff_short,
  least(
    greatest(coalesce(retl_with_term_cnt, 0), 0),
    coalesce(retl_cnt, 0)
  ) * 1926 as aur,
  coalesce(commission_from_ops, 0) + coalesce(commission_monthly, 0) as commission_total,
  coalesce(commission_from_ops, 0) + coalesce(commission_monthly, 0) + coalesce(int_component, 0) as chod,
  (
    coalesce(commission_from_ops, 0) + coalesce(commission_monthly, 0) + coalesce(int_component, 0)
    - least(greatest(coalesce(retl_with_term_cnt, 0), 0), coalesce(retl_cnt, 0)) * 1926
    - coalesce(amortization, 0)
  ) as fin_result,
  case
    when (
      coalesce(commission_from_ops, 0) + coalesce(commission_monthly, 0) + coalesce(int_component, 0)
      - least(greatest(coalesce(retl_with_term_cnt, 0), 0), coalesce(retl_cnt, 0)) * 1926
      - coalesce(amortization, 0)
    ) > 0 then 1
    else 0
  end as tsp_effective,
  coalesce(commission_from_ops, 0) / nullif(trx_sum, 0) * 100 as acq_pct,
  (
    coalesce(commission_from_ops, 0) + coalesce(commission_monthly, 0) + coalesce(int_component, 0)
  ) / nullif(trx_sum, 0) * 100 as chod_pct,
  regexp_replace(concat(
    case when rko <> 1 then 'РКО, ' else '' end,
    case when business_cards <> 1 then 'Бизнес-карта, ' else '' end,
    case when dbo <> 1 then 'ДБО, ' else '' end,
    case when insurance <> 1 then 'Страхование, ' else '' end,
    case when pravocard <> 1 then 'Правокард, ' else '' end,
    case when salary_project <> 1 then 'Зарплатный проект, ' else '' end,
    case when self_inkass <> 1 then 'Самоинкассация, ' else '' end,
    case when sms_info <> 1 then 'СМС-инфо, ' else '' end
  ), ', $', '') as recommended_products,
  -- Колонка партиции истории должна быть последней.
  substr(snapshot_month_start, 1, 7) as report_month
from base
