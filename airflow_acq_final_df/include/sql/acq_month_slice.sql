CREATE TABLE {acq_month_slice}
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
    cast(agr_id as string) as agr_id,
    inn,
    sum(commission_monthly_mpos) as commission_monthly_mpos
  from {acq_10m_mpos}
  group by cast(agr_id as string), inn
),
term_one as (
  select
    n_agr,
    max(retl_cnt) as retl_cnt,
    max(retl_with_term_cnt) as retl_with_term_cnt,
    max(term_cnt) as term_cnt,
    max(amortization) as amortization,
    max(term_calc_source) as term_calc_source
  from {acq_04_terminals}
  group by n_agr
),
trx_one as (
  select
    n_agr,
    max(trx_cnt) as trx_cnt,
    max(trx_sum) as trx_sum,
    max(commission_from_ops) as commission_from_ops,
    max(int_component) as int_component,
    max(active_term_cnt) as active_term_cnt,
    max(active_terms) as active_terms
  from {acq_05_trx_agr}
  group by n_agr
),
prod_one as (
  select
    inn,
    group_concat(distinct product_name, ',') as product_names
  from {acq_10b_products}
  where coalesce(has_product, 0) = 1
  group by inn
),
base as (
  select
    '{month_start}' as snapshot_month_start,
    p.n_agr,
    p.agr_id,
    p.inn,
    p.contract_number,
    p.company_name,
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
    fix.commission_monthly_fix,
    coalesce(oper.commission_monthly, 0) as commission_monthly,
    oper.commission_monthly_oper_raw,
    mpos.commission_monthly_mpos,
    mcc.mcc,
    mcc.mcc_profile,
    mcc.mcc_cnt,
    prod.product_names,
    kedr.kedr_obshiy_chod
  from {acq_01_sa_perimeter} p
  left join cdi_one cdi on cdi.inn = p.inn
  left join cft_one cft on cft.cdi_id = cdi.cdi_id
  left join term_one term on term.n_agr = p.n_agr
  left join trx_one trx on trx.n_agr = p.n_agr
  left join {acq_05b_active_retl} retl on retl.n_agr = p.n_agr
  left join {acq_06_cft_attrs} r2
    on cast(r2.cft_id as string) = cft.cft_id
  left join {acq_09_tariff} tar on tar.agr_id = p.agr_id
  left join {acq_08b_tariff_fix} fix
    on cast(fix.c_tariff_plan as string) = cast(tar.c_tariff_plan as string)
  left join {acq_10o_doc_oper} oper on oper.agr_id = p.agr_id
  left join mpos_one mpos on mpos.agr_id = p.agr_id and mpos.inn = p.inn
  left join {acq_mcc_by_agr} mcc on mcc.n_agr = p.n_agr
  left join prod_one prod on prod.inn = p.inn
  left join {acq_kedr_chod} kedr on kedr.inn = p.inn
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
  '{report_month}' as report_month
from base
