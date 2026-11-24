-- Колонки таблицы дашборда (все TEXT). Каждая должна быть в include/sql/acq_slice_load.sql;
-- колонки среза, которых здесь нет (term_calc_source, c_tariff_plan и др.), остаются только в озере.
-- Временную таблицу пишет Spark с типами озера: деньги и проценты округляются до копеек, как раньше в дашборде,
-- счётчики приводятся к целым, чтобы не было "12.0".
INSERT INTO {target_acq_fin} (
    report_month, snapshot_month_start, inn, company_name, agr_id, n_agr,
    contract_number, d_valid_from, d_valid_to, n_agr_actual, contract_number_acq,
    d_valid_from_actual, d_valid_to_actual, cdi_id, ssp_ocrm, cft_id, ogrn,
    filial_rf, vsp_name, vsp_code, tariff_name, tariff_short, tariff_source,
    retl_cnt, retl_with_term_cnt, active_retl_cnt, term_cnt, active_terms,
    active_term_cnt, trx_cnt, trx_sum, commission_from_ops, commission_monthly,
    commission_monthly_source, commission_monthly_mpos, commission_monthly_oper_raw,
    commission_monthly_oper_net, int_component, commission_total, aur, amortization,
    chod, fin_result, tsp_effective, acq_pct, chod_pct, mcc, mcc_profile,
    mcc_cnt, rko, accounting, business_cards, credit, dbo, deposit, insurance,
    loyalty_program, nmo, nso, pravocard, salary_project, self_inkass,
    service_package, sms_info, factoring, lising, recommended_products,
    kedr_obshiy_chod, kedr_src_cnt, kedr_obshiy_chod_contrib
)
-- filial_rf приводится к виду истории и таблицы прав rls_acq_user_filial («Алтайский РФ»):
-- обрезка после «РФ», без «АО …», регистр «Первая заглавная». Только «АО "Россельхозбанк"» -> NULL (на дашборде «ЦРМБ»).
-- Делается здесь, а не в Impala: там lower()/upper() не меняют кириллицу.
WITH f0 AS (
    SELECT t.*,
           NULLIF(BTRIM(REGEXP_REPLACE(REPLACE(CAST(t.filial_rf AS TEXT), CHR(160), ' '), '\s+', ' ', 'g')), '') AS filial_s0
    FROM {target_acq_fin_temp} t
),
f1 AS (
    SELECT f0.*,
           CASE WHEN STRPOS(LOWER(filial_s0), 'рф') > 0
                THEN LEFT(filial_s0, STRPOS(LOWER(filial_s0), 'рф') + 1)
                ELSE filial_s0
           END AS filial_s1
    FROM f0
),
f2 AS (
    SELECT f1.*,
           NULLIF(BTRIM(REGEXP_REPLACE(filial_s1, '\m(ао|пао|оао|зао)\M.*$', '', 'i'), ' ,;.-'), '') AS filial_s2
    FROM f1
)
SELECT
    cast(report_month AS text), cast(snapshot_month_start AS text), cast(inn AS text),
    cast(company_name AS text), cast(agr_id AS text), cast(n_agr AS text),
    cast(contract_number AS text), cast(d_valid_from AS text), cast(d_valid_to AS text),
    cast(n_agr_actual AS text), cast(contract_number_acq AS text),
    cast(d_valid_from_actual AS text), cast(d_valid_to_actual AS text), cast(cdi_id AS text),
    cast(ssp_ocrm AS text), cast(cft_id AS text), cast(ogrn AS text),
    REGEXP_REPLACE(UPPER(LEFT(filial_s2, 1)) || LOWER(SUBSTR(filial_s2, 2)), 'рф', 'РФ', 'gi'),
    cast(vsp_name AS text), cast(vsp_code AS text),
    cast(tariff_name AS text),
    -- tariff_short из среза не берётся: в Impala lower() не меняет кириллицу, и «Индивидуальный» не находится.
    -- STRPOS вместо LIKE: знак процента в SQL этого оператора может быть принят за параметр.
    CASE
      WHEN STRPOS(LOWER(COALESCE(CAST(tariff_name AS text), '')), 'акт') > 0 THEN 'По актам'
      WHEN STRPOS(LOWER(COALESCE(CAST(tariff_name AS text), '')), 'акцион') > 0
        OR STRPOS(LOWER(COALESCE(CAST(tariff_name AS text), '')), 'меню') > 0
        OR STRPOS(LOWER(COALESCE(CAST(tariff_name AS text), '')), 'сезон') > 0 THEN 'Акционный'
      WHEN STRPOS(LOWER(COALESCE(CAST(tariff_name AS text), '')), 'индив') > 0 THEN 'Индивидуальный'
      WHEN STRPOS(LOWER(COALESCE(CAST(tariff_name AS text), '')), 'стандарт') > 0 THEN 'Стандарт'
      ELSE 'Не сегментирован'
    END,
    cast(tariff_source AS text),
    cast(round(cast(retl_cnt AS numeric)) AS bigint)::text,
    cast(round(cast(retl_with_term_cnt AS numeric)) AS bigint)::text,
    cast(round(cast(active_retl_cnt AS numeric)) AS bigint)::text,
    cast(round(cast(term_cnt AS numeric)) AS bigint)::text,
    cast(round(cast(active_terms AS numeric)) AS bigint)::text,
    cast(round(cast(active_term_cnt AS numeric)) AS bigint)::text,
    cast(round(cast(trx_cnt AS numeric)) AS bigint)::text,
    round(cast(trx_sum AS numeric), 2)::text,
    round(cast(commission_from_ops AS numeric), 2)::text,
    round(cast(commission_monthly AS numeric), 2)::text,
    cast(commission_monthly_source AS text),
    round(cast(commission_monthly_mpos AS numeric), 2)::text,
    round(cast(commission_monthly_oper_raw AS numeric), 2)::text,
    round(cast(commission_monthly_oper_net AS numeric), 2)::text,
    round(cast(int_component AS numeric), 2)::text,
    round(cast(commission_total AS numeric), 2)::text,
    round(cast(aur AS numeric), 2)::text,
    round(cast(amortization AS numeric), 2)::text,
    round(cast(chod AS numeric), 2)::text,
    round(cast(fin_result AS numeric), 2)::text,
    cast(round(cast(tsp_effective AS numeric)) AS bigint)::text,
    round(cast(acq_pct AS numeric), 2)::text,
    round(cast(chod_pct AS numeric), 2)::text,
    cast(mcc AS text), cast(mcc_profile AS text),
    cast(round(cast(mcc_cnt AS numeric)) AS bigint)::text,
    cast(round(cast(rko AS numeric)) AS bigint)::text,
    cast(round(cast(accounting AS numeric)) AS bigint)::text,
    cast(round(cast(business_cards AS numeric)) AS bigint)::text,
    cast(round(cast(credit AS numeric)) AS bigint)::text,
    cast(round(cast(dbo AS numeric)) AS bigint)::text,
    cast(round(cast(deposit AS numeric)) AS bigint)::text,
    cast(round(cast(insurance AS numeric)) AS bigint)::text,
    cast(round(cast(loyalty_program AS numeric)) AS bigint)::text,
    cast(round(cast(nmo AS numeric)) AS bigint)::text,
    cast(round(cast(nso AS numeric)) AS bigint)::text,
    cast(round(cast(pravocard AS numeric)) AS bigint)::text,
    cast(round(cast(salary_project AS numeric)) AS bigint)::text,
    cast(round(cast(self_inkass AS numeric)) AS bigint)::text,
    cast(round(cast(service_package AS numeric)) AS bigint)::text,
    cast(round(cast(sms_info AS numeric)) AS bigint)::text,
    cast(factoring AS text),
    cast(lising AS text), cast(recommended_products AS text),
    cast(kedr_obshiy_chod AS text),
    cast(round(cast(kedr_src_cnt AS numeric)) AS bigint)::text,
    cast(kedr_obshiy_chod_contrib AS text)
FROM f2
