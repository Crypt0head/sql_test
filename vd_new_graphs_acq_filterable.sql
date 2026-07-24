-- Virtual dataset SQL for: sbx_da.vd_new_graphs_acq (replace current SQL)
--
-- Behavior (как раньше на презентации):
--   native filter "Месяц" → column report_month (якорь, single value)
--   на графике ось X → point_report_month
--   показываются все месяцы с point_report_month <= report_month
--   Пример: Месяц=2026-06 → на оси Jan..Jun
--
-- Grain of result: 1 row = 1 client (agr_id) x point_report_month x selected report_month
-- Also filterable by RF/INN/name/zone/tariff (same column names as datamart filters).
--
-- Source: sbx_da.tmp_shestopalov_acq_datamart_jan_jun
-- Rollback source if needed: sbx_da.tmp_shestopalov_acq_datamart_q1

WITH raw AS (
    SELECT
        NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') AS point_report_month,
        NULLIF(BTRIM(CAST(agr_id AS TEXT)), '') AS agr_id,
        NULLIF(BTRIM(CAST(inn AS TEXT)), '') AS inn,
        NULLIF(BTRIM(CAST(company_name AS TEXT)), '') AS company_name,
        NULLIF(BTRIM(CAST(tariff_short AS TEXT)), '') AS tariff_short,
        COALESCE(
            NULLIF(NULLIF(BTRIM(CAST(filial_rf AS TEXT)), ''), '<NULL>'),
            'Нет информации'
        ) AS filial_filter,
        COALESCE(
            NULLIF(NULLIF(BTRIM(CAST(ssp_ocrm AS TEXT)), ''), '<NULL>'),
            'Нет данных'
        ) AS ssp_ocrm,
        COALESCE(
            NULLIF(NULLIF(BTRIM(CAST(ssp_ocrm AS TEXT)), ''), '<NULL>'),
            'Нет данных'
        ) AS ssp_ocrm_filter,
        COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0) AS term_cnt_num,
        COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0) AS active_terms_num,
        COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0) AS chod_num,
        COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0) AS trx_sum_num,
        COALESCE(CAST(NULLIF(BTRIM(CAST(trx_cnt AS TEXT)), '') AS NUMERIC), 0) AS trx_cnt_num
    FROM sbx_da.tmp_shestopalov_acq_datamart_jan_jun
    WHERE NULLIF(BTRIM(CAST(agr_id AS TEXT)), '') IS NOT NULL
      AND NULLIF(BTRIM(CAST(report_month AS TEXT)), '') IS NOT NULL
),
by_client_month AS (
    SELECT
        point_report_month,
        agr_id,
        inn,
        company_name,
        tariff_short,
        filial_filter,
        ssp_ocrm,
        ssp_ocrm_filter,
        MAX(CASE WHEN active_terms_num > 0 THEN 1 ELSE 0 END) AS is_active_client,
        SUM(term_cnt_num) AS term_cnt,
        SUM(active_terms_num) AS active_terms,
        SUM(trx_sum_num) AS trx_sum,
        SUM(trx_cnt_num) AS trx_cnt,
        SUM(chod_num) AS chod_sum
    FROM raw
    WHERE point_report_month IS NOT NULL
    GROUP BY
        point_report_month,
        agr_id,
        inn,
        company_name,
        tariff_short,
        filial_filter,
        ssp_ocrm,
        ssp_ocrm_filter
),
metrics AS (
    SELECT
        point_report_month,
        agr_id,
        inn,
        company_name,
        tariff_short,
        filial_filter,
        ssp_ocrm,
        ssp_ocrm_filter,
        is_active_client,
        CASE WHEN is_active_client = 1 THEN 0 ELSE 1 END AS is_passive_client,
        term_cnt,
        active_terms,
        GREATEST(term_cnt - active_terms, 0) AS passive_terms,
        trx_sum,
        trx_cnt,
        chod_sum,
        CASE WHEN chod_sum > 0 AND chod_sum <= 2500 THEN 1 ELSE 0 END AS cohort_0_2500,
        CASE WHEN chod_sum > 2500 THEN 1 ELSE 0 END AS cohort_gt_2500,
        CASE WHEN chod_sum = 0 THEN 1 ELSE 0 END AS cohort_eq_0,
        CASE WHEN chod_sum < 0 THEN 1 ELSE 0 END AS cohort_lt_0
    FROM by_client_month
),
selected_months AS (
    SELECT DISTINCT point_report_month AS report_month
    FROM metrics
)
SELECT
    s.report_month,
    m.point_report_month,
    m.agr_id,
    m.inn,
    m.company_name,
    m.tariff_short,
    m.filial_filter,
    m.ssp_ocrm,
    m.ssp_ocrm_filter,
    m.is_active_client,
    m.is_passive_client,
    m.term_cnt,
    m.active_terms,
    m.passive_terms,
    m.trx_sum,
    m.trx_cnt,
    m.chod_sum,
    m.cohort_0_2500,
    m.cohort_gt_2500,
    m.cohort_eq_0,
    m.cohort_lt_0
FROM selected_months s
JOIN metrics m
  ON m.point_report_month <= s.report_month
;
