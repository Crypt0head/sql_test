-- Virtual dataset SQL for: sbx_da.vd_new_graphs_acq (replace current SQL)
-- Grain: 1 row = 1 client (agr_id) x report_month
-- Purpose: native dashboard filters (RF/INN/name/zone/tariff/month) can apply
-- to all graph charts that use this dataset.
--
-- Source: sbx_da.tmp_shestopalov_acq_datamart_jan_jun  (MPOS commission_monthly Jan–Jun upload)
-- Rollback source if needed: sbx_da.tmp_shestopalov_acq_datamart_q1
-- Filter column names MUST match dashboard native filter targets.

WITH raw AS (
    SELECT
        NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
        NULLIF(BTRIM(CAST(agr_id AS TEXT)), '') AS agr_id,
        NULLIF(BTRIM(CAST(inn AS TEXT)), '') AS inn,
        NULLIF(BTRIM(CAST(company_name AS TEXT)), '') AS company_name,
        NULLIF(BTRIM(CAST(tariff_short AS TEXT)), '') AS tariff_short,
        -- Physical branch column is usually filial_rf (or branch_rf).
        -- Alias MUST be filial_filter to match native filter target.
        COALESCE(
            NULLIF(NULLIF(BTRIM(CAST(filial_rf AS TEXT)), ''), '<NULL>'),
            'Нет информации'
        ) AS filial_filter,
        COALESCE(
            NULLIF(NULLIF(BTRIM(CAST(ssp_ocrm AS TEXT)), ''), '<NULL>'),
            'Нет данных'
        ) AS ssp_ocrm,
        -- Same expression under ssp_ocrm_filter so native filter can target
        -- datamart calculated column ssp_ocrm_filter (physical ssp_ocrm is taken).
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
        report_month,
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
    WHERE report_month IS NOT NULL
    GROUP BY
        report_month,
        agr_id,
        inn,
        company_name,
        tariff_short,
        filial_filter,
        ssp_ocrm,
        ssp_ocrm_filter
)
SELECT
    report_month,
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
;
