-- Virtual dataset: sbx_da.vd_chod_clients_by_month
-- Table: "Распределение клиентов по сегментам ЧОД ТЭ" (3x4 / buckets)
--
-- Unified with dashboard native filters (same names as datamart / graphs):
--   report_month, filial_filter, inn, company_name, tariff_short, ssp_ocrm_filter
--
-- Grain: 1 row = 1 client (agr_id) x report_month
-- Mode: SINGLE month (not upto) — filter Месяц = report_month shows that month only
-- Source: sbx_da.tmp_shestopalov_acq_datamart_jan_jun

WITH raw AS (
    SELECT
        NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
        NULLIF(BTRIM(CAST(agr_id AS TEXT)), '') AS agr_id_key,
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
        ) AS ssp_ocrm_filter,
        COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0) AS chod_num
    FROM sbx_da.tmp_shestopalov_acq_datamart_jan_jun
    WHERE NULLIF(BTRIM(CAST(agr_id AS TEXT)), '') IS NOT NULL
      AND NULLIF(BTRIM(CAST(report_month AS TEXT)), '') IS NOT NULL
),
by_client_month AS (
    SELECT
        report_month,
        agr_id_key,
        inn,
        company_name,
        tariff_short,
        filial_filter,
        ssp_ocrm_filter,
        SUM(chod_num) AS chod_sum
    FROM raw
    WHERE report_month IS NOT NULL
    GROUP BY
        report_month,
        agr_id_key,
        inn,
        company_name,
        tariff_short,
        filial_filter,
        ssp_ocrm_filter
)
SELECT
    report_month,
    agr_id_key,
    inn,
    company_name,
    tariff_short,
    filial_filter,
    ssp_ocrm_filter,
    CASE
        WHEN chod_sum > 0 THEN 'Клиенты с положительным ЧОД ТЭ'
        WHEN chod_sum < 0 THEN 'Клиенты с отрицательным ЧОД ТЭ'
        ELSE 'Клиенты с 0 ЧОД ТЭ'
    END AS client_bucket,
    chod_sum,
    CAST(NULL AS NUMERIC) AS total_chod_placeholder
FROM by_client_month
;
