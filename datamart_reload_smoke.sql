-- Smoke after DRP upload of Jan–Jul datamart (July = lake-only, no Excel)
-- Table: sbx_da.tmp_shestopalov_acq_datamart_jan_jun
-- Run in Superset SQL Lab (DRP) after notebook DRP upload.
-- Expect report_month coverage 2026-01 … 2026-07.

-- 1) Month coverage + key money totals
SELECT
    NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
    COUNT(*) AS rows_cnt,
    COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')) AS agr_nunique,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(amortization AS TEXT)), '') AS NUMERIC), 0)) AS amortization_sum,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0)) AS chod_sum,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(aur AS TEXT)), '') AS NUMERIC), 0)) AS aur_sum,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0)) AS fin_result_sum,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(kedr_obshiy_chod_contrib AS TEXT)), '') AS NUMERIC), 0)) AS kedr_obshiy_chod_contrib_sum
FROM sbx_da.tmp_shestopalov_acq_datamart_jan_jun
GROUP BY 1
ORDER BY 1;

-- 2) Period totals (acquiring + Kedr Общий ЧОД via contrib)
SELECT
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0)) AS chod_sum_total,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0)) AS fin_result_sum_total,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(amortization AS TEXT)), '') AS NUMERIC), 0)) AS amortization_sum_total,
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(kedr_obshiy_chod_contrib AS TEXT)), '') AS NUMERIC), 0)) AS kedr_obshiy_chod_total
FROM sbx_da.tmp_shestopalov_acq_datamart_jan_jun;

-- 3) Sanity: fin_result ≈ chod - aur - amortization (row-level drift)
SELECT
    SUM(
        ABS(
            COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0)
            - (
                COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0)
                - COALESCE(CAST(NULLIF(BTRIM(CAST(aur AS TEXT)), '') AS NUMERIC), 0)
                - COALESCE(CAST(NULLIF(BTRIM(CAST(amortization AS TEXT)), '') AS NUMERIC), 0)
            )
        )
    ) AS abs_fin_result_formula_drift
FROM sbx_da.tmp_shestopalov_acq_datamart_jan_jun;
