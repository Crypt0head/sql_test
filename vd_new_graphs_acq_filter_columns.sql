-- Calculated columns for Superset dataset: sbx_da.vd_new_graphs_acq
-- Names MUST match native filter targets from datasetId 151 (datamart).
-- Create each as a Calculated column in Dataset -> Columns.

------------------------------------------------------------------------------
-- 1) Региональный филиал -> filial_filter
------------------------------------------------------------------------------
COALESCE(
  NULLIF(NULLIF(BTRIM(CAST(filial_rf AS TEXT)), ''), '<NULL>'),
  'Нет информации'
)
-- If physical column is branch_rf, replace filial_rf with branch_rf.

------------------------------------------------------------------------------
-- 2) Наименование клиента -> company_name
------------------------------------------------------------------------------
NULLIF(BTRIM(CAST(company_name AS TEXT)), '')
-- If source column differs, alias it, e.g. CAST(client_name AS TEXT)

------------------------------------------------------------------------------
-- 3) ИНН клиента -> inn
------------------------------------------------------------------------------
NULLIF(BTRIM(CAST(inn AS TEXT)), '')

------------------------------------------------------------------------------
-- 4) Тариф -> tariff_short
------------------------------------------------------------------------------
NULLIF(BTRIM(CAST(tariff_short AS TEXT)), '')
-- If source column is tariff_name / tariff, wrap as:
-- NULLIF(BTRIM(CAST(tariff_name AS TEXT)), '')

------------------------------------------------------------------------------
-- 5) Зона ответственности -> ssp_ocrm
-- NULL / empty / '<NULL>' must become 'Нет данных'
------------------------------------------------------------------------------
COALESCE(
  NULLIF(NULLIF(BTRIM(CAST(ssp_ocrm AS TEXT)), ''), '<NULL>'),
  'Нет данных'
)

------------------------------------------------------------------------------
-- 6) Месяц -> report_month
------------------------------------------------------------------------------
NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')
-- If graphs use snapshot_month_start instead:
-- TO_CHAR(CAST(snapshot_month_start AS DATE), 'YYYY-MM')

------------------------------------------------------------------------------
-- Optional SQL Lab checks
------------------------------------------------------------------------------
-- SELECT DISTINCT filial_filter FROM sbx_da.vd_new_graphs_acq ORDER BY 1 LIMIT 50;
-- SELECT DISTINCT company_name FROM sbx_da.vd_new_graphs_acq ORDER BY 1 LIMIT 50;
-- SELECT DISTINCT inn FROM sbx_da.vd_new_graphs_acq ORDER BY 1 LIMIT 50;
-- SELECT DISTINCT tariff_short FROM sbx_da.vd_new_graphs_acq ORDER BY 1 LIMIT 50;
-- SELECT DISTINCT ssp_ocrm FROM sbx_da.vd_new_graphs_acq ORDER BY 1 LIMIT 50;
-- SELECT DISTINCT report_month FROM sbx_da.vd_new_graphs_acq ORDER BY 1 LIMIT 50;
