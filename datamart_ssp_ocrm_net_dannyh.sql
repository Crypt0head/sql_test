-- Optional calculated column for datamart dataset (native filter "Зона ответственности").
-- Preferred table: sbx_da.tmp_shestopalov_acq_datamart_jan_jun
-- (datasetId 151 if still bound to that table, or the new v2 dataset id)
-- Name must stay: ssp_ocrm
-- Purpose: show "Нет данных" in Zone filter dropdown and keep values
-- consistent with vd_new_graphs_acq_filterable.sql.

COALESCE(
  NULLIF(NULLIF(BTRIM(CAST(ssp_ocrm AS TEXT)), ''), '<NULL>'),
  'Нет данных'
)
