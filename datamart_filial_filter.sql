-- Optional calculated column for datamart dataset (native filter "Региональный филиал").
-- Dataset: sbx_da.tmp_shestopalov_acq_datamart_jan_jun  (or q1 if still on 151)
-- Calculated column name MUST be: filial_filter
-- Keep in sync with vd_new_graphs_acq_filterable.sql alias.

COALESCE(
  NULLIF(NULLIF(BTRIM(CAST(filial_rf AS TEXT)), ''), '<NULL>'),
  'Нет информации'
)

-- If physical column is branch_rf instead of filial_rf, use:
-- COALESCE(
--   NULLIF(NULLIF(BTRIM(CAST(branch_rf AS TEXT)), ''), '<NULL>'),
--   'Нет информации'
-- )
