CREATE TABLE {acq_10b_products}
AS
WITH rko_dt AS (
  SELECT max(dt_part) AS dt_part
  FROM sandbox_ai.prod__kuznetsov_lle__acc_ul_trx_features_impala
  WHERE dt_part <= date '{month_end}'
),
rko_inns AS (
  SELECT DISTINCT
    regexp_replace(trim(cast(f.inn AS string)), '[^0-9]', '') AS inn
  FROM sandbox_ai.prod__kuznetsov_lle__acc_ul_trx_features_impala f
  CROSS JOIN rko_dt d
  WHERE f.dt_part = d.dt_part
    AND f.inn IS NOT NULL
),
current_agrs AS (
  SELECT
    regexp_replace(trim(cast(inn AS string)), '[^0-9]', '') AS inn,
    lower(trim(cast(target_name AS string))) AS product_name,
    1 AS has_product
  FROM sandbox_ai.le_nbo_scoring__targets_product_agreems
  WHERE c_date_begin <= '{month_end}'
    AND (c_date_close > '{month_end}' OR c_date_close IS NULL)
    AND inn IS NOT NULL
    AND target_name IS NOT NULL
  GROUP BY 1, 2
),
union_flags AS (
  SELECT inn, product_name, has_product
  FROM current_agrs

  UNION ALL

  SELECT inn, 'rko' AS product_name, 1 AS has_product
  FROM rko_inns
)
SELECT
  inn,
  product_name,
  max(has_product) AS has_product
FROM union_flags
WHERE inn IS NOT NULL
  AND inn <> ''
GROUP BY inn, product_name
