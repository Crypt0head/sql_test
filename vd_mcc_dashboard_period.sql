-- Virtual dataset: vd_mcc_dashboard_period
-- Вкладка v2: MCC-коды (TABLE 3)
--
-- Те же dashboard filters: report_month (якорь) + period_mode (month|ytd|quarter)
--
-- Оборот / комиссия / % эквайринга — из tmp_shestopalov_acq_mcc_month (trx × MCC).
-- ТСП / терминалы / АУР — из datamart: договоры, у которых этот MCC есть в списке
-- `mcc` (через запятую). У multi-MCC договора term_cnt и aur входят в каждый код.
--
-- Метрики в чарте (GROUP BY mcc):
--   acq_pct   = SUM(commission) / SUM(trx_sum)          → .2%
--   share     = SUM(trx_sum) / SUM(SUM(trx_sum)) OVER() → .2%
--   tsp_cnt   = SUM(tsp_cnt) / COUNT(DISTINCT month)    → ,d   (среднемес.)
--   term_cnt  = SUM(term_cnt) / COUNT(DISTINCT month)   → ,d
--   aur       = SUM(aur)                                → ,.2f (поток за период)
-- На YTD/Quarter не использовать AVG(acq_pct) / AVG(share_trx_sum_pct).

{% set sel_months = filter_values('report_month', remove_filter=True) %}
{% set modes = filter_values('period_mode', remove_filter=True) %}
{% set mode = (modes[0] | lower | trim) if modes else 'month' %}
{% if mode not in ['month', 'ytd', 'quarter'] %}
  {% set mode = 'month' %}
{% endif %}
{% set anchor = (sel_months | sort | last) if sel_months else none %}
{% if anchor %}
  {% set yr = anchor[:4] %}
  {% set mo = anchor[5:7] | int %}
  {% if mode == 'ytd' %}
    {% set period_from = yr ~ '-01' %}
  {% elif mode == 'quarter' %}
    {% set qm = ((mo - 1) // 3) * 3 + 1 %}
    {% set period_from = yr ~ '-' ~ ('%02d' | format(qm)) %}
  {% else %}
    {% set period_from = anchor %}
  {% endif %}
  {% set period_to = anchor %}
{% endif %}

SELECT
    NULLIF(SUBSTRING(BTRIM(CAST(m.report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
    NULLIF(BTRIM(CAST(m.mcc AS TEXT)), '') AS mcc,
    NULLIF(BTRIM(CAST(m.mcc_name_ru AS TEXT)), '') AS mcc_name_ru,
    NULLIF(BTRIM(CAST(m.mcc_label AS TEXT)), '') AS mcc_label,
    m.trx_cnt,
    m.trx_sum,
    m.commission_from_ops,
    m.acq_pct,
    m.share_trx_sum_pct,
    COALESCE(x.tsp_cnt, 0) AS tsp_cnt,
    COALESCE(x.term_cnt, 0) AS term_cnt,
    COALESCE(x.aur, 0) AS aur,
    '{{ mode }}' AS period_mode_applied,
    {% if anchor %}
    '{{ period_from }}' AS period_from,
    '{{ period_to }}' AS period_to,
    '{{ anchor }}' AS anchor_report_month
    {% else %}
    NULL AS period_from,
    NULL AS period_to,
    NULL AS anchor_report_month
    {% endif %}
FROM sbx_da.tmp_shestopalov_acq_mcc_month AS m
LEFT JOIN (
    SELECT
        NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
        NULLIF(REGEXP_REPLACE(BTRIM(mcc_item), '\.0$', ''), '') AS mcc,
        COUNT(DISTINCT NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '')) AS tsp_cnt,
        SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(d.term_cnt AS TEXT)), '') AS NUMERIC), 0)) AS term_cnt,
        SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(d.aur AS TEXT)), '') AS NUMERIC), 0)) AS aur
    FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 AS d
    CROSS JOIN LATERAL regexp_split_to_table(
        REGEXP_REPLACE(COALESCE(CAST(d.mcc AS TEXT), ''), '[;|]', ',', 'g'),
        '\s*,\s*'
    ) AS mcc_item
    WHERE NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') IS NOT NULL
      AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') IS NOT NULL
      AND NULLIF(REGEXP_REPLACE(BTRIM(mcc_item), '\.0$', ''), '') IS NOT NULL
    {% if anchor %}
      AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') >= '{{ period_from }}'
      AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') <= '{{ period_to }}'
    {% else %}
      AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') = (
        SELECT MAX(NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), ''))
        FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2
      )
    {% endif %}
    GROUP BY 1, 2
) AS x
  ON x.report_month = NULLIF(SUBSTRING(BTRIM(CAST(m.report_month AS TEXT)) FROM 1 FOR 7), '')
 AND x.mcc = NULLIF(REGEXP_REPLACE(BTRIM(CAST(m.mcc AS TEXT)), '\.0$', ''), '')
WHERE NULLIF(BTRIM(CAST(m.mcc AS TEXT)), '') IS NOT NULL
  AND NULLIF(SUBSTRING(BTRIM(CAST(m.report_month AS TEXT)) FROM 1 FOR 7), '') IS NOT NULL
{% if anchor %}
  AND NULLIF(SUBSTRING(BTRIM(CAST(m.report_month AS TEXT)) FROM 1 FOR 7), '') >= '{{ period_from }}'
  AND NULLIF(SUBSTRING(BTRIM(CAST(m.report_month AS TEXT)) FROM 1 FOR 7), '') <= '{{ period_to }}'
{% else %}
  AND NULLIF(SUBSTRING(BTRIM(CAST(m.report_month AS TEXT)) FROM 1 FOR 7), '') = (
    SELECT MAX(NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), ''))
    FROM sbx_da.tmp_shestopalov_acq_mcc_month
  )
{% endif %}
