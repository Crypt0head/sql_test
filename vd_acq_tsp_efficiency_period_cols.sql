-- Virtual dataset: vd_acq_tsp_efficiency_period_cols
-- Таблица «Эффективность ТСП»: 4 колонки за якорный месяц + 2 средних %
--
-- period_mode НЕ читаем и НЕ режем по нему.
-- Всегда окно YYYY-01 … якорь (нужно и для YTD, и для квартала).
-- Зерно: filial_filter × report_month (уже с месячным %).
--
-- Требуется: ENABLE_TEMPLATE_PROCESSING.
-- Dashboard filter report_month — якорь; chart-level filter по месяцу НЕ ставить
--   (Jinja: remove_filter=True).
-- Этот чарт убрать из scope фильтра period_mode.
--
-- Чарт: v2_period_filial_tsp_efficiency_cols
-- Метрики: superset_metrics_dashboard_tables.txt (TABLE 1 period-cols)

{% set sel_months = filter_values('report_month', remove_filter=True) %}
{% set anchor = (sel_months | sort | last) if sel_months else none %}
{% if anchor %}
  {% set yr = anchor[:4] %}
  {% set mo = anchor[5:7] | int %}
  {% set ytd_from = yr ~ '-01' %}
  {% set qm = ((mo - 1) // 3) * 3 + 1 %}
  {% set quarter_from = yr ~ '-' ~ ('%02d' | format(qm)) %}
{% endif %}

WITH src AS (
  SELECT
    NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
    NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') AS agr_id,
    CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END AS filial_filter,
    COALESCE(CAST(NULLIF(BTRIM(CAST(d.tsp_effective AS TEXT)), '') AS NUMERIC), 0) AS tsp_effective,
    COALESCE(CAST(NULLIF(BTRIM(CAST(d.fin_result AS TEXT)), '') AS NUMERIC), 0) AS fin_result
  FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 AS d
  WHERE NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') IS NOT NULL
    AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') IS NOT NULL
{% if anchor %}
    AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') >= '{{ ytd_from }}'
    AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') <= '{{ anchor }}'
{% else %}
    AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') = (
      SELECT MAX(NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), ''))
      FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2
    )
{% endif %}
)
SELECT
    s.filial_filter,
    s.report_month,
    COUNT(DISTINCT s.agr_id) AS all_tsp,
    COUNT(DISTINCT CASE
      WHEN s.tsp_effective = 1 OR s.fin_result > 0
      THEN s.agr_id
    END) AS effective_tsp,
    COUNT(DISTINCT CASE
      WHEN s.tsp_effective = 0 AND s.fin_result <= 0
      THEN s.agr_id
    END) AS ineffective_tsp,
    100.0 * COUNT(DISTINCT CASE
      WHEN s.fin_result > 0 THEN s.agr_id
    END)
    / NULLIF(COUNT(DISTINCT s.agr_id), 0) AS pct_eff,
    {% if anchor %}
    '{{ anchor }}' AS anchor_report_month,
    '{{ ytd_from }}' AS ytd_from,
    '{{ quarter_from }}' AS quarter_from
    {% else %}
    s.report_month AS anchor_report_month,
    s.report_month AS ytd_from,
    s.report_month AS quarter_from
    {% endif %}
FROM src AS s
GROUP BY s.filial_filter, s.report_month
