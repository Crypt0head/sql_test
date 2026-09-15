-- Virtual dataset: vd_acq_dashboard_period
-- Вкладки v2: Эффективность ТСП, P&L, Терминалы (TABLE 1 / 1b / 2)
--
-- Требуется: ENABLE_TEMPLATE_PROCESSING на database DRP.
--
-- Dashboard native filters (scope → чарты v2 на вкладках 1–2):
--   1) report_month  — якорный месяц (YYYY-MM), один месяц
--      источник значений: vd_report_month_options (НЕ этот dataset)
--   2) period_mode   — month | ytd | quarter  (см. vd_period_mode_options.sql)
--
-- Логика диапазона:
--   month   → только якорный месяц
--   ytd     → YYYY-01 … якорь
--   quarter → 1-й месяц квартала якоря … якорь  (Q1=01–03, Q2=04–06, …)
--
-- P&L: SUM за диапазон. Терминалы (TABLE 2): стоки = SUM / COUNT DISTINCT month;
-- потоки = SUM; на терминал = SUM(поток) / средний сток. Нужен retl_with_term_cnt.
-- См. TABLE 2 в superset_metrics_dashboard_tables.txt и HOW_TO_period_mode_on_dashboard_copy.md

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
    NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
    NULLIF(BTRIM(CAST(d.snapshot_month_start AS TEXT)), '') AS snapshot_month_start,
    NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') AS agr_id,
    NULLIF(BTRIM(CAST(d.inn AS TEXT)), '') AS inn,
    NULLIF(BTRIM(CAST(d.company_name AS TEXT)), '') AS company_name,
    NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), '') AS filial_rf,
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
    d.tsp_effective,
    d.commission_from_ops,
    d.commission_monthly,
    d.commission_total,
    d.int_component,
    d.chod,
    d.aur,
    d.amortization,
    d.fin_result,
    d.retl_cnt,
    d.retl_with_term_cnt,
    d.term_cnt,
    d.active_terms,
    d.active_retl_cnt,
    d.trx_cnt,
    d.trx_sum,
    d.acq_pct,
    d.chod_pct,
    d.tariff_short,
    d.mcc,
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
FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 AS d
WHERE NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') IS NOT NULL
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') IS NOT NULL
{% if anchor %}
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') >= '{{ period_from }}'
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') <= '{{ period_to }}'
{% else %}
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') = (
    SELECT MAX(NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), ''))
    FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2
  )
{% endif %}
