-- Virtual dataset: vd_pnl_chod_finres_series
-- График «ЧОД ТЭ + Фин.рез.» на вкладке P&L (TEST)
--
-- Окно строк всегда YYYY-01 … якорь (иначе на оси один столбец).
-- period_mode НЕ режет окно month/ytd; в режиме quarter наружу только месяцы квартала якоря.
-- Накопительно: running sum по agr_id, затем SUM на точке оси = портфель.
--
-- Требуется: ENABLE_TEMPLATE_PROCESSING
-- report_month = якорь на всех строках (чтобы dashboard filter не оставил один месяц оси)
-- Ось X чарта: point_report_month
-- Chart-level filter по месяцу НЕ ставить.
--
-- Чарт: v2_period_pnl_chod_finres

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
  {% set ytd_from = yr ~ '-01' %}
  {% set qm = ((mo - 1) // 3) * 3 + 1 %}
  {% set quarter_from = yr ~ '-' ~ ('%02d' | format(qm)) %}
{% endif %}

WITH raw AS (
  SELECT
    NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') AS point_report_month,
    NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') AS agr_id,
    NULLIF(BTRIM(CAST(d.inn AS TEXT)), '') AS inn,
    NULLIF(BTRIM(CAST(d.company_name AS TEXT)), '') AS company_name,
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
    COALESCE(CAST(NULLIF(BTRIM(CAST(d.chod AS TEXT)), '') AS NUMERIC), 0) AS chod_num,
    COALESCE(CAST(NULLIF(BTRIM(CAST(d.fin_result AS TEXT)), '') AS NUMERIC), 0) AS fin_num
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
),
by_agr_month AS (
  SELECT
    point_report_month,
    agr_id,
    inn,
    company_name,
    filial_filter,
    SUM(chod_num) AS chod_month,
    SUM(fin_num) AS fin_month,
    SUBSTRING(point_report_month FROM 1 FOR 4)
      || '-'
      || LPAD((
           ((CAST(SUBSTRING(point_report_month FROM 6 FOR 2) AS INT) - 1) / 3) * 3 + 1
         )::TEXT, 2, '0') AS quarter_key
  FROM raw
  GROUP BY
    point_report_month,
    agr_id,
    inn,
    company_name,
    filial_filter
),
win AS (
  SELECT
    b.*,
    SUM(b.chod_month) OVER (
      PARTITION BY b.agr_id
      ORDER BY b.point_report_month
      ROWS UNBOUNDED PRECEDING
    ) AS chod_ytd,
    SUM(b.fin_month) OVER (
      PARTITION BY b.agr_id
      ORDER BY b.point_report_month
      ROWS UNBOUNDED PRECEDING
    ) AS fin_ytd,
    SUM(b.chod_month) OVER (
      PARTITION BY b.agr_id, b.quarter_key
      ORDER BY b.point_report_month
      ROWS UNBOUNDED PRECEDING
    ) AS chod_qtd,
    SUM(b.fin_month) OVER (
      PARTITION BY b.agr_id, b.quarter_key
      ORDER BY b.point_report_month
      ROWS UNBOUNDED PRECEDING
    ) AS fin_qtd
  FROM by_agr_month AS b
)
SELECT
    {% if anchor %}
    '{{ anchor }}' AS report_month,
    {% else %}
    w.point_report_month AS report_month,
    {% endif %}
    w.point_report_month,
    w.agr_id,
    w.inn,
    w.company_name,
    w.filial_filter,
    w.chod_month,
    w.fin_month,
    w.chod_ytd,
    w.fin_ytd,
    w.chod_qtd,
    w.fin_qtd,
    '{{ mode }}' AS period_mode_applied
FROM win AS w
{% if mode == 'quarter' and anchor %}
WHERE w.point_report_month >= '{{ quarter_from }}'
{% endif %}
