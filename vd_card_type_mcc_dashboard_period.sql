-- Virtual dataset: vd_card_type_mcc_dashboard_period
-- Таблица: MCC | операции | оборот | доли платёжных систем
--
-- Те же dashboard filters: report_month (якорь) + period_mode (month|ytd|quarter)
--   month    = только якорь
--   ytd      = январь года якоря … якорь
--   quarter  = первый месяц квартала якоря … якорь
--
-- Источник: sbx_da.tmp_shestopalov_acq_card_type_month
--   зерно витрины: report_month × mcc × payment_system
--   зерно чарта:   1 строка на MCC за выбранный период (месяцы уже просуммированы)
--
-- payment_system:
--   МИР / Visa / Mastercard / UnionPay / Карта РСХБ / прочие
--   «Карта РСХБ» = on-us (филиалы *РФ, Головной офис, ЦРМБ). Схема из FIID не читается.
--
-- Доли — 0–100, сумма по строке ≈ 100.
-- На чарте НЕ делать AVG по месячным долям: dataset уже взвесил SUM за период.
-- Customize долей: ,.2f (не .2% — значения уже в процентах).
-- Названия MCC — из tmp_shestopalov_acq_mcc_month (если залиты mcc_name_ru / mcc_label).

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

WITH src AS (
    SELECT
        NULLIF(SUBSTRING(BTRIM(CAST(c.report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
        NULLIF(REGEXP_REPLACE(BTRIM(CAST(c.mcc AS TEXT)), '\.0$', ''), '') AS mcc,
        NULLIF(BTRIM(CAST(c.payment_system AS TEXT)), '') AS payment_system,
        COALESCE(CAST(NULLIF(BTRIM(CAST(c.trx_cnt AS TEXT)), '') AS NUMERIC), 0) AS trx_cnt,
        COALESCE(CAST(NULLIF(BTRIM(CAST(c.trx_sum AS TEXT)), '') AS NUMERIC), 0) AS trx_sum
    FROM sbx_da.tmp_shestopalov_acq_card_type_month AS c
    WHERE NULLIF(BTRIM(CAST(c.mcc AS TEXT)), '') IS NOT NULL
      AND NULLIF(SUBSTRING(BTRIM(CAST(c.report_month AS TEXT)) FROM 1 FOR 7), '') IS NOT NULL
    {% if anchor %}
      AND NULLIF(SUBSTRING(BTRIM(CAST(c.report_month AS TEXT)) FROM 1 FOR 7), '') >= '{{ period_from }}'
      AND NULLIF(SUBSTRING(BTRIM(CAST(c.report_month AS TEXT)) FROM 1 FOR 7), '') <= '{{ period_to }}'
    {% else %}
      AND NULLIF(SUBSTRING(BTRIM(CAST(c.report_month AS TEXT)) FROM 1 FOR 7), '') = (
        SELECT MAX(NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), ''))
        FROM sbx_da.tmp_shestopalov_acq_card_type_month
      )
    {% endif %}
),
agg AS (
    SELECT
        s.mcc,
        SUM(s.trx_cnt) AS trx_cnt,
        SUM(s.trx_sum) AS trx_sum,
        SUM(CASE WHEN s.payment_system = 'МИР' THEN s.trx_cnt ELSE 0 END) AS cnt_mir,
        SUM(CASE WHEN s.payment_system = 'Visa' THEN s.trx_cnt ELSE 0 END) AS cnt_visa,
        SUM(CASE WHEN s.payment_system = 'Mastercard' THEN s.trx_cnt ELSE 0 END) AS cnt_mc,
        SUM(CASE WHEN s.payment_system = 'UnionPay' THEN s.trx_cnt ELSE 0 END) AS cnt_up,
        SUM(CASE WHEN s.payment_system = 'Карта РСХБ' THEN s.trx_cnt ELSE 0 END) AS cnt_rshb,
        SUM(CASE WHEN s.payment_system NOT IN ('МИР', 'Visa', 'Mastercard', 'UnionPay', 'Карта РСХБ')
                 THEN s.trx_cnt ELSE 0 END) AS cnt_other,
        SUM(CASE WHEN s.payment_system = 'МИР' THEN s.trx_sum ELSE 0 END) AS sum_mir,
        SUM(CASE WHEN s.payment_system = 'Visa' THEN s.trx_sum ELSE 0 END) AS sum_visa,
        SUM(CASE WHEN s.payment_system = 'Mastercard' THEN s.trx_sum ELSE 0 END) AS sum_mc,
        SUM(CASE WHEN s.payment_system = 'UnionPay' THEN s.trx_sum ELSE 0 END) AS sum_up,
        SUM(CASE WHEN s.payment_system = 'Карта РСХБ' THEN s.trx_sum ELSE 0 END) AS sum_rshb,
        SUM(CASE WHEN s.payment_system NOT IN ('МИР', 'Visa', 'Mastercard', 'UnionPay', 'Карта РСХБ')
                 THEN s.trx_sum ELSE 0 END) AS sum_other
    FROM src AS s
    GROUP BY s.mcc
),
names AS (
    SELECT
        NULLIF(REGEXP_REPLACE(BTRIM(CAST(m.mcc AS TEXT)), '\.0$', ''), '') AS mcc,
        MAX(NULLIF(BTRIM(CAST(m.mcc_name_ru AS TEXT)), '')) AS mcc_name_ru,
        MAX(NULLIF(BTRIM(CAST(m.mcc_label AS TEXT)), '')) AS mcc_label
    FROM sbx_da.tmp_shestopalov_acq_mcc_month AS m
    WHERE NULLIF(BTRIM(CAST(m.mcc AS TEXT)), '') IS NOT NULL
    GROUP BY 1
)
SELECT
    a.mcc,
    COALESCE(n.mcc_label, a.mcc) AS mcc_label,
    n.mcc_name_ru,
    a.trx_cnt,
    a.trx_sum,
    CASE WHEN a.trx_cnt = 0 THEN NULL ELSE 100.0 * a.cnt_mir / a.trx_cnt END AS share_mir_cnt_pct,
    CASE WHEN a.trx_cnt = 0 THEN NULL ELSE 100.0 * a.cnt_visa / a.trx_cnt END AS share_visa_cnt_pct,
    CASE WHEN a.trx_cnt = 0 THEN NULL ELSE 100.0 * a.cnt_mc / a.trx_cnt END AS share_mc_cnt_pct,
    CASE WHEN a.trx_cnt = 0 THEN NULL ELSE 100.0 * a.cnt_up / a.trx_cnt END AS share_up_cnt_pct,
    CASE WHEN a.trx_cnt = 0 THEN NULL ELSE 100.0 * a.cnt_rshb / a.trx_cnt END AS share_rshb_cnt_pct,
    CASE WHEN a.trx_cnt = 0 THEN NULL ELSE 100.0 * a.cnt_other / a.trx_cnt END AS share_other_cnt_pct,
    CASE WHEN a.trx_sum = 0 THEN NULL ELSE 100.0 * a.sum_mir / a.trx_sum END AS share_mir_sum_pct,
    CASE WHEN a.trx_sum = 0 THEN NULL ELSE 100.0 * a.sum_visa / a.trx_sum END AS share_visa_sum_pct,
    CASE WHEN a.trx_sum = 0 THEN NULL ELSE 100.0 * a.sum_mc / a.trx_sum END AS share_mc_sum_pct,
    CASE WHEN a.trx_sum = 0 THEN NULL ELSE 100.0 * a.sum_up / a.trx_sum END AS share_up_sum_pct,
    CASE WHEN a.trx_sum = 0 THEN NULL ELSE 100.0 * a.sum_rshb / a.trx_sum END AS share_rshb_sum_pct,
    CASE WHEN a.trx_sum = 0 THEN NULL ELSE 100.0 * a.sum_other / a.trx_sum END AS share_other_sum_pct,
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
FROM agg AS a
LEFT JOIN names AS n ON n.mcc = a.mcc
