-- =============================================================================
-- ACQ DASHBOARD v2 — ВСЕ SQL ДЛЯ ПЕРЕКЛЮЧАТЕЛЯ «МЕСЯЦ / YTD / КВАРТАЛ»
-- Файл: sources/sql/acq_dashboard_period_all.sql
-- Инструкция: HOW_TO_period_mode_on_dashboard_copy.md
--
-- КАК ПОЛЬЗОВАТЬСЯ В SUPERSET
--   Каждый блок между ===== — ОТДЕЛЬНЫЙ запрос.
--   Копируйте ТОЛЬКО один блок в SQL Lab → Run → Save dataset.
--   НЕ запускайте весь файл целиком.
--
-- НУЖНО
--   • ENABLE_TEMPLATE_PROCESSING на database DRP (блок 0 — тест)
--   • Таблицы sbx_da.tmp_shestopalov_acq_datamart_final_script_2
--     и sbx_da.tmp_shestopalov_acq_mcc_month
--   • Dashboard filters: report_month (якорь) + period_mode (month|ytd|quarter)
--
-- VIRTUAL DATASETS (имена при Save)
--   Блок 1 → vd_period_mode_options
--   Блок 2 → vd_acq_dashboard_period
--   Блок 3 → vd_mcc_dashboard_period
-- =============================================================================


-- =============================================================================
-- БЛОК 0 — ТЕСТ JINJA (SQL Lab, не сохранять как dataset)
-- =============================================================================

-- Тест 0a: Jinja вообще включена?
SELECT {{ 1 + 1 }} AS jinja_ok;
-- Ожидание: jinja_ok = 2. Иначе — зовите админа (ENABLE_TEMPLATE_PROCESSING).

-- Тест 0b: filter_values (после настройки фильтра report_month на дашборде)
-- SELECT
--   {% if filter_values('report_month', remove_filter=True) %}
--     'has_report_month_filter'
--   {% else %}
--     'no_report_month_filter'
--   {% endif %} AS status;


-- =============================================================================
-- БЛОК 1 — vd_period_mode_options
-- Native filter «Режим периода», column = period_mode
-- =============================================================================

SELECT 'month' AS period_mode, 'Месяц' AS period_mode_label
UNION ALL
SELECT 'ytd', 'Нарастающий с начала года'
UNION ALL
SELECT 'quarter', 'Квартал';


-- =============================================================================
-- БЛОК 2 — vd_acq_dashboard_period
-- Чарты-копии: v2_period_filial_tsp_efficiency, v2_period_filial_pnl_ranking,
--              v2_period_filial_terminals
-- filial_filter уже в SELECT — calculated column в dataset НЕ нужен
-- =============================================================================

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


-- =============================================================================
-- БЛОК 3 — vd_mcc_dashboard_period
-- Чарт-копия: v2_period_mcc_month
-- =============================================================================

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


-- =============================================================================
-- БЛОК 4 — МЕТРИКИ TABLE 1 (v2_period_filial_tsp_efficiency)
-- Metrics → Custom SQL, Dimensions = filial_filter
-- Chart-level filter по report_month НЕ ставить
-- =============================================================================

-- Все ТСП
-- COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), ''))

-- Эффективные ТСП
-- COUNT(DISTINCT CASE
--   WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(tsp_effective AS TEXT)), '') AS NUMERIC), 0) = 1
--     OR COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) > 0
--   THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
-- END)

-- Неэффективные ТСП
-- COUNT(DISTINCT CASE
--   WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(tsp_effective AS TEXT)), '') AS NUMERIC), 0) = 0
--    AND COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) <= 0
--   THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
-- END)

-- Процент эффективности, %
-- 100.0 * COUNT(DISTINCT CASE
--   WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) > 0
--   THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
-- END)
-- / NULLIF(COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')), 0)


-- =============================================================================
-- БЛОК 5 — МЕТРИКИ TABLE 1b P&L (v2_period_filial_pnl_ranking)
-- GROUP BY только filial_filter; все SUM — в Metrics
-- =============================================================================

-- Комиссия эквайринга, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))

-- Ср. % эквайринга, %
-- 100.0 * SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- фикс.комиссия, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_monthly AS TEXT)), '') AS NUMERIC), 0))

-- ДОХОД, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_total AS TEXT)), '') AS NUMERIC), 0))

-- int, руб. (IRF)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0))

-- сред. знач int, %
-- 100.0 * SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- АУР, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(aur AS TEXT)), '') AS NUMERIC), 0))

-- Амортизация, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(amortization AS TEXT)), '') AS NUMERIC), 0))

-- РАСХОДЫ, руб.
-- SUM(
--   COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0)
--   + COALESCE(CAST(NULLIF(BTRIM(CAST(aur AS TEXT)), '') AS NUMERIC), 0)
--   + COALESCE(CAST(NULLIF(BTRIM(CAST(amortization AS TEXT)), '') AS NUMERIC), 0)
-- )

-- ЧОД, руб. (= SUM fin_result при корректных знаках в UI)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))


-- =============================================================================
-- БЛОК 6 — МЕТРИКИ TABLE 2 Терминалы (v2_period_filial_terminals)
-- =============================================================================

-- Кол-во торговых точек
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(retl_cnt AS TEXT)), '') AS NUMERIC), 0))

-- Кол-во терминалов
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))

-- Доля активных терминалов
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Количество операций
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_cnt AS TEXT)), '') AS NUMERIC), 0))

-- Сумма операций
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))

-- Средний % эквайринга, %
-- 100.0 * SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Средний ЧОД, руб.
-- AVG(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))

-- Средний размер маржинальности, руб.
-- AVG(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0))

-- ЧОД %, (ЧОД/обороты)
-- 100.0 * SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)


-- =============================================================================
-- БЛОК 7 — МЕТРИКИ TABLE 3 MCC (v2_period_mcc_month)
-- На YTD/Quarter НЕ использовать AVG(acq_pct) / AVG(share_trx_sum_pct)
-- =============================================================================

-- Dimension: mcc (или mcc_label если добавлена)

-- Средний % эквайринга, % (взвешенно)
-- 100.0 * SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Доля в объёме, %
-- 100.0 * SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(
--     SUM(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))) OVER (),
--     0
--   )


-- =============================================================================
-- БЛОК 8 — SMOKE (SQL Lab, без Jinja)
-- =============================================================================

-- 8a. Доступ к таблицам
-- SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2;
-- SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_mcc_month;

-- 8b. YTD Jan–Aug 2026 (ожидание: больше строк, чем один месяц)
-- SELECT COUNT(*) AS rows_ytd
-- FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2
-- WHERE NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') >= '2026-01'
--   AND NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') <= '2026-08';

-- 8c. Квартал Q3 partial Jul–Aug
-- SELECT COUNT(*) AS rows_q3
-- FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2
-- WHERE NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') >= '2026-07'
--   AND NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') <= '2026-08';

-- 8d. MCC: сумма долей за месяц ≈ 100%
-- SELECT
--   NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
--   SUM(CAST(NULLIF(BTRIM(CAST(share_trx_sum_pct AS TEXT)), '') AS NUMERIC)) AS share_sum
-- FROM sbx_da.tmp_shestopalov_acq_mcc_month
-- GROUP BY 1 ORDER BY 1;
