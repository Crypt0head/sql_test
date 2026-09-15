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
--   Блок 1  → vd_period_mode_options
--   Блок 1b → vd_report_month_options   (фильтр месяца — НЕ vd_acq_*)
--   Блок 2  → vd_acq_dashboard_period     (P&L, терминалы; режет period_mode)
--   Блок 2b → vd_acq_tsp_efficiency_period_cols  (эффективность: месяц + AVG % YTD/Q)
--   Блок 2c → vd_pnl_chod_finres_series          (график ЧОД + Фин.рез. на P&L)
--   Блок 2d → vd_acq_tsp_efficiency_period_cols_excl  (эффективность с исключениями)
--   Блок 3  → vd_mcc_dashboard_period
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
-- БЛОК 1b — vd_report_month_options
-- Native filter «Помесячно», column = report_month
-- НЕ вешать этот фильтр на vd_acq_dashboard_period — там без якоря только MAX месяц
-- =============================================================================

SELECT DISTINCT
    NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month
FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2
WHERE NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') IS NOT NULL
ORDER BY 1;


-- =============================================================================
-- БЛОК 2 — vd_acq_dashboard_period
-- Чарты-копии: v2_period_filial_pnl_ranking, v2_period_filial_terminals
-- Эффективность ТСП — блок 2b (не этот dataset)
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


-- =============================================================================
-- БЛОК 2b — vd_acq_tsp_efficiency_period_cols
-- Чарт: v2_period_filial_tsp_efficiency_cols
-- period_mode игнорируется. Всегда янв … якорь. Зерно: филиал × месяц.
-- Полный SQL: sources/sql/vd_acq_tsp_efficiency_period_cols.sql
-- =============================================================================

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


-- =============================================================================
-- БЛОК 2c — vd_pnl_chod_finres_series
-- График: v2_period_pnl_chod_finres на вкладке P&L
-- Полный SQL: sources/sql/vd_pnl_chod_finres_series.sql
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


-- =============================================================================
-- БЛОК 2d — vd_acq_tsp_efficiency_period_cols_excl
-- Чарт: v2_period_filial_tsp_efficiency_cols_excl
-- Как блок 2b, но только договоры с exclude_flag = 0
--   (КУАП / ИНН из файла ГК / Акционный / договор < 30 дней на 1-е число месяца)
-- Метрики = блок 4b / TABLE 1 period-cols (тот же SQL)
-- period_mode игнорируется. Всегда янв … якорь.
-- Полный SQL: sources/sql/vd_acq_tsp_efficiency_period_cols_excl.sql
-- Нужны sbx_da.acq_kuap_inn и sbx_da.acq_gk_inn
-- =============================================================================

{% set sel_months = filter_values('report_month', remove_filter=True) %}
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
    COALESCE(CAST(NULLIF(BTRIM(CAST(d.fin_result AS TEXT)), '') AS NUMERIC), 0) AS fin_result,
    NULLIF(BTRIM(CAST(d.tariff_short AS TEXT)), '') AS tariff_short,
    NULLIF(BTRIM(CAST(d.d_valid_from AS TEXT)), '') AS d_valid_from,
    CASE
      WHEN length(regexp_replace(COALESCE(BTRIM(CAST(d.inn AS TEXT)), ''), '[^0-9]', '', 'g')) = 9
        THEN lpad(regexp_replace(BTRIM(CAST(d.inn AS TEXT)), '[^0-9]', '', 'g'), 10, '0')
      WHEN length(regexp_replace(COALESCE(BTRIM(CAST(d.inn AS TEXT)), ''), '[^0-9]', '', 'g')) = 11
        THEN lpad(regexp_replace(BTRIM(CAST(d.inn AS TEXT)), '[^0-9]', '', 'g'), 12, '0')
      ELSE NULLIF(regexp_replace(COALESCE(BTRIM(CAST(d.inn AS TEXT)), ''), '[^0-9]', '', 'g'), '')
    END AS inn_key
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
src AS (
  SELECT
    r.report_month,
    r.agr_id,
    r.filial_filter,
    r.tsp_effective,
    r.fin_result
  FROM raw AS r
  LEFT JOIN sbx_da.acq_kuap_inn AS k
    ON k.inn = r.inn_key
  LEFT JOIN sbx_da.acq_gk_inn AS g
    ON g.inn = r.inn_key
  WHERE
    CASE
      WHEN k.inn IS NOT NULL THEN 1
      WHEN BTRIM(CAST(g.is_exclude_gk AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y') THEN 1
      WHEN r.tariff_short = 'Акционный' THEN 1
      WHEN r.d_valid_from IS NOT NULL
       AND (
         TO_DATE(r.report_month || '-01', 'YYYY-MM-DD')
         - CAST(LEFT(r.d_valid_from, 10) AS DATE)
       ) < 30
      THEN 1
      ELSE 0
    END = 0
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
-- БЛОК 4b — МЕТРИКИ эффективности с колонками YTD/квартал
-- Dataset: vd_acq_tsp_efficiency_period_cols
--          и vd_acq_tsp_efficiency_period_cols_excl (тот же SQL метрик)
-- Chart: v2_period_filial_tsp_efficiency_cols
--        и v2_period_filial_tsp_efficiency_cols_excl
-- Dimensions = только filial_filter
-- period_mode НЕ в scope этих чартов
-- % колонки: pct_eff в VD 0–100 → в метрике / 100 + формат .2%
-- =============================================================================

-- Все ТСП (якорный месяц)
-- SUM(CASE
--   WHEN BTRIM(CAST(report_month AS TEXT)) = BTRIM(CAST(anchor_report_month AS TEXT))
--   THEN all_tsp
-- END)

-- Эффективные ТСП (якорный месяц)
-- SUM(CASE
--   WHEN BTRIM(CAST(report_month AS TEXT)) = BTRIM(CAST(anchor_report_month AS TEXT))
--   THEN effective_tsp
-- END)

-- Неэффективные ТСП (якорный месяц)
-- SUM(CASE
--   WHEN BTRIM(CAST(report_month AS TEXT)) = BTRIM(CAST(anchor_report_month AS TEXT))
--   THEN ineffective_tsp
-- END)

-- Процент эффективности, % (якорный месяц)
-- MAX(CASE
--   WHEN BTRIM(CAST(report_month AS TEXT)) = BTRIM(CAST(anchor_report_month AS TEXT))
--   THEN pct_eff
-- END) / 100.0

-- Средний процент эффективности с начала года
-- AVG(pct_eff) / 100.0

-- Средний процент эффективности в квартале
-- AVG(CASE
--   WHEN BTRIM(CAST(report_month AS TEXT)) >= BTRIM(CAST(quarter_from AS TEXT))
--   THEN pct_eff
-- END) / 100.0

-- Format трёх %: .2% (знак в ячейке)


-- =============================================================================
-- БЛОК 4c — МЕТРИКИ графика ЧОД ТЭ + Фин.рез. (v2_period_pnl_chod_finres)
-- Dataset: vd_pnl_chod_finres_series
-- X-axis: point_report_month  (не report_month)
-- period_mode в scope чарта; chart-level filter по месяцу НЕ ставить
-- =============================================================================

-- ЧОД
-- SUM(CASE
--   WHEN BTRIM(CAST(period_mode_applied AS TEXT)) = 'ytd' THEN chod_ytd
--   WHEN BTRIM(CAST(period_mode_applied AS TEXT)) = 'quarter' THEN chod_qtd
--   ELSE chod_month
-- END)

-- Фин.рез.
-- SUM(CASE
--   WHEN BTRIM(CAST(period_mode_applied AS TEXT)) = 'ytd' THEN fin_ytd
--   WHEN BTRIM(CAST(period_mode_applied AS TEXT)) = 'quarter' THEN fin_qtd
--   ELSE fin_month
-- END)


-- =============================================================================
-- БЛОК 5 — МЕТРИКИ TABLE 1b P&L (v2_period_filial_pnl_ranking)
-- GROUP BY только filial_filter; все SUM — в Metrics
-- =============================================================================

-- Комиссия эквайринга, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))

-- Ср. % эквайринга, %   (0–1 + формат .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- фикс.комиссия, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_monthly AS TEXT)), '') AS NUMERIC), 0))

-- ДОХОД, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_total AS TEXT)), '') AS NUMERIC), 0))

-- int, руб. (IRF)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0))

-- сред. знач int, %   (0–1 + формат .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0))
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
-- Стоки (точки, терминалы, точки без POS): AVG месячных SUM
--   = SUM(поле) / COUNT(DISTINCT report_month)  — в режиме month знаменатель = 1
-- Потоки (операции, оборот): SUM за диапазон
-- Средние на терминал: SUM(поток) / средний сток терминалов
-- Нужна колонка retl_with_term_cnt в vd_acq_dashboard_period (блок 2)
-- =============================================================================

-- Кол-во торговых точек (сток)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(retl_cnt AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- Кол-во терминалов (сток)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- Количество торговых точек без терминалов (сток)
-- SUM(GREATEST(
--   COALESCE(CAST(NULLIF(BTRIM(CAST(retl_cnt AS TEXT)), '') AS NUMERIC), 0)
--   - COALESCE(CAST(NULLIF(BTRIM(CAST(retl_with_term_cnt AS TEXT)), '') AS NUMERIC), 0)
-- , 0))
-- / NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- Доля активных терминалов   (уже 0–1; формат .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Количество операций (поток)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_cnt AS TEXT)), '') AS NUMERIC), 0))

-- Сумма операций (поток)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))

-- Средний % эквайринга, %   (0–1 + формат .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Средний ЧОД на терминал, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(
--     SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))
--     / NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)
--   , 0)

-- Средняя маржа на терминал, руб.
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(
--     SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))
--     / NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)
--   , 0)

-- Доходность на оборот, %  (бывш. ЧОД/обороты; 0–1 + .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Чистая маржа, %   (0–1 + .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)


-- =============================================================================
-- БЛОК 7 — МЕТРИКИ TABLE 3 MCC (v2_period_mcc_month)
-- На YTD/Quarter НЕ использовать AVG(acq_pct) / AVG(share_trx_sum_pct)
-- =============================================================================

-- Dimension: mcc (или mcc_label если добавлена)

-- Средний % эквайринга, % (взвешенно; 0–1 + .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
-- / NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Доля в объёме, %   (0–1 + .2%)
-- SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))
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
