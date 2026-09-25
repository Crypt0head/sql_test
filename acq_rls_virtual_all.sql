-- ACQ RLS — virtual datasets. Копируйте ОДИН блок в SQL Lab.
-- Jinja / template processing = ON. Prod Dataset A не менять.
-- HOW_TO: HOW_TO_rls_acq_roles.md

-- =============================================================================
-- vd_acq_rls_overview
-- =============================================================================
-- Virtual dataset: vd_acq_rls_overview
-- Лист: Общая информация (Big Number / графики). Jinja ON. Не вешать RLS на physical Dataset A.
-- Фильтры дашборда: report_month + period_mode (как vd_acq_dashboard_period).

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
  AND EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.overview AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
)
  AND (
  EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user u
    WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(u.is_all_filials AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
  )
  OR EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user_filial f
    WHERE lower(BTRIM(CAST(f.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(f.filial_filter AS TEXT)) = CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
  )
)


-- =============================================================================
-- vd_acq_rls_tsp_eff
-- =============================================================================
-- Virtual dataset: vd_acq_rls_tsp_eff
-- Лист: Эффективность ТСП. Jinja ON. Не вешать RLS на physical Dataset A.
-- Фильтры дашборда: report_month + period_mode (как vd_acq_dashboard_period).

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
  AND EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.tsp_eff AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
)
  AND (
  EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user u
    WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(u.is_all_filials AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
  )
  OR EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user_filial f
    WHERE lower(BTRIM(CAST(f.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(f.filial_filter AS TEXT)) = CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
  )
)


-- =============================================================================
-- vd_acq_rls_pnl
-- =============================================================================
-- Virtual dataset: vd_acq_rls_pnl
-- Лист: P&L. Jinja ON. Не вешать RLS на physical Dataset A.
-- Фильтры дашборда: report_month + period_mode (как vd_acq_dashboard_period).

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
  AND EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.pnl AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
)
  AND (
  EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user u
    WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(u.is_all_filials AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
  )
  OR EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user_filial f
    WHERE lower(BTRIM(CAST(f.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(f.filial_filter AS TEXT)) = CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
  )
)


-- =============================================================================
-- vd_acq_rls_clients
-- =============================================================================
-- Virtual dataset: vd_acq_rls_clients
-- Лист: Клиенты (слот; витрина та же, пока нет отдельной). Jinja ON. Не вешать RLS на physical Dataset A.
-- Фильтры дашборда: report_month + period_mode (как vd_acq_dashboard_period).

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
  AND EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.clients AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
)
  AND (
  EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user u
    WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(u.is_all_filials AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
  )
  OR EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user_filial f
    WHERE lower(BTRIM(CAST(f.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(f.filial_filter AS TEXT)) = CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
  )
)


-- =============================================================================
-- vd_acq_rls_terminals
-- =============================================================================
-- Virtual dataset: vd_acq_rls_terminals
-- Лист: Терминалы. Jinja ON. Не вешать RLS на physical Dataset A.
-- Фильтры дашборда: report_month + period_mode (как vd_acq_dashboard_period).

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
  AND EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.terminals AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
)
  AND (
  EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user u
    WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(u.is_all_filials AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
  )
  OR EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user_filial f
    WHERE lower(BTRIM(CAST(f.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(f.filial_filter AS TEXT)) = CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
  )
)


-- =============================================================================
-- vd_acq_rls_mcc
-- =============================================================================
-- Virtual dataset: vd_acq_rls_mcc
-- Лист MCC: только флаг role_sheet.mcc (филиала в витрине MCC нет).
-- Jinja ON. Тот же period, что vd_mcc_dashboard_period.

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
  AND EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.mcc AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
)


-- =============================================================================
-- vd_filial_filter_options
-- =============================================================================
-- Virtual dataset: vd_filial_filter_options
-- Native filter «Региональный филиал». Jinja ON.
-- ГО видит все РФ; РФ — только свои. Без этого КМ увидит чужие имена в списке.

SELECT DISTINCT
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
    END AS filial_filter
FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 AS d
WHERE NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') IS NOT NULL
  AND (
  EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user u
    WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(u.is_all_filials AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
  )
  OR EXISTS (
    SELECT 1
    FROM sbx_da.rls_acq_user_filial f
    WHERE lower(BTRIM(CAST(f.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
      AND BTRIM(CAST(f.filial_filter AS TEXT)) = CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
  )
)
ORDER BY 1


-- Баннеры «нет прав» по листу. Jinja ON. По одному dataset на лист.
-- Разрешённому пользователю — 0 строк (чарт пустой).

-- -----------------------------------------------------------------------------
-- vd_acq_rls_denied_overview
-- -----------------------------------------------------------------------------
SELECT
  'Тебе сюда нельзя' AS access_message,
  'Нет прав на лист «Общая информация». Обратитесь к владельцу дашборда.' AS access_detail
WHERE NOT EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.overview AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
);

-- -----------------------------------------------------------------------------
-- vd_acq_rls_denied_tsp_eff
-- -----------------------------------------------------------------------------
SELECT
  'Тебе сюда нельзя' AS access_message,
  'Нет прав на лист «Эффективность ТСП». Обратитесь к владельцу дашборда.' AS access_detail
WHERE NOT EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.tsp_eff AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
);

-- -----------------------------------------------------------------------------
-- vd_acq_rls_denied_pnl
-- -----------------------------------------------------------------------------
SELECT
  'Тебе сюда нельзя' AS access_message,
  'Нет прав на лист «P&L». Обратитесь к владельцу дашборда.' AS access_detail
WHERE NOT EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.pnl AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
);

-- -----------------------------------------------------------------------------
-- vd_acq_rls_denied_clients
-- -----------------------------------------------------------------------------
SELECT
  'Тебе сюда нельзя' AS access_message,
  'Нет прав на лист «Клиенты». Обратитесь к владельцу дашборда.' AS access_detail
WHERE NOT EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.clients AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
);

-- -----------------------------------------------------------------------------
-- vd_acq_rls_denied_terminals
-- -----------------------------------------------------------------------------
SELECT
  'Тебе сюда нельзя' AS access_message,
  'Нет прав на лист «Терминалы». Обратитесь к владельцу дашборда.' AS access_detail
WHERE NOT EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.terminals AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
);

-- -----------------------------------------------------------------------------
-- vd_acq_rls_denied_mcc
-- -----------------------------------------------------------------------------
SELECT
  'Тебе сюда нельзя' AS access_message,
  'Нет прав на лист «MCC-коды». Обратитесь к владельцу дашборда.' AS access_detail
WHERE NOT EXISTS (
  SELECT 1
  FROM sbx_da.rls_acq_user u
  JOIN sbx_da.rls_acq_role_sheet s
    ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
  WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
    AND BTRIM(CAST(s.mcc AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
);

-- =============================================================================
-- SMOKE (SQL Lab, БЕЗ Jinja — подставь логин)
-- Ожидание COUNT: 0 = лист закрыт; >0 = данные есть
-- =============================================================================

-- Подставь: 'Shestopalov-VYur' | тестовый ГО Бизнес | РФ РОЭ | 'elina-ad'
-- SELECT '{{ current_username() }}' AS who;   -- сначала узнать логин (Jinja ON)

/*
WITH who AS (SELECT 'Shestopalov-VYur' AS username)
SELECT
  (SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user u
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(s.role) = BTRIM(u.role)
     JOIN who w ON lower(BTRIM(u.username)) = lower(BTRIM(w.username))
     WHERE BTRIM(s.overview) IN ('1', 'true', 'Y', 'y')
   )) AS overview_cnt,
  (SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user u
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(s.role) = BTRIM(u.role)
     JOIN who w ON lower(BTRIM(u.username)) = lower(BTRIM(w.username))
     WHERE BTRIM(s.tsp_eff) IN ('1', 'true', 'Y', 'y')
   )) AS tsp_eff_cnt,
  (SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user u
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(s.role) = BTRIM(u.role)
     JOIN who w ON lower(BTRIM(u.username)) = lower(BTRIM(w.username))
     WHERE BTRIM(s.pnl) IN ('1', 'true', 'Y', 'y')
   )) AS pnl_cnt,
  (SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_mcc_month m
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user u
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(s.role) = BTRIM(u.role)
     JOIN who w ON lower(BTRIM(u.username)) = lower(BTRIM(w.username))
     WHERE BTRIM(s.mcc) IN ('1', 'true', 'Y', 'y')
   )) AS mcc_cnt;
*/
