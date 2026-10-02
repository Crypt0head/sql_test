-- Virtual dataset: vd_acq_rls_overview_trend
-- Графики «Динамика активности клиентов» и «Динамика когорт клиентов».
-- period_mode не читает. Ось всегда с января года выбранного месяца по этот месяц включительно.
-- Jinja ON. Фильтр месяца на самом чарте не ставить. Scope: только report_month.
-- Роли те же, что у vd_acq_rls_overview.

{% set sel_months = filter_values('report_month', remove_filter=True) %}
{% set anchor = (sel_months | sort | last) if sel_months else none %}
{% if anchor %}
  {% set period_from = anchor[:4] ~ '-01' %}
  {% set period_to = anchor %}
{% endif %}

SELECT
    NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
    NULLIF(BTRIM(CAST(d.snapshot_month_start AS TEXT)), '') AS snapshot_month_start,
    NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') AS agr_id,
    NULLIF(BTRIM(CAST(d.d_valid_from AS TEXT)), '') AS d_valid_from,
    NULLIF(BTRIM(CAST(d.d_valid_to AS TEXT)), '') AS d_valid_to,
    NULLIF(BTRIM(CAST(d.inn AS TEXT)), '') AS inn,
    NULLIF(BTRIM(CAST(d.company_name AS TEXT)), '') AS company_name,
    NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), '') AS filial_rf,
    CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) ILIKE '%санкт-петербург%'
        OR COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) ILIKE '%санкт петербург%'
      THEN 'Санкт-Петербургский РФ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END AS filial_filter,
    CASE
      WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(d.chod AS TEXT)), '') AS NUMERIC), 0) > 0
      THEN 'Клиенты с положительным ЧОД ТЭ'
      WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(d.chod AS TEXT)), '') AS NUMERIC), 0) < 0
      THEN 'Клиенты с отрицательным ЧОД ТЭ'
      ELSE 'Клиенты с 0 ЧОД ТЭ'
    END AS client_bucket,
    d.kedr_obshiy_chod,
    d.kedr_obshiy_chod_contrib,
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
    'ytd' AS period_mode_applied,
    {% if anchor %}
    '{{ period_from }}' AS period_from,
    '{{ period_to }}' AS period_to,
    '{{ anchor }}' AS anchor_report_month
    {% else %}
    NULL AS period_from,
    NULL AS period_to,
    NULL AS anchor_report_month
    {% endif %}
FROM sbx_da.tmp_shestopalov_acq_fin_version AS d
WHERE NULLIF(BTRIM(CAST(d.agr_id AS TEXT)), '') IS NOT NULL
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') IS NOT NULL
{% if anchor %}
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') >= '{{ period_from }}'
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '') <= '{{ period_to }}'
{% else %}
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '')
      >= (
        SELECT SUBSTRING(
                 MAX(NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')),
                 1,
                 4
               ) || '-01'
        FROM sbx_da.tmp_shestopalov_acq_fin_version
      )
  AND NULLIF(SUBSTRING(BTRIM(CAST(d.report_month AS TEXT)) FROM 1 FOR 7), '')
      <= (
        SELECT MAX(NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), ''))
        FROM sbx_da.tmp_shestopalov_acq_fin_version
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
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) ILIKE '%санкт-петербург%'
        OR COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) ILIKE '%санкт петербург%'
      THEN 'Санкт-Петербургский РФ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
  )
)
