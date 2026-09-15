-- Virtual dataset: vd_acq_tsp_efficiency_period_cols_excl
-- Таблица «Эффективность ТСП с исключениями»: те же 4 колонки якоря + 2 средних %
--
-- Как vd_acq_tsp_efficiency_period_cols, но агрегат после exclude_flag = 0.
-- Текущий _period_cols не менять (все договоры).
--
-- exclude_flag = 1 если ЛЮБОЕ (как vd_acq_efficiency_flags):
--   ИНН в КУАП (sbx_da.acq_kuap_inn)
--   ИНН в sbx_da.acq_gk_inn (файл уже только нужные ИНН; is_exclude_gk = 1)
--   tariff_short = «Акционный»
--   договор младше 30 дней на 1-е число report_month
--   (нет d_valid_from → не исключаем)
--
-- period_mode НЕ читаем и НЕ режем по нему.
-- Всегда окно YYYY-01 … якорь.
-- Зерно: filial_filter × report_month.
--
-- Требуется: ENABLE_TEMPLATE_PROCESSING; справочники залиты тетрадкой
--   Проверки в пространстве/acq_kuap_gk_lists_upload.ipynb
-- Dashboard filter report_month — якорь; chart-level filter по месяцу НЕ ставить.
-- Чарт убрать из scope фильтра period_mode.
--
-- Чарт: v2_period_filial_tsp_efficiency_cols_excl
-- Метрики: те же, что TABLE 1 period-cols / блок 4b

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
