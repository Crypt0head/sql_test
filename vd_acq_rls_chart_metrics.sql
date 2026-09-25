-- Метрики чартов на копии дашборда с RLS (vd_acq_rls_*).
-- Custom SQL в мере. Фильтр report_month на ЧАРТЕ не ставить — только дашборд.
-- Диалог смены dataset: «Очистить форму», потом вставить меру заново.
--
-- retl_with_term_cnt в текущей витрине = NULL (колонки нет).
-- После tmp_shestopalov_acq_fin_version в VD верни d.retl_with_term_cnt.

-- =============================================================================
-- ОБЩАЯ ИНФОРМАЦИЯ  Dataset: vd_acq_rls_overview
-- Big Number / линии: ось X = report_month
--   нет point_report_month, нет is_active_client
-- Динамика: смена dataset → «Очистить форму». Фильтр месяца на чарте снять.
-- period_mode month = 1 точка; ytd = янв…якорь; quarter = квартал.
-- =============================================================================

-- --- Динамика активности клиентов (Line / Area) ---
-- X-axis / Dimensions: report_month   (не point_report_month)
-- Sort: report_month ASC
-- Series labels: Активные клиенты / Пассивные клиенты

-- Активные клиенты
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0) > 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Пассивные клиенты  (= всего − активные, чтобы билось с KPI)
COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), ''))
-
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0) > 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Все клиенты / ТСП
COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), ''))

-- Активные клиенты  (есть оборот терминала в месяце)
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0) > 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Неактивные клиенты  = всего − активные  (NULL active_terms не теряются)
COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), ''))
-
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0) > 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Новые договоры  (открыты в месяце строки; в VD нужны d_valid_from / snapshot_month_start)
COUNT(DISTINCT CASE
  WHEN NULLIF(BTRIM(CAST(d_valid_from AS TEXT)), '') IS NOT NULL
   AND date_trunc('month', CAST(NULLIF(BTRIM(CAST(d_valid_from AS TEXT)), '') AS DATE))
     = date_trunc('month', CAST(NULLIF(BTRIM(CAST(snapshot_month_start AS TEXT)), '') AS DATE))
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Расторгнутые договоры  (закрыты в месяце строки; в VD нужен d_valid_to)
COUNT(DISTINCT CASE
  WHEN NULLIF(BTRIM(CAST(d_valid_to AS TEXT)), '') IS NOT NULL
   AND date_trunc('month', CAST(NULLIF(BTRIM(CAST(d_valid_to AS TEXT)), '') AS DATE))
     = date_trunc('month', CAST(NULLIF(BTRIM(CAST(snapshot_month_start AS TEXT)), '') AS DATE))
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Эффективные ТСП
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(tsp_effective AS TEXT)), '') AS NUMERIC), 0) = 1
    OR COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) > 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Неэффективные ТСП
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(tsp_effective AS TEXT)), '') AS NUMERIC), 0) = 0
   AND COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) <= 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Когорта ЧОД 0…2500
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0) >= 0
   AND COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0) <= 2500
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Когорта ЧОД > 2500
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0) > 2500
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Когорта ЧОД = 0
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0) = 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Когорта ЧОД < 0
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0) < 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- ЧОД, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))

-- Фин. рез., руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0))

-- Сумма операций
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))

-- Количество операций
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_cnt AS TEXT)), '') AS NUMERIC), 0))

-- Активные терминалы
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0))

-- Пассивные терминалы
SUM(
  GREATEST(
    COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0)
    - COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0)
  , 0)
)

-- Терминалы (сток месяца)
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))

-- =============================================================================
-- ЭФФЕКТИВНОСТЬ ТСП  Dataset: vd_acq_rls_tsp_eff
-- Table, group by: filial_filter
-- =============================================================================

-- Все ТСП
COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), ''))

-- Эффективные ТСП
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(tsp_effective AS TEXT)), '') AS NUMERIC), 0) = 1
    OR COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) > 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Неэффективные ТСП
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(tsp_effective AS TEXT)), '') AS NUMERIC), 0) = 0
   AND COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) <= 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)

-- Процент эффективности  → формат .2%  (формула 0–1)
COUNT(DISTINCT CASE
  WHEN COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0) > 0
  THEN NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')
END)
/ NULLIF(COUNT(DISTINCT NULLIF(BTRIM(CAST(agr_id AS TEXT)), '')), 0)

-- =============================================================================
-- P&L  Dataset: vd_acq_rls_pnl
-- Table, group by: filial_filter
-- =============================================================================

-- Комиссия эквайринга, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))

-- Ср. % эквайринга  → .2%  (не AVG(acq_pct))
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- фикс.комиссия, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_monthly AS TEXT)), '') AS NUMERIC), 0))

-- ДОХОД, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
+ SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_monthly AS TEXT)), '') AS NUMERIC), 0))

-- int, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0))

-- сред. знач int  → .2%
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- АУР, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(aur AS TEXT)), '') AS NUMERIC), 0))

-- Амортизация, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(amortization AS TEXT)), '') AS NUMERIC), 0))

-- РАСХОДЫ, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(int_component AS TEXT)), '') AS NUMERIC), 0))
+ SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(aur AS TEXT)), '') AS NUMERIC), 0))
+ SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(amortization AS TEXT)), '') AS NUMERIC), 0))

-- ЧОД, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))

-- Фин. рез., руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0))

-- =============================================================================
-- КЛИЕНТЫ  Dataset: vd_acq_rls_clients
-- Те же меры, что «Общая информация» (активные / пассивные / ЧОД / оборот)
-- =============================================================================

-- =============================================================================
-- ТЕРМИНАЛЫ  Dataset: vd_acq_rls_terminals
-- Table, group by: filial_filter
-- Стоки: SUM / число месяцев. Потоки: SUM.
-- =============================================================================

-- Кол-во торговых точек (сток)
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(retl_cnt AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- Кол-во терминалов (сток)
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- Точки без POS (сток). Сейчас retl_with_term_cnt = NULL → как все точки.
SUM(GREATEST(
  COALESCE(CAST(NULLIF(BTRIM(CAST(retl_cnt AS TEXT)), '') AS NUMERIC), 0)
  - COALESCE(CAST(NULLIF(BTRIM(CAST(retl_with_term_cnt AS TEXT)), '') AS NUMERIC), 0)
, 0))
/ NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- Доля активных терминалов  → .2%
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(active_terms AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Количество операций
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_cnt AS TEXT)), '') AS NUMERIC), 0))

-- Сумма операций
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))

-- Средний % эквайринга  → .2%
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Средний ЧОД на терминал, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))
    / NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)
  , 0)

-- Средняя маржа на терминал, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(
    SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))
    / NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)
  , 0)

-- Доходность на оборот  → .2%
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(chod AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Чистая маржа  → .2%
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(fin_result AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- =============================================================================
-- MCC  Dataset: vd_acq_rls_mcc
-- Table, group by: mcc
-- =============================================================================

-- Средний % эквайринга  → .2%
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(commission_from_ops AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0)), 0)

-- Доля в объёме  → .2%
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(
    SUM(SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))) OVER (),
    0
  )

-- Кол-во ТСП (среднемес.)
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(tsp_cnt AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- Кол-во терминалов (среднемес.)
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(term_cnt AS TEXT)), '') AS NUMERIC), 0))
/ NULLIF(COUNT(DISTINCT NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '')), 0)

-- АУР, руб.
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(aur AS TEXT)), '') AS NUMERIC), 0))

-- Сумма операций
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_sum AS TEXT)), '') AS NUMERIC), 0))

-- Количество операций
SUM(COALESCE(CAST(NULLIF(BTRIM(CAST(trx_cnt AS TEXT)), '') AS NUMERIC), 0))

-- =============================================================================
-- БАННЕР  Dataset: vd_acq_rls_denied_*
-- Chart type: Table. Колонки (не меры): access_message, access_detail
-- =============================================================================
