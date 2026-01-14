-- =====================================================
-- Генератор данных КСП для PostgreSQL / Apache Superset
-- =====================================================

-- Создание таблицы
DROP TABLE IF EXISTS ksp_offers CASCADE;

CREATE TABLE ksp_offers (
    proposal_id        VARCHAR(10) PRIMARY KEY,
    client_id          VARCHAR(10),
    segment            VARCHAR(10),
    rf                 VARCHAR(20),
    vsp                VARCHAR(15),
    primary_product    VARCHAR(20),
    offer_source       VARCHAR(25),
    sales_channel      VARCHAR(20),
    load_date          TIMESTAMP,
    visited            SMALLINT,
    visit_date         TIMESTAMP,
    responded          SMALLINT,
    purchased          SMALLINT,
    num_ksp_offered    SMALLINT,
    num_ksp_purchased  SMALLINT,
    num_ksp_returned_cooling SMALLINT,
    cooling_off_window_days  SMALLINT,
    is_deposit_client  SMALLINT
);

-- Генерация и вставка данных
INSERT INTO ksp_offers
WITH 
-- Генерируем 3000 строк через generate_series (эффективнее рекурсии)
base AS (
    SELECT 
        n,
        'P' || LPAD(n::TEXT, 6, '0') AS proposal_id,
        'C' || LPAD((1 + FLOOR(RANDOM() * 200000))::INT::TEXT, 6, '0') AS client_id,
        RANDOM() AS r_segment,
        RANDOM() AS r_region,
        RANDOM() AS r_vsp,
        RANDOM() AS r_product,
        RANDOM() AS r_channel,
        RANDOM() AS r_will_visit,
        RANDOM() AS r_visit_delay,
        RANDOM() AS r_responded,
        RANDOM() AS r_purchased,
        RANDOM() AS r_num_offered,
        RANDOM() AS r_num_purchased,
        RANDOM() AS r_return,
        -- Случайная дата: 2025-09-01 ... 2025-11-10 (70 дней)
        TIMESTAMP '2025-09-01 00:00:00' 
            + (FLOOR(RANDOM() * 71) || ' days')::INTERVAL 
            + (FLOOR(RANDOM() * 86400) || ' seconds')::INTERVAL AS load_date
    FROM generate_series(1, 3000) AS n
),

-- Сегмент: credit 45%, other 55%
step1 AS (
    SELECT 
        b.*,
        CASE WHEN r_segment < 0.45 THEN 'credit' ELSE 'other' END AS segment
    FROM base b
),

-- Регион и код региона
step2 AS (
    SELECT 
        s.*,
        CASE 
            WHEN r_region < 0.20 THEN 'Центральный'
            WHEN r_region < 0.40 THEN 'Северо-Западный'
            WHEN r_region < 0.60 THEN 'Сибирский'
            WHEN r_region < 0.80 THEN 'Приволжский'
            ELSE 'Южный'
        END AS rf,
        CASE 
            WHEN r_region < 0.20 THEN 'ЦЕН'
            WHEN r_region < 0.40 THEN 'СЕВ'
            WHEN r_region < 0.60 THEN 'СИБ'
            WHEN r_region < 0.80 THEN 'ПРИ'
            ELSE 'ЮЖН'
        END AS rf_code
    FROM step1 s
),

-- ВСП (12 точек на регион)
step3 AS (
    SELECT 
        s.*,
        'ВСП-' || rf_code || '-' || LPAD((1 + FLOOR(r_vsp * 12))::INT::TEXT, 2, '0') AS vsp
    FROM step2 s
),

-- Продукт, источник предложения, канал продаж
step4 AS (
    SELECT 
        s.*,
        -- Продукт
        CASE 
            WHEN segment = 'credit' THEN
                CASE WHEN r_product < 0.70 THEN 'Потребкредит' ELSE 'Ипотека' END
            ELSE
                CASE 
                    WHEN r_product < 0.25 THEN 'Дебетовая карта'
                    WHEN r_product < 0.50 THEN 'Кредитная карта'
                    WHEN r_product < 0.75 THEN 'Вклад'
                    ELSE 'Пакет услуг'
                END
        END AS primary_product,
        -- Источник
        CASE WHEN segment = 'credit' THEN 'daily_credit_app' ELSE 'monthly_active_base' END AS offer_source,
        -- Канал: Офис 75%, Интернет-банк 15%, Контакт-центр 10%
        CASE 
            WHEN r_channel < 0.75 THEN 'Офис'
            WHEN r_channel < 0.90 THEN 'Интернет-банк'
            ELSE 'Контакт-центр'
        END AS sales_channel
    FROM step3 s
),

-- Количество предложенных КСП и параметры сегмента
step5 AS (
    SELECT 
        s.*,
        -- num_ksp_offered
        CASE 
            WHEN segment = 'credit' THEN
                CASE 
                    WHEN r_num_offered < 0.15 THEN 0
                    WHEN r_num_offered < 0.70 THEN 1
                    WHEN r_num_offered < 0.90 THEN 2
                    ELSE 3
                END
            ELSE
                CASE 
                    WHEN r_num_offered < 0.25 THEN 0
                    WHEN r_num_offered < 0.75 THEN 1
                    WHEN r_num_offered < 0.93 THEN 2
                    ELSE 3
                END
        END AS num_ksp_offered,
        -- cooling_off_window_days
        CASE WHEN segment = 'credit' THEN 30 ELSE 14 END AS cooling_off_window_days,
        -- Вероятности
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit,
        CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase,
        CASE WHEN segment = 'credit' THEN 0.12 ELSE 0.06 END AS p_return,
        -- Задержка визита (дни)
        CASE 
            WHEN segment = 'credit' THEN
                CASE 
                    WHEN r_visit_delay < 0.12 THEN 0
                    WHEN r_visit_delay < 0.32 THEN 1
                    WHEN r_visit_delay < 0.50 THEN 2
                    WHEN r_visit_delay < 0.64 THEN 3
                    WHEN r_visit_delay < 0.74 THEN 4
                    WHEN r_visit_delay < 0.82 THEN 5
                    WHEN r_visit_delay < 0.89 THEN 6
                    WHEN r_visit_delay < 0.94 THEN 7
                    WHEN r_visit_delay < 0.98 THEN 8
                    ELSE 9
                END
            ELSE
                CASE 
                    WHEN r_visit_delay < 0.25 THEN 0
                    WHEN r_visit_delay < 0.50 THEN 3
                    WHEN r_visit_delay < 0.70 THEN 7
                    WHEN r_visit_delay < 0.85 THEN 14
                    WHEN r_visit_delay < 0.95 THEN 21
                    ELSE 28
                END
        END AS visit_delay
    FROM step4 s
),

-- Визит
step6 AS (
    SELECT 
        s.*,
        s.load_date + (s.visit_delay || ' days')::INTERVAL AS potential_visit_date,
        CASE 
            WHEN r_will_visit < p_visit 
                 AND s.load_date + (s.visit_delay || ' days')::INTERVAL <= TIMESTAMP '2025-11-10 23:59:59'
            THEN 1 ELSE 0 
        END AS visited
    FROM step5 s
),

-- Отклик
step7 AS (
    SELECT 
        s.*,
        CASE WHEN visited = 1 THEN potential_visit_date ELSE NULL END AS visit_date,
        CASE 
            WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response
            THEN 1 ELSE 0 
        END AS responded
    FROM step6 s
),

-- Покупка
step8 AS (
    SELECT 
        s.*,
        CASE 
            WHEN responded = 1 AND r_purchased < (p_purchase / p_response)
            THEN 1 ELSE 0 
        END AS purchased
    FROM step7 s
),

-- Количество купленных
step9 AS (
    SELECT 
        s.*,
        CASE 
            WHEN purchased = 1 THEN
                LEAST(
                    CASE WHEN r_num_purchased < 0.84 THEN 1 ELSE 2 END,
                    num_ksp_offered
                )
            ELSE 0 
        END AS num_ksp_purchased
    FROM step8 s
),

-- Возвраты (биномиальное распределение)
final AS (
    SELECT 
        s.*,
        CASE 
            WHEN num_ksp_purchased = 0 THEN 0
            WHEN num_ksp_purchased = 1 THEN
                CASE WHEN r_return < p_return THEN 1 ELSE 0 END
            WHEN num_ksp_purchased = 2 THEN
                -- P(0) = (1-p)^2, P(1) = 2p(1-p), P(2) = p^2
                CASE 
                    WHEN r_return < POWER(1 - p_return, 2) THEN 0
                    WHEN r_return < POWER(1 - p_return, 2) + 2 * p_return * (1 - p_return) THEN 1
                    ELSE 2
                END
            ELSE 0
        END AS num_ksp_returned_cooling,
        CASE WHEN segment = 'other' AND primary_product = 'Вклад' THEN 1 ELSE 0 END AS is_deposit_client
    FROM step9 s
)

SELECT 
    proposal_id,
    client_id,
    segment,
    rf,
    vsp,
    primary_product,
    offer_source,
    sales_channel,
    load_date,
    visited,
    visit_date,
    responded,
    purchased,
    num_ksp_offered,
    num_ksp_purchased,
    num_ksp_returned_cooling,
    cooling_off_window_days,
    is_deposit_client
FROM final;

-- Создание индексов для быстрых запросов в Superset
CREATE INDEX idx_ksp_load_date ON ksp_offers(load_date);
CREATE INDEX idx_ksp_segment ON ksp_offers(segment);
CREATE INDEX idx_ksp_rf ON ksp_offers(rf);
CREATE INDEX idx_ksp_vsp ON ksp_offers(vsp);
CREATE INDEX idx_ksp_visited ON ksp_offers(visited);
CREATE INDEX idx_ksp_purchased ON ksp_offers(purchased);

-- =====================================================
-- Проверка сгенерированных данных
-- =====================================================

SELECT '=== ОБЩАЯ СТАТИСТИКА ===' AS info;

SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT client_id) AS unique_clients,
    SUM(visited) AS total_visits,
    SUM(responded) AS total_responses,
    SUM(purchased) AS total_purchases,
    SUM(num_ksp_purchased) AS total_ksp_sold,
    SUM(num_ksp_returned_cooling) AS total_returns
FROM ksp_offers;

SELECT '=== ПО СЕГМЕНТАМ ===' AS info;

SELECT 
    segment,
    COUNT(*) AS cnt,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct,
    SUM(visited) AS visits,
    SUM(responded) AS responses,
    SUM(purchased) AS purchases
FROM ksp_offers
GROUP BY segment;

SELECT '=== ПО РЕГИОНАМ ===' AS info;

SELECT 
    rf,
    COUNT(*) AS cnt,
    SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(visited), 0), 1) AS conversion_pct
FROM ksp_offers
GROUP BY rf
ORDER BY cnt DESC;

SELECT '=== ПО ПРОДУКТАМ ===' AS info;

SELECT 
    primary_product,
    COUNT(*) AS cnt,
    SUM(num_ksp_offered) AS offered,
    SUM(num_ksp_purchased) AS sold,
    SUM(num_ksp_returned_cooling) AS returned
FROM ksp_offers
GROUP BY primary_product
ORDER BY cnt DESC;

SELECT '=== ПО КАНАЛАМ ПРОДАЖ ===' AS info;

SELECT 
    sales_channel,
    COUNT(*) AS cnt,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
FROM ksp_offers
GROUP BY sales_channel
ORDER BY cnt DESC;