-- =====================================================
-- ГЕНЕРАТОР ДАННЫХ КСП (только SELECT, без DDL/DML)
-- Для использования в Superset SQL Lab
-- =====================================================

-- Этот запрос генерирует все 3000 записей в памяти
-- Сохраните его как Virtual Dataset в Superset

WITH 
-- Генерируем 3000 строк
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
        TIMESTAMP '2025-09-01 00:00:00' 
            + (FLOOR(RANDOM() * 71) || ' days')::INTERVAL 
            + (FLOOR(RANDOM() * 86400) || ' seconds')::INTERVAL AS load_date
    FROM generate_series(1, 3000) AS n
),
step1 AS (
    SELECT b.*,
        CASE WHEN r_segment < 0.45 THEN 'credit' ELSE 'other' END AS segment
    FROM base b
),
step2 AS (
    SELECT s.*,
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
step3 AS (
    SELECT s.*,
        'ВСП-' || rf_code || '-' || LPAD((1 + FLOOR(r_vsp * 12))::INT::TEXT, 2, '0') AS vsp
    FROM step2 s
),
step4 AS (
    SELECT s.*,
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
        CASE WHEN segment = 'credit' THEN 'daily_credit_app' ELSE 'monthly_active_base' END AS offer_source,
        CASE 
            WHEN r_channel < 0.75 THEN 'Офис'
            WHEN r_channel < 0.90 THEN 'Интернет-банк'
            ELSE 'Контакт-центр'
        END AS sales_channel
    FROM step3 s
),
step5 AS (
    SELECT s.*,
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
        CASE WHEN segment = 'credit' THEN 30 ELSE 14 END AS cooling_off_window_days,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit,
        CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase,
        CASE WHEN segment = 'credit' THEN 0.12 ELSE 0.06 END AS p_return,
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
step6 AS (
    SELECT s.*,
        s.load_date + (s.visit_delay || ' days')::INTERVAL AS potential_visit_date,
        CASE 
            WHEN r_will_visit < p_visit 
                 AND s.load_date + (s.visit_delay || ' days')::INTERVAL <= TIMESTAMP '2025-11-10 23:59:59'
            THEN 1 ELSE 0 
        END AS visited
    FROM step5 s
),
step7 AS (
    SELECT s.*,
        CASE WHEN visited = 1 THEN potential_visit_date ELSE NULL END AS visit_date,
        CASE 
            WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response
            THEN 1 ELSE 0 
        END AS responded
    FROM step6 s
),
step8 AS (
    SELECT s.*,
        CASE 
            WHEN responded = 1 AND r_purchased < (p_purchase / p_response)
            THEN 1 ELSE 0 
        END AS purchased
    FROM step7 s
),
step9 AS (
    SELECT s.*,
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
ksp_data AS (
    SELECT 
        proposal_id,
        client_id,
        segment,
        CASE segment WHEN 'credit' THEN 'кредит' ELSE 'прочие' END AS segment_ru,
        rf,
        vsp,
        primary_product,
        offer_source,
        sales_channel,
        load_date,
        DATE(load_date) AS load_day,
        visited,
        visit_date,
        responded,
        purchased,
        num_ksp_offered,
        num_ksp_purchased,
        CASE 
            WHEN num_ksp_purchased = 0 THEN 0
            WHEN num_ksp_purchased = 1 THEN
                CASE WHEN r_return < p_return THEN 1 ELSE 0 END
            WHEN num_ksp_purchased = 2 THEN
                CASE 
                    WHEN r_return < POWER(1 - p_return, 2) THEN 0
                    WHEN r_return < POWER(1 - p_return, 2) + 2 * p_return * (1 - p_return) THEN 1
                    ELSE 2
                END
            ELSE 0
        END AS num_ksp_returned_cooling,
        cooling_off_window_days,
        CASE WHEN segment = 'other' AND primary_product = 'Вклад' THEN 1 ELSE 0 END AS is_deposit_client
    FROM step9
)
SELECT * FROM ksp_data;