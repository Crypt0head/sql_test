WITH 
base AS (SELECT n, RANDOM() AS r_segment, RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased, RANDOM() AS r_num_offered,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
                      ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase FROM base),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3)
SELECT 
    COUNT(*) AS loaded,
    SUM(visited) AS visits,
    SUM(responded) AS responses,
    SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(responded)::NUMERIC / NULLIF(SUM(visited), 0), 1) AS cr_response_pct,
    ROUND(100.0 * SUM(purchased)::NUMERIC / NULLIF(SUM(visited), 0), 1) AS cr_purchase_pct
FROM s4;

WITH 
base AS (SELECT n, RANDOM() AS r_segment, RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased, RANDOM() AS r_num_offered,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
                      ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase FROM base),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3)
SELECT * FROM (
    SELECT 1 AS sort_order, 'Загружено' AS stage, COUNT(*)::BIGINT AS value FROM s4
    UNION ALL SELECT 2, 'Визит', SUM(visited)::BIGINT FROM s4
    UNION ALL SELECT 3, 'Отклик', SUM(responded)::BIGINT FROM s4
    UNION ALL SELECT 4, 'Покупка', SUM(purchased)::BIGINT FROM s4
) t ORDER BY sort_order;

WITH 
base AS (SELECT n, RANDOM() AS r_segment, RANDOM() AS r_product, RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased, 
        RANDOM() AS r_num_offered, RANDOM() AS r_num_purchased, RANDOM() AS r_return,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN segment = 'credit' THEN CASE WHEN r_product < 0.7 THEN 'Потребкредит' ELSE 'Ипотека' END
                      ELSE CASE WHEN r_product < 0.25 THEN 'Дебетовая карта' WHEN r_product < 0.5 THEN 'Кредитная карта' WHEN r_product < 0.75 THEN 'Вклад' ELSE 'Пакет услуг' END END AS primary_product,
        CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
             ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase, CASE WHEN segment = 'credit' THEN 0.12 ELSE 0.06 END AS p_return FROM base),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3),
s5 AS (SELECT *, CASE WHEN purchased = 1 THEN LEAST(CASE WHEN r_num_purchased < 0.84 THEN 1 ELSE 2 END, num_ksp_offered) ELSE 0 END AS num_ksp_purchased FROM s4),
s6 AS (SELECT *, CASE WHEN num_ksp_purchased = 0 THEN 0 WHEN num_ksp_purchased = 1 THEN CASE WHEN r_return < p_return THEN 1 ELSE 0 END
                      ELSE CASE WHEN r_return < POWER(1-p_return, 2) THEN 0 WHEN r_return < POWER(1-p_return, 2) + 2*p_return*(1-p_return) THEN 1 ELSE 2 END END AS num_ksp_returned_cooling FROM s5)
SELECT 
    primary_product,
    ROUND(AVG(num_ksp_offered)::NUMERIC, 2) AS avg_offered,
    ROUND(AVG(num_ksp_purchased)::NUMERIC, 2) AS avg_purchased,
    ROUND(100.0 * AVG(purchased)::NUMERIC, 1) AS cr_purchase_pct,
    SUM(num_ksp_returned_cooling)::INT AS returns_count,
    ROUND(100.0 * SUM(num_ksp_returned_cooling)::NUMERIC / NULLIF(SUM(num_ksp_purchased), 0), 1) AS return_rate_pct
FROM s6
GROUP BY primary_product
ORDER BY cr_purchase_pct DESC;

WITH 
base AS (SELECT n, TIMESTAMP '2025-09-01' + (FLOOR(RANDOM() * 71)::INT || ' days')::INTERVAL AS load_date,
        RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased, RANDOM() AS r_num_offered, RANDOM() AS r_visit_delay,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
                      ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase,
        CASE WHEN segment = 'credit' THEN CASE WHEN r_visit_delay < 0.12 THEN 0 WHEN r_visit_delay < 0.32 THEN 1 WHEN r_visit_delay < 0.5 THEN 2 WHEN r_visit_delay < 0.64 THEN 3 WHEN r_visit_delay < 0.74 THEN 4 WHEN r_visit_delay < 0.82 THEN 5 WHEN r_visit_delay < 0.89 THEN 6 WHEN r_visit_delay < 0.94 THEN 7 WHEN r_visit_delay < 0.98 THEN 8 ELSE 9 END
             ELSE CASE WHEN r_visit_delay < 0.25 THEN 0 WHEN r_visit_delay < 0.5 THEN 3 WHEN r_visit_delay < 0.7 THEN 7 WHEN r_visit_delay < 0.85 THEN 14 WHEN r_visit_delay < 0.95 THEN 21 ELSE 28 END END AS visit_delay FROM base),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit AND load_date + (visit_delay || ' days')::INTERVAL <= TIMESTAMP '2025-11-10 23:59:59' THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3)
SELECT 
    load_date::DATE AS load_day,
    COUNT(*) AS loaded,
    SUM(visited) AS visits,
    SUM(responded) AS responses,
    SUM(purchased) AS purchases
FROM s4
WHERE segment = 'credit'
GROUP BY load_date::DATE
ORDER BY load_day;

WITH 
base AS (SELECT n, TIMESTAMP '2025-09-01' + (FLOOR(RANDOM() * 71)::INT || ' days')::INTERVAL AS load_date,
        RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased, RANDOM() AS r_num_offered, RANDOM() AS r_visit_delay,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
                      ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase,
        CASE WHEN segment = 'credit' THEN CASE WHEN r_visit_delay < 0.12 THEN 0 WHEN r_visit_delay < 0.32 THEN 1 WHEN r_visit_delay < 0.5 THEN 2 WHEN r_visit_delay < 0.64 THEN 3 WHEN r_visit_delay < 0.74 THEN 4 WHEN r_visit_delay < 0.82 THEN 5 WHEN r_visit_delay < 0.89 THEN 6 WHEN r_visit_delay < 0.94 THEN 7 WHEN r_visit_delay < 0.98 THEN 8 ELSE 9 END
             ELSE CASE WHEN r_visit_delay < 0.25 THEN 0 WHEN r_visit_delay < 0.5 THEN 3 WHEN r_visit_delay < 0.7 THEN 7 WHEN r_visit_delay < 0.85 THEN 14 WHEN r_visit_delay < 0.95 THEN 21 ELSE 28 END END AS visit_delay FROM base WHERE segment = 'credit'),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit AND load_date + (visit_delay || ' days')::INTERVAL <= TIMESTAMP '2025-11-10 23:59:59' THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3)
SELECT load_day, stage, sort_order, quantity FROM (
    SELECT load_date::DATE AS load_day, 'Загружено' AS stage, 1 AS sort_order, COUNT(*)::BIGINT AS quantity FROM s4 GROUP BY load_date::DATE
    UNION ALL SELECT load_date::DATE, 'Визит', 2, SUM(visited)::BIGINT FROM s4 GROUP BY load_date::DATE
    UNION ALL SELECT load_date::DATE, 'Отклик', 3, SUM(responded)::BIGINT FROM s4 GROUP BY load_date::DATE
    UNION ALL SELECT load_date::DATE, 'Покупка', 4, SUM(purchased)::BIGINT FROM s4 GROUP BY load_date::DATE
) t ORDER BY load_day, sort_order;

WITH 
base AS (SELECT n, RANDOM() AS r_region, RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased, RANDOM() AS r_num_offered,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN r_region < 0.2 THEN 'Центральный' WHEN r_region < 0.4 THEN 'Северо-Западный' WHEN r_region < 0.6 THEN 'Сибирский' WHEN r_region < 0.8 THEN 'Приволжский' ELSE 'Южный' END AS rf,
        CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
             ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase FROM base),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3)
SELECT rf AS region, COUNT(*) AS total_count, SUM(visited) AS visits, SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(purchased)::NUMERIC / NULLIF(SUM(visited), 0), 1) AS cr_pct
FROM s4 GROUP BY rf ORDER BY total_count DESC;

WITH 
base AS (SELECT n, RANDOM() AS r_channel FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN r_channel < 0.75 THEN 'Офис' WHEN r_channel < 0.9 THEN 'Интернет-банк' ELSE 'Контакт-центр' END AS sales_channel FROM base)
SELECT sales_channel, COUNT(*) AS total_count, ROUND(100.0 * COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER(), 1) AS share_pct
FROM s1 GROUP BY sales_channel ORDER BY total_count DESC;

WITH 
base AS (SELECT n, RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased, RANDOM() AS r_num_offered,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
                      ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase FROM base),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3)
SELECT CASE segment WHEN 'credit' THEN 'Кредит' ELSE 'Прочие' END AS segment_name,
    COUNT(*) AS loaded, SUM(visited) AS visits, SUM(responded) AS responses, SUM(purchased) AS purchases,
    ROUND(100.0 * SUM(responded)::NUMERIC / NULLIF(SUM(visited), 0), 1) AS cr_response_pct,
    ROUND(100.0 * SUM(purchased)::NUMERIC / NULLIF(SUM(visited), 0), 1) AS cr_purchase_pct
FROM s4 GROUP BY segment;

WITH 
base AS (SELECT n, 'C' || LPAD((1 + FLOOR(RANDOM() * 200000))::INT::TEXT, 6, '0') AS client_id,
        RANDOM() AS r_product, RANDOM() AS r_will_visit, RANDOM() AS r_responded, RANDOM() AS r_purchased,
        RANDOM() AS r_num_offered, RANDOM() AS r_num_purchased, RANDOM() AS r_return, RANDOM() AS r_ksp,
        CASE WHEN RANDOM() < 0.45 THEN 'credit' ELSE 'other' END AS segment FROM generate_series(1, 3000) AS n),
s1 AS (SELECT *, 
        CASE WHEN segment = 'credit' THEN CASE WHEN r_product < 0.7 THEN 'Потребкредит' ELSE 'Ипотека' END
             ELSE CASE WHEN r_product < 0.25 THEN 'Дебетовая карта' WHEN r_product < 0.5 THEN 'Кредитная карта' WHEN r_product < 0.75 THEN 'Вклад' ELSE 'Пакет услуг' END END AS primary_product,
        CASE WHEN segment = 'credit' THEN CASE WHEN r_num_offered < 0.15 THEN 0 WHEN r_num_offered < 0.7 THEN 1 WHEN r_num_offered < 0.9 THEN 2 ELSE 3 END
             ELSE CASE WHEN r_num_offered < 0.25 THEN 0 WHEN r_num_offered < 0.75 THEN 1 WHEN r_num_offered < 0.93 THEN 2 ELSE 3 END END AS num_ksp_offered,
        CASE WHEN segment = 'credit' THEN 0.68 ELSE 0.28 END AS p_visit, CASE WHEN segment = 'credit' THEN 0.55 ELSE 0.30 END AS p_response,
        CASE WHEN segment = 'credit' THEN 0.37 ELSE 0.17 END AS p_purchase, CASE WHEN segment = 'credit' THEN 0.12 ELSE 0.06 END AS p_return,
        CASE WHEN segment = 'other' AND r_product >= 0.5 AND r_product < 0.75 THEN 1 ELSE 0 END AS is_deposit_client FROM base),
s2 AS (SELECT *, CASE WHEN r_will_visit < p_visit THEN 1 ELSE 0 END AS visited FROM s1),
s3 AS (SELECT *, CASE WHEN visited = 1 AND num_ksp_offered > 0 AND r_responded < p_response THEN 1 ELSE 0 END AS responded FROM s2),
s4 AS (SELECT *, CASE WHEN responded = 1 AND r_purchased < (p_purchase / p_response) THEN 1 ELSE 0 END AS purchased FROM s3),
s5 AS (SELECT *, CASE WHEN purchased = 1 THEN LEAST(CASE WHEN r_num_purchased < 0.84 THEN 1 ELSE 2 END, num_ksp_offered) ELSE 0 END AS num_ksp_purchased FROM s4),
s6 AS (SELECT *, CASE WHEN num_ksp_purchased = 0 THEN 0 WHEN num_ksp_purchased = 1 THEN CASE WHEN r_return < p_return THEN 1 ELSE 0 END
                      ELSE CASE WHEN r_return < POWER(1-p_return, 2) THEN 0 WHEN r_return < POWER(1-p_return, 2) + 2*p_return*(1-p_return) THEN 1 ELSE 2 END END AS num_ksp_returned_cooling FROM s5),
ksp_catalog AS (
    SELECT * FROM (VALUES (1,'Страхование жизни заемщика',5,1),(2,'Защита от потери работы',3,1),(3,'Страхование титула',4,1),(4,'Страхование квартиры',3,2),
        (5,'Страхование дома',1,1),(6,'Страхование от несчастных случаев',2,1),(7,'Страхование путешественников',1,2),(8,'ДМС',1,2),
        (9,'Доктор онлайн/телемедицина',1,2),(10,'Защита карт и счетов',1,4),(11,'Страхование держателей кредитных карт',1,1),
        (12,'КАСКО',1,1),(13,'ОСАГО',1,1),(14,'Фарма-страхование',1,1),(15,'Юр./мед. поддержка',1,1)
    ) AS t(ksp_id, ksp_name, weight_credit, weight_other)
),
expanded AS (
    SELECT d.client_id, d.segment, d.is_deposit_client, d.num_ksp_purchased, d.num_ksp_returned_cooling, k.ksp_name,
        ROW_NUMBER() OVER (PARTITION BY d.n ORDER BY (CASE WHEN d.segment = 'credit' THEN k.weight_credit ELSE k.weight_other END) DESC, RANDOM()) AS rn
    FROM s6 d CROSS JOIN ksp_catalog k WHERE d.num_ksp_offered > 0
),
exp_filtered AS (SELECT * FROM expanded WHERE rn <= 3),
bases AS (
    SELECT COUNT(DISTINCT client_id) AS total_clients,
        COUNT(DISTINCT CASE WHEN segment = 'credit' THEN client_id END) AS credit_clients,
        COUNT(DISTINCT CASE WHEN is_deposit_client = 1 THEN client_id END) AS deposit_clients,
        GREATEST(COUNT(DISTINCT CASE WHEN segment = 'other' AND is_deposit_client = 0 THEN client_id END), 1) AS other_clients
    FROM s6
),
metrics AS (
    SELECT ksp_name, COUNT(DISTINCT client_id) AS clients_all,
        COUNT(DISTINCT CASE WHEN segment = 'credit' THEN client_id END) AS clients_credit,
        COUNT(DISTINCT CASE WHEN is_deposit_client = 1 THEN client_id END) AS clients_deposit,
        COUNT(DISTINCT CASE WHEN segment = 'other' AND is_deposit_client = 0 THEN client_id END) AS clients_other,
        SUM(CASE WHEN rn <= num_ksp_returned_cooling THEN 1 ELSE 0 END) AS returns_total
    FROM exp_filtered GROUP BY ksp_name
)
SELECT m.ksp_name,
    ROUND(100.0 * m.clients_credit::NUMERIC / b.credit_clients, 1) AS penetration_credit_pct,
    ROUND(100.0 * m.clients_deposit::NUMERIC / NULLIF(b.deposit_clients, 0), 1) AS penetration_deposit_pct,
    ROUND(100.0 * m.clients_other::NUMERIC / b.other_clients, 1) AS penetration_other_pct,
    m.returns_total
FROM metrics m CROSS JOIN bases b ORDER BY m.ksp_name;