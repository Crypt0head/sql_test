WITH src AS (
    SELECT
        t.*,
        LEAST(GREATEST(t.trade_points_count, 1), 3) AS tp_adj,
        GREATEST(
            LEAST(GREATEST(t.terminals_count, 1), 4),
            LEAST(GREATEST(t.trade_points_count, 1), 3)
        ) AS term_adj
    FROM tmp_test_equaring_synth_svy t
)
SELECT
    t.period                    AS "Период",
    t.inn                       AS "ИНН",
    t.ogrn                      AS "ОГРН",
    t.cdi_id                    AS "CDI ID",
    t.client_name               AS "Наименование клиента",
    t.contract_number           AS "Номер договора",
    t.contract_start_date       AS "Дата начала договора",
    t.contract_end_date         AS "Дата окончания договора",
    t.branch                    AS "Филиал",

    CASE MOD(ABS(t.cdi_id), 4)
        WHEN 0 THEN 'Алтайский РФ'
        WHEN 1 THEN 'Башкирский РФ'
        WHEN 2 THEN 'Воронежский РФ'
        WHEN 3 THEN 'Камчатский РФ'
    END                         AS "РФ",

    CASE MOD(ABS(t.cdi_id), 4)
        WHEN 0 THEN 'г.Славгород'
        WHEN 1 THEN 'г.Камень-на-Оби'
        WHEN 2 THEN 'г.Бийск'
        WHEN 3 THEN 'г.Новоалтайск'
    END                         AS "ВСП договора",

    t.vsp_code                  AS "Код ВСП",
    t.tariff                    AS "Тариф",
    t.mcc_code                  AS "MCC-код",
    t.tp_adj                    AS "Кол-во торговых точек",
    t.term_adj                  AS "Кол-во терминалов",
    (t.term_adj * 1926)::numeric(12,0)                                      AS "АУР",
    t.amortization::numeric(12,0)                                           AS "Амортизация",
    t.operations_count          AS "Кол-во операций",
    t.operations_sum::numeric(14,0)                                         AS "Сумма операций",
    ROUND(t.avg_acquiring_pct::numeric, 2)                                  AS "Средний процент эквайринга",
    t.commission_from_ops::numeric(12,0)                                    AS "Комиссия с операции (руб.)",
    t.commission_monthly_fee::numeric(12,0)                                 AS "Комиссия (руб./месяц)",
    t.commission_acquiring::numeric(12,0)                                   AS "Комиссия эквайринга (общая)",
    t.noi_acquiring::numeric(12,0)                                          AS "ЧОД эквайринга",
    t.noi_total::numeric(12,0)                                              AS "ЧОД общий",
    t.fin_result::numeric(12,0)                                             AS "Финрез",

    CASE
        WHEN t.seasonality = 'Y' THEN 'Сезонные'
        WHEN t.fin_result_segment IN ('Пограничный', 'Убыточный')
             OR t.months_inefficiency >= 2 THEN 'Низкоактивные'
        ELSE 'Доходные'
    END                         AS "ФР Сегмент",

    CASE
        WHEN t.seasonality != 'Y'
             AND t.fin_result_segment NOT IN ('Пограничный', 'Убыточный')
             AND t.months_inefficiency < 2
        THEN
            CASE MOD(ABS(t.cdi_id), 4)
                WHEN 0 THEN 'Колеблющиеся'
                WHEN 1 THEN 'Растущие_премиум'
                WHEN 2 THEN 'Стабильные-крупные'
                WHEN 3 THEN 'Стабильные-малые'
            END
        ELSE '-'
    END                         AS "Подсегмент",

    REPLACE(t.segment, 'ДМкБ', 'ДМ') AS "ССП",

    CASE
        WHEN t.seasonality = 'Y' THEN
            CASE
                WHEN EXTRACT(MONTH FROM t.period) IN (3, 4, 5)   THEN 'Сезонные-Рост'
                WHEN EXTRACT(MONTH FROM t.period) IN (6, 7, 8)   THEN 'Сезонные-Пик'
                WHEN EXTRACT(MONTH FROM t.period) IN (9, 10, 11) THEN 'Сезонные-Спад'
                ELSE 'Сезонные-Межсезонье'
            END
        ELSE '-'
    END                         AS "Фаза сезонности",

    t.months_inefficiency       AS "Кол-во месяцев неэффективности",

    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        t.client_products,
        'Product10', 'Кредит'),
        'Product9',  'Депозит'),
        'Product8',  'Светофор'),
        'Product7',  'Индив тариф'),
        'Product6',  'Комфортная среда (страхование)'),
        'Product5',  'Правокард'),
        'Product4',  'Эквайринг'),
        'Product3',  'Зарплатный проект'),
        'Product2',  'Корпоративная бизнес-карта'),
        'Product1',  'SMS-информирование')
    AS "Продукты клиента",

    array_to_string(
        ARRAY(
            SELECT unnest(ARRAY[
                'SMS-информирование','Корпоративная бизнес-карта','Зарплатный проект',
                'Эквайринг','Правокард','Комфортная среда (страхование)',
                'Индив тариф','Светофор','Депозит','Кредит'
            ])
            EXCEPT
            SELECT trim(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    x,
                    'Product10', 'Кредит'),
                    'Product9',  'Депозит'),
                    'Product8',  'Светофор'),
                    'Product7',  'Индив тариф'),
                    'Product6',  'Комфортная среда (страхование)'),
                    'Product5',  'Правокард'),
                    'Product4',  'Эквайринг'),
                    'Product3',  'Зарплатный проект'),
                    'Product2',  'Корпоративная бизнес-карта'),
                    'Product1',  'SMS-информирование')
            )
            FROM unnest(string_to_array(t.client_products, ',')) AS x
        ),
        ', '
    ) AS "Рекомендуемые продукты"

FROM src t
