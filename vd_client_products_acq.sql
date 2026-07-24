-- Virtual dataset: sbx_da.vd_client_products_acq (create in Superset as Virtual dataset)
-- Grain: 1 row = 1 inn x report_month
-- Purpose: table "Продукты и рекомендации"
--   ИНН | Наименование | Продукты клиента | Рекомендованные продукты
--
-- Source: sbx_da.tmp_shestopalov_acq_datamart_jan_jun
-- Native filters can target: report_month, inn, company_name
-- (add filial_filter / ssp_ocrm_filter if you need RF/zone on this table too)

WITH base AS (
    SELECT
        NULLIF(SUBSTRING(BTRIM(CAST(report_month AS TEXT)) FROM 1 FOR 7), '') AS report_month,
        NULLIF(BTRIM(CAST(inn AS TEXT)), '') AS inn,
        NULLIF(BTRIM(CAST(company_name AS TEXT)), '') AS company_name,
        NULLIF(BTRIM(CAST(recommended_products AS TEXT)), '') AS recommended_products,
        COALESCE(CAST(NULLIF(BTRIM(CAST(rko AS TEXT)), '') AS NUMERIC), 0) AS rko,
        COALESCE(CAST(NULLIF(BTRIM(CAST(accounting AS TEXT)), '') AS NUMERIC), 0) AS accounting,
        COALESCE(CAST(NULLIF(BTRIM(CAST(business_cards AS TEXT)), '') AS NUMERIC), 0) AS business_cards,
        COALESCE(CAST(NULLIF(BTRIM(CAST(credit AS TEXT)), '') AS NUMERIC), 0) AS credit,
        COALESCE(CAST(NULLIF(BTRIM(CAST(dbo AS TEXT)), '') AS NUMERIC), 0) AS dbo,
        COALESCE(CAST(NULLIF(BTRIM(CAST(deposit AS TEXT)), '') AS NUMERIC), 0) AS deposit,
        COALESCE(CAST(NULLIF(BTRIM(CAST(insurance AS TEXT)), '') AS NUMERIC), 0) AS insurance,
        COALESCE(CAST(NULLIF(BTRIM(CAST(loyalty_program AS TEXT)), '') AS NUMERIC), 0) AS loyalty_program,
        COALESCE(CAST(NULLIF(BTRIM(CAST(nmo AS TEXT)), '') AS NUMERIC), 0) AS nmo,
        COALESCE(CAST(NULLIF(BTRIM(CAST(nso AS TEXT)), '') AS NUMERIC), 0) AS nso,
        COALESCE(CAST(NULLIF(BTRIM(CAST(pravocard AS TEXT)), '') AS NUMERIC), 0) AS pravocard,
        COALESCE(CAST(NULLIF(BTRIM(CAST(salary_project AS TEXT)), '') AS NUMERIC), 0) AS salary_project,
        COALESCE(CAST(NULLIF(BTRIM(CAST(self_inkass AS TEXT)), '') AS NUMERIC), 0) AS self_inkass,
        COALESCE(CAST(NULLIF(BTRIM(CAST(service_package AS TEXT)), '') AS NUMERIC), 0) AS service_package,
        COALESCE(CAST(NULLIF(BTRIM(CAST(sms_info AS TEXT)), '') AS NUMERIC), 0) AS sms_info,
        COALESCE(
            NULLIF(NULLIF(BTRIM(CAST(filial_rf AS TEXT)), ''), '<NULL>'),
            'Нет информации'
        ) AS filial_filter,
        COALESCE(
            NULLIF(NULLIF(BTRIM(CAST(ssp_ocrm AS TEXT)), ''), '<NULL>'),
            'Нет данных'
        ) AS ssp_ocrm_filter,
        NULLIF(BTRIM(CAST(tariff_short AS TEXT)), '') AS tariff_short
    FROM sbx_da.tmp_shestopalov_acq_datamart_jan_jun
    WHERE NULLIF(BTRIM(CAST(inn AS TEXT)), '') IS NOT NULL
      AND NULLIF(BTRIM(CAST(report_month AS TEXT)), '') IS NOT NULL
),
labeled AS (
    SELECT
        report_month,
        inn,
        company_name,
        recommended_products,
        filial_filter,
        ssp_ocrm_filter,
        tariff_short,
        CONCAT_WS(
            ', ',
            CASE WHEN rko = 1 THEN 'РКО' END,
            CASE WHEN accounting = 1 THEN 'Бухгалтерия' END,
            CASE WHEN business_cards = 1 THEN 'Бизнес-карта' END,
            CASE WHEN credit = 1 THEN 'Кредиты' END,
            CASE WHEN dbo = 1 THEN 'ДБО' END,
            CASE WHEN deposit = 1 THEN 'Депозиты' END,
            CASE WHEN insurance = 1 THEN 'Страхование' END,
            CASE WHEN loyalty_program = 1 THEN 'Программа лояльности' END,
            CASE WHEN nmo = 1 THEN 'МНО' END,
            CASE WHEN nso = 1 THEN 'НСО' END,
            CASE WHEN pravocard = 1 THEN 'Правокард' END,
            CASE WHEN salary_project = 1 THEN 'Зарплатный проект' END,
            CASE WHEN self_inkass = 1 THEN 'Самоинкассация' END,
            CASE WHEN service_package = 1 THEN 'Сервис пэкэдж' END,
            CASE WHEN sms_info = 1 THEN 'СМС-инфо' END
        ) AS client_products
    FROM base
)
SELECT
    report_month,
    inn,
    MAX(company_name) AS company_name,
    MAX(NULLIF(BTRIM(client_products), '')) AS client_products,
    MAX(recommended_products) AS recommended_products,
    MAX(filial_filter) AS filial_filter,
    MAX(ssp_ocrm_filter) AS ssp_ocrm_filter,
    MAX(tariff_short) AS tariff_short
FROM labeled
GROUP BY
    report_month,
    inn
;
