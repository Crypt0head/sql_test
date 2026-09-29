-- Задача 12554. Актуальные клиенты продукта «Торговый эквайринг».
--
-- Зерно: один договор SA за отчётный месяц. Периметр = секция 01 final_df.
-- Договор входит в месяц, если:
--   d_valid_from <= month_end
--   и (d_valid_to is null или d_valid_to >= month_start)
-- и есть неудалённая привязка agr_terms с cf_ter_type = 'P', которая пересекает месяц:
--   d_valid_from <= month_end
--   и (d_valid_to is null или d_valid_to > month_start)
--
-- СБП (acq_class = QP) не входит. Оборот и установленный терминал для входа не нужны.
--
-- Откуда поля (та же цепочка, что в витрине, секции 01 / 02 / 03 / 06):
--   ИНН              ods_alpha.scd1_companies.c_inn
--   ЦФТ ID           ИНН → ocrm_ul.s_org_ext → cdiul.ext_id_org (OCRM) → cdiul.ext_id_org (CFT)
--   Номер договора   ods_alpha.scd1_agreements.c_agr_number
--   Дата открытия    agreements.d_valid_from
--   Дата закрытия    agreements.d_valid_to (NULL, если договор ещё открыт)
--   РФ               ЦФТ → ods.scd1_z_r2_ip_merchants → z_depart → z_branch.c_shortlabel
--                    В SQL это сырой c_shortlabel.
--                    Вид «Чувашский РФ» делает тетрадка
--                    trade_acquiring_clients_month_excel.ipynb функцией normalize_filial_rf,
--                    как в final_df. lower()/translate() в Impala портят кириллицу, в запросе их нет.
--
-- Класс SA — торговый эквайринг. QP (СБП) в выборку не входит.
-- Клиент без CDI/ЦФТ остаётся в результате: обязательные поля будут NULL.
-- Если у одного CDI несколько ЦФТ, берётся тот, у которого заполнен РФ.
--
-- Impala. Месяц задаётся в CTE params ниже. Excel за выбранный месяц пишет тетрадка
-- Проверки в пространстве/trade_acquiring_clients_month_excel.ipynb
-- При таймауте см. комментарий внизу файла.

WITH params AS (
  -- Отчётный месяц, как month_start / month_end в final_df. Сейчас — текущий месяц.
  SELECT
    cast(trunc(now(), 'MM') AS date) AS month_start,
    cast(last_day(now()) AS date) AS month_end
  -- Май 2026:
  -- SELECT cast('2026-05-01' AS date) AS month_start, cast('2026-05-31' AS date) AS month_end
),

sa_contracts AS (
  SELECT DISTINCT
    cast(a.n_agr AS string) AS n_agr,
    trim(cast(a.c_agr_number AS string)) AS contract_number,
    cast(a.d_valid_from AS date) AS open_dt,
    cast(a.d_valid_to AS date) AS close_dt,
    regexp_replace(trim(cast(c.c_inn AS string)), '[^0-9]', '') AS inn
  FROM ods_alpha.scd1_agreements a
  JOIN ods_alpha.scd1_companies c
    ON c.n_cmp = a.n_cmp_client
  WHERE upper(trim(cast(a.acq_class AS string))) = 'SA'
    AND cast(a.d_valid_from AS date) <= (SELECT month_end FROM params)
    AND (
      a.d_valid_to IS NULL
      OR cast(a.d_valid_to AS date) >= (SELECT month_start FROM params)
    )
    AND coalesce(a.ods_deleted_flg, '0') <> '1'
    AND coalesce(c.ods_deleted_flg, '0') <> '1'
    AND c.c_inn IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM ods_alpha.scd1_agr_terms t
      WHERE cast(t.n_agr AS string) = cast(a.n_agr AS string)
        AND cast(t.d_valid_from AS date) <= (SELECT month_end FROM params)
        AND (
          t.d_valid_to IS NULL
          OR cast(t.d_valid_to AS date) > (SELECT month_start FROM params)
        )
        AND upper(trim(cast(t.cf_ter_type AS string))) = 'P'
        AND coalesce(t.ods_deleted_flg, '0') <> '1'
    )
),

sa_inns AS (
  SELECT DISTINCT inn
  FROM sa_contracts
  WHERE inn IS NOT NULL
    AND inn <> ''
),

ocrm_ranked AS (
  SELECT
    i.inn,
    cast(soe.row_id AS string) AS row_id,
    row_number() OVER (
      PARTITION BY i.inn
      ORDER BY cast(soe.created AS timestamp) DESC, cast(soe.row_id AS string) DESC
    ) AS rn
  FROM sa_inns i
  JOIN ocrm_ul.s_org_ext soe
    ON regexp_replace(trim(cast(soe.x_inn AS string)), '[^0-9]', '') = i.inn
  WHERE coalesce(soe.x_removed_flg, 'N') = 'N'
    AND coalesce(soe.x_duplicate_flg, 'N') = 'N'
),

ocrm_one AS (
  SELECT inn, row_id
  FROM ocrm_ranked
  WHERE rn = 1
),

cdi_ranked AS (
  SELECT
    o.inn,
    cast(e.party_id AS string) AS cdi_id,
    row_number() OVER (
      PARTITION BY o.inn
      ORDER BY
        CASE WHEN e.party_id IS NULL THEN 1 ELSE 0 END,
        cast(e.party_id AS string) DESC
    ) AS rn
  FROM ocrm_one o
  LEFT JOIN cdiul.ext_id_org e
    ON cast(e.cmo_ext_party_source_id AS string) = o.row_id
   AND upper(cast(e.cmo_ext_source_system AS string)) LIKE 'OCRM%'
),

cdi_one AS (
  SELECT inn, cdi_id
  FROM cdi_ranked
  WHERE rn = 1
    AND cdi_id IS NOT NULL
    AND cdi_id <> ''
),

cft_links AS (
  SELECT
    c.inn,
    c.cdi_id,
    cast(e.cmo_ext_party_source_id AS string) AS cft_id
  FROM cdi_one c
  JOIN cdiul.ext_id_org e
    ON cast(e.party_id AS string) = c.cdi_id
   AND upper(cast(e.cmo_ext_source_system AS string)) LIKE 'CFT%'
  WHERE e.cmo_ext_party_source_id IS NOT NULL
),

cft_ids AS (
  SELECT DISTINCT cft_id
  FROM cft_links
  WHERE cft_id IS NOT NULL
    AND cft_id <> ''
),

r2_ranked AS (
  SELECT
    cast(m.c_cl_org AS string) AS cft_id,
    m.c_depart AS c_depart_raw,
    row_number() OVER (
      PARTITION BY cast(m.c_cl_org AS string)
      ORDER BY
        CASE WHEN m.c_tariff_plan IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN m.c_depart IS NOT NULL THEN 0 ELSE 1 END,
        m.id DESC
    ) AS rn
  FROM ods.scd1_z_r2_ip_merchants m
  JOIN cft_ids ids
    ON cast(m.c_cl_org AS string) = ids.cft_id
  WHERE m.c_cl_org IS NOT NULL
    AND coalesce(m.ods_deleted_flg, '0') <> '1'
),

r2_one AS (
  SELECT
    r2.cft_id,
    nullif(trim(cast(br.c_shortlabel AS string)), '') AS filial_rf_raw
  FROM r2_ranked r2
  LEFT JOIN ods.scd1_z_depart dep
    ON dep.id = r2.c_depart_raw
  LEFT JOIN ods.scd1_z_branch br
    ON br.id = dep.c_filial
  WHERE r2.rn = 1
),

contract_cft AS (
  SELECT
    s.n_agr,
    s.contract_number,
    s.open_dt,
    s.close_dt,
    s.inn,
    l.cft_id,
    r.filial_rf_raw,
    row_number() OVER (
      PARTITION BY s.n_agr
      ORDER BY
        CASE WHEN r.filial_rf_raw IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN l.cft_id IS NOT NULL AND l.cft_id <> '' THEN 0 ELSE 1 END,
        l.cft_id
    ) AS rn
  FROM sa_contracts s
  LEFT JOIN cft_links l
    ON l.inn = s.inn
  LEFT JOIN r2_one r
    ON r.cft_id = l.cft_id
  WHERE s.inn IS NOT NULL
    AND s.inn <> ''
)
SELECT
  inn AS `ИНН`,
  cft_id AS `ЦФТ ID`,
  nullif(contract_number, '') AS `Номер договора`,
  open_dt AS `Дата открытия договора`,
  close_dt AS `Дата закрытия договора`,
  filial_rf_raw AS `РФ, в котором обслуживается клиент`
FROM contract_cft
WHERE rn = 1
ORDER BY 6, 1, 3
;


-- Если запрос упирается в таймаут, режь по CTE и гоняй тремя шагами:
--   1) sa_contracts
--   2) cft_links от списка ИНН
--   3) r2_one от списка ЦФТ и финальный select
--
-- Контроль после прогона:
--   count(*)                         — договоры
--   count(distinct `ИНН`)            — клиенты
--   count(`ЦФТ ID`) / count(*)       — заполненность ЦФТ
--   count(`РФ, в котором обслуживается клиент`) / count(*)  — заполненность сырого филиала
-- В Excel колонку РФ приводит normalize_filial_rf (тетрадка выгрузки).
-- Договор из периметра месяца: дата закрытия пустая либо >= month_start.
-- Привязка P пересекает месяц строже: d_valid_to пустая либо > month_start.
