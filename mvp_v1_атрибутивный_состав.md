# MVP v1 — атрибутивный состав (для Confluence)

| Атрибут | Таблица источник | Поле в таблице | Расчетный показатель (Y/N) |
|---|---|---|---|
| Филиал (РФ) | `ods.scd1_z_branch` | `c_shortlabel` | N |
| ИНН | `ods_alpha.scd1_companies` | `c_inn` | N |
| ОГРН | `ods.scd1_z_cl_corp` | `c_register_gos_reg_num_rec` | N |
| Наименование клиента | `ods_alpha.scd1_trx` | `c_mrc_name` | N |
| ССП |  |  | N |
| CDI ID | `ods_alpha.scd1_agreements` | `abs_agr_id` | N |
| Номер договора | `ods_alpha.scd1_agreements` | `c_agr_number` | N |
| Дата начала договора | `ods_alpha.scd1_agreements` | `d_valid_from` | N |
| Дата окончания договора | `ods_alpha.scd1_agreements` | `d_valid_to` | N |
| ВСП договора | `ods.scd1_z_depart` | `c_name` | N |
| Код ВСП | `ods.scd1_z_depart` | `c_code` | N |
| Тариф | `ods_alpha.scd1_trx_acq` | `acq_tar_id` | N |
| Кол-во торговых точек | `{merch_stats}` | `cnt_retl` | Y |
| Кол-во терминалов | `{merch_stats}` | `cnt_term` | Y |
| АУР | SQL-расчет | `cnt_term * 1926` | Y |
| Амортизация | Excel (внешний источник) |  | N |
| Кол-во операций | `{agg_trx_tbl}` | `cnt_trx` | Y |
| Сумма операций | `{agg_trx_tbl}` | `sum_trx_amt` | Y |
| Комиссия (% с операции) | `{agg_trx_tbl}` | `sum_tax_acq` | Y |
| Комиссия (руб./месяц) |  |  | Y |
| Комиссия эквайринга (общая) | SQL-расчет | `Комиссия(%)+Комиссия(руб./мес)` | Y |
| ЧОД ТЭ | SQL-расчет |  | Y |
| Количество клиентов | SQL-агрегация | `count(distinct inn)` | Y |
| Клиенты с положительным ФР | SQL-агрегация |  | Y |
| Клиенты с отрицательным ФР | SQL-агрегация |  | Y |
| Количество активных терминалов | SQL-расчет | `active_90d` | Y |
| Количество неактивных терминалов | SQL-расчет | `inactive_90d` | Y |
| ФР итого, руб. | SQL-расчет |  | Y |
| ФР положительный | SQL-расчет |  | Y |
| ФР отрицательный | SQL-расчет |  | Y |
| Среднее кол-во активных терминалов | SQL-расчет |  | Y |
| Средний ЧОД на терминал | SQL-расчет |  | Y |
| ДОА активных терминалов | SQL-расчет |  | Y |
| Средний ЧОД, руб | SQL-расчет |  | Y |

