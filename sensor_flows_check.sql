-- Почему падает check_new_data: когда каждый поток ODS из сенсора впервые успешно отработал в каждый день.
-- Impala (Hue или Jupyter). Только чтение.
-- Сенсор запуска с Logical date D (запуск D+1 в 12:00) ждёт успешный запуск каждого потока не раньше D+1 00:00
-- и сдаётся через acq_sensor_timeout_seconds (сейчас на сервере 4 часа, то есть в 16:00).

REFRESH ekm.tm_task_run;

-- 1. По дням: первый успешный старт потока. Пусто или позже 16:00 = из-за этого потока сенсор упал.
SELECT
    lower(run_name) AS run_name,
    to_date(cast(run_start_dt AS timestamp)) AS load_day,
    min(CASE WHEN run_status = 'Succeeded' THEN cast(run_start_dt AS timestamp) END) AS first_success,
    group_concat(DISTINCT run_status) AS statuses,
    count(*) AS runs
FROM ekm.tm_task_run
WHERE cast(run_start_dt AS timestamp) >= cast('2026-10-30' AS timestamp)
  AND lower(run_name) IN (
    'app_ods/wf_ods2dl_ora2hive_scd1_agreements',
    'app_ods/wf_ods2dl_ora2hive_scd1_companies',
    'app_ods/wf_ods2dl_ora2hive_scd1_agr_terms',
    'app_ods/wf_ods2dl_ora2hive_scd1_trx',
    'app_ods/wf_ods2dl_ora2hive_scd1_trx_acq',
    'app_ods/wf_ods2dl_ora2hive_scd1_trx_int',
    'app_ods/wf_ods2dl_ora2hive_scd1_base24_fiids',
    'app_ods/wf_ods2dl_ora2hive_scd1_merchants',
    'app_ods/wf_ods2dl_ora2hive_scd1_pos_terminals',
    'app_ods_efr/wf_ods2dl_ora2hive_scd1_mrc_pos_rent',
    'app_ods_cft_d4/wf_ods2dl_ora2hive_scd1_z_r2_ip_merchants',
    'app_ods_cft_d4/wf_ods2dl_ora2hive_scd1_z_r2_ip_dog_oper',
    'app_ods_cft_d4/wf_ods2dl_ora2hive_scd1_z_r2_tariff_plan',
    'app_ods_cft_d1/wf_ods2dl_ora2hive_scd1_z_client',
    'app_ods_cft_d1/wf_ods2dl_ora2hive_scd1_z_branch',
    'app_ods_cft_d1/wf_ods2dl_ora2hive_scd1_z_cl_corp',
    'app_ods_cft_d2/wf_ods2dl_ora2hive_scd1_z_depart'
  )
GROUP BY 1, 2
ORDER BY 1, 2;


-- 2. Потоки, которых в журнале нет совсем за эти дни (опечатка в имени или поток переименовали).
--    Должно вернуть 0 строк.
SELECT n.run_name
FROM (
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_agreements' AS run_name UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_companies' UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_agr_terms' UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_trx' UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_trx_acq' UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_trx_int' UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_base24_fiids' UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_merchants' UNION ALL
    SELECT 'app_ods/wf_ods2dl_ora2hive_scd1_pos_terminals' UNION ALL
    SELECT 'app_ods_efr/wf_ods2dl_ora2hive_scd1_mrc_pos_rent' UNION ALL
    SELECT 'app_ods_cft_d4/wf_ods2dl_ora2hive_scd1_z_r2_ip_merchants' UNION ALL
    SELECT 'app_ods_cft_d4/wf_ods2dl_ora2hive_scd1_z_r2_ip_dog_oper' UNION ALL
    SELECT 'app_ods_cft_d4/wf_ods2dl_ora2hive_scd1_z_r2_tariff_plan' UNION ALL
    SELECT 'app_ods_cft_d1/wf_ods2dl_ora2hive_scd1_z_client' UNION ALL
    SELECT 'app_ods_cft_d1/wf_ods2dl_ora2hive_scd1_z_branch' UNION ALL
    SELECT 'app_ods_cft_d1/wf_ods2dl_ora2hive_scd1_z_cl_corp' UNION ALL
    SELECT 'app_ods_cft_d2/wf_ods2dl_ora2hive_scd1_z_depart'
) n
LEFT JOIN (
    SELECT DISTINCT lower(run_name) AS run_name
    FROM ekm.tm_task_run
    WHERE cast(run_start_dt AS timestamp) >= cast('2026-10-30' AS timestamp)
) t ON t.run_name = n.run_name
WHERE t.run_name IS NULL;
