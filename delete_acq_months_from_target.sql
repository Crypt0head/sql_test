DELETE FROM {target_acq_fin}
WHERE substring(btrim(cast(report_month AS text)) FROM 1 FOR 7)
        BETWEEN to_char(date '{exec_dt}' - {acq_prev_month_days}, 'YYYY-MM') AND to_char(date '{exec_dt}', 'YYYY-MM')
   OR substring(btrim(cast(snapshot_month_start AS text)) FROM 1 FOR 7)
        BETWEEN to_char(date '{exec_dt}' - {acq_prev_month_days}, 'YYYY-MM') AND to_char(date '{exec_dt}', 'YYYY-MM')
