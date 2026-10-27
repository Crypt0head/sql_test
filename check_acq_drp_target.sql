SELECT CAST(
    CASE
        WHEN (
            SELECT count(*) FROM {target_acq_fin}
            WHERE substring(btrim(cast(report_month AS text)) FROM 1 FOR 7)
                BETWEEN to_char(date '{exec_dt}' - {acq_prev_month_days}, 'YYYY-MM') AND to_char(date '{exec_dt}', 'YYYY-MM')
        ) <> (SELECT count(*) FROM {target_acq_fin_temp})
            THEN 'DQ failed: target rows for loaded months differ from temp'
        ELSE '1'
    END AS integer
) AS dq_ok
