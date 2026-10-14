SELECT CAST(
    CASE
        WHEN (SELECT count(*) FROM {target_acq_fin_temp}) = 0
            THEN 'DQ failed: temp is empty'
        WHEN (
            SELECT count(*) - count(DISTINCT cast(report_month AS text) || '|' || cast(n_agr AS text))
            FROM {target_acq_fin_temp}
        ) <> 0
            THEN 'DQ failed: duplicate n_agr within a month'
        WHEN EXISTS (
            SELECT 1 FROM {target_acq_fin_temp}
            WHERE substring(btrim(cast(report_month AS text)) FROM 1 FOR 7) NOT IN ({expected_months})
        )
            THEN 'DQ failed: rows outside expected months'
        ELSE '1'
    END AS integer
) AS dq_ok
