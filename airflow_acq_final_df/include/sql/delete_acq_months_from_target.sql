DELETE FROM {target_acq_fin}
WHERE substring(btrim(cast(report_month AS text)) FROM 1 FOR 7) IN ({expected_months})
   OR substring(btrim(cast(snapshot_month_start AS text)) FROM 1 FOR 7) IN ({expected_months})
