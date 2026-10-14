INSERT INTO {target_acq_fin} ({insert_cols})
SELECT {select_cols}
FROM {target_acq_fin_temp}
