INSERT OVERWRITE {acq_slice_hist} PARTITION (report_month)
SELECT
    *
FROM {acq_slice_load}
