CREATE TABLE IF NOT EXISTS {acq_slice_hist}
PARTITIONED BY (report_month)
STORED AS PARQUET
AS
SELECT * FROM {acq_slice_load} WHERE 1 = 0
