CREATE TABLE {acq_00_months}
AS
-- Окно расчёта: месяцы, в которые попадают последние {acq_prev_month_days} дней до exec_dt (1 или 2 месяца).
with params as (
  select
    trunc(cast('{exec_dt}' as timestamp), 'MM') as cur_month,
    trunc(date_sub(cast('{exec_dt}' as timestamp), {acq_prev_month_days}), 'MM') as window_start
),
candidates as (
  select cur_month as month_ts, window_start from params
  union all
  select months_sub(cur_month, 1) as month_ts, window_start from params
)
select
  cast(month_ts as date) as month_start,
  cast(date_sub(months_add(month_ts, 1), 1) as date) as month_end,
  from_timestamp(month_ts, 'yyyy-MM') as report_month,
  from_timestamp(month_ts, 'yyyyMM') as yearmm
from candidates
where month_ts >= window_start
