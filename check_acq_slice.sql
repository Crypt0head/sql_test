-- Выполняется в Hive: assert_true роняет запрос, а в Impala cast('текст' as int) ошибки не даёт.
-- По каждому месяцу окна: срез не пустой, договор в месяце один раз, строк столько же, сколько в периметре,
-- и обязательные части (операции, CDI, ЦФТ, MCC) присоединились хотя бы к одному договору.
SELECT assert_true(
    coalesce(s.n_rows, 0) > 0
    AND s.n_rows = s.n_agrs
    AND s.n_rows = coalesce(p.n_rows, 0)
    AND s.n_with_trx > 0
    AND s.n_with_cdi > 0
    AND s.n_with_cft > 0
    AND s.n_with_mcc > 0
) AS dq_ok
FROM {acq_00_months} m
LEFT JOIN (
    SELECT
        report_month,
        count(*) AS n_rows,
        count(DISTINCT n_agr) AS n_agrs,
        count(trx_cnt) AS n_with_trx,
        count(cdi_id) AS n_with_cdi,
        count(cft_id) AS n_with_cft,
        count(mcc) AS n_with_mcc
    FROM {acq_slice_load}
    GROUP BY report_month
) s ON s.report_month = m.report_month
LEFT JOIN (
    SELECT report_month, count(*) AS n_rows
    FROM {acq_01_sa_perimeter}
    GROUP BY report_month
) p ON p.report_month = m.report_month
