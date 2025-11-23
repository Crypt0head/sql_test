WITH LAST_TRX_CFT_ID_RN AS (
        SELECT cft_id
        FROM corpcard_trx_temp
),
LAST_TRX AS (
        SELECT cft_id
        FROM LAST_TRX_CFT_ID_RN
),
AGG_TRX_DT1 AS (
        SELECT cft_id
        FROM corpcard_trx_temp_dt1
),
AGG_TRX_DT0 AS (
        SELECT cft_id
        FROM corpcard_trx_temp_dt0

),
CLIENT_OSTATOK_ALL AS (
        SELECT cft_id
        FROM corpcard_ostatok co
        INNER JOIN z_ac_fin a ON co.collection_id = a.c_arc_move
        INNER JOIN z_rko r ON a.id = r.c_account
),
CLIENT_OSTATOK_TODAY AS (
        SELECT cft_id
        FROM CLIENT_OSTATOK_ALL
),
AVG_OST AS (
        SELECT cft_id
        FROM CLIENT_OSTATOK_ALL
),
TRX_VID_WITH_LAG AS (
        SELECT *
        FROM corpcard_trx_temp_dt1 tv
),
TRX_VID_WITH_DIFF AS (
        SELECT *
        FROM TRX_VID_WITH_LAG tvwl
),
AVG_VID AS (
        SELECT cft_id
        FROM TRX_VID_WITH_DIFF
),
TRX_POST_WITH_LAG AS (
        SELECT *
        FROM corpcard_trx_temp_dt0 tp
),
TRX_POST_WITH_DIFF AS (
        SELECT *
        FROM TRX_POST_WITH_LAG tpwl
),
AVG_POST AS (
        SELECT cft_id
        FROM TRX_POST_WITH_DIFF
),
ACTIVE_CLIENTS AS (
        SELECT c.id AS cft_id
        FROM z_rko r 
        INNER JOIN z_product p ON p.id = r.id
        INNER JOIN z_client c ON c.id = r.c_client
        INNER JOIN z_branch b ON b.id = p.c_filial
        INNER JOIN z_ac_fin a ON a.id = r.c_account
        INNER JOIN z_rko_types rt ON rt.id = r.c_rko_type
),
DISTINCT_CLIENTS AS (
        SELECT DISTINCT cft_id
        FROM ACTIVE_CLIENTS
),
TO_INSERT AS (
        SELECT dc.cft_id
        FROM DISTINCT_CLIENTS dc
        LEFT JOIN LAST_TRX lt ON lt.cft_id = dc.cft_id
        LEFT JOIN AGG_TRX_DT1 a720_dt1 ON a720_dt1.cft_id = dc.cft_id
        LEFT JOIN AGG_TRX_DT0 a720_dt0 ON a720_dt0.cft_id = dc.cft_id
        LEFT JOIN AVG_OST ao ON ao.cft_id = dc.cft_id
        LEFT JOIN CLIENT_OSTATOK_TODAY cot ON cot.cft_id = dc.cft_id
        LEFT JOIN AVG_VID av ON av.cft_id = dc.cft_id
        LEFT JOIN AVG_POST ap ON ap.cft_id = dc.cft_id
)
INSERT INTO corpcard_trx_fetures_impala_temp
SELECT * 
FROM TO_INSERT