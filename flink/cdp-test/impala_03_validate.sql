-- Flink Job 실행 후 결과 확인 (Impala)

USE kdap;

REFRESH ltas_5min_flink;
REFRESH sgi_5min_flink;
REFRESH sgi_5min_flink_v2;
REFRESH mdt_5min_flink;

-- LTAS 5분 집계 결과
SELECT 'ltas_5min_flink' AS tbl, window_start, window_end, bts_alt_key, region_cd, total_val, record_cnt
FROM ltas_5min_flink
ORDER BY window_start DESC
LIMIT 10;

-- SGi v1 결과
SELECT 'sgi_5min_flink' AS tbl, window_start, window_end, bts_alt_key, region_cd, total_val, record_cnt
FROM sgi_5min_flink
ORDER BY window_start DESC
LIMIT 10;

-- SGi v2 결과
SELECT 'sgi_5min_flink_v2' AS tbl, window_start, window_end, bts_alt_key, total_val, record_cnt
FROM sgi_5min_flink_v2
ORDER BY window_start DESC
LIMIT 10;

-- MDT 결과
SELECT 'mdt_5min_flink' AS tbl, window_start, window_end, bts_alt_key, record_cnt
FROM mdt_5min_flink
ORDER BY window_start DESC
LIMIT 10;

-- 소스 건수 (분=45 필터 대상)
SELECT 'cdr_sgi_raw' AS tbl, COUNT(*) AS cnt FROM cdr_sgi_raw WHERE SUBSTR(rqt_st_dt, 11, 2) = '45';
SELECT 'cdr_mdt_smsng_raw' AS tbl, COUNT(*) AS cnt FROM cdr_mdt_smsng_raw WHERE SUBSTR(rqt_st_dt, 11, 2) = '45';
