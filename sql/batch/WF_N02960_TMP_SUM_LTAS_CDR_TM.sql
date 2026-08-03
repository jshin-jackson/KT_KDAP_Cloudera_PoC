-- WF_N02960_TMP_SUM_LTAS_CDR_TM.sql
-- Scenario 1: LTAS batch aggregation (~130억 rows, est. 4-5h)
-- Batch filter: ETL_DATE, BASE_DATE only (no encryption functions)
-- Replace with KT-provided SQL when received

INSERT OVERWRITE kdap.sum_ltas_cdr_tm
SELECT
  s.etl_date,
  SUBSTR(s.rqt_st_dt, 1, 10) AS base_hour,
  b.bts_alt_key,
  b.region_cd,
  SUM(s.val) AS total_val,
  COUNT(*) AS record_cnt,
  NOW() AS created_at
FROM kdap.cdr_sgi s
LEFT JOIN kdap.bts_master b
  ON s.cell_id = b.cell_id
WHERE s.etl_date = '${ETL_DATE}'
  AND s.base_date = '${BASE_DATE}'
  -- Real-time variant adds: AND SUBSTR(s.rqt_st_dt, 11, 2) = '45'
GROUP BY
  s.etl_date,
  SUBSTR(s.rqt_st_dt, 1, 10),
  b.bts_alt_key,
  b.region_cd;
