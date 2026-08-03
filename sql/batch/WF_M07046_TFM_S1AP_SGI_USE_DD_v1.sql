-- WF_M07046_TFM_S1AP_SGI_USE_DD.sql (Variant 1)
-- Scenario 1: SGi signal + BTS join with OR on alternate cell (~254억 rows, est. 7-8h)
-- Single day filter per PoC agreement

INSERT OVERWRITE kdap.tfm_s1ap_sgi_use_dd_v1
SELECT
  s.etl_date,
  s.cell_id,
  SUM(CASE WHEN s.use_flag = 'Y' THEN 1 ELSE 0 END) AS use_cnt,
  NOW() AS created_at
FROM kdap.cdr_sgi s
LEFT JOIN kdap.bts_master b
  ON s.cell_id = b.cell_id
  OR s.alt_cell_id = b.alt_cell_id
WHERE s.etl_date = '${ETL_DATE}'
  AND s.base_date = '${BASE_DATE}'
  AND s.signal_type IN ('S1AP', 'SGI')
GROUP BY s.etl_date, s.cell_id;
