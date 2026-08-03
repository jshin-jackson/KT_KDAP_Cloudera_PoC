-- WF_M07046_TFM_S1AP_SGI_USE_DD.sql (Variant 2)
-- Scenario 1: Similar SGi query, separate target table (~14억 rows)

INSERT OVERWRITE kdap.tfm_s1ap_sgi_use_dd_v2
SELECT
  s.etl_date,
  COALESCE(b.bts_alt_key, s.cell_id) AS cell_id,
  COUNT(*) AS use_cnt,
  NOW() AS created_at
FROM kdap.cdr_sgi s
INNER JOIN kdap.bts_master b
  ON (s.cell_id = b.cell_id OR s.alt_cell_id = b.alt_cell_id)
WHERE s.etl_date = '${ETL_DATE}'
  AND s.base_date = '${BASE_DATE}'
GROUP BY s.etl_date, COALESCE(b.bts_alt_key, s.cell_id);
