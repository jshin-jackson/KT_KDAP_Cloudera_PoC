-- MSTR Query 4: Placeholder — replace with KT-provided SQL
-- Scenario 3: 4 concurrent MSTR queries

SELECT
  '${QUERY_ID}' AS query_id,
  b.region_cd,
  SUM(s.val) AS total_val
FROM kdap.cdr_sgi s
JOIN kdap.bts_master b ON s.cell_id = b.cell_id
WHERE s.etl_date = '${ETL_DATE}'
GROUP BY b.region_cd;
