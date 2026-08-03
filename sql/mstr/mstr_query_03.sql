-- MSTR Query 3: Placeholder — replace with KT-provided SQL
-- Scenario 3: 4 concurrent MSTR queries

SELECT
  '${QUERY_ID}' AS query_id,
  COUNT(*) AS row_cnt
FROM kdap.cdr_sgi
WHERE etl_date = '${ETL_DATE}'
  AND base_date = '${BASE_DATE}';
