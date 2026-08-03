-- MSTR Query 2: VoLTE 절단율
-- Scenario 3: 5-min interval, avg 1h on Presto baseline
-- Joins LTE call + contract + product + CDR

SELECT
  c.base_date,
  COUNT(*) AS total_calls,
  SUM(CASE WHEN c.drop_flag = 'Y' AND c.volte_flag = 'Y' THEN 1 ELSE 0 END) AS dropped_volte,
  CAST(SUM(CASE WHEN c.drop_flag = 'Y' AND c.volte_flag = 'Y' THEN 1 ELSE 0 END) AS DOUBLE)
    / NULLIF(SUM(CASE WHEN c.volte_flag = 'Y' THEN 1 ELSE 0 END), 0) AS volte_drop_rate
FROM kdap.lte_call_fact c
JOIN kdap.contract_dim ct ON c.contract_id = ct.contract_id
JOIN kdap.product_dim p ON c.product_id = p.product_id
WHERE c.base_date = '${BASE_DATE}'
  AND p.volte_eligible = 'Y'
GROUP BY c.base_date;
