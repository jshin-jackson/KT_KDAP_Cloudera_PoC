-- Scenario 4: HBase replacement — Iceberg variant

SELECT
  lookup_key,
  call_id,
  rsrp,
  rsrq,
  drop_flag,
  base_date,
  detail_json
FROM kdap.call_quality_iceberg
WHERE lookup_key = '${LOOKUP_KEY}'
  AND base_date = '${BASE_DATE}';
