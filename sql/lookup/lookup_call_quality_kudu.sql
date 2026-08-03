-- Scenario 4: HBase replacement — single key lookup
-- Target: 2-3 sec response (current HBase: 2-5 sec)
-- Run against Kudu and Iceberg variants for A/B comparison

-- Kudu lookup (point query on primary key)
SELECT
  lookup_key,
  call_id,
  rsrp,
  rsrq,
  drop_flag,
  base_date,
  detail_json
FROM kdap.call_quality_kudu
WHERE lookup_key = '${LOOKUP_KEY}';

-- Iceberg lookup (uncomment for A/B test)
-- SELECT lookup_key, call_id, rsrp, rsrq, drop_flag, base_date, detail_json
-- FROM kdap.call_quality_iceberg
-- WHERE lookup_key = '${LOOKUP_KEY}'
--   AND base_date = '${BASE_DATE}';
