-- MSTR Query 1: LTE RSRP 불량률
-- Scenario 3: Daily aggregation, ~20 runs/day, avg 30 min on Presto baseline
-- Pattern from Architecture PDF Wing example

SELECT
  bd.sido_name,
  bd.gungu_name,
  bd.bemd_name,
  bd.bdri_name,
  SUM(CASE WHEN m.dlearfcn_cd IN ('1550','1650','1694','1700') AND m.rsrp > -75 THEN 1 END) AS rsrp_18_o75,
  SUM(CASE WHEN m.dlearfcn_cd IN ('1550','1650','1694','1700') AND m.rsrp <= -110 THEN 1 END) AS rsrp_18_u110,
  SUM(CASE WHEN m.dlearfcn_cd IN ('1550','1650','1694','1700') THEN 1 END) AS rsrp_18_tot,
  SUM(CASE WHEN m.rsrp > -75 THEN 1 END) AS rsrp_o75,
  SUM(CASE WHEN m.rsrp <= -110 THEN 1 END) AS rsrp_u110,
  COUNT(*) AS rsrp_tot
FROM kdap.cdr_mdt_smsng m
JOIN kdap.tbm_gpot25 g
  ON SPLIT_PART(m.gnss_metric_id, '_', 1) = CAST(g.gpot25_x AS STRING)
 AND SPLIT_PART(m.gnss_metric_id, '_', 2) = CAST(g.gpot25_y AS STRING)
JOIN kdap.tbm_bdri bd
  ON CAST(g.bdri_cd AS STRING) = bd.bdri_cd
WHERE m.base_date LIKE '${BASE_DATE_PREFIX}%'
GROUP BY bd.sido_name, bd.gungu_name, bd.bemd_name, bd.bdri_name;
