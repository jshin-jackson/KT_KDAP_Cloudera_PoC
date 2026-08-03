-- WF_N05923_CNV_MDT_MERGE.sql
-- Scenario 1: MDT preprocessing, BTS mapping + distance (~180억 rows, est. 2-3h)
-- Based on Wing query pattern with ST_DISTANCE (tune with bounding-box pre-filter)

INSERT OVERWRITE kdap.cnv_mdt_merge_stg
SELECT
  m.mdt_id,
  m.gnss_utmkx,
  m.gnss_utmky,
  b.bts_alt_key,
  ST_DISTANCE(
    ST_POINT(m.gnss_utmkx, m.gnss_utmky),
    ST_GEOMFROMTEXT(b.rpot_geom)
  ) AS distance_m,
  m.base_date,
  NOW() AS created_at
FROM kdap.cdr_mdt_smsng m
LEFT JOIN kdap.bts_master b
  ON m.bts_market_nm = b.bts_market_nm
LEFT JOIN kdap.tbm_rpot_m r
  ON b.bts_market_nm = r.bts_market_nm
WHERE m.base_date = '${BASE_DATE}'
  AND (
    (b.bts_market_nm = 'ERICSSON' AND m.trigr_ev = 'PERIODIC')
    OR (b.bts_market_nm = 'NOKIA' AND m.trigr_ev IS NULL)
    OR (b.bts_market_nm = 'SAMSUNG' AND m.trigr_ev = '')
  )
  AND ST_DISTANCE(
    ST_POINT(m.gnss_utmkx, m.gnss_utmky),
    ST_GEOMFROMTEXT(r.rpot_geom)
  ) < CAST(r.rpot_line_cnt AS INT) * 3;
