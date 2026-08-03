-- CDP Flink 테스트용 샘플 데이터
-- rqt_st_dt 형식: YYYYMMDDHHmmss → 11~12번째 문자 = '45' (분 필터 통과)
-- event_time 은 현재 시각 근처로 넣어야 Flink WATERMARK 가 인식합니다.

USE kdap;

-- 기존 테스트 데이터 삭제 (재실행 시)
DELETE FROM bts_master;
DELETE FROM cdr_sgi_raw;
DELETE FROM cdr_mdt_smsng_raw;

-- 기지국 3건
INSERT INTO bts_master VALUES
  ('CELL_A', 'CELL_A_ALT', 'BTS_SEOUL_01', 'SAMSUNG', 'SEOUL'),
  ('CELL_B', 'CELL_B_ALT', 'BTS_BUSAN_01', 'SAMSUNG', 'BUSAN'),
  ('CELL_C', 'CELL_C_ALT', 'BTS_DAEGU_01', 'ERICSSON', 'DAEGU');

-- SGi 원천 6건 (분=45 조건 충족)
INSERT INTO cdr_sgi_raw (sgi_id, cell_id, alt_cell_id, rqt_st_dt, etl_date, val, use_flag, signal_type, event_time) VALUES
  ('SGI001', 'CELL_A', 'CELL_A_ALT', '20260803124500', '20260803', 100, 'Y', 'SGI',   NOW()),
  ('SGI002', 'CELL_A', 'CELL_A_ALT', '20260803124530', '20260803', 200, 'Y', 'S1AP',  NOW()),
  ('SGI003', 'CELL_B', 'CELL_B_ALT', '20260803124510', '20260803', 150, 'N', 'SGI',   NOW()),
  ('SGI004', 'CELL_B', 'CELL_B_ALT', '20260803124520', '20260803',  50, 'Y', 'S1AP',  NOW()),
  ('SGI005', 'CELL_C', 'CELL_C_ALT', '20260803124540', '20260803', 300, 'Y', 'SGI',   NOW()),
  ('SGI006', 'CELL_A', 'CELL_A_ALT', '20260803124600', '20260803', 999, 'Y', 'SGI',   NOW());

-- MDT 원천 4건
INSERT INTO cdr_mdt_smsng_raw (mdt_id, gnss_utmkx, gnss_utmky, rqt_st_dt, bts_market_nm, event_time) VALUES
  ('MDT001', 961000.0, 1946000.0, '20260803124500', 'SAMSUNG',   NOW()),
  ('MDT002', 962000.0, 1947000.0, '20260803124515', 'SAMSUNG',   NOW()),
  ('MDT003', 963000.0, 1948000.0, '20260803124530', 'ERICSSON',  NOW()),
  ('MDT004', 964000.0, 1949000.0, '20260803124600', 'SAMSUNG',   NOW());

-- Impala 메타데이터 갱신
REFRESH bts_master;
REFRESH cdr_sgi_raw;
REFRESH cdr_mdt_smsng_raw;
