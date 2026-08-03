# SDV — Flink 시나리오 대용량 합성 데이터

SDV(Synthetic Data Vault)로 KT KDAP Flink 5분 TUMBLE 테스트용 데이터를 생성합니다.

## 생성 테이블

| 테이블 | Flink 역할 | 기본 건수 |
|--------|------------|-----------|
| `bts_master` | 차원 (기지국 JOIN) | 5,000 |
| `cdr_sgi_raw` | LTAS / SGi 스트리밍 소스 | 1,000,000 |
| `cdr_mdt_smsng_raw` | MDT 스트리밍 소스 | 1,000,000 |

## SDV 모델

- **bts_master + cdr_sgi_raw:** `HMASynthesizer` (FK: cell_id, referential integrity)
- **cdr_mdt_smsng_raw:** `GaussianCopulaSynthesizer`

## Flink 정렬 후처리

- `event_time`: 최근 N시간 균등 분포 (WATERMARK / 5분 TUMBLE)
- `rqt_st_dt`: `YYYYMMDDHHmmss`, 기본 85%가 분=`45` (Flink WHERE 조건)
- `cell_id` / `alt_cell_id`: `bts_master` 와 JOIN 가능하도록 정합

## 실행 (CDP Edge — python3.11)

```
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r sdv/requirements.txt
```

```
python3.11 sdv/generate_flink_data.py --scale bts=5000 --scale sgi=1000000 --scale mdt=1000000
```

소규모 smoke test:

```
python3.11 sdv/generate_flink_data.py --scale bts=100 --scale sgi=10000 --scale mdt=10000 --output sdv/output/smoke
```

> CDP Edge의 `python3`(3.8 parcel)은 사용하지 마세요. `.env.example` 참고.

## 출력

```
sdv/output/
  bts_master.parquet
  cdr_sgi_raw.parquet
  cdr_mdt_smsng_raw.parquet
  generation_summary.json
```

CDP 적재 명령: [docs/runbooks/flink-sdv-data-load.md](../docs/runbooks/flink-sdv-data-load.md)
