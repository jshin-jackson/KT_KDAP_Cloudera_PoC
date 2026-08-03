#!/usr/bin/env python3
"""
KT KDAP Flink Scenario — SDV synthetic data generator.

Uses SDV HMASynthesizer (bts_master + cdr_sgi_raw) and GaussianCopulaSynthesizer (mdt)
to produce large-scale test data aligned with Flink 5-min TUMBLE filters.

Usage:
  pip install -r sdv/requirements.txt
  python3.11 sdv/generate_flink_data.py --scale sgi=1000000 --scale mdt=1000000 --scale bts=5000
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from sdv.metadata import MultiTableMetadata, SingleTableMetadata
    from sdv.multi_table import HMASynthesizer
    from sdv.single_table import GaussianCopulaSynthesizer
except ImportError:
    print("ERROR: SDV not installed. Run: pip install -r sdv/requirements.txt", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
SEED_DIR = ROOT / "seed"
DEFAULT_OUTPUT = ROOT / "output"

REGIONS = ["SEOUL", "BUSAN", "DAEGU", "INCHEON", "GWANGJU", "DAEJEON", "ULSAN", "SUWON", "JEJU", "CHANGWON"]
MARKETS = ["SAMSUNG", "ERICSSON", "NOKIA"]
SIGNAL_TYPES = ["SGI", "S1AP"]
USE_FLAGS = ["Y", "N"]


def load_seed() -> dict[str, pd.DataFrame]:
    data = {}
    for name in ("bts_master", "cdr_sgi_raw", "cdr_mdt_smsng_raw"):
        path = SEED_DIR / f"{name}.csv"
        df = pd.read_csv(path)
        if "event_time" in df.columns:
            df["event_time"] = pd.to_datetime(df["event_time"])
        data[name] = df
    return data


def build_hma_metadata(seed: dict[str, pd.DataFrame]) -> MultiTableMetadata:
    metadata = MultiTableMetadata()
    metadata.detect_table_from_dataframe("bts_master", seed["bts_master"])
    metadata.detect_table_from_dataframe("cdr_sgi_raw", seed["cdr_sgi_raw"])

    metadata.update_column(
        "bts_master",
        "cell_id",
        sdtype="id",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "sgi_id",
        sdtype="id",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "cell_id",
        sdtype="id",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "alt_cell_id",
        sdtype="id",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "val",
        sdtype="numerical",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "event_time",
        sdtype="datetime",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "rqt_st_dt",
        sdtype="categorical",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "etl_date",
        sdtype="categorical",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "use_flag",
        sdtype="categorical",
    )
    metadata.update_column(
        "cdr_sgi_raw",
        "signal_type",
        sdtype="categorical",
    )

    metadata.add_relationship(
        parent_table_name="bts_master",
        parent_primary_key="cell_id",
        child_table_name="cdr_sgi_raw",
        child_foreign_key="cell_id",
    )
    return metadata


def build_mdt_metadata(seed: pd.DataFrame) -> SingleTableMetadata:
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(seed)
    metadata.update_column("mdt_id", sdtype="id")
    metadata.update_column("gnss_utmkx", sdtype="numerical")
    metadata.update_column("gnss_utmky", sdtype="numerical")
    metadata.update_column("rqt_st_dt", sdtype="categorical")
    metadata.update_column("bts_market_nm", sdtype="categorical")
    metadata.update_column("event_time", sdtype="datetime")
    return metadata


def _random_rqt_st_dt(base: datetime, minute_45: bool, rng: np.random.Generator) -> str:
    dt = base.replace(
        hour=int(rng.integers(0, 24)),
        second=int(rng.integers(0, 60)),
    )
    minute = 45 if minute_45 else int(rng.choice([i for i in range(60) if i != 45]))
    dt = dt.replace(minute=minute)
    return dt.strftime("%Y%m%d%H%M%S")


def apply_flink_sgi_postprocess(
    df: pd.DataFrame,
    bts: pd.DataFrame,
    *,
    hours_span: int,
    filter_pass_ratio: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Align SDV output with Flink WHERE + WATERMARK requirements."""
    out = df.copy()
    n = len(out)

    valid_cells = set(bts["cell_id"].astype(str))
    valid_alts = dict(zip(bts["cell_id"].astype(str), bts["alt_cell_id"].astype(str)))

    out["cell_id"] = out["cell_id"].astype(str).where(
        out["cell_id"].astype(str).isin(valid_cells),
        rng.choice(list(valid_cells), size=n),
    )
    out["alt_cell_id"] = out["cell_id"].map(valid_alts).fillna(out["alt_cell_id"])

    base_end = datetime.utcnow().replace(microsecond=0)
    base_start = base_end - timedelta(hours=hours_span)
    event_times = [
        base_start + timedelta(seconds=float(x))
        for x in rng.uniform(0, hours_span * 3600, size=n)
    ]
    out["event_time"] = pd.to_datetime(event_times)

    pass_mask = rng.random(n) < filter_pass_ratio
    out["rqt_st_dt"] = [
        _random_rqt_st_dt(ts.to_pydatetime(), bool(pass_mask[i]), rng)
        for i, ts in enumerate(out["event_time"])
    ]
    out["etl_date"] = out["event_time"].dt.strftime("%Y%m%d")

    out["val"] = out["val"].fillna(0).astype(np.int64).clip(0, 10_000_000)
    out["use_flag"] = out["use_flag"].where(
        out["use_flag"].isin(USE_FLAGS),
        rng.choice(USE_FLAGS, size=n),
    )
    out["signal_type"] = out["signal_type"].where(
        out["signal_type"].isin(SIGNAL_TYPES),
        rng.choice(SIGNAL_TYPES, size=n),
    )
    out["sgi_id"] = [f"SGI{i:012d}" for i in range(n)]
    return out


def apply_flink_mdt_postprocess(
    df: pd.DataFrame,
    bts: pd.DataFrame,
    *,
    hours_span: int,
    filter_pass_ratio: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    out = df.copy()
    n = len(out)
    markets = bts["bts_market_nm"].astype(str).unique().tolist() or MARKETS

    base_end = datetime.utcnow().replace(microsecond=0)
    base_start = base_end - timedelta(hours=hours_span)
    event_times = [
        base_start + timedelta(seconds=float(x))
        for x in rng.uniform(0, hours_span * 3600, size=n)
    ]
    out["event_time"] = pd.to_datetime(event_times)

    pass_mask = rng.random(n) < filter_pass_ratio
    out["rqt_st_dt"] = [
        _random_rqt_st_dt(ts.to_pydatetime(), bool(pass_mask[i]), rng)
        for i, ts in enumerate(out["event_time"])
    ]

    out["bts_market_nm"] = out["bts_market_nm"].where(
        out["bts_market_nm"].isin(markets),
        rng.choice(markets, size=n),
    )
    out["gnss_utmkx"] = out["gnss_utmkx"].fillna(961000.0).clip(900000, 1100000)
    out["gnss_utmky"] = out["gnss_utmky"].fillna(1946000.0).clip(1900000, 2100000)
    out["mdt_id"] = [f"MDT{i:012d}" for i in range(n)]
    return out


def expand_bts(seed_bts: pd.DataFrame, target_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    if target_rows <= len(seed_bts):
        return seed_bts.head(target_rows).copy()

    rows = []
    for i in range(target_rows):
        region = REGIONS[i % len(REGIONS)]
        market = MARKETS[i % len(MARKETS)]
        cid = f"CELL_{i:06d}"
        rows.append(
            {
                "cell_id": cid,
                "alt_cell_id": f"{cid}_ALT",
                "bts_alt_key": f"BTS_{region}_{i:04d}",
                "bts_market_nm": market,
                "region_cd": region,
            }
        )
    return pd.DataFrame(rows)


def synthesize_sgi_with_hma(
    seed: dict[str, pd.DataFrame],
    target_rows: int,
    verbose: bool,
) -> pd.DataFrame:
    seed_sgi_rows = len(seed["cdr_sgi_raw"])
    scale = max(target_rows / seed_sgi_rows, 1.0)

    metadata = build_hma_metadata(seed)
    synthesizer = HMASynthesizer(metadata, verbose=verbose)
    synthesizer.fit({"bts_master": seed["bts_master"], "cdr_sgi_raw": seed["cdr_sgi_raw"]})
    synthetic = synthesizer.sample(scale=scale)

    sgi = synthetic["cdr_sgi_raw"].head(target_rows).copy()
    bts = synthetic["bts_master"].copy()
    return sgi, bts


def synthesize_mdt(
    seed_mdt: pd.DataFrame,
    target_rows: int,
    verbose: bool,
) -> pd.DataFrame:
    seed_rows = len(seed_mdt)
    scale = max(target_rows / seed_rows, 1.0)

    metadata = build_mdt_metadata(seed_mdt)
    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(seed_mdt)
    return synthesizer.sample(num_rows=target_rows)


def write_parquet(datasets: dict[str, pd.DataFrame], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in datasets.items():
        path = output_dir / f"{name}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
        print(f"  wrote {path} ({len(df):,} rows, {path.stat().st_size / 1024 / 1024:.1f} MB)")


def write_summary(datasets: dict[str, pd.DataFrame], output_dir: Path, config: dict) -> None:
    summary = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "config": config,
        "row_counts": {k: len(v) for k, v in datasets.items()},
        "sgi_filter_pass_est": int(
            (datasets["cdr_sgi_raw"]["rqt_st_dt"].str[10:12] == "45").sum()
        ),
        "mdt_filter_pass_est": int(
            (datasets["cdr_mdt_smsng_raw"]["rqt_st_dt"].str[10:12] == "45").sum()
        ),
    }
    path = output_dir / "generation_summary.json"
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"  wrote {path}")


def parse_scales(args: argparse.Namespace) -> dict[str, int]:
    defaults = {"bts": 5_000, "sgi": 1_000_000, "mdt": 1_000_000}
    for item in args.scale:
        key, _, val = item.partition("=")
        key = key.strip().lower()
        if key in ("bts", "bts_master"):
            defaults["bts"] = int(val)
        elif key in ("sgi", "cdr_sgi", "cdr_sgi_raw"):
            defaults["sgi"] = int(val)
        elif key in ("mdt", "cdr_mdt", "cdr_mdt_smsng_raw"):
            defaults["mdt"] = int(val)
        else:
            raise ValueError(f"Unknown scale key: {key}")
    return defaults


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Flink scenario data with SDV")
    parser.add_argument(
        "--scale",
        action="append",
        default=[],
        metavar="TABLE=ROWS",
        help="Row targets: bts=5000, sgi=1000000, mdt=1000000 (repeatable)",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output directory")
    parser.add_argument("--hours-span", type=int, default=6, help="event_time spread (hours)")
    parser.add_argument(
        "--filter-pass-ratio",
        type=float,
        default=0.85,
        help="Ratio of rows with rqt_st_dt minute=45 (Flink filter)",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--verbose", action="store_true", help="SDV verbose output")
    args = parser.parse_args()

    scales = parse_scales(args)
    rng = np.random.default_rng(args.seed)

    print("==> Loading seed data")
    seed = load_seed()

    print(f"==> SDV HMA: synthesizing cdr_sgi_raw (target {scales['sgi']:,} rows)")
    sgi, bts_from_hma = synthesize_sgi_with_hma(seed, scales["sgi"], args.verbose)

    print(f"==> Expanding bts_master (target {scales['bts']:,} rows)")
    bts = expand_bts(bts_from_hma if len(bts_from_hma) >= scales["bts"] else seed["bts_master"], scales["bts"], rng)

    print(f"==> SDV GaussianCopula: synthesizing cdr_mdt_smsng_raw (target {scales['mdt']:,} rows)")
    mdt = synthesize_mdt(seed["cdr_mdt_smsng_raw"], scales["mdt"], args.verbose)

    print("==> Post-processing for Flink (event_time, rqt_st_dt minute=45, FK alignment)")
    sgi = apply_flink_sgi_postprocess(
        sgi,
        bts,
        hours_span=args.hours_span,
        filter_pass_ratio=args.filter_pass_ratio,
        rng=rng,
    )
    mdt = apply_flink_mdt_postprocess(
        mdt,
        bts,
        hours_span=args.hours_span,
        filter_pass_ratio=args.filter_pass_ratio,
        rng=rng,
    )

    datasets = {
        "bts_master": bts,
        "cdr_sgi_raw": sgi,
        "cdr_mdt_smsng_raw": mdt,
    }

    print(f"==> Writing Parquet to {args.output}")
    write_parquet(datasets, args.output)
    write_summary(
        datasets,
        args.output,
        {
            "scales": scales,
            "hours_span": args.hours_span,
            "filter_pass_ratio": args.filter_pass_ratio,
            "seed": args.seed,
        },
    )

    print("==> Done")
    print(f"    bts_master:          {len(bts):>12,} rows")
    print(f"    cdr_sgi_raw:         {len(sgi):>12,} rows")
    print(f"    cdr_mdt_smsng_raw:   {len(mdt):>12,} rows")
    print(f"    sgi minute=45:       {(sgi['rqt_st_dt'].str[10:12] == '45').sum():>12,} rows")
    print(f"    mdt minute=45:       {(mdt['rqt_st_dt'].str[10:12] == '45').sum():>12,} rows")


if __name__ == "__main__":
    main()
