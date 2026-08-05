#!/usr/bin/env python3
"""Load SDV Parquet files from HDFS staging into kdap Iceberg tables (PySpark).

Run on CDP edge (after kinit + HADOOP_CONF_DIR):

  export HADOOP_CONF_DIR=/etc/hadoop/conf
  spark-submit --driver-memory 4g --executor-memory 8g --num-executors 4 \\
    sdv/load_sdv_to_iceberg.py

HMS URI defaults to hive-site.xml under HADOOP_CONF_DIR (recommended on CDP).
Override only when needed, e.g. jshin HA HMS:

  spark-submit ... sdv/load_sdv_to_iceberg.py \\
    --hms-uri 'thrift://ccycloud-1.jshin.root.comops.site:9083,thrift://ccycloud-3.jshin.root.comops.site:9083'

Interactive debugging — Iceberg configs must be set at startup (not via spark.conf.set):

  export HADOOP_CONF_DIR=/etc/hadoop/conf
  pyspark --driver-memory 4g --executor-memory 8g --num-executors 4 \\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \\
    --conf spark.sql.catalog.spark_catalog.type=hive \\
    --conf spark.sql.catalog.spark_catalog.warehouse=hdfs://ns1/user/hive/warehouse \\
    --conf spark.hadoop.fs.defaultFS=hdfs://ns1

Table qualifier must match the catalog name: spark_catalog.kdap.* (not iceberg.kdap.*).
ccycloud-5 is Impala — not Hive Metastore (HMS: ccycloud-1, ccycloud-3).

SDV Parquet event_time is often TIMESTAMP(NANOS) from pyarrow; Spark 3.x cannot infer it.
This module reads event_time as INT64 and converts to timestamp (see read_sgi_raw/read_mdt_raw).
Regenerate Parquet with coerce_timestamps=us to avoid the workaround.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ICEBERG_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"
CATALOG_NAME = "spark_catalog"
DEFAULT_WAREHOUSE = "hdfs://ns1/user/hive/warehouse"
JSHIN_HMS_URI = (
    "thrift://ccycloud-1.jshin.root.comops.site:9083,"
    "thrift://ccycloud-3.jshin.root.comops.site:9083"
)

SGI_SCHEMA = StructType(
    [
        StructField("sgi_id", StringType(), True),
        StructField("cell_id", StringType(), True),
        StructField("alt_cell_id", StringType(), True),
        StructField("rqt_st_dt", StringType(), True),
        StructField("etl_date", StringType(), True),
        StructField("val", LongType(), True),
        StructField("use_flag", StringType(), True),
        StructField("signal_type", StringType(), True),
        StructField("event_time", LongType(), True),
    ]
)

MDT_SCHEMA = StructType(
    [
        StructField("mdt_id", StringType(), True),
        StructField("gnss_utmkx", DoubleType(), True),
        StructField("gnss_utmky", DoubleType(), True),
        StructField("rqt_st_dt", StringType(), True),
        StructField("bts_market_nm", StringType(), True),
        StructField("event_time", LongType(), True),
    ]
)


def create_spark(default_fs: str, hms_uri: str | None, warehouse: str) -> SparkSession:
    prefix = f"spark.sql.catalog.{CATALOG_NAME}"
    builder = (
        SparkSession.builder.appName("sdv-load-iceberg")
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config(prefix, ICEBERG_CATALOG_CLASS)
        .config(f"{prefix}.type", "hive")
        .config(f"{prefix}.warehouse", warehouse)
        .config("spark.hadoop.fs.defaultFS", default_fs)
    )
    if hms_uri:
        builder = builder.config(f"{prefix}.uri", hms_uri)
    return builder.getOrCreate()


def table(name: str) -> str:
    return f"{CATALOG_NAME}.kdap.{name}"


def _event_time_from_parquet_nanos(column: str) -> DataFrame:
    return expr(f"timestamp_micros(cast({column} / 1000 as bigint))")


def read_sgi_raw(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(SGI_SCHEMA)
        .parquet(path)
        .withColumn("event_time", _event_time_from_parquet_nanos("event_time"))
    )


def read_mdt_raw(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(MDT_SCHEMA)
        .parquet(path)
        .withColumn("event_time", _event_time_from_parquet_nanos("event_time"))
    )


def load_tables(spark: SparkSession, staging: str) -> None:
    staging = staging.rstrip("/")
    spark.read.parquet(f"{staging}/bts_master.parquet").writeTo(
        table("bts_master")
    ).overwritePartitions()

    read_sgi_raw(spark, f"{staging}/cdr_sgi_raw.parquet").writeTo(
        table("cdr_sgi_raw")
    ).append()

    read_mdt_raw(spark, f"{staging}/cdr_mdt_smsng_raw.parquet").writeTo(
        table("cdr_mdt_smsng_raw")
    ).append()


def main() -> None:
    parser = argparse.ArgumentParser(description="SDV Parquet → kdap Iceberg (PySpark)")
    parser.add_argument(
        "--staging",
        default="hdfs://ns1/user/kdap/staging/sdv",
        help="HDFS path containing bts_master, cdr_sgi_raw, cdr_mdt_smsng_raw parquet",
    )
    parser.add_argument(
        "--default-fs",
        default="hdfs://ns1",
        help="HDFS nameservice (must match cluster HA config)",
    )
    parser.add_argument(
        "--hms-uri",
        default=None,
        help=(
            "Hive Metastore URI for Iceberg catalog. "
            "Omit to use hive.metastore.uris from HADOOP_CONF_DIR/hive-site.xml. "
            f"jshin override: {JSHIN_HMS_URI!r}"
        ),
    )
    parser.add_argument(
        "--warehouse",
        default=DEFAULT_WAREHOUSE,
        help="Iceberg warehouse path",
    )
    args = parser.parse_args()

    spark = create_spark(args.default_fs, args.hms_uri, args.warehouse)
    load_tables(spark, args.staging.rstrip("/"))
    spark.stop()


if __name__ == "__main__":
    main()
