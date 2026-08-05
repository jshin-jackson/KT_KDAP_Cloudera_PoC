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
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ICEBERG_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"
CATALOG_NAME = "spark_catalog"
DEFAULT_WAREHOUSE = "hdfs://ns1/user/hive/warehouse"
# jshin HA HMS (Impala is on ccycloud-5 — do not use for metastore)
JSHIN_HMS_URI = (
    "thrift://ccycloud-1.jshin.root.comops.site:9083,"
    "thrift://ccycloud-3.jshin.root.comops.site:9083"
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


def load_tables(spark: SparkSession, staging: str) -> None:
    spark.read.parquet(f"{staging}/bts_master.parquet").writeTo(
        table("bts_master")
    ).overwritePartitions()

    spark.read.parquet(f"{staging}/cdr_sgi_raw.parquet").writeTo(
        table("cdr_sgi_raw")
    ).append()

    spark.read.parquet(f"{staging}/cdr_mdt_smsng_raw.parquet").writeTo(
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
