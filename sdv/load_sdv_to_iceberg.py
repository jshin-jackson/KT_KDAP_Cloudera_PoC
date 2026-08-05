#!/usr/bin/env python3
"""Load SDV Parquet files from HDFS staging into kdap Iceberg tables (PySpark).

Run on CDP edge (after kinit + HADOOP_CONF_DIR):

  spark-submit --driver-memory 4g --executor-memory 8g --num-executors 4 \\
    sdv/load_sdv_to_iceberg.py

Interactive debugging — Iceberg configs must be set at startup (not via spark.conf.set):

  export HADOOP_CONF_DIR=/etc/hadoop/conf
  pyspark --driver-memory 4g --executor-memory 8g --num-executors 4 \\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \\
    --conf spark.sql.catalog.spark_catalog.type=hive \\
    --conf spark.hadoop.fs.defaultFS=hdfs://ns1
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ICEBERG_CATALOG = "org.apache.iceberg.spark.SparkCatalog"


def create_spark(default_fs: str) -> SparkSession:
    return (
        SparkSession.builder.appName("sdv-load-iceberg")
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config("spark.sql.catalog.spark_catalog", ICEBERG_CATALOG)
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.hadoop.fs.defaultFS", default_fs)
        .getOrCreate()
    )


def load_tables(spark: SparkSession, staging: str) -> None:
    spark.read.parquet(f"{staging}/bts_master.parquet").writeTo(
        "iceberg.kdap.bts_master"
    ).overwritePartitions()

    spark.read.parquet(f"{staging}/cdr_sgi_raw.parquet").writeTo(
        "iceberg.kdap.cdr_sgi_raw"
    ).append()

    spark.read.parquet(f"{staging}/cdr_mdt_smsng_raw.parquet").writeTo(
        "iceberg.kdap.cdr_mdt_smsng_raw"
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
    args = parser.parse_args()

    spark = create_spark(args.default_fs)
    load_tables(spark, args.staging.rstrip("/"))
    spark.stop()


if __name__ == "__main__":
    main()
