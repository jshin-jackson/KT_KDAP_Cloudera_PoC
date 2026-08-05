#!/usr/bin/env python3
"""Load SDV Parquet files from HDFS staging into kdap Iceberg tables (PySpark).

Run on CDP edge (after kinit + HADOOP_CONF_DIR):

  pyspark3 --driver-memory 4g --executor-memory 8g --num-executors 4 \\
    sdv/load_sdv_to_iceberg.py

Or:

  spark3-submit --driver-memory 4g --executor-memory 8g --num-executors 4 \\
    sdv/load_sdv_to_iceberg.py
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def configure_iceberg(spark: SparkSession, default_fs: str) -> None:
    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.spark_catalog.type", "hive")
    spark.conf.set("spark.hadoop.fs.defaultFS", default_fs)
    spark.conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
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

    spark = SparkSession.builder.appName("sdv-load-iceberg").getOrCreate()
    configure_iceberg(spark, args.default_fs)
    load_tables(spark, args.staging.rstrip("/"))
    spark.stop()


if __name__ == "__main__":
    main()
