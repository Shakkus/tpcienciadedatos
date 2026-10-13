#!/usr/bin/env python3
"""Import crm.riders to HDFS with Spark JDBC as the Sqoop fallback."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession


DEFAULT_OUTPUT = "hdfs://namenode:9000/nexo/bronze/riders_sqoop"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="HDFS Parquet destination")
    args = parser.parse_args()

    host = os.environ.get("MYSQL_HOST", "mysql")
    port = os.environ.get("MYSQL_PORT", "3306")
    database = os.environ.get("MYSQL_DATABASE", "crm")
    user = os.environ.get("MYSQL_USER", "root")
    password = os.environ.get("MYSQL_PASSWORD", "nexo")
    jdbc_url = (
        f"jdbc:mysql://{host}:{port}/{database}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
    )

    spark = SparkSession.builder.appName("nexo_m7_import_riders").getOrCreate()
    try:
        riders = spark.read.jdbc(
            url=jdbc_url,
            table="riders",
            properties={
                "user": user,
                "password": password,
                "driver": MYSQL_DRIVER,
            },
        )
        row_count = riders.count()
        riders.write.mode("errorifexists").parquet(args.output)
        print(f"Importadas {row_count} filas desde {database}.riders a {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
