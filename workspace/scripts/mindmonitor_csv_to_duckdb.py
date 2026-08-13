#!/usr/bin/env python3

import argparse
import os
import re

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Mind Monitor CSV files into DuckDB."
    )
    parser.add_argument(
        "--csv-dir",
        default="./data/csv/",
        help="Directory containing CSV files (default: ./csv/)",
    )
    parser.add_argument(
        "--db-path",
        default="./data/eeg.duckdb",
        help="DuckDB database path (default: ./eeg.duckdb)",
    )
    parser.add_argument(
        "--table-name",
        default="mind_monitor",
        help="Destination table name (default: mind_monitor)",
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.table_name):
        parser.error("table name must be a valid SQL identifier")
    return args


def main() -> None:
    args = parse_args()
    csv_dir = args.csv_dir if args.csv_dir.endswith("/") else f"{args.csv_dir}/"
    if not os.path.isdir(csv_dir):
        raise SystemExit(
            f"CSV directory does not exist or is not a directory: {csv_dir}"
        )

    db_path = args.db_path
    table_name = args.table_name

    con = duckdb.connect(database=db_path)

    con.execute(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE IF NOT EXISTS {table_name} (
            "timestamp" TIMESTAMP_MS,
            "Delta_TP9" DOUBLE, "Delta_AF7" DOUBLE, "Delta_AF8" DOUBLE, "Delta_TP10" DOUBLE,
            "Theta_TP9" DOUBLE, "Theta_AF7" DOUBLE, "Theta_AF8" DOUBLE, "Theta_TP10" DOUBLE,
            "Alpha_TP9" DOUBLE, "Alpha_AF7" DOUBLE, "Alpha_AF8" DOUBLE, "Alpha_TP10" DOUBLE,
            "Beta_TP9" DOUBLE, "Beta_AF7" DOUBLE, "Beta_AF8" DOUBLE, "Beta_TP10" DOUBLE,
            "Gamma_TP9" DOUBLE, "Gamma_AF7" DOUBLE, "Gamma_AF8" DOUBLE, "Gamma_TP10" DOUBLE,
            "RAW_TP9" DOUBLE, "RAW_AF7" DOUBLE, "RAW_AF8" DOUBLE, "RAW_TP10" DOUBLE,
            "AUX_RIGHT" DOUBLE, "AUX_LEFT" DOUBLE,
            "Accelerometer_X" DOUBLE, "Accelerometer_Y" DOUBLE, "Accelerometer_Z" DOUBLE,
            "Gyro_X" DOUBLE, "Gyro_Y" DOUBLE, "Gyro_Z" DOUBLE,
            "PPG_Ambient" DOUBLE, "PPG_IR" DOUBLE, "PPG_Red" DOUBLE,
            "Heart_Rate" DOUBLE,
            "HeadBandOn" INT,
            "HSI_TP9" DOUBLE, "HSI_AF7" DOUBLE, "HSI_AF8" DOUBLE, "HSI_TP10" DOUBLE,
            "Battery" DOUBLE,
            "Elements" VARCHAR,
            PRIMARY KEY ("TimeStamp")
        );
    """
    )

    con.execute("INSTALL zipfs FROM community; LOAD zipfs;")
    result = con.execute(
        f"""
        INSERT INTO {table_name} BY NAME
        SELECT *
        FROM read_csv(
            'zip://{csv_dir}/*.zip/*.csv',
            header=true,
            union_by_name=true,
            null_padding=true,
            strict_mode=false
        )
        WHERE "TimeStamp" IS NOT NULL
        ON CONFLICT ("TimeStamp") DO NOTHING;
    """
    )
    print(f"Rows inserted: {result.fetchall()}")


if __name__ == "__main__":
    main()
