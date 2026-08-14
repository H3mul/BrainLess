#!/usr/bin/env python3

import argparse
import csv
import glob
import os
import re
import zipfile
from pathlib import Path

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
        default=None,
        help="Database path or connection string (TimescaleDB defaults to TIMESCALEDB_URL)",
    )
    parser.add_argument(
        "--table-name",
        default="mind_monitor",
        help="Destination table name (default: mind_monitor)",
    )
    parser.add_argument(
        "--db-type",
        choices=("duckdb", "timescaledb"),
        default="timescaledb",
        help="Database backend (default: timescaledb)",
    )
    parser.add_argument(
        "--drop-table",
        action="store_true",
        help="Drop the destination table before loading; preserves existing data by default",
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.table_name):
        parser.error("table name must be a valid SQL identifier")
    return args


CSV_COLUMNS = [
    "TimeStamp",
    "Delta_TP9",
    "Delta_AF7",
    "Delta_AF8",
    "Delta_TP10",
    "Theta_TP9",
    "Theta_AF7",
    "Theta_AF8",
    "Theta_TP10",
    "Alpha_TP9",
    "Alpha_AF7",
    "Alpha_AF8",
    "Alpha_TP10",
    "Beta_TP9",
    "Beta_AF7",
    "Beta_AF8",
    "Beta_TP10",
    "Gamma_TP9",
    "Gamma_AF7",
    "Gamma_AF8",
    "Gamma_TP10",
    "RAW_TP9",
    "RAW_AF7",
    "RAW_AF8",
    "RAW_TP10",
    "AUX_RIGHT",
    "AUX_LEFT",
    "Accelerometer_X",
    "Accelerometer_Y",
    "Accelerometer_Z",
    "Gyro_X",
    "Gyro_Y",
    "Gyro_Z",
    "PPG_Ambient",
    "PPG_IR",
    "PPG_Red",
    "Heart_Rate",
    "HeadBandOn",
    "HSI_TP9",
    "HSI_AF7",
    "HSI_AF8",
    "HSI_TP10",
    "Battery",
    "Elements",
]

NUMERIC_COLUMNS = set(CSV_COLUMNS[1:-1]) - {"Elements"}


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_timescaledb(
    csv_dir: str, db_path: str | None, table_name: str, drop_table: bool
) -> None:
    try:
        import psycopg
    except ImportError as error:
        raise SystemExit(
            "TimescaleDB support requires the 'psycopg' package"
        ) from error

    numeric_definitions = {
        column: ("INTEGER" if column == "HeadBandOn" else "DOUBLE PRECISION")
        for column in NUMERIC_COLUMNS
    }
    definitions = ['"TimeStamp" TIMESTAMPTZ']
    definitions.extend(
        f'"{column}" {numeric_definitions[column]}' for column in CSV_COLUMNS[1:-1]
    )
    definitions.append('"Elements" TEXT')

    connection_string = f"postgresql://{os.environ.get('TIMESCALE_DB_USER')}:{os.environ.get('TIMESCALE_DB_PASS')}@{os.environ.get('TIMESCALE_DB_ENDPOINT')}:5432/{os.environ.get('TIMESCALE_DB_NAME')}"

    if not connection_string:
        raise SystemExit(
            "TimescaleDB requires --db-path or TIMESCALEDB_URL in workspace/.env"
        )
    with psycopg.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            if drop_table:
                cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(definitions)}, '
                'PRIMARY KEY ("TimeStamp", "Elements"))'
            )
            cursor.execute(
                "SELECT create_hypertable(%s, %s, if_not_exists => TRUE)",
                (table_name, "TimeStamp"),
            )

            placeholders = ", ".join(["%s"] * len(CSV_COLUMNS))
            columns = ", ".join(f'"{column}"' for column in CSV_COLUMNS)
            insert_sql = (
                f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders}) '
                'ON CONFLICT ("TimeStamp", "Elements") DO NOTHING'
            )

            inserted = 0
            for archive_path in sorted(glob.glob(os.path.join(csv_dir, "*.zip"))):
                with zipfile.ZipFile(archive_path) as archive:
                    for member in archive.namelist():
                        if not member.lower().endswith(".csv"):
                            continue
                        with archive.open(member) as source:
                            rows = csv.DictReader(
                                (line.decode("utf-8-sig") for line in source)
                            )
                            for row in rows:
                                if not row.get("TimeStamp") or not row.get("Elements"):
                                    continue
                                values = []
                                for column in CSV_COLUMNS:
                                    value = row.get(column, "")
                                    values.append(None if value == "" else value)
                                cursor.execute(insert_sql, values)
                                inserted += cursor.rowcount
            connection.commit()
            print(f"Rows inserted: {inserted}")


def main() -> None:
    load_env_file(Path(__file__).resolve().parents[1] / ".env")
    args = parse_args()
    csv_dir = args.csv_dir if args.csv_dir.endswith("/") else f"{args.csv_dir}/"
    if not os.path.isdir(csv_dir):
        raise SystemExit(
            f"CSV directory does not exist or is not a directory: {csv_dir}"
        )

    db_path = args.db_path
    table_name = args.table_name

    if args.db_type == "timescaledb":
        load_timescaledb(csv_dir, db_path, table_name, args.drop_table)
        return

    con = duckdb.connect(database=db_path or "./data/eeg.duckdb")

    con.execute(
        f"""
        {f"DROP TABLE IF EXISTS {table_name};" if args.drop_table else ""}
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
