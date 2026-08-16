#!/usr/bin/env python3

import argparse
import csv
import getpass
import io
import logging
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import cast

import psycopg
from jinja2 import Environment, FileSystemLoader
from psycopg import sql
from typing_extensions import LiteralString

TIMESCALE_SCHEMA_TEMPLATE = "mindmonitor_timescaledb_schema.jinja.sql"

TIMESCALE_PROVISION_TEMPLATE = "provision_timescale_db.jinja.sql"

logger = logging.getLogger(__name__)

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


def repair_row(row: list[str]) -> list[str | None]:
    expected_cols = len(CSV_COLUMNS)
    fixed_row = cast(list[str | None], row)
    if len(fixed_row) < expected_cols:
        fixed_row.extend([None] * (expected_cols - len(fixed_row)))

    fixed_row = row[:expected_cols]

    parsed_row = []
    for idx, val in enumerate(fixed_row):
        val_str = val.strip() if val else ""
        col_name = CSV_COLUMNS[idx]

        if not val_str:
            parsed_row.append(None)
            continue

        # Convert Timestamp to native Python datetime
        if col_name == "TimeStamp":
            try:
                # Adjust format if your Mind Monitor CSVs use a different date layout
                parsed_row.append(datetime.fromisoformat(val_str))
            except ValueError:
                # Fallback parser if space-separated
                parsed_row.append(datetime.strptime(val_str, "%Y-%m-%d %H:%M:%S.%f"))
        # Convert HeadBandOn to integer
        elif col_name == "HeadBandOn":
            parsed_row.append(int(float(val_str)))
        # Keep Elements as string
        elif col_name == "Elements":
            parsed_row.append(val_str)
        # All numeric sensor readings to float
        else:
            parsed_row.append(float(val_str))

    return parsed_row


def load_sops_env(path: Path) -> None:
    logger.info("Decrypting environment file: %s", path)
    if not path.is_file():
        raise SystemExit(f"SOPS environment file does not exist: {path}")
    try:
        result = subprocess.run(
            ["sops", "-d", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as error:
        raise SystemExit(
            "The 'sops' executable is required to decrypt the secrets file"
        ) from error
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"Unable to decrypt {path}: {error.stderr.strip()}") from error

    load_env_lines(result.stdout.splitlines())
    logger.debug("SOPS environment file loaded")


def load_env_file(path: Path) -> None:
    logger.info("Loading environment file: %s", path)
    if not path.is_file():
        raise SystemExit(f"Environment file does not exist: {path}")
    load_env_lines(path.read_text(encoding="utf-8").splitlines())


def load_env_lines(lines: list[str]) -> None:
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Mind Monitor CSV files into TimescaleDB."
    )
    parser.add_argument(
        "--csv-file",
        required=True,
        help="CSV file to load",
    )

    parser.add_argument(
        "--table-name",
        default="mind_monitor",
        help="Destination table name",
        type=str,
    )
    parser.add_argument(
        "--drop-table", action="store_true", help="Drop the table before loading"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Provision the database and users before loading",
    )
    parser.add_argument(
        "--secrets-file",
        default=None,
        help="Optional SOPS-encrypted environment file",
    )
    parser.add_argument(
        "--superuser",
        default=None,
        help="Provisioning superuser name; defaults to TIMESCALE_SUPERUSER",
    )
    parser.add_argument(
        "--superuser-password",
        default=None,
        help="Provisioning superuser password; otherwise prompt or use TIMESCALE_SUPERUSER_PASSWORD",
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.table_name):
        parser.error("table name must be a valid SQL identifier")
    return args


def get_database_config(*, require_superuser: bool = False) -> dict[str, str | None]:
    config = {
        "endpoint": os.environ.get("TIMESCALE_DB_ENDPOINT", "localhost"),
        "port": os.environ.get("TIMESCALE_DB_PORT", "5432"),
        "database": os.environ.get("TIMESCALE_DB_NAME"),
        "app_user": os.environ.get("TIMESCALE_DB_USER"),
        "app_password": os.environ.get("TIMESCALE_DB_PASS"),
        "superuser": os.environ.get("TIMESCALE_SUPERUSER", "postgres"),
        "superuser_password": os.environ.get("TIMESCALE_SUPERUSER_PASSWORD"),
        "read_user": os.environ.get("TIMESCALE_DB_READ_USER"),
        "read_password": os.environ.get("TIMESCALE_DB_READ_USER_PASS"),
    }
    required = {
        "TIMESCALE_DB_NAME": config["database"],
        "TIMESCALE_DB_USER": config["app_user"],
        "TIMESCALE_DB_PASS": config["app_password"],
    }
    if require_superuser:
        required["TIMESCALE_SUPERUSER"] = config["superuser"]
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise SystemExit(f"Missing database settings: {', '.join(missing)}")
    return config


def provision_timescaledb(args: argparse.Namespace) -> None:
    logger.debug("Validating provisioning database settings")
    config = get_database_config(require_superuser=True)
    superuser = args.superuser or config["superuser"]
    password = args.superuser_password or config["superuser_password"]
    if not password:
        password = getpass.getpass(f"Password for TimescaleDB superuser {superuser}: ")

    endpoint = config["endpoint"]
    port = config["port"]
    database = config["database"]
    app_user = config["app_user"]
    app_password = config["app_password"]
    read_user = config["read_user"]
    read_password = config["read_password"]
    provision_read_user = bool(read_user and read_password)

    database_sql = render_sql(
        TIMESCALE_PROVISION_TEMPLATE,
        include_database=True,
        include_users=False,
        provision_read_user=provision_read_user,
        database=database,
        app_user=app_user,
        app_password=app_password,
        read_user=read_user,
        read_password=read_password,
    )

    admin_info = {
        "host": cast(str, endpoint),
        "port": cast(str, port),
        "dbname": "postgres",
        "user": cast(str, superuser),
        "password": cast(str, password),
    }

    logger.debug("Creating database if needed")
    admin_conninfo = psycopg.conninfo.make_conninfo(**admin_info)
    with psycopg.connect(admin_conninfo, autocommit=True) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {};").format(
                        sql.Identifier(cast(str, database))
                    )
                )
            except psycopg.errors.DuplicateDatabase:
                logger.info("Database already exists")

    logger.debug("Reconnecting to database and provisioning")
    # Reconnect to connect to the database
    database_info = {**admin_info, "dbname": cast(str, database)}
    database_conninfo = psycopg.conninfo.make_conninfo(**database_info)
    with psycopg.connect(database_conninfo, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(database_sql)


def render_sql(template_name: str, **values: object) -> LiteralString:
    logger.debug("Rendering SQL template: %s", template_name)
    sql_dir = Path(__file__).resolve().parent / "sql"
    environment = Environment(loader=FileSystemLoader(sql_dir), autoescape=False)
    environment.filters["identifier"] = lambda value: (
        '"' + str(value).replace('"', '""') + '"'
    )
    environment.filters["literal"] = lambda value: (
        "'" + str(value).replace("'", "''") + "'"
    )
    renderd_sql = environment.get_template(template_name).render(**values)
    logger.debug("Rendered SQL: %s", renderd_sql)
    return cast(LiteralString, renderd_sql)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    logger.info("Starting Mind Monitor CSV load")
    workspace_dir = Path(__file__).resolve().parents[1]
    if args.secrets_file:
        load_sops_env(workspace_dir / args.secrets_file)
    else:
        env_file = workspace_dir / ".env"
        if env_file.is_file():
            load_env_file(env_file)
        else:
            logger.info("Using process environment for database settings")

    if args.provision:
        logger.info("Provisioning database")
        provision_timescaledb(args)

    csv_file = Path(args.csv_file)
    if not csv_file.is_file():
        raise SystemExit(f"CSV file does not exist: {csv_file}")

    logger.debug("Validating application database settings")
    config = get_database_config()
    connection_string: str = psycopg.conninfo.make_conninfo(
        host=config["endpoint"],
        port=config["port"],
        dbname=config["database"],
        user=config["app_user"],
        password=config["app_password"],
        sslmode="require",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    schema_sql = render_sql(
        TIMESCALE_SCHEMA_TEMPLATE,
        table_name=args.table_name,
        drop_table=args.drop_table,
    )
    logger.info("Opening database connection for %s", csv_file)
    # Keep schema setup and each file load in explicit transactions.
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            if args.drop_table:
                logger.info("Recreating db table")
            logger.info("Applying database schema")
            cur.execute(schema_sql)
            conn.commit()
            logger.info("Database schema ready")

            logger.info("Loading CSV: %s", csv_file)
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(args.table_name),
                sql.SQL(", ").join(sql.Identifier(column) for column in CSV_COLUMNS),
            )

            row_count = 0
            with csv_file.open("rb") as source:
                text_stream = io.TextIOWrapper(source, encoding="utf-8-sig")
                reader = csv.reader(text_stream)
                # next(reader, None)

                with cur.copy(copy_sql) as copy:
                    for _, row in enumerate(reader, start=1):
                        clean_row = repair_row(row)
                        copy.write_row(clean_row)
                        row_count += 1
                        if row_count % 1000 == 0:
                            logger.debug("Streamed %d rows", row_count)

            conn.commit()
            logger.info(
                "Successfully loaded and committed %d rows from %s", row_count, csv_file
            )

    logger.info("CSV load complete")


if __name__ == "__main__":
    main()
