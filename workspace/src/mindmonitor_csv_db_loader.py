#!/usr/bin/env python3

import argparse
import csv
import getpass
import io
import logging
import os
import re
import subprocess
import tarfile
import zipfile
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, cast

import psycopg
from jinja2 import Environment, FileSystemLoader
from psycopg import sql
from psycopg.conninfo import make_conninfo
from typing_extensions import LiteralString

CsvReader = Iterator[list[str]]
CsvSource = tuple[str, CsvReader, int]

TIMESCALE_SCHEMA_TEMPLATE = "mindmonitor_timescaledb_schema.jinja.sql"
TIMESCALE_INSERT_TEMPLATE = "mindmonitor_timescaledb_insert.jinja.sql"
TIMESCALE_PROVISION_TEMPLATE = "provision_timescale_db.jinja.sql"

CSV_COLUMNS = {
    "time": "TIMESTAMPTZ NOT NULL",
    "Delta_TP9": "DOUBLE PRECISION",
    "Delta_AF7": "DOUBLE PRECISION",
    "Delta_AF8": "DOUBLE PRECISION",
    "Delta_TP10": "DOUBLE PRECISION",
    "Theta_TP9": "DOUBLE PRECISION",
    "Theta_AF7": "DOUBLE PRECISION",
    "Theta_AF8": "DOUBLE PRECISION",
    "Theta_TP10": "DOUBLE PRECISION",
    "Alpha_TP9": "DOUBLE PRECISION",
    "Alpha_AF7": "DOUBLE PRECISION",
    "Alpha_AF8": "DOUBLE PRECISION",
    "Alpha_TP10": "DOUBLE PRECISION",
    "Beta_TP9": "DOUBLE PRECISION",
    "Beta_AF7": "DOUBLE PRECISION",
    "Beta_AF8": "DOUBLE PRECISION",
    "Beta_TP10": "DOUBLE PRECISION",
    "Gamma_TP9": "DOUBLE PRECISION",
    "Gamma_AF7": "DOUBLE PRECISION",
    "Gamma_AF8": "DOUBLE PRECISION",
    "Gamma_TP10": "DOUBLE PRECISION",
    "RAW_TP9": "DOUBLE PRECISION",
    "RAW_AF7": "DOUBLE PRECISION",
    "RAW_AF8": "DOUBLE PRECISION",
    "RAW_TP10": "DOUBLE PRECISION",
    "AUX_RIGHT": "DOUBLE PRECISION",
    "AUX_LEFT": "DOUBLE PRECISION",
    "Accelerometer_X": "DOUBLE PRECISION",
    "Accelerometer_Y": "DOUBLE PRECISION",
    "Accelerometer_Z": "DOUBLE PRECISION",
    "Gyro_X": "DOUBLE PRECISION",
    "Gyro_Y": "DOUBLE PRECISION",
    "Gyro_Z": "DOUBLE PRECISION",
    "PPG_Ambient": "DOUBLE PRECISION",
    "PPG_IR": "DOUBLE PRECISION",
    "PPG_Red": "DOUBLE PRECISION",
    "Heart_Rate": "DOUBLE PRECISION",
    "HeadBandOn": "INTEGER",
    "HSI_TP9": "DOUBLE PRECISION",
    "HSI_AF7": "DOUBLE PRECISION",
    "HSI_AF8": "DOUBLE PRECISION",
    "HSI_TP10": "DOUBLE PRECISION",
    "Battery": "DOUBLE PRECISION",
    "Elements": "TEXT",
}

logger = logging.getLogger(__name__)


def repair_row(row: list[str]) -> list[object | None]:
    expected_cols = len(CSV_COLUMNS)
    fixed_row = cast(list[str | None], row[:expected_cols])
    if len(fixed_row) < expected_cols:
        fixed_row.extend([None] * (expected_cols - len(fixed_row)))

    parsed_row: list[object | None] = []
    for (_, column_type), val in zip(CSV_COLUMNS.items(), fixed_row):
        val_str = val.strip() if val else ""

        if not val_str:
            parsed_row.append(None)
            continue

        # Convert timestamp to native Python datetime.
        if column_type.startswith("TIMESTAMPTZ"):
            try:
                parsed_row.append(datetime.fromisoformat(val_str))
            except ValueError:
                parsed_row.append(datetime.strptime(val_str, "%Y-%m-%d %H:%M:%S.%f"))
        # Convert integer columns to integers.
        elif column_type == "INTEGER":
            parsed_row.append(int(float(val_str)))
        # Keep text columns as strings.
        elif column_type == "TEXT":
            parsed_row.append(val_str)
        # All numeric sensor readings to float
        else:
            parsed_row.append(float(val_str))

    return parsed_row


def split_paths(values: list[str]) -> list[Path]:
    return [
        Path(value.strip())
        for item in values
        for value in item.split(",")
        if value.strip()
    ]


def collect_csv_files(csv_file_args: list[str], csv_dir_args: list[str]) -> list[Path]:
    files = split_paths(csv_file_args)
    for directory in split_paths(csv_dir_args):
        if not directory.is_dir():
            raise SystemExit(f"CSV directory does not exist: {directory}")
        files.extend(
            path
            for path in sorted(directory.rglob("*"))
            if path.is_file()
            and (
                path.name.lower().endswith(".csv")
                or path.name.lower().endswith(".csv.tar.gz")
                or path.name.lower().endswith(".zip")
            )
        )

    if not files:
        raise SystemExit("Provide --csv-file or --csv-dir")
    for path in files:
        if not path.is_file():
            raise SystemExit(f"CSV source does not exist: {path}")
    return files


def _count_rows(stream: BinaryIO) -> int:
    text_stream = io.TextIOWrapper(stream, encoding="utf-8-sig")
    return max(0, sum(1 for _ in csv.reader(text_stream)) - 1)


def _reader(stream: BinaryIO) -> CsvReader:
    text_stream = io.TextIOWrapper(stream, encoding="utf-8-sig")
    reader = csv.reader(text_stream)
    # Consume csv header
    next(reader, None)
    return reader


def create_csv_readers(paths: list[Path]) -> list[CsvSource]:
    readers: list[CsvSource] = []
    for path in paths:
        lower_name = path.name.lower()
        if lower_name.endswith(".zip"):
            archive = zipfile.ZipFile(path)
            for member in archive.infolist():
                if member.filename.lower().endswith(".csv"):
                    with archive.open(member) as source:
                        total_rows = _count_rows(cast(BinaryIO, source))
                    readers.append(
                        (
                            f"{path}!{member.filename}",
                            _reader(cast(BinaryIO, archive.open(member))),
                            total_rows,
                        )
                    )
        elif lower_name.endswith(".csv.tar.gz"):
            archive = tarfile.open(path, "r:gz")
            for member in archive.getmembers():
                if member.isfile() and member.name.lower().endswith(".csv"):
                    stream = archive.extractfile(member)
                    if stream is not None:
                        total_rows = _count_rows(cast(BinaryIO, stream))
                        stream = archive.extractfile(member)
                        if stream is None:
                            continue
                        readers.append(
                            (
                                f"{path}!{member.name}",
                                _reader(cast(BinaryIO, stream)),
                                total_rows,
                            )
                        )
        elif lower_name.endswith(".csv"):
            with path.open("rb") as source:
                total_rows = _count_rows(cast(BinaryIO, source))
            readers.append(
                (
                    str(path),
                    _reader(cast(BinaryIO, path.open("rb"))),
                    total_rows,
                )
            )
        else:
            raise SystemExit(f"Unsupported CSV source: {path}")
    if not readers:
        raise SystemExit("No CSV files found in the provided sources")
    return readers


def load_csv_readers(
    cur: psycopg.Cursor,
    table_name: str,
    readers: list[CsvSource],
) -> int:
    temp_table_name = f"temp_{table_name}"
    copy_sql = render_sql(
        TIMESCALE_INSERT_TEMPLATE,
        table_name=temp_table_name,
        columns=CSV_COLUMNS,
    )
    total_rows = sum(total for _, _, total in readers)
    ingested_rows = 0
    for source_name, reader, source_total in readers:
        logger.debug("Loading CSV: %s", source_name)
        row_count = 0
        with cur.copy(copy_sql) as copy:
            for row in reader:
                copy.write_row(repair_row(row))
                row_count += 1
                if row_count % 10000 == 0:
                    logger.debug(
                        "Streamed %d rows to COPY from %s", row_count, source_name
                    )
        ingested_rows += row_count
        progress = ingested_rows / total_rows * 100 if total_rows else 100.0
        logger.info(
            "(%.1f%%) Added %d rows from %s",
            progress,
            row_count,
            source_name,
        )
    return ingested_rows


def insert_temp_rows(cur: psycopg.Cursor, table_name: str) -> int:
    temp_table = sql.Identifier(f"temp_{table_name}")
    target_table = sql.Identifier(table_name)
    statement = sql.SQL(
        'INSERT INTO {} SELECT * FROM {} ON CONFLICT ("time") DO NOTHING'
    ).format(target_table, temp_table)
    cur.execute(statement)
    return max(cur.rowcount, 0)


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
        action="append",
        default=[],
        help="CSV file(s), comma-separated or repeated",
    )
    parser.add_argument(
        "--csv-dir",
        action="append",
        default=[],
        help="Directory containing CSV sources, comma-separated or repeated",
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
    admin_conninfo = make_conninfo(**admin_info)
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

    logger.info(f"Provisioning users and privileges")
    # Reconnect to connect to the database
    database_info = {**admin_info, "dbname": cast(str, database)}
    database_conninfo = make_conninfo(**database_info)
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
        if not args.csv_file and not args.csv_dir:
            logger.info("Provisioning complete; no CSV sources provided")
            return

    csv_paths = collect_csv_files(args.csv_file, args.csv_dir)

    logger.debug("Validating application database settings")
    config = get_database_config()
    connection_string = make_conninfo(
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
        columns=CSV_COLUMNS,
        drop_table=args.drop_table,
    )
    logger.info("Opening database connection for %d CSV source(s)", len(csv_paths))
    # Keep schema setup and each file load in explicit transactions.
    with psycopg.connect(connection_string) as conn:
        readers = create_csv_readers(csv_paths)
        with conn.transaction():
            with conn.cursor() as cur:
                if args.drop_table:
                    logger.info("Recreating db table")
                logger.info("Applying database schema")
                cur.execute(schema_sql)

                temp_table_sql = sql.SQL(
                    "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP"
                ).format(
                    sql.Identifier(f"temp_{args.table_name}"),
                    sql.Identifier(args.table_name),
                )
                logger.info("Creating temp table")
                cur.execute(temp_table_sql)
                ingested_rows = load_csv_readers(cur, args.table_name, readers)
                logger.info(
                    "Loaded %d rows from CSV into temp table, inserting into main table",
                    ingested_rows,
                )
                inserted_rows = insert_temp_rows(cur, args.table_name)
                logger.info(
                    "Final insert: %d deduplicated rows inserted from %d ingested rows",
                    inserted_rows,
                    ingested_rows,
                )

    logger.info("CSV load complete")


if __name__ == "__main__":
    main()
