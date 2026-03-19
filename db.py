import datetime
import os
from typing import Iterable, Mapping, Any

import pymysql

DB_NAME = "pitc"
EVENT_LOGS_TABLE = "event_logs"
HISTORY_TABLE = "history"
TRIPPING_TABLE = "tripping"
MAINTENANCE_TABLE = "maintenance"
MAINTENANCE_SCH_TABLE = "maintenance_sch"
BILL_TABLE = "bill"


def _get_env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v else default


def _connect(db: str | None = None):
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=_get_env_int("MYSQL_PORT", 3306),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=db,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_db_and_tables():
    with _connect(None) as conn, conn.cursor() as cur:
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )

    with _connect(DB_NAME) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{EVENT_LOGS_TABLE}` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `event_time` DATETIME NOT NULL,
                `event` VARCHAR(32) NOT NULL,
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uq_event_time` (`event_time`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """.strip()
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{HISTORY_TABLE}` (
                `day` DATE NOT NULL,
                `hour` TINYINT NOT NULL,
                `minutes_out` TINYINT NOT NULL,
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`day`, `hour`),
                CONSTRAINT `chk_history_hour` CHECK (`hour` >= 0 AND `hour` <= 23),
                CONSTRAINT `chk_history_minutes_out` CHECK (`minutes_out` >= 0 AND `minutes_out` <= 60)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """.strip()
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{TRIPPING_TABLE}` (
                `day` DATE NOT NULL,
                `hour` TINYINT NOT NULL,
                `minutes_out` TINYINT NOT NULL,
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`day`, `hour`),
                CONSTRAINT `chk_tripping_hour` CHECK (`hour` >= 0 AND `hour` <= 23),
                CONSTRAINT `chk_tripping_minutes_out` CHECK (`minutes_out` >= 0 AND `minutes_out` <= 60)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """.strip()
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{MAINTENANCE_TABLE}` (
                `day` DATE NOT NULL,
                `hour` TINYINT NOT NULL,
                `minutes_out` TINYINT NOT NULL,
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`day`, `hour`),
                CONSTRAINT `chk_maintenance_hour` CHECK (`hour` >= 0 AND `hour` <= 23),
                CONSTRAINT `chk_maintenance_minutes_out` CHECK (`minutes_out` >= 0 AND `minutes_out` <= 60)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """.strip()
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{MAINTENANCE_SCH_TABLE}` (
                `day` DATE NOT NULL,
                `hour` TINYINT NOT NULL,
                `minutes_out` TINYINT NOT NULL,
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`day`, `hour`),
                CONSTRAINT `chk_maintenance_sch_hour` CHECK (`hour` >= 0 AND `hour` <= 23),
                CONSTRAINT `chk_maintenance_sch_minutes_out` CHECK (`minutes_out` >= 0 AND `minutes_out` <= 60)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """.strip()
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{BILL_TABLE}` (
                `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                `month` DATE NOT NULL,
                `units` INT NOT NULL DEFAULT 0,
                `cost` INT NOT NULL DEFAULT 0,
                `prev_read` INT NULL,
                `pres_read` INT NULL,
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uq_bill_month` (`month`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """.strip()
        )


def parse_day_key(key: str) -> datetime.date:
    if not key.startswith("dt_") or len(key) != 11:
        raise ValueError(f"Invalid day key: {key!r}")
    yyyymmdd = key[3:]
    try:
        return datetime.datetime.strptime(yyyymmdd, "%Y%m%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid day key: {key!r}") from e


def _validate_minutes_by_hour(minutes_by_hour: list[int]) -> None:
    if len(minutes_by_hour) != 24:
        raise ValueError(f"minutes_by_hour must have length 24, got {len(minutes_by_hour)}")
    for i, v in enumerate(minutes_by_hour):
        if not isinstance(v, int):
            raise ValueError(f"minutes_by_hour[{i}] must be int, got {type(v).__name__}")
        if v < 0 or v > 60:
            raise ValueError(f"minutes_by_hour[{i}] must be in range 0..60, got {v}")


def _upsert_hourly_minutes(table: str, day: datetime.date, minutes_by_hour: list[int]) -> int:
    _validate_minutes_by_hour(minutes_by_hour)

    sql = f"""
        INSERT INTO `{table}` (`day`, `hour`, `minutes_out`)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            minutes_out = CASE
                WHEN minutes_out = 0 AND VALUES(minutes_out) <> 0 THEN VALUES(minutes_out)
                ELSE minutes_out
            END
    """.strip()

    values = [(day, hour, minutes_by_hour[hour]) for hour in range(24)]

    with _connect(DB_NAME) as conn, conn.cursor() as cur:
        cur.executemany(sql, values)
        return cur.rowcount


def upsert_history(day: datetime.date, minutes_by_hour: list[int]) -> int:
    return _upsert_hourly_minutes(HISTORY_TABLE, day, minutes_by_hour)


def upsert_tripping(day: datetime.date, minutes_by_hour: list[int]) -> int:
    return _upsert_hourly_minutes(TRIPPING_TABLE, day, minutes_by_hour)


def upsert_maintenance(day: datetime.date, minutes_by_hour: list[int]) -> int:
    return _upsert_hourly_minutes(MAINTENANCE_TABLE, day, minutes_by_hour)


def upsert_maintenance_sch(day: datetime.date, minutes_by_hour: list[int]) -> int:
    return _upsert_hourly_minutes(MAINTENANCE_SCH_TABLE, day, minutes_by_hour)


def upsert_bill_months(rows: list[dict]) -> int:
    """
    Each row: {month: date, units: int, cost: int, prev_read: int|None, pres_read: int|None}
    Upsert rule: on duplicate month, update units/cost only if incoming values are non-zero.
                 prev_read/pres_read updated only when provided (not None).
    """
    if not rows:
        return 0

    sql = f"""
        INSERT INTO `{BILL_TABLE}` (`month`, `units`, `cost`, `prev_read`, `pres_read`)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            units     = CASE WHEN VALUES(units) <> 0 THEN VALUES(units) ELSE units END,
            cost      = CASE WHEN VALUES(cost)  <> 0 THEN VALUES(cost)  ELSE cost  END,
            prev_read = CASE WHEN VALUES(prev_read) IS NOT NULL THEN VALUES(prev_read) ELSE prev_read END,
            pres_read = CASE WHEN VALUES(pres_read) IS NOT NULL THEN VALUES(pres_read) ELSE pres_read END
    """.strip()

    values = [
        (r["month"], r["units"], r["cost"], r.get("prev_read"), r.get("pres_read"))
        for r in rows
    ]

    with _connect(DB_NAME) as conn, conn.cursor() as cur:
        cur.executemany(sql, values)
        return cur.rowcount


def insert_event_logs(rows: Iterable[Mapping[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    sql = f"""
        INSERT INTO `{EVENT_LOGS_TABLE}` (`event_time`, `event`)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            `event` = VALUES(`event`)
    """.strip()

    values = [(r.get("event_time"), r.get("event")) for r in rows]

    with _connect(DB_NAME) as conn, conn.cursor() as cur:
        cur.executemany(sql, values)
        return cur.rowcount
