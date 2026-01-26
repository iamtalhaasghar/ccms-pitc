import json
import os
from pathlib import Path
import datetime
import logging

import requests
from dotenv import load_dotenv
from fake_useragent import UserAgent

from db import (
    ensure_db_and_tables,
    insert_event_logs,
    parse_day_key,
    upsert_history,
    upsert_maintenance,
    upsert_tripping,
    upsert_maintenance_sch,
)

API_URL_TEMPLATE = "https://ccms.pitc.com.pk/get-loadinfo/{ref_no}"


def _get_data0(root: dict) -> dict:
    return root.get("load", [{}])[0].get("response", {}).get("data", [{}])[0] or {}


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_debug_response() -> dict:
    path = Path(__file__).parent / "responses" / "get-loadinfo.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _setup_logging() -> logging.Logger:
    log_path = Path("/var/log/pitc/app.log")
       
    logger = logging.getLogger("ccms_pitc")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)

        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(fmt)

        logger.addHandler(fh)
        logger.addHandler(sh)

    return logger


def _session_with_user_agent() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UserAgent().random})
    return s


def main():
    logger = _setup_logging()
    load_dotenv()

    debug = _is_truthy(os.getenv("DEBUG"))

    try:
        if debug:
            data = _load_debug_response()
            logger.info("DEBUG=true: using responses/get-loadinfo.json")
        else:
            ref_no = os.getenv("REF_NO")
            if not ref_no:
                raise SystemExit("Missing REF_NO. Set it in .env (e.g., REF_NO=12345).")

            url = API_URL_TEMPLATE.format(ref_no=ref_no)

            session = _session_with_user_agent()
            logger.info("Request User-Agent (session): %s", session.headers.get("User-Agent"))
            resp = session.get(url, timeout=30)
            resp.raise_for_status()

            data = resp.json()

            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            
            out_dir = Path("/var/lib/pitc")
            
            out_path = out_dir / f"get-loadinfo-{ts}.json"
            out_path.write_text(json.dumps(data, indent=4), encoding="utf-8")
            logger.info("Saved response to %s", out_path)

        ensure_db_and_tables()

        data0 = _get_data0(data)

        event_logs = data0.get("event_logs", []) or []
        n = insert_event_logs(event_logs)
        logger.info("Inserted/updated event_logs rows: %s", n)

        history_data = data0.get("history_data", {}) or {}
        h_total = 0
        for day_key, minutes_by_hour in history_data.items():
            h_total += upsert_history(parse_day_key(day_key), minutes_by_hour)
        logger.info("Inserted/updated history rows: %s", h_total)

        maintenance_data = data0.get("maintenance_data", {}) or {}
        m_total = 0
        for day_key, minutes_by_hour in maintenance_data.items():
            m_total += upsert_maintenance(parse_day_key(day_key), minutes_by_hour)
        logger.info("Inserted/updated maintenance rows: %s", m_total)

        cdate = data0.get("cdate")
        if cdate:
            day = datetime.datetime.strptime(cdate, "%Y-%m-%d %H:%M:%S").date()

            tripping = data0.get("tripping", []) or []
            t = upsert_tripping(day, tripping)
            logger.info("Inserted/updated tripping rows: %s", t)

            maintenance_sch = data0.get("maintenance_sch", []) or []
            ms = upsert_maintenance_sch(day, maintenance_sch)
            logger.info("Inserted/updated maintenance_sch rows: %s", ms)
        else:
            logger.warning("Skipping tripping/maintenance_sch upsert: missing cdate")

    except Exception:
        logger.exception("Run failed")
        raise


if __name__ == "__main__":
    main()
