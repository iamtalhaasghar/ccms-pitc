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
    upsert_bill_months,
)

API_URL_TEMPLATE = "https://ccms.pitc.com.pk/get-loadinfo/{ref_no}"
API_BILL_URL_TEMPLATE = "https://ccms.pitc.com.pk/api/details/bill?reference={ref_no}"


def _get_data0(root: dict) -> dict:
    return root.get("load", [{}])[0].get("response", {}).get("data", [{}])[0] or {}


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_debug_response() -> dict:
    path = Path(__file__).parent / "responses" / "get-loadinfo.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _load_debug_bill_response() -> dict:
    path = Path(__file__).parent / "responses" / "bill.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_hist_month(label: str) -> datetime.date:
    """'Mar-25' -> date(2025, 3, 1)"""
    return datetime.datetime.strptime(label, "%b-%y").replace(day=1).date()


def _parse_bill_rows(data: dict) -> list[dict]:
    bill = data.get("bill", {})
    basic = bill.get("basicInfo", {})
    hist  = bill.get("histInfo", {})
    meters = bill.get("metersInfo", [{}])
    meter = meters[0] if meters else {}

    rows: dict[datetime.date, dict] = {}

    # --- histInfo: up to 13 months ---
    i = 1
    while f"gbHistMM{i}" in hist:
        try:
            month = _parse_hist_month(hist[f"gbHistMM{i}"])
            units = int(hist.get(f"gbHistUnits{i}"))
            cost  = int(hist.get(f"payment{i}"))
            rows[month] = {"month": month, "units": units, "cost": cost,
                           "prev_read": None, "pres_read": None}
        except (ValueError, TypeError):
            pass
        i += 1

    # --- basicInfo: current month ---
    try:
        bill_month_raw = basic.get("billMonth")
        cur_month = datetime.datetime.fromisoformat(bill_month_raw).replace(day=1).date()
        cur_units = int(basic.get("totCurCons"))
        cur_cost  = int(basic.get("currAmntDue"))
        prev_read = int(meter.get("mtrKwhPrvRead")) or None
        pres_read = int(meter.get("mtrKwhPrsRead")) or None

        if cur_month in rows:
            rows[cur_month]["prev_read"] = prev_read
            rows[cur_month]["pres_read"] = pres_read
            if cur_units:
                rows[cur_month]["units"] = cur_units
            if cur_cost:
                rows[cur_month]["cost"] = cur_cost
        else:
            rows[cur_month] = {"month": cur_month, "units": cur_units, "cost": cur_cost,
                               "prev_read": prev_read, "pres_read": pres_read}
    except (ValueError, TypeError):
        pass

    return list(rows.values())


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
    today = datetime.datetime.now()
    out_dir = Path("/var/lib/pitc") / today.strftime("%Y") / today.strftime("%m") / today.strftime("%d")
    
    out_dir.mkdir(parents=True, exist_ok=True)
    

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

        # --- Bill ---
        if debug:
            bill_data = _load_debug_bill_response()
            logger.info("DEBUG=true: using responses/bill.json")
        else:
            bill_url = API_BILL_URL_TEMPLATE.format(ref_no=ref_no)
            session2 = _session_with_user_agent()
            logger.info("Bill request User-Agent: %s", session2.headers.get("User-Agent"))
            bill_resp = session2.get(bill_url, timeout=30)
            bill_resp.raise_for_status()
            bill_data = bill_resp.json()

            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            bill_path = out_dir / f"bill-{ts}.json"
            bill_path.write_text(json.dumps(bill_data, indent=4), encoding="utf-8")
            logger.info("Saved bill response to %s", bill_path)

        bill_rows = _parse_bill_rows(bill_data)
        b = upsert_bill_months(bill_rows)
        logger.info("Inserted/updated bill rows: %s", b)

    except Exception:
        logger.exception("Run failed")
        raise


if __name__ == "__main__":
    main()
