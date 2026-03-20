"""
BTS T-100 Domestic Segment Collector

Fetches passenger counts, ASMs, RPMs, load factors, and departures
from the Bureau of Transportation Statistics T-100 Domestic Segment database.

Release schedule: ~60-day lag (e.g., January data available mid-March)
URL: https://www.transtats.bts.gov/DL_SelectFields.aspx?Table_ID=311

Output: data/current/bts_traffic.json
"""

import json
import csv
import io
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

from backend.config import CARRIERS, TOP_AIRPORTS, data_file

logger = logging.getLogger(__name__)

BTS_API_BASE = "https://www.transtats.bts.gov/api"
BTS_DOWNLOAD_URL = "https://www.transtats.bts.gov/DownLoad_Table.asp"

# T-100 field IDs for domestic segment
T100_FIELDS = [
    "PASSENGERS",
    "DEPARTURES_PERFORMED",
    "SEATS",
    "AVAILABLE_SEAT_MILES",  # ASM
    "REV_PAX_MILES_140",     # RPM
    "UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "YEAR",
    "MONTH",
]


def get_latest_available_period() -> tuple[int, int]:
    """Estimate the latest BTS period available (~60-day lag)."""
    cutoff = date.today() - timedelta(days=60)
    return cutoff.year, cutoff.month


def collect_bts_traffic(year: int = None, month: int = None) -> dict[str, Any]:
    """
    Collect BTS T-100 domestic traffic data.

    In production, this would download from BTS or use their API.
    For now, returns the collection schema and instructions.

    Returns:
        dict with keys: market_totals, carrier_totals, airport_totals, routes
    """
    if year is None or month is None:
        year, month = get_latest_available_period()

    logger.info(f"Collecting BTS T-100 data for {year}-{month:02d}")

    # ── Collection template ──
    # In production, replace with actual BTS API call or CSV download parsing
    result = {
        "source": "BTS T-100 Domestic Segment",
        "period": f"{year}-{month:02d}",
        "collected_at": date.today().isoformat(),
        "market_totals": {
            "passengers_ttm": None,       # Rolling 12-month total
            "asm_total": None,             # Total available seat miles
            "rpm_total": None,             # Revenue passenger miles
            "load_factor": None,           # RPM / ASM
            "departures": None,            # Total departures performed
            "vs_2019_rpm_pct": None,       # Recovery vs 2019 baseline
        },
        "carrier_totals": {
            # Keyed by carrier code
            carrier: {
                "passengers": None,
                "asm": None,
                "rpm": None,
                "load_factor": None,
                "departures": None,
                "domestic_share_pct": None,
            }
            for carrier in CARRIERS
        },
        "airport_totals": {
            # Keyed by airport code
            airport: {
                "passengers": None,
                "departures": None,
                "yoy_pct": None,
                "dominant_carrier": None,
                "dominant_share_pct": None,
                "hhi": None,  # Herfindahl-Hirschman Index
            }
            for airport in TOP_AIRPORTS
        },
        "top_routes": [],  # Top 20 O&D pairs by passengers
        "_instructions": (
            "To populate: Download T-100 Domestic Segment from "
            "https://www.transtats.bts.gov/DL_SelectFields.aspx?Table_ID=311 "
            f"for period {year}-{month:02d}. Select fields: {', '.join(T100_FIELDS)}. "
            "Parse CSV and aggregate by carrier, airport, and O&D pair."
        ),
    }

    return result


def parse_bts_csv(csv_path: Path) -> dict[str, Any]:
    """
    Parse a downloaded BTS T-100 CSV file into structured data.

    Args:
        csv_path: Path to the downloaded CSV file

    Returns:
        Structured traffic data dict
    """
    logger.info(f"Parsing BTS CSV: {csv_path}")

    carrier_totals = {}
    airport_totals = {}
    route_totals = {}
    total_pax = 0
    total_asm = 0
    total_rpm = 0

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pax = int(row.get("PASSENGERS", 0) or 0)
            asm = int(row.get("AVAILABLE_SEAT_MILES", 0) or 0)
            rpm = int(row.get("REV_PAX_MILES_140", 0) or 0)
            carrier = row.get("UNIQUE_CARRIER", "")
            origin = row.get("ORIGIN", "")
            dest = row.get("DEST", "")

            total_pax += pax
            total_asm += asm
            total_rpm += rpm

            # Carrier aggregation
            if carrier not in carrier_totals:
                carrier_totals[carrier] = {"passengers": 0, "asm": 0, "rpm": 0}
            carrier_totals[carrier]["passengers"] += pax
            carrier_totals[carrier]["asm"] += asm
            carrier_totals[carrier]["rpm"] += rpm

            # Airport aggregation (origin side)
            if origin not in airport_totals:
                airport_totals[origin] = {"passengers": 0, "carriers": {}}
            airport_totals[origin]["passengers"] += pax
            airport_totals[origin]["carriers"][carrier] = (
                airport_totals[origin]["carriers"].get(carrier, 0) + pax
            )

            # Route aggregation
            route_key = f"{min(origin, dest)}-{max(origin, dest)}"
            if route_key not in route_totals:
                route_totals[route_key] = {"passengers": 0, "carriers": {}}
            route_totals[route_key]["passengers"] += pax
            route_totals[route_key]["carriers"][carrier] = (
                route_totals[route_key]["carriers"].get(carrier, 0) + pax
            )

    # Compute load factor
    load_factor = (total_rpm / total_asm * 100) if total_asm > 0 else 0

    # Compute HHI for airports
    for apt, data in airport_totals.items():
        total = data["passengers"]
        if total > 0:
            shares = [(c_pax / total * 100) for c_pax in data["carriers"].values()]
            data["hhi"] = round(sum(s ** 2 for s in shares))
            dominant = max(data["carriers"], key=data["carriers"].get)
            data["dominant_carrier"] = dominant
            data["dominant_share_pct"] = round(data["carriers"][dominant] / total * 100, 1)

    return {
        "market_totals": {
            "passengers": total_pax,
            "asm": total_asm,
            "rpm": total_rpm,
            "load_factor": round(load_factor, 1),
        },
        "carrier_totals": carrier_totals,
        "airport_totals": airport_totals,
        "top_routes": sorted(
            [{"route": k, **v} for k, v in route_totals.items()],
            key=lambda x: x["passengers"],
            reverse=True,
        )[:20],
    }


def save(data: dict) -> Path:
    """Save collected BTS data to the current data directory."""
    out = data_file("bts_traffic")
    out.write_text(json.dumps(data, indent=2))
    logger.info(f"Saved BTS data to {out}")
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = collect_bts_traffic()
    save(data)
    print(f"BTS collection complete for period {data['period']}")
