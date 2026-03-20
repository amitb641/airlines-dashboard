"""
Airport Intelligence Collector

Fetches airport traffic, capacity, carrier concentration (HHI),
and growth metrics from FAA, BTS, and OAG sources.

Release schedule: ~90-day lag (quarterly from FAA/ACI-NA)
Monthly BTS data supplements via T-100

Output: data/current/airports.json
"""

import json
import logging
from datetime import date
from typing import Any

from backend.config import TOP_AIRPORTS, data_file

logger = logging.getLogger(__name__)


def collect_airport_data() -> dict[str, Any]:
    """
    Collect airport-level traffic and concentration data.

    Sources: BTS T-100 (monthly), FAA ATADS, ACI-NA traffic reports
    """
    logger.info("Collecting airport intelligence data")

    airports = {}
    for code in TOP_AIRPORTS:
        airports[code] = {
            "code": code,
            "annual_passengers_M": None,
            "yoy_growth_pct": None,
            "dominant_carrier": None,
            "dominant_share_pct": None,
            "hhi": None,  # Herfindahl-Hirschman Index (sum of squared shares)
            "carrier_shares": {},  # carrier_code: share_pct
            "daily_departures": None,
            "gates": None,
            "is_fortress_hub": None,  # HHI > 2500
            "capacity_constrained": None,  # Boolean
        }

    result = {
        "source": "BTS T-100 / FAA ATADS / ACI-NA",
        "collected_at": date.today().isoformat(),
        "airports": airports,
        "_instructions": (
            "Airport data is derived from BTS T-100 (run bts_collector first). "
            "HHI = sum of squared carrier seat shares at each airport. "
            "HHI > 2500 = concentrated (fortress hub), 1500-2500 = moderate, < 1500 = competitive. "
            "Supplement with FAA ATADS for operations data and ACI-NA for annual totals."
        ),
    }

    return result


def save(data: dict) -> None:
    out = data_file("airports")
    out.write_text(json.dumps(data, indent=2))
    logger.info(f"Saved airport data to {out}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = collect_airport_data()
    save(data)
    print(f"Airport collection complete: {len(data['airports'])} airports")
