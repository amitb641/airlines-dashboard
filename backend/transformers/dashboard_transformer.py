"""
Dashboard Transformer

Takes raw collected data (BTS, financials, card offers, airports)
and transforms it into the exact JSON structures that index.html
expects: YEAR_DATA, PULSE_DATA, OTL_BY_YEAR, YEAR_TEXT.

This is the bridge between raw data and the dashboard.
"""

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from backend.config import CURRENT_DIR, data_file

logger = logging.getLogger(__name__)


def load_collected(name: str) -> dict:
    """Load a collected data file from the current directory."""
    path = data_file(name)
    if path.exists():
        return json.loads(path.read_text())
    logger.warning(f"Missing data file: {path}")
    return {}


def build_market_kpis(bts: dict, year: int) -> dict:
    """
    Transform BTS traffic data into Tab 1 market KPI structure.

    Expected output shape (matches YEAR_DATA.marketKpis[year]):
    {
        pax: "921M",
        paxChg: "+2.4%",
        load: "85.6%",
        loadChg: "+0.8pp",
        fare: "$391",
        fareChg: "+1.9%",
        asm: "1.08T",
        asmChg: "+3.1%",
        vs2019: "+5.4%"
    }
    """
    totals = bts.get("market_totals", {})

    pax = totals.get("passengers_ttm")
    load = totals.get("load_factor")
    vs2019 = totals.get("vs_2019_rpm_pct")

    return {
        "pax": format_large_number(pax, "M") if pax else None,
        "paxChg": None,
        "load": f"{load}%" if load else None,
        "loadChg": None,
        "fare": None,  # From DOT O&D survey, not BTS T-100
        "fareChg": None,
        "asm": format_large_number(totals.get("asm_total"), "T") if totals.get("asm_total") else None,
        "asmChg": None,
        "vs2019": f"+{vs2019}%" if vs2019 and vs2019 > 0 else f"{vs2019}%" if vs2019 else None,
    }


def build_carrier_shares(bts: dict) -> dict:
    """
    Transform BTS carrier totals into market share structure.

    Expected output shape (matches YEAR_DATA.mktShare[year]):
    {
        labels: ["American", "Southwest", ...],
        data: [21, 19, ...]
    }
    """
    carrier_totals = bts.get("carrier_totals", {})
    total_pax = sum(c.get("passengers", 0) or 0 for c in carrier_totals.values())

    if total_pax == 0:
        return {"labels": [], "data": []}

    shares = []
    for code, data in carrier_totals.items():
        pax = data.get("passengers", 0) or 0
        share = round(pax / total_pax * 100, 1)
        shares.append({"label": code, "share": share})

    shares.sort(key=lambda x: x["share"], reverse=True)

    return {
        "labels": [s["label"] for s in shares],
        "data": [s["share"] for s in shares],
    }


def build_financial_charts(financials: dict) -> dict:
    """
    Transform SEC financial data into chart structures.

    Builds: margin chart, revenue chart, net income chart, TRASM/CASM chart
    """
    carriers = financials.get("carriers", {})
    fuel = financials.get("fuel", {})

    result = {
        "margins": {},       # carrier: [margin_y1, margin_y2, margin_y3]
        "revenue": {},       # carrier: revenue_B
        "net_income": {},    # carrier: [ni_y1, ni_y2, ni_y3]
        "trasm_casm": {},    # carrier: {trasm, casm, casm_ex}
        "fuel_price": fuel.get("monthly_prices", {}),
    }

    for code, data in carriers.items():
        if data.get("data") is None:
            continue
        inc = data.get("income_statement", {})
        unit = data.get("unit_economics", {})
        result["revenue"][code] = inc.get("revenue_B")
        result["trasm_casm"][code] = {
            "trasm": unit.get("trasm_cents"),
            "casm": unit.get("casm_cents"),
            "casm_ex_fuel": unit.get("casm_ex_fuel_cents"),
        }

    return result


def build_pulse_data(card_offers: dict) -> list:
    """
    Transform card offers into PULSE_DATA structure for the offer pulse grid.

    Expected output shape (matches PULSE_DATA[year]):
    [{
        code: "DL",
        airline: "Delta Reserve",
        color: "var(--delta)",
        logoBg: "#C8102E",
        cardName: "Amex Delta SkyMiles Reserve · $550/yr",
        bonus: "80K",
        unit: "SkyMiles sign-up bonus",
        status: "cool",
        tag: "standard",
        tagText: "— STANDARD OFFER",
        rows: [["Spend req.", "$5K / 6mo"], ...]
    }]
    """
    offers = card_offers.get("offers", [])
    pulse_cards = []

    STATUS_MAP = {
        "elevated": {"status": "hot", "tag": "elevated", "prefix": "↑ ELEVATED"},
        "new": {"status": "warm", "tag": "new", "prefix": "⚡ NEW"},
        "limited": {"status": "warm", "tag": "limited", "prefix": "⏰ LIMITED"},
        "standard": {"status": "cool", "tag": "standard", "prefix": "— STANDARD OFFER"},
    }

    for offer in offers:
        if offer.get("current_bonus") is None:
            continue

        status_key = offer.get("status", "standard")
        status_info = STATUS_MAP.get(status_key, STATUS_MAP["standard"])

        pulse_cards.append({
            "code": offer["carrier"][:2].upper(),
            "airline": f"{offer['carrier']} {offer['card_name']}",
            "cardName": f"{offer['bank']} {offer['carrier']} {offer['card_name']} · ${offer.get('current_annual_fee', offer['base_annual_fee'])}/yr",
            "bonus": offer["current_bonus"],
            "unit": f"{offer['carrier']} miles sign-up bonus",
            "status": status_info["status"],
            "tag": status_info["tag"],
            "tagText": f"{status_info['prefix']} — {offer.get('notes', '')}",
            "rows": [
                ["Spend req.", offer.get("spend_requirement", "TBD")],
            ],
        })

    return pulse_cards


def build_otl_month(card_offers: dict) -> list:
    """
    Transform card offers into a single month entry for OTL_BY_YEAR.

    Returns list of offer dicts to append to each carrier's offers array.
    """
    month_idx = card_offers.get("month_index", 0)
    offers = card_offers.get("offers", [])
    otl_entries = []

    for offer in offers:
        if offer.get("current_bonus") is None:
            continue

        otl_entries.append({
            "m": month_idx,
            "card": offer["card_name"],
            "bonus": offer["current_bonus"],
            "af": f"${offer.get('current_annual_fee', offer['base_annual_fee'])}",
            "spend": offer.get("spend_requirement", "TBD"),
            "type": offer.get("status", "standard"),
            "note": f"{offer.get('notes', '')} Source: {offer.get('source', 'issuer')}",
        })

    return otl_entries


def format_large_number(n, suffix="") -> str:
    """Format a large number: 921000000 → '921M', 1080000000000 → '1.08T'."""
    if n is None:
        return None
    if suffix == "T" and n >= 1e12:
        return f"{n / 1e12:.2f}T"
    if suffix == "M" or n >= 1e6:
        return f"{round(n / 1e6)}M"
    return str(n)


def build_all(year: int = None) -> dict:
    """
    Build the complete transformed dataset for the dashboard.

    Returns a dict with all data structures ready for injection into index.html.
    """
    if year is None:
        year = date.today().year

    logger.info(f"Building transformed data for {year}")

    bts = load_collected("bts_traffic")
    financials = load_collected("financials")
    card_offers = load_collected("card_offers")
    airports = load_collected("airports")

    return {
        "year": year,
        "built_at": date.today().isoformat(),
        "market_kpis": build_market_kpis(bts, year),
        "carrier_shares": build_carrier_shares(bts),
        "financial_charts": build_financial_charts(financials),
        "pulse_data": build_pulse_data(card_offers),
        "otl_month": build_otl_month(card_offers),
        "airports": airports,
    }


def save(data: dict) -> Path:
    out = data_file("dashboard_transformed")
    out.write_text(json.dumps(data, indent=2))
    logger.info(f"Saved transformed data to {out}")
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = build_all()
    save(data)
    print(f"Transform complete for year {data['year']}")
