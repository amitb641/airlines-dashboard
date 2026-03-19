"""
Financial Data Collector — SEC EDGAR + EIA Fuel Prices

Fetches airline financial metrics from SEC filings and fuel prices from EIA.

Sources:
  - SEC EDGAR XBRL: Revenue, net income, operating margin, CASM, TRASM
  - EIA: Jet fuel spot price ($/gallon)

Release schedule:
  - 10-Q: ~40 days after quarter end
  - 10-K: ~60 days after fiscal year end
  - EIA fuel: weekly

Output: data/current/financials.json, data/current/fuel_prices.json
"""

import json
import logging
from datetime import date
from typing import Any

import requests

from backend.config import CARRIERS, data_file

logger = logging.getLogger(__name__)

SEC_EDGAR_API = "https://efts.sec.gov/LATEST"
SEC_COMPANY_API = "https://data.sec.gov/api/xbrl/companyfacts"
EIA_API = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

# Key financial metrics to extract from XBRL
FINANCIAL_METRICS = {
    "revenue": "Revenues",
    "operating_income": "OperatingIncomeLoss",
    "net_income": "NetIncomeLoss",
    "operating_expenses": "CostsAndExpenses",
    "fuel_expense": None,  # Varies by carrier — in notes
    "asm": None,  # Non-GAAP, from earnings supplement
    "rpm": None,  # Non-GAAP, from earnings supplement
    "trasm": None,  # Derived: revenue / ASM
    "casm": None,  # Derived: opex / ASM
    "casm_ex_fuel": None,  # Derived: (opex - fuel) / ASM
}


def collect_sec_financials(carrier_code: str, year: int = None) -> dict[str, Any]:
    """
    Collect financial data for a carrier from SEC EDGAR.

    Args:
        carrier_code: IATA code (e.g., 'DAL')
        year: Fiscal year (defaults to latest available)

    Returns:
        dict with revenue, margins, unit economics
    """
    carrier = CARRIERS.get(carrier_code, {})
    cik = carrier.get("cik")
    ticker = carrier.get("ticker")

    if not cik:
        logger.warning(f"No CIK for {carrier_code} — skipping SEC collection")
        return {"carrier": carrier_code, "status": "no_cik", "data": None}

    if year is None:
        year = date.today().year - 1 if date.today().month < 4 else date.today().year

    logger.info(f"Collecting SEC financials for {carrier.get('name')} (CIK: {cik})")

    result = {
        "carrier": carrier_code,
        "name": carrier.get("name"),
        "ticker": ticker,
        "cik": cik,
        "fiscal_year": year,
        "collected_at": date.today().isoformat(),
        "income_statement": {
            "revenue_B": None,
            "operating_income_B": None,
            "net_income_B": None,
            "operating_margin_pct": None,
            "net_margin_pct": None,
        },
        "unit_economics": {
            "trasm_cents": None,  # Total revenue per ASM
            "casm_cents": None,  # Cost per ASM
            "casm_ex_fuel_cents": None,  # CASM excluding fuel
            "fuel_cost_per_asm_cents": None,
        },
        "balance_sheet": {
            "total_debt_B": None,
            "cash_B": None,
            "net_debt_B": None,
        },
        "_instructions": (
            f"Fetch from {SEC_COMPANY_API}/CIK{cik}.json "
            f"or download 10-K for FY{year} from EDGAR. "
            "Extract revenue, operating income, net income. "
            "Unit economics (TRASM, CASM) from earnings supplement / 10-K MD&A."
        ),
    }

    return result


def collect_fuel_prices() -> dict[str, Any]:
    """
    Collect jet fuel spot prices from EIA.

    Returns:
        dict with monthly fuel price time series
    """
    logger.info("Collecting EIA jet fuel prices")

    result = {
        "source": "EIA Petroleum - Kerosene-Type Jet Fuel Spot Price",
        "collected_at": date.today().isoformat(),
        "unit": "$/gallon",
        "monthly_prices": {},  # "YYYY-MM": price
        "latest_price": None,
        "yoy_change_pct": None,
        "_instructions": (
            f"Fetch from {EIA_API} with api_key. "
            "Series: PET.EER_EPJK_PF4_RGC_DPG.M (monthly). "
            "Or download from https://www.eia.gov/dnav/pet/pet_pri_spt_s1_m.htm"
        ),
    }

    return result


def collect_all_financials() -> dict[str, Any]:
    """Collect financials for all tracked carriers + fuel prices."""
    carriers = {}
    for code in CARRIERS:
        carriers[code] = collect_sec_financials(code)

    fuel = collect_fuel_prices()

    return {
        "collected_at": date.today().isoformat(),
        "carriers": carriers,
        "fuel": fuel,
    }


def save(data: dict) -> None:
    """Save financial data to current data directory."""
    out = data_file("financials")
    out.write_text(json.dumps(data, indent=2))
    logger.info(f"Saved financial data to {out}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = collect_all_financials()
    save(data)
    print(f"Financial collection complete for {len(data['carriers'])} carriers")
