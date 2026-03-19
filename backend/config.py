"""
Airlines Dashboard — Pipeline Configuration

Monthly refresh schedule:  18th of each month
Why the 18th?
  - BTS T-100 domestic data publishes ~60-day lag (Jan data → mid-March)
  - DOT consumer airfare report publishes ~90-day lag, quarterly
  - SEC 10-Q filings due 40 days after quarter end
  - OAG schedule updates land by mid-month
  - Credit card offers cycle on 1st/15th — the 18th catches the latest round
  - Avoids month-end close periods at airlines
  - Falls on a weekday in most months (reduces weekend skew)
"""

from pathlib import Path
from datetime import date

# ── Paths ──────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CURRENT_DIR = DATA_DIR / "current"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
BACKEND_DIR = ROOT / "backend"
INDEX_HTML = ROOT / "index.html"

# ── Schedule ───────────────────────────────────────────────────────
REFRESH_DAY = 18  # Day of month to run pipeline
REFRESH_CRON = "0 6 18 * *"  # 6 AM UTC on the 18th

# ── Data source release calendar ───────────────────────────────────
# Each source has a typical release lag from the period it covers.
# The pipeline runs on the 18th to catch the latest available data.
SOURCE_CALENDAR = {
    "bts_t100": {
        "description": "BTS T-100 Domestic Segment (passengers, ASMs, RPMs)",
        "url": "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoession_ID=4&Table_ID=311",
        "release_lag_days": 60,
        "frequency": "monthly",
        "format": "CSV download",
    },
    "dot_od_survey": {
        "description": "DOT Origin-Destination Survey (fares, itineraries)",
        "url": "https://www.transtats.bts.gov/Fields.asp?gnoession_ID=4&Table_ID=272",
        "release_lag_days": 180,
        "frequency": "quarterly",
        "format": "CSV download",
    },
    "sec_edgar": {
        "description": "SEC EDGAR filings (10-K, 10-Q financials)",
        "url": "https://efts.sec.gov/LATEST/search-index?q=%22airline%22&dateRange=custom",
        "release_lag_days": 45,
        "frequency": "quarterly",
        "format": "XBRL / HTML",
    },
    "oag_schedules": {
        "description": "OAG flight schedules (frequency, capacity, routes)",
        "url": "https://www.oag.com/airline-schedules-data",
        "release_lag_days": 0,
        "frequency": "continuous",
        "format": "API / CSV",
    },
    "eia_fuel": {
        "description": "EIA jet fuel spot prices",
        "url": "https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm",
        "release_lag_days": 7,
        "frequency": "weekly",
        "format": "API / CSV",
    },
    "card_offers": {
        "description": "Co-brand credit card offers (TPG, DoC, USCG)",
        "urls": [
            "https://thepointsguy.com/credit-cards/airlines/",
            "https://www.doctorofcredit.com/best-current-credit-card-sign-up-bonuses/",
            "https://www.uscreditcardguide.com/en/",
        ],
        "release_lag_days": 0,
        "frequency": "real-time",
        "format": "Web scrape",
    },
    "a4a_reports": {
        "description": "Airlines for America industry reports",
        "url": "https://www.airlines.org/dataset/",
        "release_lag_days": 30,
        "frequency": "monthly",
        "format": "PDF / Web",
    },
    "faa_airport": {
        "description": "FAA / ACI-NA airport traffic statistics",
        "url": "https://www.faa.gov/airports/planning_capacity/passenger_allcargo_stats",
        "release_lag_days": 90,
        "frequency": "quarterly",
        "format": "PDF / Excel",
    },
}

# ── Carriers ───────────────────────────────────────────────────────
CARRIERS = {
    "DAL": {"name": "Delta Air Lines", "color": "#C8102E", "ticker": "DAL", "cik": "0000027904"},
    "UAL": {"name": "United Airlines", "color": "#0C2340", "ticker": "UAL", "cik": "0000100517"},
    "AAL": {"name": "American Airlines", "color": "#0078D2", "ticker": "AAL", "cik": "0000006201"},
    "LUV": {"name": "Southwest Airlines", "color": "#E06700", "ticker": "LUV", "cik": "0000093410"},
    "ALK": {"name": "Alaska Air Group", "color": "#01B7A8", "ticker": "ALK", "cik": "0000766421"},
    "JBLU": {"name": "JetBlue Airways", "color": "#003DA5", "ticker": "JBLU", "cik": "0000898293"},
    "ULCC": {"name": "Frontier Group", "color": "#2A7A35", "ticker": "ULCC", "cik": "0001823340"},
    "MX": {"name": "Breeze Airways", "color": "#CC3370", "ticker": None, "cik": None},
}

# ── Airports (top hubs) ───────────────────────────────────────────
TOP_AIRPORTS = [
    "ATL", "DFW", "DEN", "ORD", "LAX", "CLT", "MCO", "LAS",
    "PHX", "IAH", "MIA", "SEA", "SFO", "EWR", "BOS", "MSP",
    "DTW", "JFK", "SLC", "DCA", "BNA", "AUS", "HNL", "SAN",
]


def current_snapshot_path() -> Path:
    """Return path for this month's data snapshot."""
    today = date.today()
    return SNAPSHOTS_DIR / f"{today.strftime('%Y-%m')}.json"


def data_file(name: str) -> Path:
    """Return path for a current data JSON file."""
    return CURRENT_DIR / f"{name}.json"
