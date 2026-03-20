"""
Co-Brand Credit Card Offer Collector

Tracks current sign-up bonuses, annual fees, and offer status for all
airline co-brand credit cards. Sources: TPG, Doctor of Credit, USCG,
and issuer public offer pages.

Release schedule: Real-time (offers change any day)
Run: Monthly on the 18th, but can run ad-hoc for flash offers

Output: data/current/card_offers.json
"""

import json
import logging
from datetime import date
from typing import Any

from backend.config import data_file

logger = logging.getLogger(__name__)

# ── Card inventory ─────────────────────────────────────────────────
# Every tracked co-brand card with its baseline attributes.
# Monthly collection updates: bonus, status, notes
CARD_INVENTORY = [
    {
        "carrier": "Delta",
        "bank": "Amex",
        "cards": [
            {"name": "Reserve", "slug": "amex-delta-reserve", "base_af": 650, "base_bonus": "80K"},
            {"name": "Platinum", "slug": "amex-delta-platinum", "base_af": 350, "base_bonus": "70K"},
            {"name": "Gold", "slug": "amex-delta-gold", "base_af": 150, "base_bonus": "50K"},
            {"name": "Blue", "slug": "amex-delta-blue", "base_af": 0, "base_bonus": "10K"},
        ],
    },
    {
        "carrier": "United",
        "bank": "Chase",
        "cards": [
            {"name": "Club Infinite", "slug": "chase-united-club-infinite", "base_af": 525, "base_bonus": "80K"},
            {"name": "Quest", "slug": "chase-united-quest", "base_af": 250, "base_bonus": "60K"},
            {"name": "Explorer", "slug": "chase-united-explorer", "base_af": 95, "base_bonus": "60K"},
            {"name": "Gateway", "slug": "chase-united-gateway", "base_af": 0, "base_bonus": "15K"},
        ],
    },
    {
        "carrier": "American",
        "bank": "Citi",
        "cards": [
            {"name": "Executive", "slug": "citi-aadvantage-executive", "base_af": 595, "base_bonus": "60K"},
            {"name": "Platinum Select", "slug": "citi-aadvantage-platinum", "base_af": 99, "base_bonus": "50K"},
            {"name": "MileUp", "slug": "citi-aadvantage-mileup", "base_af": 0, "base_bonus": "10K"},
        ],
    },
    {
        "carrier": "Southwest",
        "bank": "Chase",
        "cards": [
            {"name": "Priority", "slug": "chase-sw-priority", "base_af": 149, "base_bonus": "60K"},
            {"name": "Plus", "slug": "chase-sw-plus", "base_af": 69, "base_bonus": "40K"},
            {"name": "Premier", "slug": "chase-sw-premier", "base_af": 99, "base_bonus": "50K"},
        ],
    },
    {
        "carrier": "Alaska",
        "bank": "BoA",
        "cards": [
            {"name": "Visa Signature", "slug": "boa-alaska-visa-sig", "base_af": 95, "base_bonus": "50K"},
            {"name": "Business", "slug": "boa-alaska-biz", "base_af": 75, "base_bonus": "40K"},
        ],
    },
    {
        "carrier": "JetBlue",
        "bank": "Barclays",
        "cards": [
            {"name": "Plus", "slug": "barclays-jetblue-plus", "base_af": 99, "base_bonus": "50K"},
            {"name": "Business", "slug": "barclays-jetblue-biz", "base_af": 99, "base_bonus": "40K"},
        ],
    },
    {
        "carrier": "Frontier",
        "bank": "Barclays",
        "cards": [
            {"name": "World Elite Mastercard", "slug": "barclays-frontier-we", "base_af": 89, "base_bonus": "50K"},
        ],
    },
    {
        "carrier": "Breeze",
        "bank": "Barclays",
        "cards": [
            {"name": "Mastercard", "slug": "barclays-breeze-mc", "base_af": 89, "base_bonus": "20K"},
        ],
    },
]

# Tracking sources for verification
SOURCES = {
    "tpg": {
        "name": "The Points Guy",
        "url": "https://thepointsguy.com/credit-cards/airlines/",
        "reliability": "high",
    },
    "doc": {
        "name": "Doctor of Credit",
        "url": "https://www.doctorofcredit.com/best-current-credit-card-sign-up-bonuses/",
        "reliability": "high",
    },
    "uscg": {
        "name": "US Credit Card Guide",
        "url": "https://www.uscreditcardguide.com/en/",
        "reliability": "high",
    },
}


def collect_card_offers() -> dict[str, Any]:
    """
    Collect current card offers for all tracked co-brand cards.

    In production, this would scrape TPG/DoC/USCG or use their APIs.
    Returns collection template with instructions.
    """
    today = date.today()
    month_idx = today.month - 1  # 0-indexed for OTL_BY_YEAR format

    logger.info(f"Collecting card offers for {today.strftime('%B %Y')}")

    offers = []
    for carrier_group in CARD_INVENTORY:
        for card in carrier_group["cards"]:
            offers.append({
                "carrier": carrier_group["carrier"],
                "bank": carrier_group["bank"],
                "card_name": card["name"],
                "slug": card["slug"],
                "base_annual_fee": card["base_af"],
                "base_bonus": card["base_bonus"],
                # Fields to populate during collection:
                "current_bonus": None,          # e.g., "100K"
                "current_annual_fee": None,      # May differ from base if waived Y1
                "spend_requirement": None,       # e.g., "$6K/6mo"
                "status": None,                  # "standard" | "elevated" | "new" | "limited"
                "is_elevated": None,             # True if current > base
                "elevation_pct": None,           # How much above base (%)
                "source": None,                  # "tpg" | "doc" | "uscg" | "issuer"
                "source_url": None,
                "notes": None,                   # Any special context
                "verified_date": None,
            })

    result = {
        "source": "TPG / DoC / USCG / Issuer pages",
        "period": today.strftime("%Y-%m"),
        "month_index": month_idx,
        "collected_at": today.isoformat(),
        "total_cards_tracked": len(offers),
        "offers": offers,
        "_instructions": (
            "For each card: visit the issuer's public application page and "
            "cross-reference with TPG/DoC/USCG. Record current bonus, AF, "
            "spend requirement, and whether the offer is above the historical base. "
            "Mark status as 'elevated' if bonus > base_bonus, 'new' if card launched "
            "within last 6 months, 'limited' if offer has a stated end date."
        ),
    }

    return result


def save(data: dict) -> None:
    """Save card offer data to current data directory."""
    out = data_file("card_offers")
    out.write_text(json.dumps(data, indent=2))
    logger.info(f"Saved card offers to {out}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = collect_card_offers()
    save(data)
    print(f"Card offer collection complete: {data['total_cards_tracked']} cards tracked")
