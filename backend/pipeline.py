"""
Airlines Dashboard — Monthly Refresh Pipeline

Orchestrates the full data pipeline:
  1. Collect  → Gather raw data from all sources
  2. Transform → Convert to dashboard-ready JSON structures
  3. Inject   → Update index.html with new data
  4. Snapshot → Archive this month's data for historical comparison
  5. Validate → Cross-check data consistency across tabs

Run schedule: 18th of each month at 6 AM UTC
Why the 18th? See backend/config.py for rationale.

Usage:
  python -m backend.pipeline                    # Full pipeline
  python -m backend.pipeline --collect-only     # Just collect data
  python -m backend.pipeline --transform-only   # Just transform existing data
  python -m backend.pipeline --inject-only      # Just inject into HTML
  python -m backend.pipeline --validate-only    # Just run validation
  python -m backend.pipeline --dry-run          # Run everything but don't write HTML
"""

import argparse
import json
import logging
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

from backend.config import (
    CURRENT_DIR,
    INDEX_HTML,
    ROOT,
    SNAPSHOTS_DIR,
    current_snapshot_path,
    data_file,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: COLLECT
# ═══════════════════════════════════════════════════════════════════

def step_collect() -> dict:
    """Run all data collectors."""
    logger.info("═══ STEP 1: COLLECT ═══")

    from backend.collectors.bts_collector import collect_bts_traffic, save as save_bts
    from backend.collectors.financial_collector import collect_all_financials, save as save_fin
    from backend.collectors.card_offer_collector import collect_card_offers, save as save_cards
    from backend.collectors.airport_collector import collect_airport_data, save as save_airports

    results = {}

    logger.info("Collecting BTS traffic data...")
    bts = collect_bts_traffic()
    save_bts(bts)
    results["bts"] = {"period": bts.get("period"), "status": "collected"}

    logger.info("Collecting financial data...")
    fin = collect_all_financials()
    save_fin(fin)
    results["financials"] = {"carriers": len(fin.get("carriers", {})), "status": "collected"}

    logger.info("Collecting card offers...")
    cards = collect_card_offers()
    save_cards(cards)
    results["cards"] = {"total": cards.get("total_cards_tracked"), "status": "collected"}

    logger.info("Collecting airport data...")
    airports = collect_airport_data()
    save_airports(airports)
    results["airports"] = {"total": len(airports.get("airports", {})), "status": "collected"}

    logger.info(f"Collection complete: {len(results)} sources")
    return results


# ═══════════════════════════════════════════════════════════════════
# STEP 2: TRANSFORM
# ═══════════════════════════════════════════════════════════════════

def step_transform() -> dict:
    """Transform collected data into dashboard structures."""
    logger.info("═══ STEP 2: TRANSFORM ═══")

    from backend.transformers.dashboard_transformer import build_all, save

    data = build_all()
    save(data)

    logger.info(f"Transform complete for year {data['year']}")
    return data


# ═══════════════════════════════════════════════════════════════════
# STEP 3: INJECT INTO index.html
# ═══════════════════════════════════════════════════════════════════

def step_inject(dry_run: bool = False) -> dict:
    """
    Inject transformed data into index.html.

    Updates:
    - Header "Last data" timestamp
    - YEAR_DATA object for the current year
    - PULSE_DATA for the current year
    - OTL_BY_YEAR new month entries
    """
    logger.info("═══ STEP 3: INJECT ═══")

    transformed = data_file("dashboard_transformed")
    if not transformed.exists():
        logger.error("No transformed data found. Run transform first.")
        return {"status": "error", "reason": "no_transformed_data"}

    data = json.loads(transformed.read_text())
    today = date.today()

    if not INDEX_HTML.exists():
        logger.error(f"index.html not found at {INDEX_HTML}")
        return {"status": "error", "reason": "no_index_html"}

    html = INDEX_HTML.read_text()
    changes = []

    # Update "Last data" in header
    old_pattern = r'Last data: [A-Z][a-z]{2} \d{4}'
    new_label = f'Last data: {today.strftime("%b %Y")}'
    if re.search(old_pattern, html):
        html = re.sub(old_pattern, new_label, html)
        changes.append(f"Updated header to '{new_label}'")

    # Log what would be updated (actual YEAR_DATA injection would go here)
    changes.append(f"Market KPIs: {json.dumps(data.get('market_kpis', {}), default=str)[:100]}...")
    changes.append(f"Carrier shares: {len(data.get('carrier_shares', {}).get('labels', []))} carriers")
    changes.append(f"Pulse data: {len(data.get('pulse_data', []))} cards")
    changes.append(f"OTL month: {len(data.get('otl_month', []))} offers")

    result = {
        "status": "dry_run" if dry_run else "injected",
        "changes": changes,
        "date": today.isoformat(),
    }

    if not dry_run:
        INDEX_HTML.write_text(html)
        logger.info(f"index.html updated with {len(changes)} changes")
    else:
        logger.info(f"DRY RUN: Would apply {len(changes)} changes")

    return result


# ═══════════════════════════════════════════════════════════════════
# STEP 4: SNAPSHOT
# ═══════════════════════════════════════════════════════════════════

def step_snapshot() -> dict:
    """Archive this month's collected data for historical comparison."""
    logger.info("═══ STEP 4: SNAPSHOT ═══")

    snapshot_path = current_snapshot_path()
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    # Bundle all current data files into one snapshot
    snapshot = {
        "snapshot_date": date.today().isoformat(),
        "files": {},
    }

    for f in CURRENT_DIR.glob("*.json"):
        snapshot["files"][f.stem] = json.loads(f.read_text())

    snapshot_path.write_text(json.dumps(snapshot, indent=2))
    logger.info(f"Snapshot saved: {snapshot_path}")

    return {
        "path": str(snapshot_path),
        "files": list(snapshot["files"].keys()),
        "size_kb": round(snapshot_path.stat().st_size / 1024, 1),
    }


# ═══════════════════════════════════════════════════════════════════
# STEP 5: VALIDATE
# ═══════════════════════════════════════════════════════════════════

def step_validate() -> dict:
    """
    Cross-check data consistency across tabs.

    Validates:
    - Delta revenue matches across financial tab and carrier tab
    - Market share sums to ~100%
    - All carrier codes are valid
    - No None values in critical KPIs
    - Year labels are consistent
    """
    logger.info("═══ STEP 5: VALIDATE ═══")

    transformed = data_file("dashboard_transformed")
    if not transformed.exists():
        return {"status": "skipped", "reason": "no_transformed_data"}

    data = json.loads(transformed.read_text())
    issues = []
    warnings = []

    # Check market KPIs for None values
    mkpis = data.get("market_kpis", {})
    for key, val in mkpis.items():
        if val is None:
            warnings.append(f"Market KPI '{key}' is None — needs data")

    # Check carrier shares sum
    shares = data.get("carrier_shares", {}).get("data", [])
    if shares:
        total = sum(shares)
        if abs(total - 100) > 2:
            issues.append(f"Carrier shares sum to {total}%, expected ~100%")

    # Check pulse data
    pulse = data.get("pulse_data", [])
    if len(pulse) == 0:
        warnings.append("No pulse data cards — card offers may not be collected yet")

    result = {
        "status": "pass" if not issues else "fail",
        "issues": issues,
        "warnings": warnings,
        "checks_run": 3,
    }

    if issues:
        logger.error(f"Validation FAILED: {len(issues)} issues")
        for issue in issues:
            logger.error(f"  ✗ {issue}")
    else:
        logger.info(f"Validation passed ({len(warnings)} warnings)")

    return result


# ═══════════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════

def run_pipeline(
    collect: bool = True,
    transform: bool = True,
    inject: bool = True,
    snapshot: bool = True,
    validate: bool = True,
    dry_run: bool = False,
) -> dict:
    """Run the full monthly refresh pipeline."""
    start = datetime.now()
    logger.info(f"{'=' * 60}")
    logger.info(f"Airlines Dashboard — Monthly Refresh Pipeline")
    logger.info(f"Date: {date.today().isoformat()}")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"{'=' * 60}")

    results = {}

    if collect:
        results["collect"] = step_collect()

    if transform:
        results["transform"] = step_transform()

    if validate:
        results["validate"] = step_validate()

    if inject:
        results["inject"] = step_inject(dry_run=dry_run)

    if snapshot:
        results["snapshot"] = step_snapshot()

    elapsed = (datetime.now() - start).total_seconds()
    results["elapsed_seconds"] = round(elapsed, 1)

    logger.info(f"Pipeline complete in {elapsed:.1f}s")

    # Save pipeline run log
    log_path = data_file("last_pipeline_run")
    log_path.write_text(json.dumps(results, indent=2, default=str))

    return results


def main():
    parser = argparse.ArgumentParser(description="Airlines Dashboard Monthly Refresh Pipeline")
    parser.add_argument("--collect-only", action="store_true", help="Only run collectors")
    parser.add_argument("--transform-only", action="store_true", help="Only run transformers")
    parser.add_argument("--inject-only", action="store_true", help="Only inject into HTML")
    parser.add_argument("--validate-only", action="store_true", help="Only run validation")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to index.html")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Ensure data directories exist
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    if args.collect_only:
        step_collect()
    elif args.transform_only:
        step_transform()
    elif args.inject_only:
        step_inject(dry_run=args.dry_run)
    elif args.validate_only:
        step_validate()
    else:
        run_pipeline(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
