#!/usr/bin/env python3
"""
Seed Indicators — Tier 1 Bootstrap

Seeds the Blackboard with Tier 1 indicators from the master corpus.
Tier 1: indicators that connect directly to existing MBOEvent/trade data
(order flow, volume-weighted momentum, volatility envelopes).

Reads from: data/indicator-corpus-v1.0.jsonl
Posts to:    POST /v1/formulas/seed

Usage:
  # From Docker (recommended):
  docker exec foundry_worker python scripts/seed_indicators.py

  # From host:
  python3 scripts/seed_indicators.py [--all] [--api-url URL]

Options:
  --all       Seed all 98 indicators, not just Tier 1
  --api-url   Blackboard API URL (default: http://localhost:8000,
              auto-detects Docker as http://foundry_api:8000)
  --dry-run   Parse and validate without POSTing
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import httpx

# ---------------------------------------------------------------------------
# Tier 1 indicator names — connect directly to MBOEvent/trade data
# ---------------------------------------------------------------------------
TIER_1_NAMES = {
    # Order Flow & Microstructure (our core data)
    "Order Flow Imbalance (OFI)",
    "Cumulative Volume Delta (CVD)",
    "Kyle's Lambda (Price Impact Coefficient)",
    "Hawkes Intensity (Self-Exciting Process)",
    "Trade Arrival Rate",
    "VPIN (Volume-Synchronized Probability of Informed Trading)",
    "Order Book Imbalance (Depth Ratio)",
    "Bid-Ask Spread",
    "Shannon Entropy (Order Book)",
    "Footprint Imbalance Ratio",
    "Aggressive Ratio",
    "Volume Profile (POC/VAH/VAL)",
    # Volume indicators computable from trades
    "On Balance Volume (OBV)",
    "Accumulation/Distribution Line (ADL)",
    "Chaikin Money Flow (CMF)",
    "Money Flow Index (MFI)",
    "Volume Weighted Average Price (VWAP)",
    # Momentum from price (derivable from trade stream)
    "Relative Strength Index (RSI)",
    # Volatility envelopes
    "Average True Range (ATR)",
    "Bollinger Bands",
    "Keltner Channels",
    # Statistical fundamentals
    "Z-Score",
    "Hurst Exponent",
    "Autocorrelation",
}


def _generate_behavioral_mappings(indicator: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate FormulaDNA-compatible behavioral_mappings from the indicator record.

    Every indicator gets at least one mapping: the output variable itself.
    The behavioral_claim and formula structure inform the psychological_state.
    """
    name = indicator["name"]
    claim = indicator["behavioral_claim"]

    # Primary output mapping — every indicator has this
    primary = {
        "variable": name.split("(")[0].strip().replace(" ", "_").lower(),
        "latex_symbol": indicator["formula_latex"].split("=")[0].strip() if "=" in indicator["formula_latex"] else name,
        "psychological_state": claim[:200],  # Truncate to fit
        "measurement_proxy": f"Computed from trade/book data per formula definition",
        "unit": "dimensionless",
    }

    # Price input mapping
    price_mapping = {
        "variable": "C_t",
        "latex_symbol": r"C_t",
        "psychological_state": "Last traded price — the market's instantaneous consensus valuation",
        "measurement_proxy": "Last trade price from MBOEvent stream or OHLCV bar close",
        "unit": "price",
    }

    # Volume input mapping (for volume-dependent indicators)
    volume_categories = {"VLUM", "OF"}
    volume_tags = {"volume", "order_flow", "money_flow", "cumulative_delta"}
    has_volume = (
        indicator["category"] in volume_categories
        or any(t in indicator.get("tags", []) for t in volume_tags)
    )

    mappings = [primary, price_mapping]
    if has_volume:
        mappings.append({
            "variable": "V_t",
            "latex_symbol": r"V_t",
            "psychological_state": "Trade volume — the intensity of participant commitment at each price",
            "measurement_proxy": "Aggregate volume from MBOEvent stream (satoshis for BTC, contracts for futures)",
            "unit": "volume",
        })

    return mappings


def _to_formula_payload(indicator: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a corpus indicator record to a FormulaDNA-compatible seed payload."""
    return {
        "name": indicator["name"],
        "description": indicator["behavioral_claim"],
        "symbolic_expression_latex": indicator["formula_latex"],
        "symbolic_expression_sympy": indicator.get("formula_sympy"),
        "behavioral_claim": indicator["behavioral_claim"],
        "behavioral_mappings": _generate_behavioral_mappings(indicator),
        "axiomatic_origin": ["SYSTEM", indicator.get("source", "unknown")],
        "mathematical_wing": indicator["mathematical_wing"],
        "proof_status": "hypothesis",
        "tags": indicator.get("tags", []) + [
            "indicator_corpus",
            f"category_{indicator['category'].lower()}",
            "tier_1" if indicator["name"] in TIER_1_NAMES else "tier_2_3",
        ],
    }


def load_corpus(path: Path) -> List[Dict[str, Any]]:
    """Load and parse the JSONL corpus file."""
    indicators = []
    with open(path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                indicators.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  ⚠ Line {line_num}: JSON parse error: {e}")
    return indicators


def _detect_api_url() -> str:
    """Auto-detect whether we're inside Docker or on the host."""
    # If BLACKBOARD_API_URL is set (Docker worker), use it
    env_url = os.environ.get("BLACKBOARD_API_URL")
    if env_url:
        return env_url
    # Try Docker internal URL first
    try:
        r = httpx.get("http://foundry_api:8000/health", timeout=2.0)
        if r.status_code == 200:
            return "http://foundry_api:8000"
    except Exception:
        pass
    return "http://localhost:8000"


async def seed(
    api_base_url: str,
    indicators: List[Dict[str, Any]],
    tier_1_only: bool = True,
    dry_run: bool = False,
) -> None:
    if tier_1_only:
        indicators = [i for i in indicators if i["name"] in TIER_1_NAMES]
        print(f"Tier 1 filter: {len(indicators)} indicators selected")
    else:
        print(f"All indicators: {len(indicators)} selected")

    payloads = [_to_formula_payload(i) for i in indicators]

    if dry_run:
        print(f"\n{'='*60}")
        print(f"DRY RUN — {len(payloads)} indicators validated:")
        for p in payloads:
            wings = ", ".join(p["mathematical_wing"])
            print(f"  • {p['name']} [{wings}]")
        return

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Health check
        health = await client.get(f"{api_base_url}/health")
        health.raise_for_status()
        print(f"✓ Blackboard API online: {health.json()}")

        # Check for existing indicator_corpus formulas to avoid duplicates
        try:
            existing = await client.get(
                f"{api_base_url}/v1/formulas",
                params={"tag": "indicator_corpus"},
            )
            if existing.status_code == 200:
                existing_names = {f["name"] for f in existing.json()}
                dupes = [p for p in payloads if p["name"] in existing_names]
                if dupes:
                    print(f"⚠ Skipping {len(dupes)} already-seeded indicators:")
                    for d in dupes:
                        print(f"    • {d['name']}")
                    payloads = [p for p in payloads if p["name"] not in existing_names]
        except Exception:
            pass  # Tag query may not be supported; proceed without dedup

        seeded = 0
        failed = 0
        for payload in payloads:
            response = await client.post(
                f"{api_base_url}/v1/formulas/seed",
                json=payload,
            )
            if response.status_code == 201:
                result = response.json()
                print(f"✓ Seeded: '{payload['name']}' → UUID: {result['uuid']}")
                seeded += 1
            else:
                print(f"✗ Failed '{payload['name']}': {response.status_code} {response.text[:200]}")
                failed += 1

        print(f"\n{'='*60}")
        print(f"Seed complete: {seeded} succeeded, {failed} failed")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed Blackboard with indicator corpus")
    parser.add_argument("--all", action="store_true", help="Seed all indicators, not just Tier 1")
    parser.add_argument("--api-url", default=None, help="Blackboard API URL")
    parser.add_argument("--dry-run", action="store_true", help="Validate without POSTing")
    parser.add_argument(
        "--corpus", default="data/indicator-corpus-v1.0.jsonl",
        help="Path to corpus JSONL file",
    )
    args = parser.parse_args()

    corpus_path = Path(args.corpus)
    if not corpus_path.exists():
        # Try relative to script location
        corpus_path = Path(__file__).resolve().parent.parent / args.corpus
    if not corpus_path.exists():
        print(f"✗ Corpus file not found: {args.corpus}")
        sys.exit(1)

    indicators = load_corpus(corpus_path)
    print(f"Loaded {len(indicators)} indicators from {corpus_path.name}")

    api_url = args.api_url or _detect_api_url()
    print(f"API target: {api_url}")

    asyncio.run(seed(
        api_base_url=api_url,
        indicators=indicators,
        tier_1_only=not args.all,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
