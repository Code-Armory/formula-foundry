"""
Seed Corpus — Phase 1 Bootstrap

Injects the foundational behavioral finance formulas.
Run with:
  PYTHONPATH=. python scripts/seed_corpus.py
"""

from __future__ import annotations

import asyncio
import httpx
import sys

SEED_CORPUS = [
    {
        "name": "Kyle's Lambda",
        "description": (
            "Measures the price impact of order flow. Higher lambda = liquidity is thin "
            "and participants are informed or panicked."
        ),
        "symbolic_expression_latex": r"\Delta p_t = \lambda \cdot x_t + \epsilon_t",
        "behavioral_claim": (
            "Models how aggressively market makers move prices in response to signed order flow. "
            "High lambda = participants believe orders carry information or represent panic."
        ),
        "behavioral_mappings": [
            {"variable": "delta_p_t", "latex_symbol": r"\Delta p_t",
             "psychological_state": "Market maker capitulation — price concession per unit of aggression",
             "measurement_proxy": "Midpoint price change per unit time, Databento MBO",
             "unit": "basis points"},
            {"variable": "lambda", "latex_symbol": r"\lambda",
             "psychological_state": "Informed-trader fear coefficient",
             "measurement_proxy": "OLS regression: price_change ~ signed_volume",
             "unit": "basis points / share",
             "empirical_range": {"min": 0.0, "max": 0.05}},
            {"variable": "x_t", "latex_symbol": r"x_t",
             "psychological_state": "Net aggressive intent — signed order flow imbalance",
             "measurement_proxy": "OFI from Databento MBO", "unit": "shares"},
            {"variable": "epsilon_t", "latex_symbol": r"\epsilon_t",
             "psychological_state": "Noise — non-informational price variation",
             "measurement_proxy": "OLS residual", "unit": "basis points"},
        ],
        "axiomatic_origin": ["SYSTEM"],
        "mathematical_wing": ["probability_information", "game_theory"],
        "proof_status": "hypothesis",
        "tags": ["illiquidity", "order_flow", "informed_trading", "price_impact", "seed_corpus"],
    },
    {
        "name": "Amihud Illiquidity Ratio",
        "description": (
            "A daily illiquidity measure: ratio of absolute return to dollar volume. "
            "High values = thin, fearful market."
        ),
        "symbolic_expression_latex": r"ILLIQ_t = \frac{1}{D} \sum_{d=1}^{D} \frac{|R_{id}|}{VOL_{id}}",
        "behavioral_claim": (
            "Quantifies market-wide illiquidity as an aggregated fear signal. "
            "ILLIQ spike = participants collectively withdrawing liquidity."
        ),
        "behavioral_mappings": [
            {"variable": "ILLIQ_t", "latex_symbol": r"ILLIQ_t",
             "psychological_state": "Collective liquidity withdrawal intensity",
             "measurement_proxy": "Computed daily from OHLCV; spike via 20-day z-score",
             "unit": "percent / million USD"},
            {"variable": "D", "latex_symbol": r"D",
             "psychological_state": "Observation window — recency of fear sample",
             "measurement_proxy": "Rolling window in trading days",
             "unit": "days"},
            {"variable": "R_id", "latex_symbol": r"R_{id}",
             "psychological_state": "Price conviction — how far participants pushed price",
             "measurement_proxy": "Daily log return from Databento OHLCV",
             "unit": "percent"},
            {"variable": "VOL_id", "latex_symbol": r"VOL_{id}",
             "psychological_state": "Participation — total capital committed",
             "measurement_proxy": "Daily dollar volume from Databento OHLCV",
             "unit": "USD"},
        ],
        "axiomatic_origin": ["SYSTEM"],
        "mathematical_wing": ["probability_information"],
        "proof_status": "hypothesis",
        "tags": ["illiquidity", "fear_signal", "volume", "seed_corpus"],
    },
    {
        "name": "Hawkes Process Intensity (Baseline)",
        "description": (
            "The self-exciting point process that models how events trigger further events. "
            "Baseline version Agent 089 extends to the panic fingerprint trigger."
        ),
        "symbolic_expression_latex": r"\lambda(t) = \mu + \sum_{t_i < t} \alpha \cdot e^{-\beta(t - t_i)}",
        "behavioral_claim": (
            "Models the self-reinforcing cascade of sell orders during panic. "
            "Each aggressive sell increases the intensity of the next."
        ),
        "behavioral_mappings": [
            {"variable": "lambda_t", "latex_symbol": r"\lambda(t)",
             "psychological_state": "Panic intensity — instantaneous rate of aggressive sell events",
             "measurement_proxy": "Local event rate from Databento MBO",
             "unit": "events/second"},
            {"variable": "mu", "latex_symbol": r"\mu",
             "psychological_state": "Background anxiety — baseline sell rate absent trigger",
             "measurement_proxy": "Mean event rate during non-triggered sessions",
             "unit": "events/second",
             "empirical_range": {"min": 0.001, "max": 2.0}},
            {"variable": "alpha", "latex_symbol": r"\alpha",
             "psychological_state": "Contagion coefficient — how strongly one sell triggers next",
             "measurement_proxy": "MLE fit to historical order book sequences",
             "unit": "dimensionless",
             "empirical_range": {"min": 0.0, "max": 0.95}},
            {"variable": "beta", "latex_symbol": r"\beta",
             "psychological_state": "Panic decay rate — how quickly contagion fades",
             "measurement_proxy": "Exponential decay fit to post-event activity",
             "unit": "1/second",
             "empirical_range": {"min": 0.1, "max": 10.0}},
            {"variable": "t_i", "latex_symbol": r"t_i",
             "psychological_state": "Historical panic events — memory of past sell cascades",
             "measurement_proxy": "Timestamps of aggressive market sell orders",
             "unit": "nanoseconds"},
        ],
        "axiomatic_origin": ["SYSTEM", "agent_089"],
        "mathematical_wing": ["probability_information", "functional_analysis"],
        "proof_status": "hypothesis",
        "tags": ["hawkes", "self_exciting", "panic_cascade", "order_flow", "seed_corpus", "panic_liquidity"],
    },
]


async def seed(api_base_url: str = "http://localhost:8000") -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        health = await client.get(f"{api_base_url}/health")
        health.raise_for_status()
        print(f"✓ Blackboard API online: {health.json()}")

        for formula_data in SEED_CORPUS:
            response = await client.post(f"{api_base_url}/v1/formulas/seed", json=formula_data)
            if response.status_code == 201:
                result = response.json()
                print(f"✓ Seeded: '{formula_data['name']}' → UUID: {result['uuid']}")
            else:
                print(f"✗ Failed '{formula_data['name']}': {response.text}")

        library = await client.get(f"{api_base_url}/v1/formulas?status=hypothesis&tag=seed_corpus")
        seeded = library.json()
        print(f"\n{'='*60}")
        print(f"Seed corpus loaded: {len(seeded)} formulas")
        for f in seeded:
            print(f"  • {f.get('name', f['uuid'][:8])} — {f['proof_status']}")


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    asyncio.run(seed(url))
