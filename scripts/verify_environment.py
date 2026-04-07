#!/usr/bin/env python3
"""
Formula Foundry — Environment Verification

Pre-flight check for the complete development environment.
Run this before the ignition sequence to catch every known failure mode.

Usage:
  PYTHONPATH=. python scripts/verify_environment.py

Exit codes:
  0 — All hard checks passed (warnings are noted but do not block)
  1 — One or more hard failures — fix before running ignition sequence

Hard failures (exit 1):
  - Python < 3.12
  - ANTHROPIC_API_KEY not set
  - Critical source files missing (Dockerfile, agents, flows)
  - Key bug fixes not applied (authority table, proposal intercept)
  - numpy not importable (triggers.py will crash on import)

Warnings (printed but do not block):
  - Docker services not running yet (start them first, then re-run)
  - Lean worker not ready (Mathlib still loading — normal on first boot)
  - Prefect not installed (flows run without orchestration via shim)
  - pyproject.toml missing numpy (functional but inconsistent)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Colour support (degrades gracefully on terminals without ANSI)
# ---------------------------------------------------------------------------

_USE_COLOUR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None

def _c(text: str, code: str) -> str:
    if not _USE_COLOUR:
        return text
    return f"\033[{code}m{text}\033[0m"

OK   = _c("✓", "32")    # green
FAIL = _c("✗", "31")    # red
WARN = _c("~", "33")    # yellow
INFO = _c("·", "36")    # cyan


# ---------------------------------------------------------------------------
# State tracking
# ---------------------------------------------------------------------------

_hard_failures: List[str] = []
_warnings: List[str] = []

def _ok(label: str, detail: str = "") -> None:
    suffix = f"  ({detail})" if detail else ""
    print(f"  {OK} {label}{suffix}")

def _fail(label: str, fix: str = "") -> None:
    _hard_failures.append(label)
    print(f"  {FAIL} {label}")
    if fix:
        print(f"      FIX: {fix}")

def _warn(label: str, note: str = "") -> None:
    _warnings.append(label)
    suffix = f"  ({note})" if note else ""
    print(f"  {WARN} {label}{suffix}")

def _info(label: str) -> None:
    print(f"  {INFO} {label}")

def _section(title: str) -> None:
    print(f"\n[{title}]")


# ---------------------------------------------------------------------------
# HTTP helper (stdlib only — httpx may not be installed yet)
# ---------------------------------------------------------------------------

def _get_json(url: str, timeout: int = 5) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """GET a URL and parse JSON. Returns (data, error_message)."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode()), None
    except urllib.error.URLError as exc:
        return None, str(exc.reason)
    except Exception as exc:
        return None, str(exc)


# ---------------------------------------------------------------------------
# Docker helper (subprocess — no docker SDK required)
# ---------------------------------------------------------------------------

def _docker_container_status(container_name: str) -> Optional[str]:
    """Return container State.Status or None if not found."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", container_name],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def _docker_healthy(container_name: str) -> bool:
    """Return True if container healthcheck is 'healthy'."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container_name],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip() == "healthy"
        return False
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


# ---------------------------------------------------------------------------
# Check groups
# ---------------------------------------------------------------------------

def check_system() -> None:
    _section("SYSTEM")

    # Python version
    major, minor = sys.version_info.major, sys.version_info.minor
    version_str = f"{major}.{minor}.{sys.version_info.micro}"
    if major == 3 and minor >= 12:
        _ok(f"Python {version_str}")
    else:
        _fail(
            f"Python {version_str} — requires 3.12+",
            "Install Python 3.12: https://python.org/downloads or use pyenv",
        )

    # Platform note (SymPy executor uses Linux resource limits)
    if sys.platform != "linux":
        _warn(
            f"Platform: {sys.platform}",
            "SymPy OS resource limits (RLIMIT_AS) only apply on Linux — "
            "subprocess isolation still protects you on macOS/Windows",
        )
    else:
        _ok(f"Platform: {sys.platform}")


def check_environment() -> None:
    _section("ENVIRONMENT VARIABLES")

    # Hard required
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        masked = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
        _ok(f"ANTHROPIC_API_KEY ({masked})")
    else:
        _fail(
            "ANTHROPIC_API_KEY not set",
            "export ANTHROPIC_API_KEY=sk-ant-... or add to .env file",
        )

    # Optional with defaults
    api_url = os.environ.get("BLACKBOARD_API_URL", "")
    if api_url:
        _ok(f"BLACKBOARD_API_URL={api_url}")
    else:
        _warn(
            "BLACKBOARD_API_URL not set",
            "will default to http://localhost:8000 in flow scripts",
        )

    lean_url = os.environ.get("LEAN_WORKER_URL", "")
    if lean_url:
        _ok(f"LEAN_WORKER_URL={lean_url}")
    else:
        _warn(
            "LEAN_WORKER_URL not set",
            "synthesis_flow defaults to http://lean_worker:8080 (correct inside Docker)",
        )

    # ANTHROPIC_API_KEY architecture note
    _info(
        "ANTHROPIC_API_KEY is NOT needed in docker-compose — "
        "agents run as standalone scripts, not inside the API container"
    )


def check_files() -> None:
    _section("SOURCE FILES")

    # Critical files that must exist
    required: List[Tuple[str, str]] = [
        # Core schema
        ("foundry/core/schema/formula.py",        "FormulaDNA schema"),
        ("foundry/core/schema/isomorphism.py",    "Isomorphism schema"),
        # DB
        ("foundry/core/db/neo4j_client.py",       "Blackboard client"),
        # API
        ("foundry/core/api/main.py",              "FastAPI app"),
        # Base agent
        ("foundry/agents/base.py",                "BaseAgent"),
        ("foundry/agents/sympy_executor.py",      "SymPy executor"),
        # Specialists
        ("foundry/agents/specialist/agent_089.py", "Agent 089 (Hawkes)"),
        ("foundry/agents/specialist/agent_105.py", "Agent 105 (Synthesizer)"),
        ("foundry/agents/specialist/agent_151.py", "Agent 151 (Lean Auditor)"),
        # Orchestration
        ("foundry/agents/orchestration/librarian.py", "Librarian Router"),
        # Evolutionary
        ("foundry/agents/evolutionary/agent_201.py", "Agent 201 (Temporal Bridger)"),
        # DAG
        ("foundry/dag/triggers.py",               "Panic trigger"),
        ("foundry/dag/flows.py",                  "Panic flow (Agent 089)"),
        ("foundry/dag/synthesis_flow.py",         "Synthesis flow (105+151)"),
        ("foundry/dag/evolutionary_flow.py",      "Evolutionary flow (201)"),
        # Lean worker
        ("lean/Dockerfile",                       "Lean worker Dockerfile"),
        ("lean/server.py",                        "Lean REPL HTTP wrapper"),
        # Root
        ("Dockerfile",                            "API Dockerfile"),
        ("docker-compose.yml",                    "Docker Compose"),
        ("pyproject.toml",                        "Python project config"),
        # Scripts
        ("scripts/seed_corpus.py",                "Seed corpus script"),
        ("scripts/seed_rejection.py",             "Seed rejection script"),
    ]

    repo_root = Path(__file__).parent.parent
    missing: List[str] = []
    for rel_path, description in required:
        path = repo_root / rel_path
        if path.exists():
            _ok(f"{rel_path}", description)
        else:
            _fail(
                f"MISSING: {rel_path}  ({description})",
                f"Apply the generated file to your repo at: {rel_path}",
            )
            missing.append(rel_path)

    # __init__.py files (less critical — Python still imports without them in 3.3+)
    init_files = [
        "foundry/__init__.py",
        "foundry/agents/__init__.py",
        "foundry/agents/specialist/__init__.py",
        "foundry/agents/orchestration/__init__.py",
        "foundry/agents/evolutionary/__init__.py",
        "foundry/core/__init__.py",
        "foundry/core/api/__init__.py",
        "foundry/core/db/__init__.py",
        "foundry/core/schema/__init__.py",
        "foundry/dag/__init__.py",
    ]
    missing_inits = [p for p in init_files if not (repo_root / p).exists()]
    if missing_inits:
        _warn(
            f"{len(missing_inits)} __init__.py files missing",
            f"touch {' '.join(missing_inits[:3])}{'...' if len(missing_inits) > 3 else ''}",
        )
    else:
        _ok("All __init__.py files present")


def check_bug_fixes() -> None:
    _section("BUG FIX VERIFICATION")
    _info("Confirming all generated fixes were applied to source files")

    repo_root = Path(__file__).parent.parent

    # Fix 1: LAYER_1 in SYNTACTICALLY_CORRECT authority
    neo4j_path = repo_root / "foundry/core/db/neo4j_client.py"
    if neo4j_path.exists():
        src = neo4j_path.read_text()
        if "AgentLayer.LAYER_1, AgentLayer.LAYER_2, AgentLayer.LAYER_3," in src:
            _ok("neo4j_client.py: LAYER_1 in SYNTACTICALLY_CORRECT authority")
        else:
            _fail(
                "neo4j_client.py: LAYER_1 missing from SYNTACTICALLY_CORRECT authority",
                "Replace _WRITE_AUTHORITY dict with the corrected version from the generated file",
            )
    else:
        _warn("neo4j_client.py not found — skipping fix verification")

    # Fix 2: _PROPOSAL_TOOL_NAMES frozenset in base.py
    base_path = repo_root / "foundry/agents/base.py"
    if base_path.exists():
        src = base_path.read_text()
        if "_PROPOSAL_TOOL_NAMES" in src and "propose_unified_formula" in src:
            _ok("base.py: _PROPOSAL_TOOL_NAMES frozenset includes propose_unified_formula")
        elif "if tool_name == \"propose_formula_to_blackboard\":" in src:
            _fail(
                "base.py: terminal action intercept still hardcoded to propose_formula_to_blackboard",
                "Apply the corrected base.py — Agent 105's propose_unified_formula will never trigger",
            )
        else:
            _warn("base.py: could not determine intercept state — inspect manually")
    else:
        _warn("base.py not found — skipping fix verification")

    # Fix 3: MEASURE_THEORY + ERGODIC_THEORY in MathematicalWing enum
    formula_path = repo_root / "foundry/core/schema/formula.py"
    if formula_path.exists():
        src = formula_path.read_text()
        has_measure = 'MEASURE_THEORY = "measure_theory"' in src
        has_ergodic = 'ERGODIC_THEORY = "ergodic_theory"' in src
        if has_measure and has_ergodic:
            _ok("formula.py: MEASURE_THEORY + ERGODIC_THEORY in MathematicalWing enum")
        else:
            missing_vals = []
            if not has_measure:
                missing_vals.append("MEASURE_THEORY")
            if not has_ergodic:
                missing_vals.append("ERGODIC_THEORY")
            _fail(
                f"formula.py: missing enum values: {', '.join(missing_vals)}",
                "Apply the corrected formula.py from the generated output",
            )
    else:
        _warn("formula.py not found — skipping fix verification")


def check_dependencies() -> None:
    _section("PYTHON DEPENDENCIES")

    packages = [
        ("fastapi",         "FastAPI — Blackboard API layer"),
        ("uvicorn",         "ASGI server"),
        ("pydantic",        "Schema validation"),
        ("neo4j",           "Neo4j async driver"),
        ("asyncpg",         "PostgreSQL async driver"),
        ("anthropic",       "Claude API client"),
        ("sympy",           "SymPy — formula validation gate"),
        ("httpx",           "Async HTTP client (agent-to-agent)"),
        ("numpy",           "Numpy — spread percentile in triggers.py"),
        ("dotenv",          "python-dotenv — env var loading"),
    ]

    for module, description in packages:
        try:
            __import__(module)
            _ok(f"{module}", description)
        except ImportError:
            if module == "numpy":
                _fail(
                    f"{module} not installed  ({description})",
                    "pip install numpy  AND  add 'numpy = \"^1.26.0\"' to pyproject.toml",
                )
            else:
                _fail(
                    f"{module} not installed  ({description})",
                    f"pip install {module}  or  poetry install",
                )

    # Optional: Prefect
    try:
        import prefect  # noqa: F401
        _ok("prefect", "Prefect orchestration (optional)")
    except ImportError:
        _warn(
            "prefect not installed",
            "flows run without orchestration via shim — acceptable for local testing",
        )

    # Check numpy actually works (import isn't enough — sometimes broken installs)
    try:
        import numpy as np
        arr = np.array([1.0, 2.0, 3.0])
        pct = float(np.mean(arr <= 2.0) * 100.0)
        if abs(pct - 66.666) < 1.0:
            _ok("numpy functional check passed")
        else:
            _fail("numpy percentile computation returned unexpected value", "Reinstall numpy")
    except Exception as exc:
        _fail(f"numpy functional check failed: {exc}", "pip install --force-reinstall numpy")

    # Check SymPy executor works with an integral expression
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from foundry.agents.sympy_executor import validate_formula
        result = validate_formula("Integral(lambda_t, (t, t_open, t_close)) / VOL_d", timeout=15)
        if result.get("valid"):
            symbols = result.get("free_symbols", [])
            _ok(f"SymPy executor: Integral expression valid  (symbols: {symbols})")
        else:
            _fail(
                f"SymPy executor: Integral expression failed: {result.get('error')}",
                "Check sympy_executor.py whitelist — Integral should be present",
            )
    except Exception as exc:
        _warn(f"SymPy executor check skipped: {exc}")


def check_docker() -> None:
    _section("DOCKER SERVICES")

    # Check Docker daemon
    try:
        result = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=10
        )
        if result.returncode == 0:
            _ok("Docker daemon accessible")
        else:
            _fail(
                "Docker daemon not responding",
                "Start Docker Desktop or run: sudo systemctl start docker",
            )
            return
    except FileNotFoundError:
        _fail(
            "Docker not installed or not in PATH",
            "Install Docker: https://docs.docker.com/get-docker/",
        )
        return
    except subprocess.TimeoutExpired:
        _fail("Docker daemon timed out", "Check Docker Desktop is running")
        return

    # Check each service
    services = [
        ("foundry_blackboard",   "neo4j",        True),   # has healthcheck
        ("foundry_dag_state",    "postgres",      True),   # has healthcheck
        ("foundry_api",          "api",           False),  # no healthcheck in compose
        ("foundry_lean_worker",  "lean_worker",   True),   # has healthcheck
    ]

    for container, name, has_health in services:
        status = _docker_container_status(container)
        if status is None:
            _warn(
                f"{name}: container not found",
                f"docker-compose up -d {name}",
            )
        elif status != "running":
            _warn(
                f"{name}: status={status}",
                f"docker-compose up -d {name}",
            )
        elif has_health:
            healthy = _docker_healthy(container)
            if healthy:
                _ok(f"{name}: running (healthy)")
            else:
                _warn(
                    f"{name}: running but not yet healthy",
                    "healthcheck still initializing — wait and re-run",
                )
        else:
            _ok(f"{name}: running")


def check_api() -> None:
    _section("BLACKBOARD API")

    api_url = os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000")

    # Health
    data, err = _get_json(f"{api_url}/health")
    if err:
        _warn(
            f"API unreachable at {api_url}",
            "docker-compose up -d api  then wait ~10s and re-run",
        )
        return

    version = data.get("version", "unknown")
    _ok(f"API health: {data.get('status')}  (v{version})")

    if version == "0.1.0":
        _warn(
            "API version is 0.1.0 — expected 0.2.0",
            "The updated main.py (with Librarian endpoint) was not applied — rebuild container",
        )
    elif version == "0.2.0":
        _ok("API version: 0.2.0 (Librarian endpoint present)")

    # Librarian endpoint
    data, err = _get_json(f"{api_url}/v1/librarian/next-pair")
    if err:
        _fail(
            f"GET /v1/librarian/next-pair failed: {err}",
            "Apply the updated main.py and rebuild: docker-compose build api",
        )
    else:
        available = data.get("available", False)
        if available:
            _ok(f"Librarian: pair available  ({data.get('formula_a_name')} ↔ {data.get('formula_b_name')})")
        else:
            _info(f"Librarian: no pairs available  ({data.get('reason', 'unknown reason')})")


def check_lean_worker() -> None:
    _section("LEAN WORKER")

    lean_url = os.environ.get("LEAN_WORKER_URL", "http://localhost:8080")

    data, err = _get_json(f"{lean_url}/health")
    if err:
        _warn(
            f"Lean worker unreachable at {lean_url}",
            "docker-compose up -d lean_worker  (first build takes 30-50 min)",
        )
        return

    ready = data.get("ready", False)
    status = data.get("status", "unknown")

    if ready:
        startup_s = data.get("startup_elapsed_seconds")
        requests = data.get("requests_served", 0)
        base_env = data.get("base_env_index")
        _ok(f"Lean worker: READY  (startup={startup_s:.1f}s, env={base_env}, served={requests})")
    else:
        _warn(
            f"Lean worker: {status}  (Mathlib still loading)",
            "Wait 3 minutes on first boot, then re-run. "
            "Check: docker-compose logs -f lean_worker",
        )

    # Verify REPL binary path from /v1/status
    status_data, err = _get_json(f"{lean_url}/v1/status")
    if status_data:
        config = status_data.get("config", {})
        binary = config.get("repl_binary", "unknown")
        _ok(f"REPL binary: {binary}")

        # Confirm binary name is 'repl' (lowercase) — this was the outstanding
        # question from the handoff brief. lean/Dockerfile confirms it IS lowercase.
        if binary.endswith("/repl"):
            _ok("REPL binary name: 'repl' (lowercase) — correct")
        elif binary.endswith("/Repl"):
            _fail(
                "REPL binary name is 'Repl' (capital R) — server.py REPL_BINARY env is wrong",
                "Set ENV REPL_BINARY=/repl/.lake/build/bin/Repl in lean/Dockerfile",
            )
        else:
            _warn(f"REPL binary name: unexpected path: {binary}")

        heartbeats = config.get("lean_max_heartbeats")
        timeout = config.get("proof_timeout_seconds")
        _info(f"Proof config: maxHeartbeats={heartbeats}, timeout={timeout}s")


def check_blackboard_state() -> None:
    _section("BLACKBOARD STATE")

    api_url = os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000")

    # Check API is up first
    _, err = _get_json(f"{api_url}/health")
    if err:
        _warn("API not reachable — skipping Blackboard state check")
        return

    # Formulas by status
    total_formulas = 0
    for status in ("hypothesis", "syntactically_correct", "formally_verified", "falsified"):
        data, err = _get_json(f"{api_url}/v1/formulas?status={status}")
        if err:
            _warn(f"Could not query status={status}: {err}")
            continue
        if isinstance(data, list):
            count = len(data)
            total_formulas += count
            if count > 0:
                names = [f.get("name", f.get("uuid", "?")[:8]) for f in data[:3]]
                suffix = f"+ {count-3} more" if count > 3 else ""
                _ok(f"{status}: {count}  ({', '.join(names)}{suffix})")
            else:
                _info(f"{status}: 0 formulas")

    if total_formulas == 0:
        _warn(
            "Blackboard is empty",
            "Run: PYTHONPATH=. python scripts/seed_corpus.py",
        )

    # Librarian readiness: need ≥1 syntactically_correct formula
    sc_data, _ = _get_json(f"{api_url}/v1/formulas?status=syntactically_correct")
    if isinstance(sc_data, list) and len(sc_data) == 0:
        _warn(
            "No syntactically_correct formulas — Librarian will return no_pairs",
            "Run Agent 089 first: PYTHONPATH=. python -m foundry.dag.flows --test",
        )

    # IP library
    ip_data, err = _get_json(f"{api_url}/v1/ip-library")
    if not err and isinstance(ip_data, list):
        if ip_data:
            _ok(f"IP Library: {len(ip_data)} formally verified formula(s)")
        else:
            _info("IP Library: empty (no formally verified formulas yet)")

    # Rejections
    rej_data, err = _get_json(f"{api_url}/v1/rejections")
    if not err and isinstance(rej_data, list):
        if rej_data:
            versions = {r.get("agent_version", "?") for r in rej_data}
            _ok(f"Rejections: {len(rej_data)}  (agent versions: {', '.join(sorted(versions))})")
            synthetic = [r for r in rej_data if r.get("agent_version") == "0.0.0-synthetic"]
            if synthetic:
                _ok(f"  Synthetic seed rejection present — Agent 201 has a target")
            else:
                _warn(
                    "No synthetic rejection found",
                    "Run: PYTHONPATH=. python scripts/seed_rejection.py",
                )
        else:
            _warn(
                "No rejections in graph — Agent 201 will return no_rejections",
                "Run: PYTHONPATH=. python scripts/seed_rejection.py",
            )


def check_pyproject_gaps() -> None:
    _section("PYPROJECT.TOML GAPS")

    repo_root = Path(__file__).parent.parent
    pyproject = repo_root / "pyproject.toml"

    if not pyproject.exists():
        _fail("pyproject.toml not found")
        return

    content = pyproject.read_text()

    if "numpy" not in content:
        _warn(
            "numpy missing from pyproject.toml",
            "Add to [tool.poetry.dependencies]: numpy = \"^1.26.0\"",
        )
    else:
        _ok("numpy in pyproject.toml")

    if "prefect" not in content:
        _warn(
            "prefect not in pyproject.toml",
            "Optional: poetry add prefect — flows run via shim without it",
        )
    else:
        _ok("prefect in pyproject.toml")

    # Check anthropic version supports claude-sonnet-4-5
    if "anthropic" in content:
        # base.py uses _AGENT_MODEL = "claude-sonnet-4-5"
        _info("anthropic SDK version — verify claude-sonnet-4-5 is available in your plan")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(_c("=" * 60, "1"))
    print(_c("  FORMULA FOUNDRY — ENVIRONMENT VERIFICATION", "1"))
    print(_c("=" * 60, "1"))

    check_system()
    check_environment()
    check_files()
    check_bug_fixes()
    check_dependencies()
    check_docker()
    check_api()
    check_lean_worker()
    check_blackboard_state()
    check_pyproject_gaps()

    # Summary
    print(f"\n{'='*60}")
    if _hard_failures:
        print(_c(f"RESULT: {len(_hard_failures)} hard failure(s)  —  fix before running ignition sequence", "31"))
        print()
        for i, f in enumerate(_hard_failures, 1):
            print(f"  {i}. {f}")
        if _warnings:
            print(_c(f"\n  + {len(_warnings)} warning(s) — see above for details", "33"))
        return 1
    else:
        print(_c("RESULT: All hard checks passed", "32"))
        if _warnings:
            print(_c(f"  {len(_warnings)} warning(s) — review above but proceed", "33"))
        print()
        print("Ready. Run the ignition sequence:")
        print("  1. PYTHONPATH=. python scripts/seed_corpus.py")
        print("  2. PYTHONPATH=. python scripts/seed_rejection.py")
        print("  3. ANTHROPIC_API_KEY=sk-... PYTHONPATH=. python -m foundry.dag.flows --test")
        print("  4. ANTHROPIC_API_KEY=sk-... PYTHONPATH=. python -m foundry.dag.synthesis_flow --test")
        print("  5. ANTHROPIC_API_KEY=sk-... PYTHONPATH=. python -m foundry.dag.evolutionary_flow --test")
        print("  6. ANTHROPIC_API_KEY=sk-... PYTHONPATH=. python -m foundry.dag.synthesis_flow --test")
        return 0


if __name__ == "__main__":
    sys.exit(main())


