#!/usr/bin/env python3
"""Generate the Formula Foundry complete concat file from the local repo."""
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Ordered file list matching the canonical concat structure
FILES = [
    "AGENT_MATRIX.md",
    "Dockerfile",
    "docker-compose.yml",
    "pyproject.toml",
    "foundry/__init__.py",
    "foundry/core/__init__.py",
    "foundry/core/schema/__init__.py",
    "foundry/core/schema/formula.py",
    "foundry/core/schema/isomorphism.py",
    "foundry/core/db/__init__.py",
    "foundry/core/db/neo4j_client.py",
    "foundry/core/api/__init__.py",
    "foundry/core/api/main.py",
    "foundry/agents/__init__.py",
    "foundry/agents/base.py",
    "foundry/agents/sympy_executor.py",
    "foundry/agents/specialist/__init__.py",
    "foundry/agents/specialist/agent_050.py",
    "foundry/agents/specialist/agent_051.py",
    "foundry/agents/specialist/agent_060.py",
    "foundry/agents/specialist/agent_089.py",
    "foundry/agents/specialist/agent_105.py",
    "foundry/agents/specialist/agent_151.py",
    "foundry/agents/specialist/agent_152.py",
    "foundry/agents/orchestration/__init__.py",
    "foundry/agents/orchestration/librarian.py",
    "foundry/agents/orchestration/agent_002.py",
    "foundry/agents/orchestration/agent_003.py",
    "foundry/agents/evolutionary/__init__.py",
    "foundry/agents/evolutionary/agent_201.py",
    "foundry/agents/evolutionary/agent_202.py",
    "foundry/agents/evolutionary/agent_203.py",
    "foundry/agents/evolutionary/agent_204.py",
    "foundry/agents/evolutionary/agent_205.py",
    "foundry/dag/__init__.py",
    "foundry/dag/triggers.py",
    "foundry/dag/triggers_adverse_selection.py",
    "foundry/dag/flows.py",
    "foundry/dag/synthesis_flow.py",
    "foundry/dag/evolutionary_flow.py",
    "foundry/dag/evolutionary_flow_202.py",
    "foundry/dag/evolutionary_flow_203.py",
    "foundry/dag/evolutionary_flow_204.py",
    "foundry/dag/evolutionary_flow_205.py",
    "foundry/dag/adverse_selection_flow.py",
    "foundry/dag/cross_asset_flow.py",
    "foundry/dag/entropy_flow.py",
    "foundry/ingest/__init__.py",
    "foundry/ingest/databento_ingest.py",
    "foundry/ingest/databento_ingest_adverse_selection_patch.py",
    "lean/Dockerfile",
    "lean/server.py",
    "scripts/seed_corpus.py",
    "scripts/seed_rejection.py",
    "scripts/verify_environment.py",
]

HEADER = """FORMULA FOUNDRY — COMPLETE SOURCE REPOSITORY
Version: {version}
Generated: {date}
Changes:
  v3.5.0 — agent_050 documentation sync: Agent 153 stale refs purged (6 hunks)
  v3.5.1 — librarian.py: Jaccard → semantic embedding scoring (text-embedding-3-small)
             wing(0.50) + semantic(0.35) + tag(0.15) | Jaccard fallback preserved
  v3.5.2 — base.py: claude-sonnet-4-6 + AsyncAnthropic + await
             docker-compose: foundry_worker service, REPL_COMMIT master, lake cache fix
             lean/server.py: elan PATH, cwd, env=None, blank-line protocol, multi-line JSON
             lean/Dockerfile: master branch, lake build repl, Mathlib dependency injection
  v3.5.3 — agent_151.py: pacing fix (max 2 syntax checks, AGENT_151_MAX_ITERATIONS=15)
             synthesis_flow.py: _get_agent_151_config() for dedicated 151 budget
             lean/Dockerfile: native toolchain, no Mathlib rev pin, LEAN_PATH via lean_path.txt
             lean/server.py: reads /tmp/lean_path.txt at startup for subprocess LEAN_PATH
  v3.5.4 — lean/Dockerfile: pin REPL SHA c9cde4d4, remove || true
             lean/server.py: check_expression regex strip + input guard
             librarian.py: exponential backoff on OpenAI 429/5xx (3 retries)
             formula.py: INFORMATION_GEOMETRY added to MathematicalWing enum
             base.py + synthesis_flow.py: per-agent model overrides via AgentConfig
Files: {file_count}"""


def main():
    version = sys.argv[1] if len(sys.argv) > 1 else "3.5.4"
    date = sys.argv[2] if len(sys.argv) > 2 else "2026-04-08 UTC"

    missing = [f for f in FILES if not (REPO_ROOT / f).exists()]
    if missing:
        print(f"ERROR: {len(missing)} files not found:", file=sys.stderr)
        for m in missing:
            print(f"  {m}", file=sys.stderr)
        sys.exit(1)

    parts = []
    parts.append(HEADER.format(version=version, date=date, file_count=len(FILES)))
    parts.append("")
    parts.append("=" * 65)
    parts.append("FILE INDEX")
    parts.append("=" * 65)
    for f in FILES:
        parts.append(f"  {f}")
    parts.append("")
    parts.append("")

    for f in FILES:
        parts.append("=" * 65)
        parts.append(f"FILE: {f}")
        parts.append("=" * 65)
        parts.append((REPO_ROOT / f).read_text(encoding="utf-8"))
        parts.append("")

    output = "\n".join(parts)
    outpath = REPO_ROOT / f"formula-foundry-complete-concat-v{version.replace('.', '_')}.txt"
    outpath.write_text(output, encoding="utf-8")
    print(f"Generated: {outpath} ({len(FILES)} files, {len(output):,} chars)")


if __name__ == "__main__":
    main()
