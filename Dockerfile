# =============================================================================
# Formula Foundry — Blackboard API
# =============================================================================
#
# FastAPI layer over Neo4j (the Consensus Mesh) and PostgreSQL (DAG state).
#
# DEV MODE (docker-compose default):
#   Source is mounted at /app via volumes: - .:/app
#   uvicorn runs with --reload so code changes are picked up immediately.
#   PYTHONPATH=/app ensures the 'foundry' package is importable from the mount.
#
# PRODUCTION:
#   Remove the volume mount from docker-compose.yml and uncomment the
#   COPY instruction below to bake source into the image.
#
# ANTHROPIC_API_KEY:
#   NOT set in docker-compose.yml — this is intentional.
#   The API container is the Blackboard only (Neo4j HTTP layer).
#   Agents run as standalone Python processes (Prefect tasks) and receive
#   ANTHROPIC_API_KEY from the shell environment at invocation time:
#     ANTHROPIC_API_KEY=sk-... python -m foundry.dag.flows --test
#
# MISSING DEPENDENCY NOTE:
#   numpy is required by foundry/dag/triggers.py but is absent from
#   pyproject.toml. It is installed explicitly here. Add it to
#   pyproject.toml under [tool.poetry.dependencies].
# =============================================================================

FROM python:3.12-slim

LABEL maintainer="Formula Foundry"
LABEL description="Blackboard API — Neo4j Consensus Mesh over FastAPI"

WORKDIR /app

# System dependencies
# curl: healthcheck probe and API testing
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Python dependencies
# Installed from pinned ranges matching pyproject.toml.
# Installed as a separate layer so Docker cache survives source changes.
#
# numpy is listed here even though it is absent from pyproject.toml —
# it is required by foundry/dag/triggers.py (_compute_spread_percentile).
# ---------------------------------------------------------------------------
RUN pip install --no-cache-dir \
    "fastapi>=0.111.0,<0.200.0" \
    "uvicorn[standard]>=0.30.0,<0.40.0" \
    "pydantic>=2.7.0,<3.0.0" \
    "neo4j>=5.21.0,<6.0.0" \
    "asyncpg>=0.29.0,<0.30.0" \
    "sqlalchemy[asyncio]>=2.0.0,<3.0.0" \
    "anthropic>=0.29.0,<0.60.0" \
    "sympy>=1.12,<2.0" \
    "httpx>=0.27.0,<0.30.0" \
    "python-dotenv>=1.0.0,<2.0.0" \
    "numpy>=1.26.0,<3.0.0"

# ---------------------------------------------------------------------------
# PYTHONPATH: ensures 'foundry' package is importable from the mounted volume.
# In dev mode the source arrives via docker-compose volumes: - .:/app
# In production mode uncomment the COPY line below and remove the volume mount.
# ---------------------------------------------------------------------------
ENV PYTHONPATH=/app

# PRODUCTION ONLY — uncomment when baking source into the image:
# COPY foundry/ ./foundry/

EXPOSE 8000

# Liveness probe used by docker-compose healthcheck (if added later)
HEALTHCHECK \
    --interval=15s \
    --timeout=10s \
    --start-period=30s \
    --retries=8 \
    CMD curl -sf http://localhost:8000/health || exit 1

# Default command — overridden by docker-compose for --reload in dev
CMD ["uvicorn", "foundry.core.api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--log-level", "info"]
