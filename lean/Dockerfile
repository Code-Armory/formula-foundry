# =============================================================================
# Formula Foundry — Lean 4 Verification Worker
# =============================================================================
#
# Builds the leanprover-community/repl binary with pre-cached Mathlib4
# and serves it behind a Python HTTP wrapper.
#
# BUILD TIME (first build): ~30-50 min (Mathlib4 cache download ~4GB)
# BUILD TIME (subsequent): ~2-5 min (Docker layer cache)
# RUNTIME MEMORY: ~4-8 GB for warm Mathlib environment
#
# REPL binary: lake build repl → .lake/build/bin/repl (lowercase)
# ENV REPL_BINARY=/repl/.lake/build/bin/repl (confirmed correct)
# =============================================================================

FROM ubuntu:22.04

LABEL maintainer="Formula Foundry"
LABEL description="Lean 4 + Mathlib4 persistent REPL for formal proof verification"

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    python3 \
    python3-pip \
    wget \
    ca-certificates \
    libgmp-dev \
    && rm -rf /var/lib/apt/lists/*

ENV ELAN_HOME=/root/.elan
ENV PATH="$ELAN_HOME/bin:$PATH"

RUN curl -sSfL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
    | sh -s -- --default-toolchain none -y \
    && elan --version

ARG REPL_COMMIT=master
# PRODUCTION: Replace "main" with a specific commit SHA

WORKDIR /repl
RUN git clone https://github.com/leanprover-community/repl.git . \
    && git checkout ${REPL_COMMIT} \
    && echo "REPL at commit: $(git log --oneline -1)" \
    && cat lean-toolchain

RUN elan toolchain install "$(cat lean-toolchain)" \
    && elan override set "$(cat lean-toolchain)" \
    && lean --version

# Add Mathlib as a dependency so `import Mathlib` works in the REPL.
# The REPL master branch is standalone — we inject the require here.
RUN echo '' >> lakefile.toml && \
    echo '[[require]]' >> lakefile.toml && \
    echo 'name = "mathlib"' >> lakefile.toml && \
    echo 'scope = "leanprover-community"' >> lakefile.toml && \
    echo 'rev = "v4.29.0"' >> lakefile.toml && \
    cat lakefile.toml

RUN lake update 2>&1 | tee /tmp/lake-update.log && \
    echo "Lake update complete." && \
    tail -10 /tmp/lake-update.log

RUN echo "Fetching Mathlib prebuilt cache..." && \
    lake exe cache get 2>&1 | tee /tmp/mathlib-cache.log && \
    echo "Cache fetch complete." && \
    tail -20 /tmp/mathlib-cache.log || \
    echo "Cache fetch had errors — proceeding with lake build"

RUN lake build repl 2>&1 | tee /tmp/repl-build.log \
    && echo "Build complete. Verifying binary:" \
    && ls -la .lake/build/bin/ \
    && .lake/build/bin/repl --help 2>&1 | head -5 || true

RUN pip3 install --no-cache-dir \
    "fastapi>=0.111.0" \
    "uvicorn[standard]>=0.30.0" \
    "pydantic>=2.7.0"

COPY server.py /app/server.py

# Binary path: lake build repl produces .lake/build/bin/repl (lowercase)
ENV REPL_BINARY=/repl/.lake/build/bin/repl
ENV LEAN_MAX_HEARTBEATS=400000
ENV PROOF_TIMEOUT_SECONDS=120
ENV PORT=8080

EXPOSE 8080

HEALTHCHECK \
    --interval=30s \
    --timeout=10s \
    --start-period=180s \
    --retries=5 \
    CMD curl -sf http://localhost:8080/health | python3 -c \
        "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('ready') else 1)"

WORKDIR /app
CMD ["python3", "/app/server.py"]
