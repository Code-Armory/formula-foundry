# =============================================================================
# Formula Foundry — Lean 4 Verification Worker
# =============================================================================
FROM ubuntu:22.04

LABEL maintainer="Formula Foundry"
LABEL description="Lean 4 + Mathlib4 persistent REPL for formal proof verification"

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git python3 python3-pip wget ca-certificates libgmp-dev \
    && rm -rf /var/lib/apt/lists/*

ENV ELAN_HOME=/root/.elan
ENV PATH="$ELAN_HOME/bin:$PATH"

RUN curl -sSfL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
    | sh -s -- --default-toolchain none -y && elan --version

# Pinned 2026-04-08: Lean 4.30.0-rc1 toolchain.
ARG REPL_COMMIT=c9cde4d461fcf1d56e121bbe7d0c96a7ad40e492

WORKDIR /repl
RUN git clone https://github.com/leanprover-community/repl.git . \
    && git checkout ${REPL_COMMIT} \
    && echo "REPL at commit: $(git log --oneline -1)" \
    && cat lean-toolchain

RUN elan toolchain install "$(cat lean-toolchain)" \
    && elan override set "$(cat lean-toolchain)" \
    && lean --version

# Add Mathlib — no rev pin, let lake resolve compatible version for this toolchain
RUN echo '' >> lakefile.toml && \
    echo '[[require]]' >> lakefile.toml && \
    echo 'name = "mathlib"' >> lakefile.toml && \
    echo 'scope = "leanprover-community"' >> lakefile.toml && \
    cat lakefile.toml

RUN lake update 2>&1 | tee /tmp/lake-update.log && \
    echo "Lake update complete." && tail -10 /tmp/lake-update.log

RUN echo "Fetching Mathlib prebuilt cache..." && \
    lake exe cache get 2>&1 | tee /tmp/mathlib-cache.log && \
    echo "Cache fetch complete." && \
    tail -20 /tmp/mathlib-cache.log || \
    echo "Cache fetch had errors — proceeding with lake build"

RUN lake build repl 2>&1 | tee /tmp/repl-build.log \
    && echo "Build complete. Verifying binary:" \
    && ls -la .lake/build/bin/ \
    && .lake/build/bin/repl --help 2>&1 | head -5

# Compute full LEAN_PATH: core Lean lib + ALL Lake package libs
# server.py reads /tmp/lean_path.txt at startup
RUN CORE_LIB=$(find /root/.elan/toolchains -name "Init.olean" -not -path "*/Lake/*" -exec dirname {} \; | head -1) && \
    PKG_LIBS=$(find /repl/.lake -path "*/build/lib/lean" -type d | tr "\n" ":") && \
    FULL_PATH="${CORE_LIB}:${PKG_LIBS}" && \
    echo "LEAN_PATH=${FULL_PATH}" && \
    echo "${FULL_PATH}" > /tmp/lean_path.txt

RUN pip3 install --no-cache-dir \
    "fastapi>=0.111.0" "uvicorn[standard]>=0.30.0" "pydantic>=2.7.0"

COPY server.py /app/server.py

ENV REPL_BINARY=/repl/.lake/build/bin/repl
ENV LEAN_MAX_HEARTBEATS=400000
ENV PROOF_TIMEOUT_SECONDS=120
ENV PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=180s --retries=5 \
    CMD curl -sf http://localhost:8080/health | python3 -c \
        "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('ready') else 1)"

WORKDIR /app
CMD ["python3", "/app/server.py"]
