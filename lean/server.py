#!/usr/bin/env python3
"""
Lean 4 REPL HTTP Server — Formula Foundry Verification Worker

Wraps the leanprover-community/repl binary with an async HTTP API.
Maintains one persistent Lean 4 subprocess with Mathlib loaded in memory.

Why one subprocess, not a pool:
  The Lean elaborator is not designed for concurrent access. Its internal
  state is single-threaded. Concurrent requests corrupt env indices.
  We serialize with asyncio.Lock. For Phase 1 with one Agent 151 instance,
  throughput is not the bottleneck.

REPL subprocess protocol:
  Input  (stdin):  {"cmd": "..."}          (no env field on first command)
                   {"cmd": "...", "env": N} (env field on subsequent commands)
  Output (stdout): {"env": N+1, "messages": [...]}\\n

Startup sequence:
  1. Spawn REPL subprocess (with elan on PATH, cwd=/repl)
  2. Send "import Mathlib" with NO env field (env=0 is rejected by this REPL)
  3. Store base_env index from response
  4. Mark ready=True
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger("lean_worker")

REPL_BINARY = os.environ.get("REPL_BINARY", "/repl/.lake/build/bin/repl")
LEAN_MAX_HEARTBEATS = int(os.environ.get("LEAN_MAX_HEARTBEATS", "400000"))
PROOF_TIMEOUT_SECONDS = int(os.environ.get("PROOF_TIMEOUT_SECONDS", "120"))
MATHLIB_LOAD_TIMEOUT = int(os.environ.get("MATHLIB_LOAD_TIMEOUT", "300"))
SERVER_PORT = int(os.environ.get("PORT", "8080"))


class LeanREPLManager:
    def __init__(self) -> None:
        self._process: Optional[subprocess.Popen] = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._base_env: Optional[int] = None
        self._ready: bool = False
        self._startup_error: Optional[str] = None
        self._requests_served: int = 0
        self._startup_elapsed: Optional[float] = None

    @property
    def is_ready(self) -> bool:
        return self._ready and self._base_env is not None

    async def startup(self) -> None:
        if not os.path.exists(REPL_BINARY):
            self._startup_error = f"REPL binary not found: {REPL_BINARY}"
            logger.error(self._startup_error)
            raise FileNotFoundError(self._startup_error)

        # Build environment with elan on PATH so the REPL binary can find lean.
        repl_env = os.environ.copy()
        elan_home = repl_env.get("ELAN_HOME", "/root/.elan")
        repl_env["PATH"] = f"{elan_home}/bin:" + repl_env.get("PATH", "")
        # Load LEAN_PATH from build-time computed file, or fall back to ENV.
        lean_path_file = "/tmp/lean_path.txt"
        if os.path.exists(lean_path_file):
            with open(lean_path_file, "r") as f:
                repl_env["LEAN_PATH"] = f.read().strip()
            logger.info("Lean REPL: loaded LEAN_PATH from %s", lean_path_file)
        elif "LEAN_PATH" in repl_env:
            logger.info("Lean REPL: using LEAN_PATH from environment")
        else:
            logger.warning("Lean REPL: NO LEAN_PATH set — Mathlib will not load!")

        logger.info("Lean REPL: spawning subprocess: %s", REPL_BINARY)
        self._process = subprocess.Popen(
            [REPL_BINARY],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            cwd="/repl",
            env=repl_env,
        )
        logger.info("Lean REPL: subprocess PID %d", self._process.pid)
        logger.info("Lean REPL: loading Mathlib (maxHeartbeats=%d, timeout=%ds)...",
                    LEAN_MAX_HEARTBEATS, MATHLIB_LOAD_TIMEOUT)

        start = time.monotonic()
        try:
            # NOTE: "import Mathlib" must be sent WITHOUT an env field.
            # The REPL rejects {"cmd": "...", "env": 0} on the first command.
            result = await self._send_raw("import Mathlib", env=None,
                                          timeout=MATHLIB_LOAD_TIMEOUT)
        except asyncio.TimeoutError:
            self._startup_error = f"Mathlib failed to load within {MATHLIB_LOAD_TIMEOUT}s."
            logger.error(self._startup_error)
            raise TimeoutError(self._startup_error)

        elapsed = time.monotonic() - start

        if "env" not in result:
            self._startup_error = f"Mathlib load returned no env: {result.get('messages', [])}"
            logger.error(self._startup_error)
            raise RuntimeError(self._startup_error)

        self._base_env = result["env"]

        # Set maxHeartbeats in the Mathlib environment.
        try:
            await self._send_raw(
                f"set_option maxHeartbeats {LEAN_MAX_HEARTBEATS}",
                env=self._base_env,
                timeout=10,
            )
        except Exception:
            pass  # Non-fatal — heartbeat default is acceptable

        self._ready = True
        self._startup_elapsed = elapsed
        logger.info("Lean REPL: READY. Mathlib loaded in %.1fs. Base env: %d",
                    elapsed, self._base_env)

    async def shutdown(self) -> None:
        if self._process:
            if self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait()
        logger.info("Lean REPL: shutdown. Served %d proof requests.", self._requests_served)

    async def verify(self, theorem_statement: str, proof_body: str,
                     timeout: int = PROOF_TIMEOUT_SECONDS) -> Dict[str, Any]:
        if not self.is_ready:
            return {"valid": False, "errors": ["REPL not ready. Retry shortly."],
                    "warnings": [], "elapsed_seconds": 0.0, "env": None, "raw_messages": []}

        indented_proof = "\n  ".join(proof_body.strip().splitlines())
        cmd = f"{theorem_statement.strip()} := by\n  {indented_proof}"

        async with self._lock:
            self._assert_process_alive()
            start = time.monotonic()
            try:
                result = await self._send_raw(cmd, env=self._base_env, timeout=timeout)
            except asyncio.TimeoutError:
                elapsed = time.monotonic() - start
                return {"valid": False,
                        "errors": [f"Proof timed out after {timeout}s."],
                        "warnings": [], "elapsed_seconds": round(elapsed, 3),
                        "env": None, "raw_messages": []}
            elapsed = time.monotonic() - start
            self._requests_served += 1

        messages = result.get("messages", [])
        errors = [m.get("data", str(m)) for m in messages if m.get("severity") == "error"]
        warnings = [m.get("data", str(m)) for m in messages if m.get("severity") == "warning"]
        valid = ("env" in result) and len(errors) == 0

        logger.info("Proof %s in %.3fs | errors=%d warnings=%d",
                    "ACCEPTED" if valid else "REJECTED", elapsed, len(errors), len(warnings))
        return {
            "valid": valid, "errors": errors, "warnings": warnings,
            "elapsed_seconds": round(elapsed, 3),
            "env": result.get("env"), "raw_messages": messages,
        }

    async def check_expression(self, expression: str) -> Dict[str, Any]:
        if not self.is_ready:
            return {"valid": False, "errors": ["REPL not ready"]}

        cmd = f"#check ({expression})"
        async with self._lock:
            self._assert_process_alive()
            try:
                result = await self._send_raw(cmd, env=self._base_env, timeout=30)
            except asyncio.TimeoutError:
                return {"valid": False, "errors": ["#check timed out"]}

        messages = result.get("messages", [])
        errors = [m.get("data", "") for m in messages if m.get("severity") == "error"]
        info = [m.get("data", "") for m in messages if m.get("severity") == "info"]
        return {"valid": len(errors) == 0, "type_info": info[0] if info else None, "errors": errors}

    def _assert_process_alive(self) -> None:
        if self._process is None or self._process.poll() is not None:
            exit_code = self._process.poll() if self._process else None
            self._ready = False
            self._base_env = None
            raise RuntimeError(
                f"REPL subprocess terminated (exit code {exit_code}). "
                "Docker will restart the container."
            )

    async def _send_raw(self, cmd: str, env: Optional[int],
                        timeout: int) -> Dict[str, Any]:
        # Omit the env field entirely when env is None.
        # The REPL rejects {"env": null} and {"env": 0} on the very first command.
        msg: Dict[str, Any] = {"cmd": cmd}
        if env is not None:
            msg["env"] = env
        payload = json.dumps(msg, ensure_ascii=False) + "\n\n"

        def _blocking_io() -> Dict[str, Any]:
            assert self._process is not None
            self._process.stdin.write(payload.encode("utf-8"))
            self._process.stdin.flush()
            buf = ""
            while True:
                raw = self._process.stdout.readline()
                if not raw:
                    raise RuntimeError("REPL subprocess closed stdout")
                line = raw.decode("utf-8")
                if line.strip() == "":
                    if buf.strip():
                        return json.loads(buf.strip())
                    continue
                buf += line
                try:
                    return json.loads(buf.strip())
                except json.JSONDecodeError:
                    pass

        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(loop.run_in_executor(None, _blocking_io), timeout=timeout)


_manager: Optional[LeanREPLManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _manager
    _manager = LeanREPLManager()
    try:
        await _manager.startup()
    except Exception as exc:
        logger.error("REPL startup failed: %s\nServer will run but /v1/verify returns 503.", exc)
    yield
    if _manager:
        await _manager.shutdown()


app = FastAPI(
    title="Formula Foundry — Lean 4 Verification Worker",
    description="Persistent Lean 4 REPL with pre-loaded Mathlib4.",
    version="0.1.0",
    lifespan=lifespan,
)


class VerifyRequest(BaseModel):
    theorem_statement: str
    proof_body: str
    context: Optional[str] = None
    timeout_seconds: Optional[int] = Field(None, ge=1, le=600)


class CheckRequest(BaseModel):
    expression: str


class VerifyResponse(BaseModel):
    valid: bool
    errors: List[str]
    warnings: List[str]
    elapsed_seconds: float
    lean4_env_index: Optional[int] = None


@app.get("/health", tags=["Infrastructure"])
async def health():
    if _manager is None:
        return {"status": "initializing", "ready": False}
    if _manager._startup_error:
        return {"status": "error", "ready": False, "error": _manager._startup_error}
    return {
        "status": "ready" if _manager.is_ready else "loading_mathlib",
        "ready": _manager.is_ready,
        "mathlib_loaded": _manager.is_ready,
        "base_env_index": _manager._base_env,
        "startup_elapsed_seconds": _manager._startup_elapsed,
        "requests_served": _manager._requests_served,
    }


@app.post("/v1/verify", response_model=VerifyResponse, tags=["Verification"])
async def verify_proof(req: VerifyRequest):
    if _manager is None or not _manager.is_ready:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="Lean REPL not ready. Mathlib still loading.")
    if req.context:
        logger.info("Verify: %s", req.context[:100])
    timeout = req.timeout_seconds or PROOF_TIMEOUT_SECONDS
    try:
        result = await _manager.verify(req.theorem_statement, req.proof_body, timeout=timeout)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return VerifyResponse(
        valid=result["valid"], errors=result["errors"], warnings=result["warnings"],
        elapsed_seconds=result["elapsed_seconds"], lean4_env_index=result.get("env"),
    )


@app.post("/v1/check", response_model=dict, tags=["Verification"])
async def check_expression(req: CheckRequest):
    if _manager is None or not _manager.is_ready:
        raise HTTPException(status_code=503, detail="REPL not ready")
    return await _manager.check_expression(req.expression)


@app.get("/v1/status", tags=["Infrastructure"])
async def repl_status():
    if _manager is None:
        return {"ready": False, "phase": "pre_init"}
    return {
        "ready": _manager.is_ready,
        "phase": "ready" if _manager.is_ready else "loading_mathlib",
        "base_env_index": _manager._base_env,
        "requests_served": _manager._requests_served,
        "startup_elapsed_seconds": _manager._startup_elapsed,
        "process_alive": _manager._process is not None and _manager._process.poll() is None,
        "config": {
            "repl_binary": REPL_BINARY,
            "lean_max_heartbeats": LEAN_MAX_HEARTBEATS,
            "proof_timeout_seconds": PROOF_TIMEOUT_SECONDS,
            "mathlib_load_timeout": MATHLIB_LOAD_TIMEOUT,
        },
    }


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=SERVER_PORT,
                log_level="info", access_log=True)
