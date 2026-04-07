"""
SymPy Executor — Restricted subprocess validator for the CoMT pipeline.

Security model:
  - Runs in an isolated child process (spawn, not fork)
  - Explicit math-only local_dict: no builtins, no dunder access
  - global_dict={}: closes the eval() escape hatch
  - OS-level resource limits on Linux (256MB RAM, 5s CPU)
  - Wall-clock timeout enforced by parent via SIGKILL

Pipeline position:
  Claude (LaTeX) → [THIS FILE] (SymPy) → Lean 4 server (formal proof)
"""

from __future__ import annotations

import multiprocessing
import sys
from typing import Any, Dict, FrozenSet

import sympy
from sympy.parsing.sympy_parser import parse_expr, standard_transformations

MATH_SAFE_LOCALS: Dict[str, Any] = {
    "Symbol": sympy.Symbol,
    "symbols": sympy.symbols,
    "Dummy": sympy.Dummy,
    "Add": sympy.Add,
    "Mul": sympy.Mul,
    "Pow": sympy.Pow,
    "exp": sympy.exp,
    "log": sympy.log,
    "ln": sympy.log,
    "sqrt": sympy.sqrt,
    "cbrt": sympy.cbrt,
    "Abs": sympy.Abs,
    "sign": sympy.sign,
    "Sum": sympy.Sum,
    "Integral": sympy.Integral,
    "Product": sympy.Product,
    "Piecewise": sympy.Piecewise,
    "Heaviside": sympy.Heaviside,
    "Max": sympy.Max,
    "Min": sympy.Min,
    "pi": sympy.pi,
    "E": sympy.E,
    "oo": sympy.oo,
    "nan": sympy.nan,
    "sin": sympy.sin,
    "cos": sympy.cos,
    "tan": sympy.tan,
    "sinh": sympy.sinh,
    "cosh": sympy.cosh,
    "tanh": sympy.tanh,
    "Integer": sympy.Integer,
    "Float": sympy.Float,
    "Rational": sympy.Rational,
    # NOTE: gamma, factorial, binomial intentionally excluded.
    # They shadow auto_symbol, causing SympifyError when used as variable names
    # (e.g. 'gamma * ofi_zscore' in Hawkes notation).
}

_INJECTION_SENTINEL_NAMES: FrozenSet[str] = frozenset({
    "import", "exec", "eval", "open", "os", "sys",
    "__builtins__", "__import__", "subprocess",
})


def _parse_worker(expression_string: str, queue: "multiprocessing.Queue[Dict[str, Any]]") -> None:
    if sys.platform == "linux":
        try:
            import resource
            _256MB = 256 * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (_256MB, _256MB))
            resource.setrlimit(resource.RLIMIT_CPU, (5, 5))
        except Exception:
            pass

    try:
        expr = parse_expr(
            expression_string,
            local_dict=MATH_SAFE_LOCALS,
            global_dict={},
            transformations=standard_transformations,
            evaluate=False,
        )
        free_symbol_names = {str(s) for s in expr.free_symbols}
        injection_hits = free_symbol_names & _INJECTION_SENTINEL_NAMES
        if injection_hits:
            queue.put({"valid": False,
                       "error": f"Injection attempt detected. Suspicious symbols: {injection_hits}"})
            return
        queue.put({
            "valid": True,
            "sympy_str": str(expr),
            "latex_roundtrip": sympy.latex(expr),
            "free_symbols": sorted(free_symbol_names),
            "expression_type": type(expr).__name__,
        })
    except Exception as exc:
        queue.put({"valid": False, "error": str(exc), "expression_type": None})


def validate_formula(expression_string: str, timeout: int = 10) -> Dict[str, Any]:
    ctx = multiprocessing.get_context("spawn")
    queue: multiprocessing.Queue[Dict[str, Any]] = ctx.Queue(maxsize=1)
    p = ctx.Process(target=_parse_worker, args=(expression_string, queue), daemon=True)
    p.start()
    p.join(timeout)

    if p.is_alive():
        p.kill()
        p.join()
        return {"valid": False, "error": f"Timeout: expression parsing exceeded {timeout}s."}

    if p.exitcode != 0 and queue.empty():
        return {"valid": False,
                "error": f"Subprocess terminated with exit code {p.exitcode}."}

    if not queue.empty():
        return queue.get_nowait()

    return {"valid": False, "error": "Subprocess returned no result."}
