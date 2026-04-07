"""
Base Agent — Abstract interface for all Formula Foundry specialist agents.

Every agent in the Think Tank implements this interface. The base class handles:
  - Anthropic API client management
  - The agentic tool-use loop (tool calls → results → next message)
  - Iteration budget enforcement (no runaway agents)
  - Structured logging of every reasoning step

Subclasses provide:
  - AGENT_ID: unique identifier matching the 250-agent matrix
  - AGENT_LAYER: authority level (layer1/layer2/layer3)
  - MATHEMATICAL_WING: domain specialization
  - SYSTEM_PROMPT: the axiomatic core
  - tools(): the tool schemas available to this agent
  - handle_tool_call(): dispatch for each tool name
  - build_initial_message(): formats the trigger data into a user message
  - extract_formula(): constructs FormulaDNA from the terminal proposal call

Terminal action tool names intercepted by this base class:
  "propose_formula_to_blackboard" — Agent 089, Agent 201 (single-formula proposal)
  "propose_unified_formula"       — Agent 105 (synthesis of two parents)

Both names trigger extract_formula() and set the formula result that
terminates the agentic loop. Any new agent using a different terminal
action name must either use one of these names or set self._run_terminated
directly (as Agent 105 does for its rejection path and Agent 151 does
for update_formula_status).
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncio
import anthropic

from foundry.core.schema.formula import AgentLayer, FormulaDNA, MathematicalWing

logger = logging.getLogger(__name__)

# The model all agents use. Specified once here — never hardcoded in subclasses.
_AGENT_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS = 8192

# Tool names that trigger extract_formula() in the agentic loop.
# Agent 089 and Agent 201 use propose_formula_to_blackboard.
# Agent 105 uses propose_unified_formula.
# Adding a new terminal tool name: add it here, not in the loop body.
_PROPOSAL_TOOL_NAMES = frozenset({
    "propose_formula_to_blackboard",
    "propose_unified_formula",
})


# ---------------------------------------------------------------------------
# Data contracts
# ---------------------------------------------------------------------------


@dataclass
class AgentConfig:
    anthropic_api_key: str
    blackboard_api_url: str
    lean_worker_url: str = "http://localhost:8080"
    max_iterations: int = 8
    sympy_timeout_seconds: int = 10


@dataclass
class ReasoningStep:
    """One complete round in the agentic loop: agent thinks → calls tool → receives result."""
    iteration: int
    agent_thinking: str
    tool_name: str
    tool_input: Dict[str, Any]
    tool_result: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class AgentRunResult:
    """
    Complete output of one agent run.

    Two success paths:
      was_synthesized: formula is not None     — proposal accepted
      was_rejected:    output_data is not None — formal rejection recorded (Agent 105)

    Failure path: success=False, formula=None, output_data=None
    """
    agent_id: str
    triggered_at: datetime
    completed_at: datetime
    formula: Optional[FormulaDNA]
    reasoning_trace: List[ReasoningStep]
    iterations_used: int
    success: bool
    failure_reason: Optional[str] = None
    output_data: Optional[Dict[str, Any]] = None

    @property
    def was_synthesized(self) -> bool:
        return self.success and self.formula is not None

    @property
    def was_rejected(self) -> bool:
        """True only for Agent 105 isomorphism rejections (outcome=rejected)."""
        return (
            self.success
            and self.formula is None
            and self.output_data is not None
            and self.output_data.get("outcome") == "rejected"
        )

    @property
    def audit_outcome(self) -> "Optional[str]":
        """For Agent 151 runs: 'formally_verified', 'falsified', or 'syntactically_correct'."""
        if self.output_data and self.output_data.get("outcome") in (
            "formally_verified", "falsified", "syntactically_correct"
        ):
            return self.output_data["outcome"]
        return None


# ---------------------------------------------------------------------------
# Base agent
# ---------------------------------------------------------------------------


class BaseAgent(ABC):
    """
    Abstract base for all Formula Foundry specialist agents.

    The agentic loop:
      1. Build initial message from trigger data
      2. Send to Claude with tools
      3. For each tool call in response: dispatch → get result → append to messages
      4. Continue until agent calls a terminal tool OR max_iterations is reached
      5. Return AgentRunResult
    """

    AGENT_ID: str
    AGENT_LAYER: AgentLayer
    MATHEMATICAL_WING: List[MathematicalWing]
    SYSTEM_PROMPT: str

    def __init__(self, config: AgentConfig) -> None:
        self._config = config
        self._client = anthropic.AsyncAnthropic(api_key=config.anthropic_api_key)
        self._run_terminated: bool = False
        self._run_output_data: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def tools(self) -> List[Dict[str, Any]]:
        """Return the Anthropic tool schemas available to this agent."""

    @abstractmethod
    async def handle_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Dispatch a tool call and return the result dict."""

    @abstractmethod
    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        """Format the trigger event data into the first user message."""

    @abstractmethod
    def extract_formula(
        self,
        tool_input: Dict[str, Any],
        validated_sympy_str: str,
    ) -> FormulaDNA:
        """
        Construct a FormulaDNA from the agent's proposal tool call.
        Called when the agent invokes any tool in _PROPOSAL_TOOL_NAMES.
        """

    # ------------------------------------------------------------------
    # The agentic loop
    # ------------------------------------------------------------------

    async def run(self, trigger_data: Dict[str, Any]) -> AgentRunResult:
        triggered_at = datetime.now(timezone.utc)
        reasoning_trace: List[ReasoningStep] = []
        messages: List[Dict[str, Any]] = [
            {"role": "user", "content": self.build_initial_message(trigger_data)}
        ]

        last_validated_sympy: Optional[str] = None
        formula: Optional[FormulaDNA] = None
        iteration = 0

        logger.info(
            "[%s] Run started. Trigger: %s",
            self.AGENT_ID, trigger_data.get("event_type", list(trigger_data.keys())),
        )

        while iteration < self._config.max_iterations:
            iteration += 1
            logger.debug("[%s] Iteration %d/%d", self.AGENT_ID, iteration, self._config.max_iterations)

            response = await self._client.messages.create(
                model=_AGENT_MODEL,
                max_tokens=_MAX_TOKENS,
                system=self.SYSTEM_PROMPT,
                tools=self.tools(),
                messages=messages,
            )

            agent_text = " ".join(
                block.text for block in response.content
                if hasattr(block, "text")
            )

            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
            if not tool_use_blocks:
                if response.stop_reason == "end_turn":
                    logger.warning(
                        "[%s] Agent stopped without calling a terminal action. "
                        "Response: %s",
                        self.AGENT_ID, agent_text[:200],
                    )
                break

            # Extract only API-valid fields from SDK response objects.
            # model_dump() includes internal SDK fields (e.g. 'caller') that
            # the API rejects when sent back in subsequent messages.
            assistant_content = []
            for block in response.content:
                if hasattr(block, 'type'):
                    if block.type == "text":
                        assistant_content.append({"type": "text", "text": block.text})
                    elif block.type == "tool_use":
                        assistant_content.append({
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": block.input,
                        })
                    else:
                        assistant_content.append({"type": block.type})
                else:
                    assistant_content.append(block)
            messages.append({"role": "assistant", "content": assistant_content})

            tool_results_content = []
            for tool_block in tool_use_blocks:
                tool_name = tool_block.name
                tool_input = tool_block.input

                logger.info(
                    "[%s] Tool call: %s(%s)",
                    self.AGENT_ID, tool_name, list(tool_input.keys()),
                )

                result = await self.handle_tool_call(tool_name, tool_input)

                if tool_name == "validate_sympy_expression" and result.get("valid"):
                    last_validated_sympy = result.get("sympy_str")

                # Terminal action: agent is proposing a formula.
                # Intercepts both proposal tool names used across all agents.
                if tool_name in _PROPOSAL_TOOL_NAMES:
                    if not last_validated_sympy:
                        result = {
                            "error": (
                                "You must call validate_sympy_expression and receive "
                                "valid=True before proposing."
                            ),
                            "accepted": False,
                        }
                    else:
                        try:
                            formula = self.extract_formula(tool_input, last_validated_sympy)
                            result = {"accepted": True, "uuid": formula.uuid}
                            logger.info(
                                "[%s] Formula proposed: %s", self.AGENT_ID, formula.uuid
                            )
                        except Exception as exc:
                            result = {"error": str(exc), "accepted": False}

                reasoning_trace.append(ReasoningStep(
                    iteration=iteration,
                    agent_thinking=agent_text,
                    tool_name=tool_name,
                    tool_input=tool_input,
                    tool_result=result,
                ))

                tool_results_content.append({
                    "type": "tool_result",
                    "tool_use_id": tool_block.id,
                    "content": json.dumps(result),
                })

            messages.append({"role": "user", "content": tool_results_content})

            if formula is not None or self._run_terminated:
                break

        completed_at = datetime.now(timezone.utc)
        success = formula is not None or self._run_terminated

        if not success:
            logger.error(
                "[%s] Failed to produce any output after %d iterations.",
                self.AGENT_ID, iteration,
            )

        return AgentRunResult(
            agent_id=self.AGENT_ID,
            triggered_at=triggered_at,
            completed_at=completed_at,
            formula=formula,
            reasoning_trace=reasoning_trace,
            iterations_used=iteration,
            success=success,
            failure_reason=None if success else f"No terminal action after {iteration} iterations",
            output_data=self._run_output_data,
        )
