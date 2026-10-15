"""
epcp_types.py - Pydantic Data Contracts and Mathematical Types for EPCP Engine
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Data contracts and priority definitions for Epistemic Priority Ceiling Protocol (EPCP)
and concurrent multi-agent distributed transaction arbitration.
"""

from __future__ import annotations
from enum import Enum
from typing import Dict, List, Optional, Set, Any
from pydantic import BaseModel, Field


class LockMode(str, Enum):
    """Concurrency lock modes for database resource records."""
    SHARED_READ = "SHARED_READ"
    EXCLUSIVE_WRITE = "EXCLUSIVE_WRITE"


class TransactionStatus(str, Enum):
    """Lifecycle states of an autonomous agent distributed transaction."""
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    COMMITTED = "COMMITTED"
    ABORTED_DEADLOCK = "ABORTED_DEADLOCK"
    ROLLED_BACK = "ROLLED_BACK"


class AgentEpistemicState(BaseModel):
    """
    Epistemic state of an autonomous agent at time t.
    Quantifies task depth, resource commitment, and reasoning uncertainty.
    """
    agent_id: str
    task_depth: int = Field(..., ge=0, description="Depth in hierarchical delegation tree (d_i)")
    accumulated_cost_omega: float = Field(..., ge=0.0, description="Tokens/FLOPs/resources invested so far (Omega_i)")
    epistemic_entropy_h: float = Field(..., ge=0.0, le=1.0, description="Model uncertainty / entropy H_i in [0, 1]")
    base_priority: float = Field(default=1.0, ge=0.0)

    def compute_epistemic_priority(
        self,
        weight_depth: float = 1.5,
        weight_cost: float = 1.0,
        weight_certainty: float = 2.0
    ) -> float:
        """
        Calculates dynamic epistemic priority Pi_i(t):
            Pi_i(t) = base + w_d * d_i + w_omega * log(1 + Omega_i) + w_c * (1 - H_i)
        Higher priority indicates deeper investment and high certainty, warranting precedence.
        """
        import math
        p = (
            self.base_priority +
            (weight_depth * self.task_depth) +
            (weight_cost * math.log(1.0 + self.accumulated_cost_omega)) +
            (weight_certainty * (1.0 - self.epistemic_entropy_h))
        )
        return round(p, 4)


class ResourceDescriptor(BaseModel):
    """Shared database entity or row under lock management."""
    resource_id: str
    table_name: str
    row_key: str
    ceiling_priority: float = Field(default=0.0, description="Dynamic ceiling priority C(R_j)")
    current_holder_agent_id: Optional[str] = None
    lock_mode: Optional[LockMode] = None


class DeadlockTelemetry(BaseModel):
    """Empirical telemetry recorded across live concurrency benchmark runs."""
    concurrency_level: int
    protocol: str
    total_transactions: int
    committed_transactions: int
    aborted_deadlocks: int
    abort_rate_pct: float
    total_duration_sec: float
    throughput_tps: float
    mean_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_priority_inversion_duration_ms: float
