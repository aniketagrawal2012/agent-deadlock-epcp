r"""
epcp_types.py - Pydantic Data Contracts and Mathematical Types for EPCP Engine
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Data contracts and priority definitions for the Epistemic Priority Ceiling Protocol (EPCP)
and concurrent multi-agent transactional database arbitration.
Implements:
  - Dual-level priorities: Monotonically Latched Dynamic Priority \tilde{\Pi}_i(t)
    and Static Transaction Upper-Envelope Preemption Level \Pi_i^{\max}
  - Exact 64-bit integer lexicographical tuple ordering (\lfloor 10^6 \Pi^{\text{score}} \rfloor, -rank(T_i))
  - Senior saturation horizon promotion at T_sat = \Omega_max / \gamma_age
  - 64-partition intent-scoped footprint bitmasks (B_part = 64)
"""

from __future__ import annotations
import math
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from pydantic import BaseModel, Field


B_PART: int = 64
PI_BASE: float = 1.0
W_DEPTH: float = 2.0
W_OMEGA: float = 1.5
W_CERTAINTY: float = 3.0
D_MAX: int = 4
OMEGA_MAX: float = 10000.0
GAMMA_AGE: float = 0.05
EPSILON_TIE: float = 1e-7
SCORE_SCALE: int = 1_000_000

PI_SAT_SCORE: float = (
    PI_BASE + W_DEPTH * D_MAX + W_OMEGA * math.log(1.0 + OMEGA_MAX) + W_CERTAINTY
)
T_SAT_SEC: float = OMEGA_MAX / GAMMA_AGE


def compute_normalized_shannon_entropy(probs: List[float]) -> float:
    """
    Computes normalized token-level Shannon entropy H_i(t) in [0, 1] over candidate
    tool-action probability distribution p_{i,t}(v):
        H_i(t) = - (1 / ln |V_act|) * sum_{v} p(v) ln p(v)  for |V_act| > 1, else 0.0.
    """
    valid = [p for p in probs if p > 0.0]
    n_v = len(probs)
    if n_v <= 1 or len(valid) <= 1:
        return 0.0
    total = sum(valid)
    normed = [p / total for p in valid]
    raw_h = -sum(p * math.log(p) for p in normed if p > 0.0)
    max_h = math.log(float(n_v))
    if max_h <= 0.0:
        return 0.0
    return min(1.0, max(0.0, raw_h / max_h))


def partition_for_resource(resource_id: str, num_partitions: int = B_PART) -> int:
    """Deterministic FNV-1a 64-bit hash mapping resource_id -> {0, ..., B_part - 1}."""
    h = 14695981039346656037
    for b in resource_id.encode("utf-8"):
        h ^= b
        h = (h * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return int(h % num_partitions)


def footprint_to_bitmask(partitions: Set[int]) -> int:
    """Packs a set of partition indices in {0, ..., 63} into a 64-bit integer bitmask."""
    mask = 0
    for p in partitions:
        if 0 <= p < B_PART:
            mask |= (1 << p)
    return mask


class LockMode(str, Enum):
    """Concurrency lock modes for database resource records."""
    SHARED_READ = "SHARED_READ"
    EXCLUSIVE_WRITE = "EXCLUSIVE_WRITE"


class TransactionStatus(str, Enum):
    """Lifecycle states of an autonomous agent database transaction."""
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    COMMITTED = "COMMITTED"
    BIZ_RULE_REJECT = "BIZ_RULE_REJECT"
    ABORTED_DEADLOCK = "ABORTED_DEADLOCK"
    ROLLED_BACK = "ROLLED_BACK"


class AgentEpistemicState(BaseModel):
    """
    Epistemic state of an autonomous agent executing transaction T_i at time t.
    Quantifies task delegation depth d_i, cumulative token/aging investment Omega_i(t),
    normalized Shannon entropy H_i(t), arrival rank(T_i), and footprint F(T_i).
    """
    agent_id: str
    task_depth: int = Field(..., ge=0, description="Depth in hierarchical delegation tree (d_i)")
    accumulated_cost_omega: float = Field(default=0.0, ge=0.0, description="Clipped token + aging investment Omega_i(t)")
    tokens_expended: int = Field(default=0, ge=0, description="Cumulative billed LLM tokens Tokens_i(t)")
    epistemic_entropy_h: float = Field(default=0.5, ge=0.0, le=1.0, description="Normalized action entropy H_i(t) in [0, 1]")
    base_priority: float = Field(default=PI_BASE, ge=0.0)
    arrival_rank: int = Field(default=1, ge=1, description="Monotonic arrival sequence ID rank(T_i)")
    start_time_sec: float = Field(default=0.0, description="Transaction start timestamp t_{start,i}")
    latched_max_score: float = Field(default=0.0, description="Monotonically latched max_{tau <= t} Pi_i(tau)")
    promoted_saturated: bool = Field(default=False, description="True once senior saturation horizon T_sat is reached")
    declared_partitions: Set[int] = Field(default_factory=set, description="Declared partition footprint F(T_i)")

    def compute_omega(self, elapsed_sec: float = 0.0) -> float:
        """
        Omega_i(t) = min(Omega_max, Tokens_i(t)/100 + gamma_age * elapsed_sec + accumulated_cost_omega)
        """
        raw = self.accumulated_cost_omega + (self.tokens_expended / 100.0) + (GAMMA_AGE * max(0.0, elapsed_sec))
        return min(OMEGA_MAX, max(0.0, raw))

    def compute_epistemic_priority(
        self,
        weight_depth: float = W_DEPTH,
        weight_cost: float = W_OMEGA,
        weight_certainty: float = W_CERTAINTY,
        elapsed_sec: float = 0.0,
    ) -> float:
        r"""
        Calculates instantaneous epistemic score Pi_i(t) and updates the monotonic latch:
            Pi_i(t) = Pi_base + w_d * d_i + w_Omega * ln(1 + Omega_i(t)) + w_c * (1 - H_i(t))
        Returns the monotonically latched dynamic priority:
            \tilde{\Pi}_i(t) = max_{tau <= t} Pi_i(tau) - epsilon_tie * rank(T_i).
        """
        if self.promoted_saturated or elapsed_sec >= T_SAT_SEC:
            self.promoted_saturated = True
            self.latched_max_score = max(self.latched_max_score, PI_SAT_SCORE)
            return round(self.latched_max_score - EPSILON_TIE * self.arrival_rank, 7)

        omega = self.compute_omega(elapsed_sec=elapsed_sec)
        score = (
            self.base_priority
            + (weight_depth * self.task_depth)
            + (weight_cost * math.log(1.0 + omega))
            + (weight_certainty * (1.0 - self.epistemic_entropy_h))
        )
        if score > self.latched_max_score:
            self.latched_max_score = score
        return round(self.latched_max_score - EPSILON_TIE * self.arrival_rank, 7)

    def compute_upper_envelope_priority(
        self,
        weight_depth: float = W_DEPTH,
        weight_cost: float = W_OMEGA,
        weight_certainty: float = W_CERTAINTY,
    ) -> float:
        r"""
        Calculates the Transaction Upper-Envelope Preemption Level \Pi_i^{\max}:
            \Pi_i^{\max} = Pi_base + w_d * d_i + w_Omega * ln(1 + Omega_max) + w_c - epsilon_tie * rank(T_i)
        If promoted via senior saturation (T_sat), upgrades to \Pi_sat - epsilon_tie * rank(T_i).
        """
        eff_depth = max(D_MAX, self.task_depth) if self.promoted_saturated else self.task_depth
        max_score = (
            self.base_priority
            + (weight_depth * eff_depth)
            + (weight_cost * math.log(1.0 + OMEGA_MAX))
            + weight_certainty
        )
        return round(max_score - EPSILON_TIE * self.arrival_rank, 7)

    def dynamic_priority_tuple(self, elapsed_sec: float = 0.0) -> Tuple[int, int]:
        r"""Exact 64-bit integer lexicographical pair (\lfloor 10^6 \Pi_i^{score}(t) \rfloor, -rank(T_i))."""
        self.compute_epistemic_priority(elapsed_sec=elapsed_sec)
        return (int(math.floor(SCORE_SCALE * self.latched_max_score)), -int(self.arrival_rank))

    def upper_envelope_tuple(self) -> Tuple[int, int]:
        r"""Exact 64-bit integer lexicographical pair (\lfloor 10^6 \Pi_{i,max}^{score} \rfloor, -rank(T_i))."""
        eff_depth = max(D_MAX, self.task_depth) if self.promoted_saturated else self.task_depth
        max_score = (
            self.base_priority
            + (W_DEPTH * eff_depth)
            + (W_OMEGA * math.log(1.0 + OMEGA_MAX))
            + W_CERTAINTY
        )
        return (int(math.floor(SCORE_SCALE * max_score)), -int(self.arrival_rank))


class ResourceDescriptor(BaseModel):
    """Shared database entity or row under lock management."""
    resource_id: str
    table_name: str
    row_key: str
    partition_id: int = Field(default=0, ge=0, lt=B_PART)
    ceiling_priority: float = Field(default=0.0, description="Partition ceiling priority Pi(part(R_j), t)")
    current_holder_agent_id: Optional[str] = None
    lock_mode: Optional[LockMode] = None


class DeadlockTelemetry(BaseModel):
    """Empirical telemetry recorded across live concurrency benchmark runs."""
    concurrency_level: int
    protocol: str
    total_transactions: int
    committed_transactions: int
    biz_rule_rejects: int = 0
    aborted_deadlocks: int
    abort_rate_pct: float
    total_duration_sec: float
    throughput_tps: float
    total_tps: float = 0.0
    mean_latency_ms: float
    committed_mean_latency_ms: float = 0.0
    p95_latency_ms: float
    committed_p95_latency_ms: float = 0.0
    p99_latency_ms: float
    max_latency_ms: float = 0.0
    max_priority_inversion_duration_ms: float = 0.0
    wasted_tokens_per_1k_attempts: float = 0.0
    wasted_tokens_per_1k_committed: float = 0.0
