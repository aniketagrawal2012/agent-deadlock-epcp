r"""
epcp_engine.py - Epistemic Priority Ceiling Protocol (EPCP) Arbitration Engine
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Implements the Epistemic Priority Ceiling Protocol (EPCP) with:
  1. Dual-level priorities: Static Transaction Upper-Envelope Preemption Level (\Pi_i^{\max})
     and Monotonically Latched Dynamic Priority (\tilde{\Pi}_i(t)) over exact 64-bit integer
     lexicographical tuples (\lfloor 10^6 \Pi^{\text{score}} \rfloor, -rank(T_i)).
  2. Intersecting-Footprint Partition Priority Ceilings (C_{-A_i}^*(t; F(T_i)) and C^*(t; F(T_i)))
     with 64-bit partition footprint bitmasks (B_part = 64).
  3. Guarded Inheritance Pass-Through Admissibility Rule (Eq. 11) and Unified Transitive
     Epistemic Priority Inheritance (Eq. 12-13) over both contention and ceiling wait edges.
  4. Senior Saturation Promotion at T_sat = \Omega_max / \gamma_age (atomically upgrading
     \Pi_i^{\max} and \Pi(P_m, t) and propagating via inheritance).
  5. State-Version-Keyed SHA-256 Inference Memoization Cache for out-of-footprint expansion.
"""

from __future__ import annotations
import hashlib
import json
import threading
import time
from typing import Dict, Set, Optional, List, Tuple, Any
from epcp_types import (
    B_PART,
    T_SAT_SEC,
    LockMode,
    AgentEpistemicState,
    ResourceDescriptor,
    partition_for_resource,
    footprint_to_bitmask,
)


class OutOfFootprintException(Exception):
    """
    Raised when an agent requests a resource whose partition part(R_req) is outside
    its declared footprint F(T_i), triggering safe lock release, footprint expansion,
    and state-version-verified memoized inference replay.
    """
    def __init__(self, agent_id: str, resource_id: str, req_partition: int):
        super().__init__(
            f"Resource {resource_id} (partition {req_partition}) outside declared footprint of {agent_id}"
        )
        self.agent_id = agent_id
        self.resource_id = resource_id
        self.req_partition = req_partition


class EPCPEngine:
    """
    Epistemic Priority Ceiling Protocol (EPCP) Concurrency Manager.
    Provably eliminates deadlocks (Theorem 1), bounds priority inversion to at most one
    critical section per disjoint partition component (Theorem 2), and guarantees
    conflict serializability and starvation-free termination (Theorem 3).
    """

    def __init__(self, use_live_dynamic_urgency: bool = True, enforce_strict_footprint: bool = False):
        self._mutex = threading.RLock()
        self.use_live_dynamic_urgency = use_live_dynamic_urgency
        self.enforce_strict_footprint = enforce_strict_footprint

        # Monotonic arrival rank counter (rebased when active set is empty)
        self._next_rank: int = 1
        self._active_agents: Set[str] = set()

        # agent_id -> AgentEpistemicState
        self._agents: Dict[str, AgentEpistemicState] = {}
        # agent_id -> effective dynamic priority scalar \tilde{\Pi}_i^{eff}(t) (backwards-compatible)
        self._active_priorities: Dict[str, float] = {}
        # agent_id -> effective upper-envelope preemption scalar \Pi_i^{eff}(t)
        self._effective_upper_priorities: Dict[str, float] = {}
        # agent_id -> exact 64-bit integer tuple for \Pi_i^{eff}(t)
        self._effective_upper_tuples: Dict[str, Tuple[int, int]] = {}
        # agent_id -> exact 64-bit integer tuple for \tilde{\Pi}_i^{eff}(t)
        self._effective_dynamic_tuples: Dict[str, Tuple[int, int]] = {}

        # agent_id -> 64-bit partition footprint bitmask
        self._agent_footprint_masks: Dict[str, int] = {}
        # partition_id in {0..63} -> set of active registered agent_ids U(P_m, t)
        self._partition_users: Dict[int, Set[str]] = {p: set() for p in range(B_PART)}
        # partition_id in {0..63} -> partition ceiling tuple and scalar
        self._partition_ceiling_tuples: Dict[int, Tuple[int, int]] = {p: (0, 0) for p in range(B_PART)}
        self._partition_ceilings: Dict[int, float] = {p: 0.0 for p in range(B_PART)}

        # resource_id -> ResourceDescriptor
        self._resources: Dict[str, ResourceDescriptor] = {}
        # agent_id -> set of currently held resource_ids Held(A_i, t)
        self._held_resources: Dict[str, Set[str]] = {}
        # resource_id -> set of registered potential users
        self._resource_users: Dict[str, Set[str]] = {}

        # Unified Wait-For-Graph edges: waiter_agent_id -> set of blocking holder_agent_ids
        self._wait_edges: Dict[str, Set[str]] = {}
        # agent_id -> Condition variable
        self._conditions: Dict[str, threading.Condition] = {}

        # Transaction-local staged write buffers and state-version-keyed inference memoization cache
        # (agent_id, step_name, state_sha256) -> memoized_inference_output
        self._inference_memo_cache: Dict[Tuple[str, str, str], Any] = {}
        self._staged_writes: Dict[str, List[Tuple[str, Any]]] = {}

        # Telemetry counters
        self.priority_inheritance_count: int = 0
        self.deadlocks_prevented_count: int = 0
        self.preemptive_suspensions_count: int = 0
        self.footprint_expansions_count: int = 0
        self.senior_promotions_count: int = 0

    @staticmethod
    def compute_state_version_digest(row_attributes: Dict[str, Any]) -> str:
        """Computes a canonical SHA-256 digest of read row attributes to key memoized inference."""
        payload = json.dumps(row_attributes, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def memoize_inference(self, agent_id: str, step_id: str, row_state: Dict[str, Any], output: Any):
        """Stores deterministic inference output keyed by SHA-256 digest of read row attributes."""
        with self._mutex:
            digest = self.compute_state_version_digest(row_state)
            self._inference_memo_cache[(agent_id, step_id, digest)] = output

    def lookup_memoized_inference(self, agent_id: str, step_id: str, row_state: Dict[str, Any]) -> Optional[Any]:
        """Retrieves memoized inference output if and only if the underlying row attributes are unchanged."""
        with self._mutex:
            digest = self.compute_state_version_digest(row_state)
            return self._inference_memo_cache.get((agent_id, step_id, digest))

    def stage_write(self, agent_id: str, resource_id: str, mutation_payload: Any):
        """Stages an uncommitted mutation in the transaction-local write buffer until Commit."""
        with self._mutex:
            self._staged_writes.setdefault(agent_id, []).append((resource_id, mutation_payload))

    def pop_staged_writes(self, agent_id: str) -> List[Tuple[str, Any]]:
        """Pops all staged writes for atomic flush at Commit."""
        with self._mutex:
            return self._staged_writes.pop(agent_id, [])

    def register_agent(
        self,
        state: AgentEpistemicState,
        footprint_partitions: Optional[Set[int]] = None,
        preserve_rank: bool = False,
    ):
        """
        Registers or updates an agent's dynamic epistemic state and optional partition footprint F(T_i).
        Rebases arrival rank counter when no active transactions are holding/waiting.
        """
        with self._mutex:
            if not self._active_agents and not self._held_resources:
                self._next_rank = 1

            existing = self._agents.get(state.agent_id)
            if preserve_rank and existing is not None:
                state.arrival_rank = existing.arrival_rank
                state.start_time_sec = existing.start_time_sec
            else:
                state.arrival_rank = self._next_rank
                self._next_rank += 1
                if state.start_time_sec <= 0.0:
                    state.start_time_sec = time.perf_counter()

            if footprint_partitions is not None:
                state.declared_partitions = set(footprint_partitions)

            self._agents[state.agent_id] = state
            self._active_agents.add(state.agent_id)

            dyn_p = state.compute_epistemic_priority(elapsed_sec=0.0)
            max_p = state.compute_upper_envelope_priority()
            self._active_priorities[state.agent_id] = dyn_p
            self._effective_upper_priorities[state.agent_id] = max_p
            self._effective_upper_tuples[state.agent_id] = state.upper_envelope_tuple()
            self._effective_dynamic_tuples[state.agent_id] = state.dynamic_priority_tuple(elapsed_sec=0.0)

            if state.declared_partitions:
                self._agent_footprint_masks[state.agent_id] = footprint_to_bitmask(state.declared_partitions)
                for p in state.declared_partitions:
                    self._partition_users[p].add(state.agent_id)
                    self._recalculate_partition_ceiling(p)
            else:
                self._agent_footprint_masks.setdefault(state.agent_id, 0)

            if state.agent_id not in self._conditions:
                self._conditions[state.agent_id] = threading.Condition(self._mutex)

            # Recompute ceilings of any already-registered resources listing this agent
            for res_id, users in self._resource_users.items():
                if state.agent_id in users:
                    self._recalculate_resource_ceiling(res_id)

    def update_agent_epistemic_telemetry(
        self,
        agent_id: str,
        tokens_delta: int = 0,
        new_entropy_h: Optional[float] = None,
    ):
        r"""
        Updates an active agent's billed LLM token expenditure Tokens_i(t) and normalized
        Shannon action entropy H_i(t), monotonically latching \tilde{\Pi}_i(t).
        """
        with self._mutex:
            st = self._agents.get(agent_id)
            if not st:
                return
            st.tokens_expended += max(0, tokens_delta)
            if new_entropy_h is not None:
                st.epistemic_entropy_h = min(1.0, max(0.0, new_entropy_h))
            elapsed = max(0.0, time.perf_counter() - st.start_time_sec)
            dyn_p = st.compute_epistemic_priority(elapsed_sec=elapsed)
            if dyn_p > self._active_priorities.get(agent_id, 0.0):
                self._active_priorities[agent_id] = dyn_p
            dyn_tuple = st.dynamic_priority_tuple(elapsed_sec=elapsed)
            if dyn_tuple > self._effective_dynamic_tuples.get(agent_id, (0, 0)):
                self._effective_dynamic_tuples[agent_id] = dyn_tuple

    def register_resource(
        self,
        resource_id: str,
        table_name: str,
        row_key: str,
        potential_users: Set[str],
        partition_id: Optional[int] = None,
    ):
        """Registers a shared resource, assigns its partition part(R_j), and updates ceilings."""
        with self._mutex:
            part_id = partition_id if partition_id is not None else partition_for_resource(resource_id)
            users = set(potential_users) | self._resource_users.get(resource_id, set())
            self._resource_users[resource_id] = users

            existing = self._resources.get(resource_id)
            if existing is None:
                self._resources[resource_id] = ResourceDescriptor(
                    resource_id=resource_id,
                    table_name=table_name,
                    row_key=row_key,
                    partition_id=part_id,
                    ceiling_priority=0.0,
                )
            for uid in users:
                self._partition_users[part_id].add(uid)
                st = self._agents.get(uid)
                if st is not None:
                    st.declared_partitions.add(part_id)
                    self._agent_footprint_masks[uid] = footprint_to_bitmask(st.declared_partitions)

            self._recalculate_partition_ceiling(part_id)
            self._recalculate_resource_ceiling(resource_id)

    def _recalculate_partition_ceiling(self, part_id: int):
        r"""Partition ceiling \Pi(P_m, t) = max_{A_q in U(P_m, t)} \Pi_q^{\max}."""
        users = self._partition_users.get(part_id, set())
        max_tuple = (0, 0)
        max_scalar = 0.0
        for uid in users:
            if uid not in self._active_agents:
                continue
            st = self._agents.get(uid)
            if st:
                t_up = st.upper_envelope_tuple()
                s_up = st.compute_upper_envelope_priority()
                if t_up > max_tuple:
                    max_tuple = t_up
                if s_up > max_scalar:
                    max_scalar = s_up
        self._partition_ceiling_tuples[part_id] = max_tuple
        self._partition_ceilings[part_id] = max_scalar

    def _recalculate_resource_ceiling(self, resource_id: str):
        r"""Resource ceiling C(R_j, t) = max_{A_q in Users(R_j)} \Pi_q(t)."""
        users = self._resource_users.get(resource_id, set())
        max_p = 0.0
        for uid in users:
            st = self._agents.get(uid)
            if st:
                p = st.compute_epistemic_priority()
                if p > max_p:
                    max_p = p
        if resource_id in self._resources:
            self._resources[resource_id].ceiling_priority = max_p

    def _check_and_promote_senior_saturation(self, agent_id: str):
        r"""
        Theorem 3 Senior Saturation Promotion:
        If elapsed wait time reaches T_sat = \Omega_max / \gamma_age, atomically promotes
        both \tilde{\Pi}_i(t) and \Pi_i^{\max} to \Pi_sat - \epsilon_tie * rank(T_i),
        updates \Pi(P_m, t) for all P_m in F(T_i), and propagates via Eq. (12).
        """
        st = self._agents.get(agent_id)
        if not st or st.promoted_saturated:
            return
        elapsed = max(0.0, time.perf_counter() - st.start_time_sec)
        if elapsed >= T_SAT_SEC:
            st.promoted_saturated = True
            self.senior_promotions_count += 1
            dyn_p = st.compute_epistemic_priority(elapsed_sec=elapsed)
            max_p = st.compute_upper_envelope_priority()
            self._active_priorities[agent_id] = max(self._active_priorities.get(agent_id, 0.0), dyn_p)
            self._effective_upper_priorities[agent_id] = max(
                self._effective_upper_priorities.get(agent_id, 0.0), max_p
            )
            self._effective_upper_tuples[agent_id] = max(
                self._effective_upper_tuples.get(agent_id, (0, 0)), st.upper_envelope_tuple()
            )
            self._effective_dynamic_tuples[agent_id] = max(
                self._effective_dynamic_tuples.get(agent_id, (0, 0)), st.dynamic_priority_tuple(elapsed_sec=elapsed)
            )
            for p in st.declared_partitions:
                self._recalculate_partition_ceiling(p)
            for res_id, users in self._resource_users.items():
                if agent_id in users:
                    self._recalculate_resource_ceiling(res_id)

    def _get_intersecting_ceiling_excluding(
        self, agent_id: str, req_resource_id: str
    ) -> Tuple[float, Optional[str], Set[str]]:
        """
        Computes External Intersecting-Footprint Ceiling C_{-A_i}^*(t; F(T_i)) and
        identifies all external lock-holding agents A_k != agent_id whose footprint
        intersects F(T_i) (or shares potential resource usage).
        Returns:
            (max_external_ceiling, primary_blocking_holder, all_blocking_holders)
        """
        max_c = 0.0
        primary_holder: Optional[str] = None
        blocking_holders: Set[str] = set()

        my_mask = self._agent_footprint_masks.get(agent_id, 0)
        active_p = self._active_priorities.get(agent_id, 1.0)

        for res_id, res in self._resources.items():
            holder = res.current_holder_agent_id
            if holder and holder != agent_id:
                holder_mask = self._agent_footprint_masks.get(holder, 0)
                # Check if footprints intersect via O(1) 64-bit bitwise AND, or via shared resource users
                intersects = (my_mask & holder_mask) != 0 if (my_mask and holder_mask) else True
                if not intersects:
                    continue
                c_val = res.ceiling_priority
                if c_val > max_c:
                    max_c = c_val
                    primary_holder = holder
                if c_val >= active_p:
                    blocking_holders.add(holder)

        if primary_holder and not blocking_holders:
            blocking_holders.add(primary_holder)

        return max_c, primary_holder, blocking_holders

    def _propagate_transitive_inheritance(self, start_waiter_id: str):
        r"""
        Unified Transitive Epistemic Priority Inheritance (Eq. 12-13):
        Propagates both effective preemption priority \Pi^{eff} and live dynamic urgency
        \tilde{\Pi}^{eff} along all directed wait paths in G_wait(t).
        """
        queue = [start_waiter_id]
        visited: Set[str] = set()
        while queue:
            curr = queue.pop(0)
            if curr in visited:
                continue
            visited.add(curr)
            curr_p = self._active_priorities.get(curr, 1.0)
            curr_up_t = self._effective_upper_tuples.get(curr, (0, 0))
            curr_dyn_t = self._effective_dynamic_tuples.get(curr, (0, 0))

            for holder in self._wait_edges.get(curr, set()):
                if holder == curr:
                    continue
                elevated = False
                if curr_p > self._active_priorities.get(holder, 1.0):
                    self._active_priorities[holder] = curr_p
                    elevated = True
                if curr_up_t > self._effective_upper_tuples.get(holder, (0, 0)):
                    self._effective_upper_tuples[holder] = curr_up_t
                    elevated = True
                if curr_dyn_t > self._effective_dynamic_tuples.get(holder, (0, 0)):
                    self._effective_dynamic_tuples[holder] = curr_dyn_t
                    elevated = True
                if elevated:
                    self.priority_inheritance_count += 1
                    if holder in self._conditions:
                        self._conditions[holder].notify_all()
                    queue.append(holder)

    def acquire_lock(
        self,
        agent_id: str,
        resource_id: str,
        mode: LockMode,
        timeout_sec: float = 3.0,
    ) -> bool:
        """
        Acquires a lock under EPCP Rules 1-3 (Section 3.3):
          Rule 1 (Reentrancy): Immediate grant if already held by agent_id.
          Rule 2 (Direct Contention): Suspend on CV(R_req) and add edge (A_i -> holder).
          Rule 3 (Ceiling Admissibility & Guarded Inheritance Pass-Through, Eq. 11):
                 Grant free resource iff active_p > C_{-A_i}^*(t; F(T_i)) OR
                 (Held(A_i, t) != empty and active_p >= C^*(t; F(T_i))).
                 Otherwise suspend on CV_ceil and propagate transitive inheritance (Eq. 12).
        """
        with self._mutex:
            if agent_id not in self._conditions:
                self._conditions[agent_id] = threading.Condition(self._mutex)
            cond = self._conditions[agent_id]
            start_time = time.time()

            # Check strict footprint declaration if enabled
            if self.enforce_strict_footprint and agent_id in self._agents:
                req_part = (
                    self._resources[resource_id].partition_id
                    if resource_id in self._resources
                    else partition_for_resource(resource_id)
                )
                declared = self._agents[agent_id].declared_partitions
                if declared and req_part not in declared:
                    self.footprint_expansions_count += 1
                    raise OutOfFootprintException(agent_id, resource_id, req_part)

            while True:
                self._check_and_promote_senior_saturation(agent_id)

                res = self._resources.get(resource_id)
                if not res:
                    self.register_resource(resource_id, "Generic", resource_id, {agent_id})
                    res = self._resources[resource_id]

                # Rule 1: Reentrancy
                current_holder = res.current_holder_agent_id
                if current_holder == agent_id:
                    return True

                active_p = self._active_priorities.get(agent_id, 1.0)
                is_free = current_holder is None

                # Rule 3: Evaluate External Intersecting-Footprint Ceiling C_{-A_i}^*(t; F(T_i))
                sys_ceiling, primary_blocker, all_blockers = self._get_intersecting_ceiling_excluding(
                    agent_id, resource_id
                )
                already_holding = bool(self._held_resources.get(agent_id))

                # Guarded Inheritance Pass-Through Rule (Eq. 11):
                # Initial entry (Held == empty) requires strict inequality active_p > sys_ceiling
                # (or sys_ceiling == 0.0 when K_ext == empty).
                # Once Held != empty, an agent owning or inheriting the top ceiling passes through.
                ceiling_test_passed = (
                    (active_p > sys_ceiling)
                    or (sys_ceiling == 0.0)
                    or (already_holding and active_p >= sys_ceiling)
                )

                if is_free and ceiling_test_passed:
                    res.current_holder_agent_id = agent_id
                    res.lock_mode = mode
                    self._held_resources.setdefault(agent_id, set()).add(resource_id)
                    self._wait_edges.pop(agent_id, None)
                    return True

                # Record Unified Wait-For-Graph edges (Rule 2 or Rule 3) and propagate inheritance
                if not is_free and current_holder and current_holder != agent_id:
                    self._wait_edges[agent_id] = {current_holder}
                else:
                    self._wait_edges[agent_id] = set(all_blockers)
                    self.preemptive_suspensions_count += 1
                    self.deadlocks_prevented_count += 1

                self._propagate_transitive_inheritance(agent_id)

                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    self._wait_edges.pop(agent_id, None)
                    return False

                cond.wait(timeout=remaining)

    def expand_footprint_and_reregister(self, agent_id: str, new_partition: int):
        """
        Handles safe out-of-footprint expansion (Section 2.1):
        Releases held locks prior to any physical writes, expands F(T_i) <- F(T_i) U {new_partition},
        and re-registers while preserving original arrival_rank and start_time_sec.
        """
        with self._mutex:
            self._staged_writes.pop(agent_id, None)
            self.release_all_locks(agent_id, deregister_active=False)
            st = self._agents.get(agent_id)
            if st is not None:
                st.declared_partitions.add(new_partition)
                self.register_agent(st, footprint_partitions=st.declared_partitions, preserve_rank=True)

    def release_all_locks(self, agent_id: str, deregister_active: bool = False):
        r"""
        Releases all locks held by agent_id, removes incoming wait edges in G_wait(t),
        reverts effective priority to base epistemic priority, and signals waiting agents
        ordered by live dynamic urgency \tilde{\Pi}_k^{eff}(t) (or static \Pi_k^{\max} in ablation).
        """
        with self._mutex:
            held = self._held_resources.pop(agent_id, set())
            for res_id in held:
                res = self._resources.get(res_id)
                if res and res.current_holder_agent_id == agent_id:
                    res.current_holder_agent_id = None
                    res.lock_mode = None

            self._wait_edges.pop(agent_id, None)
            for waiter, targets in list(self._wait_edges.items()):
                targets.discard(agent_id)
                if not targets:
                    del self._wait_edges[waiter]

            if deregister_active:
                self._active_agents.discard(agent_id)
                st = self._agents.get(agent_id)
                if st:
                    for p in st.declared_partitions:
                        self._partition_users[p].discard(agent_id)
                        self._recalculate_partition_ceiling(p)

            agent_state = self._agents.get(agent_id)
            if agent_state:
                elapsed = max(0.0, time.perf_counter() - agent_state.start_time_sec)
                self._active_priorities[agent_id] = agent_state.compute_epistemic_priority(elapsed_sec=elapsed)
                self._effective_upper_priorities[agent_id] = agent_state.compute_upper_envelope_priority()
                self._effective_upper_tuples[agent_id] = agent_state.upper_envelope_tuple()
                self._effective_dynamic_tuples[agent_id] = agent_state.dynamic_priority_tuple(elapsed_sec=elapsed)

            # Wake waiting agents in descending order of live dynamic urgency \tilde{\Pi}_k^{eff}(t)
            # (or static upper-envelope \Pi_k^{\max} when use_live_dynamic_urgency is False)
            waiter_order = sorted(
                self._conditions.keys(),
                key=lambda aid: (
                    self._effective_dynamic_tuples.get(aid, (0, 0))
                    if self.use_live_dynamic_urgency
                    else self._effective_upper_tuples.get(aid, (0, 0))
                ),
                reverse=True,
            )
            for aid in waiter_order:
                self._conditions[aid].notify_all()
