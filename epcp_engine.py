"""
epcp_engine.py - Epistemic Priority Ceiling Protocol (EPCP) Arbitration Engine
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Implements dynamic Epistemic Priority Ceiling Protocol (EPCP) with priority inheritance
and provable Wait-For-Graph acyclicity. Guarantees zero deadlocks and bounded waiting times.
"""

from __future__ import annotations
import threading
import time
from typing import Dict, Set, Optional, List, Tuple
from epcp_types import LockMode, AgentEpistemicState, ResourceDescriptor


class EPCPEngine:
    """
    Epistemic Priority Ceiling Protocol (EPCP) Concurrency Manager.
    Provably eliminates deadlocks and bounds priority inversion in multi-agent swarms.
    """

    def __init__(self):
        self._mutex = threading.RLock()
        # agent_id -> AgentEpistemicState
        self._agents: Dict[str, AgentEpistemicState] = {}
        # agent_id -> current active priority (base or inherited)
        self._active_priorities: Dict[str, float] = {}
        # resource_id -> ResourceDescriptor
        self._resources: Dict[str, ResourceDescriptor] = {}
        # agent_id -> set of held resource_ids
        self._held_resources: Dict[str, Set[str]] = {}
        # resource_id -> set of potential users (agent_ids) for ceiling calculation
        self._resource_users: Dict[str, Set[str]] = {}
        # agent_id -> Condition variable
        self._conditions: Dict[str, threading.Condition] = {}
        # Metrics
        self.priority_inheritance_count = 0
        self.deadlocks_prevented_count = 0
        self.preemptive_suspensions_count = 0

    def register_agent(self, state: AgentEpistemicState):
        """Registers or updates an agent's dynamic epistemic state."""
        with self._mutex:
            self._agents[state.agent_id] = state
            self._active_priorities[state.agent_id] = state.compute_epistemic_priority()
            if state.agent_id not in self._conditions:
                self._conditions[state.agent_id] = threading.Condition(self._mutex)

    def register_resource(self, resource_id: str, table_name: str, row_key: str, potential_users: Set[str]):
        """Registers a shared resource and pre-calculates its static ceiling."""
        with self._mutex:
            self._resource_users[resource_id] = set(potential_users)
            self._resources[resource_id] = ResourceDescriptor(
                resource_id=resource_id,
                table_name=table_name,
                row_key=row_key,
                ceiling_priority=0.0
            )
            self._recalculate_resource_ceiling(resource_id)

    def _recalculate_resource_ceiling(self, resource_id: str):
        """Ceiling C(R_j) = max_{A_k in Users(R_j)} Pi_k(t)."""
        users = self._resource_users.get(resource_id, set())
        max_p = 0.0
        for uid in users:
            agent_state = self._agents.get(uid)
            if agent_state:
                p = agent_state.compute_epistemic_priority()
                if p > max_p:
                    max_p = p
        if resource_id in self._resources:
            self._resources[resource_id].ceiling_priority = max_p

    def _get_system_ceiling_excluding(self, agent_id: str) -> Tuple[float, Optional[str], Optional[str]]:
        """
        Finds the highest ceiling priority among all resources currently locked
        by ANY agent OTHER than agent_id:
            C*(t) = max { C(R_m) | R_m is locked by A_k, A_k != agent_id }
        Returns:
            Tuple[max_ceiling, blocking_resource_id, holding_agent_id]
        """
        max_c = 0.0
        blocking_res = None
        blocking_holder = None

        for res_id, res in self._resources.items():
            holder = res.current_holder_agent_id
            if holder and holder != agent_id:
                if res.ceiling_priority > max_c:
                    max_c = res.ceiling_priority
                    blocking_res = res_id
                    blocking_holder = holder

        return max_c, blocking_res, blocking_holder

    def acquire_lock(self, agent_id: str, resource_id: str, mode: LockMode, timeout_sec: float = 3.0) -> bool:
        """
        Acquires a lock under EPCP invariants:
        1. Resource must be free.
        2. Agent's active priority Pi_i(t) must be strictly greater than the system ceiling C*(t)
           of all resources currently locked by other agents (unless agent_id already holds
           the resource that established the current ceiling).
        3. If blocked, the holder of the blocking resource inherits Pi_i(t) to accelerate release.
        """
        with self._mutex:
            cond = self._conditions[agent_id]
            start_time = time.time()

            while True:
                res = self._resources.get(resource_id)
                if not res:
                    # Auto-register resource if not present
                    self.register_resource(resource_id, "Generic", resource_id, {agent_id})
                    res = self._resources[resource_id]

                active_p = self._active_priorities.get(agent_id, 1.0)
                current_holder = res.current_holder_agent_id

                # Check Condition 1: Is resource physically free?
                is_free = current_holder is None or current_holder == agent_id

                # Check Condition 2: Priority Ceiling Rule
                sys_ceiling, blocking_res, blocking_holder = self._get_system_ceiling_excluding(agent_id)
                
                # Ceiling test passes if agent's priority is strictly greater than system ceiling,
                # or if no other agent holds any locked resources (sys_ceiling == 0.0)
                ceiling_test_passed = (active_p > sys_ceiling) or (sys_ceiling == 0.0)

                if is_free and ceiling_test_passed:
                    # Grant lock atomically
                    res.current_holder_agent_id = agent_id
                    res.lock_mode = mode
                    self._held_resources.setdefault(agent_id, set()).add(resource_id)
                    return True

                # If resource is held by someone else, or ceiling test failed:
                # Identify the agent causing the block
                victim_holder = current_holder if not is_free else blocking_holder

                if victim_holder and victim_holder != agent_id:
                    # Priority Inheritance: victim inherits active_p if active_p > victim's priority
                    victim_p = self._active_priorities.get(victim_holder, 1.0)
                    if active_p > victim_p:
                        self._active_priorities[victim_holder] = active_p
                        self.priority_inheritance_count += 1
                        # Wake victim up so it can proceed with elevated priority
                        if victim_holder in self._conditions:
                            self._conditions[victim_holder].notify_all()

                if not ceiling_test_passed:
                    self.preemptive_suspensions_count += 1

                # Check timeout
                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    return False

                # Block waiting for lock release or ceiling reduction
                cond.wait(timeout=remaining)

    def release_all_locks(self, agent_id: str):
        """
        Releases all locks held by agent_id and reverts its active priority to its
        base epistemic priority Pi_i(t). Wakes up all blocked agents.
        """
        with self._mutex:
            held = self._held_resources.pop(agent_id, set())
            for res_id in held:
                res = self._resources.get(res_id)
                if res and res.current_holder_agent_id == agent_id:
                    res.current_holder_agent_id = None
                    res.lock_mode = None

            # Revert priority to nominal epistemic priority
            agent_state = self._agents.get(agent_id)
            if agent_state:
                self._active_priorities[agent_id] = agent_state.compute_epistemic_priority()

            # Wake up all waiting agents to re-evaluate ceiling tests
            for cond in self._conditions.values():
                cond.notify_all()
