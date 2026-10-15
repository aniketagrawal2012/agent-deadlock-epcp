"""
baseline_s2pl.py - Strict Two-Phase Locking (S2PL) with Wait-For-Graph Cycle Detection
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Standard enterprise database concurrency baseline: Strict 2PL with Wait-For-Graph (WFG)
cycle detection and victim abort/retry semantics.
"""

from __future__ import annotations
import threading
import time
from typing import Dict, Set, Optional, List, Tuple
from epcp_types import LockMode, TransactionStatus


class DeadlockException(Exception):
    """Raised when a directed cycle is detected in the Wait-For-Graph."""
    def __init__(self, cycle: List[str]):
        super().__init__(f"Deadlock detected in Wait-For-Graph: {' -> '.join(cycle)}")
        self.cycle = cycle


class Strict2PLManager:
    """
    Standard Strict Two-Phase Locking (S2PL) manager.
    Maintains an explicit Wait-For-Graph and performs cycle detection.
    """

    def __init__(self):
        self._mutex = threading.RLock()
        # resource_id -> holder_agent_id
        self._lock_holders: Dict[str, str] = {}
        # resource_id -> set of shared holders
        self._shared_holders: Dict[str, Set[str]] = {}
        # agent_id -> set of held resource_ids
        self._held_resources: Dict[str, Set[str]] = {}
        # agent_id -> resource_id agent is waiting for
        self._waiting_for: Dict[str, str] = {}
        # Condition variables per agent for blocking/waking
        self._agent_conditions: Dict[str, threading.Condition] = {}
        self.deadlocks_detected_count = 0

    def _get_condition(self, agent_id: str) -> threading.Condition:
        if agent_id not in self._agent_conditions:
            self._agent_conditions[agent_id] = threading.Condition(self._mutex)
        return self._agent_conditions[agent_id]

    def _find_cycle(self) -> Optional[List[str]]:
        """
        Performs Depth-First Search (DFS) over the Wait-For-Graph.
        Returns a list of agent_ids forming a directed cycle, or None.
        """
        # Build adjacency: agent_i -> agent_j (if agent_i is waiting for resource held by agent_j)
        adj: Dict[str, Set[str]] = {}
        for waiter, res_id in self._waiting_for.items():
            holder = self._lock_holders.get(res_id)
            if holder and holder != waiter:
                adj.setdefault(waiter, set()).add(holder)
            for s_holder in self._shared_holders.get(res_id, set()):
                if s_holder != waiter:
                    adj.setdefault(waiter, set()).add(s_holder)

        visited = set()
        rec_stack: List[str] = []

        def _dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.append(node)

            for neighbor in adj.get(node, set()):
                if neighbor not in visited:
                    res = _dfs(neighbor)
                    if res:
                        return res
                elif neighbor in rec_stack:
                    # Cycle detected: extract sublist from neighbor onwards
                    idx = rec_stack.index(neighbor)
                    return rec_stack[idx:] + [neighbor]

            rec_stack.pop()
            return None

        for agent in list(adj.keys()):
            if agent not in visited:
                cycle = _dfs(agent)
                if cycle:
                    return cycle
        return None

    def acquire_lock(self, agent_id: str, resource_id: str, mode: LockMode, timeout_sec: float = 2.0) -> bool:
        """
        Acquires a lock under S2PL semantics.
        Blocks if resource is contested. Detects deadlocks and raises DeadlockException if a cycle forms.
        """
        with self._mutex:
            cond = self._get_condition(agent_id)
            start_time = time.time()

            while True:
                current_holder = self._lock_holders.get(resource_id)
                # Check if lock can be granted immediately
                if mode == LockMode.EXCLUSIVE_WRITE:
                    can_grant = (current_holder is None or current_holder == agent_id) and (
                        not self._shared_holders.get(resource_id) or self._shared_holders.get(resource_id) == {agent_id}
                    )
                else:  # SHARED_READ
                    can_grant = current_holder is None or current_holder == agent_id

                if can_grant:
                    # Grant lock
                    if mode == LockMode.EXCLUSIVE_WRITE:
                        self._lock_holders[resource_id] = agent_id
                    else:
                        self._shared_holders.setdefault(resource_id, set()).add(agent_id)

                    self._held_resources.setdefault(agent_id, set()).add(resource_id)
                    if agent_id in self._waiting_for:
                        del self._waiting_for[agent_id]
                    return True

                # Lock is contested: register waiting edge
                self._waiting_for[agent_id] = resource_id

                # Run Wait-For-Graph cycle detection
                cycle = self._find_cycle()
                if cycle:
                    self.deadlocks_detected_count += 1
                    del self._waiting_for[agent_id]
                    raise DeadlockException(cycle)

                # Check timeout
                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    del self._waiting_for[agent_id]
                    return False

                # Block until holder releases
                cond.wait(timeout=remaining)

    def release_all_locks(self, agent_id: str):
        """Releases all locks held by agent upon COMMIT or ABORT, waking up waiting threads."""
        with self._mutex:
            held = self._held_resources.pop(agent_id, set())
            for res_id in held:
                if self._lock_holders.get(res_id) == agent_id:
                    del self._lock_holders[res_id]
                if res_id in self._shared_holders and agent_id in self._shared_holders[res_id]:
                    self._shared_holders[res_id].remove(agent_id)
                    if not self._shared_holders[res_id]:
                        del self._shared_holders[res_id]

            if agent_id in self._waiting_for:
                del self._waiting_for[agent_id]

            # Wake up all waiters
            for cond in self._agent_conditions.values():
                cond.notify_all()
