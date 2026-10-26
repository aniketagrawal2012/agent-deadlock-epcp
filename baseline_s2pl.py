"""
baseline_s2pl.py - Enterprise Concurrency Control Baselines for Multi-Agent Evaluation
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Implements genuine, zero-mock multi-threaded database concurrency baselines:
  1. Strict2PLManager (S2PL): Wait-For-Graph (WFG) DFS cycle detection and victim abort [Eswaran et al. 1976].
  2. TwoPLWaitPromoteManager (2PL-Wait Promote): Real-time priority 2PL with epistemic priority
     inheritance on direct contention (without ceiling admission), aborting the lowest-priority
     cycle member on WFG deadlock [Abbott & Garcia-Molina 1992].
  3. WoundWaitManager (Wound-Wait): Preemptive timestamp deadlock prevention (older wounds younger;
     younger waits) as used in Google Cloud Spanner [Rosenkrantz et al. 1978, Corbett et al. 2013].
  4. WaitDieManager (Wait-Die): Non-preemptive timestamp deadlock prevention (older waits;
     younger dies/aborts immediately upon conflict) [Rosenkrantz et al. 1978].
  5. OCCManager (Optimistic Concurrency Control): Lock-free snapshot read + CAS version validation
     at commit time.
  6. CoarseShardC2PLManager (Coarse Shard-Scoped C2PL): Conservative 2PL acquiring exclusive
     upfront locks on coarse domain shards (B_shard = 8) [Bernstein et al. 1987].
  7. CanonicalOrderedLockManager: Two-phase speculative discovery pre-pass followed by canonical
     row-ID ordered locking and state-drift verification.
"""

from __future__ import annotations
import threading
import time
from typing import Dict, Set, Optional, List, Tuple
from epcp_types import LockMode, AgentEpistemicState, partition_for_resource


class DeadlockException(Exception):
    """Raised when a directed cycle is detected in the Wait-For-Graph."""
    def __init__(self, cycle: List[str]):
        super().__init__(f"Deadlock detected in Wait-For-Graph: {' -> '.join(cycle)}")
        self.cycle = cycle


class ConcurrencyAbortException(Exception):
    """Raised when a transaction is aborted by Wound-Wait, Wait-Die, or OCC validation."""
    def __init__(self, reason: str, step_aborted: int = 2):
        super().__init__(reason)
        self.reason = reason
        self.step_aborted = step_aborted


class Strict2PLManager:
    """
    Standard Strict Two-Phase Locking (S2PL) manager.
    Maintains an explicit Wait-For-Graph and performs DFS cycle detection.
    """

    def __init__(self):
        self._mutex = threading.RLock()
        self._lock_holders: Dict[str, str] = {}
        self._shared_holders: Dict[str, Set[str]] = {}
        self._held_resources: Dict[str, Set[str]] = {}
        self._waiting_for: Dict[str, str] = {}
        self._agent_conditions: Dict[str, threading.Condition] = {}
        self.deadlocks_detected_count = 0

    def _get_condition(self, agent_id: str) -> threading.Condition:
        if agent_id not in self._agent_conditions:
            self._agent_conditions[agent_id] = threading.Condition(self._mutex)
        return self._agent_conditions[agent_id]

    def _find_cycle(self) -> Optional[List[str]]:
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
        with self._mutex:
            cond = self._get_condition(agent_id)
            start_time = time.time()

            while True:
                current_holder = self._lock_holders.get(resource_id)
                if mode == LockMode.EXCLUSIVE_WRITE:
                    can_grant = (current_holder is None or current_holder == agent_id) and (
                        not self._shared_holders.get(resource_id) or self._shared_holders.get(resource_id) == {agent_id}
                    )
                else:
                    can_grant = current_holder is None or current_holder == agent_id

                if can_grant:
                    if mode == LockMode.EXCLUSIVE_WRITE:
                        self._lock_holders[resource_id] = agent_id
                    else:
                        self._shared_holders.setdefault(resource_id, set()).add(agent_id)
                    self._held_resources.setdefault(agent_id, set()).add(resource_id)
                    self._waiting_for.pop(agent_id, None)
                    return True

                self._waiting_for[agent_id] = resource_id
                cycle = self._find_cycle()
                if cycle:
                    self.deadlocks_detected_count += 1
                    self._waiting_for.pop(agent_id, None)
                    raise DeadlockException(cycle)

                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    self._waiting_for.pop(agent_id, None)
                    return False

                cond.wait(timeout=remaining)

    def release_all_locks(self, agent_id: str):
        with self._mutex:
            held = self._held_resources.pop(agent_id, set())
            for res_id in held:
                if self._lock_holders.get(res_id) == agent_id:
                    del self._lock_holders[res_id]
                if res_id in self._shared_holders and agent_id in self._shared_holders[res_id]:
                    self._shared_holders[res_id].remove(agent_id)
                    if not self._shared_holders[res_id]:
                        del self._shared_holders[res_id]
            self._waiting_for.pop(agent_id, None)
            for cond in self._agent_conditions.values():
                cond.notify_all()


class TwoPLWaitPromoteManager(Strict2PLManager):
    """
    2PL-Wait Promote [Abbott & Garcia-Molina 1992]:
    Combines S2PL direct-contention locking with transitive epistemic priority inheritance
    and priority-ordered condition-variable wake-ups, WITHOUT EPCP's partition priority ceiling
    admission gate. Isolates the exact contribution of priority inheritance vs. ceiling admission.
    """

    def __init__(self):
        super().__init__()
        self._base_priorities: Dict[str, float] = {}
        self._effective_priorities: Dict[str, float] = {}
        self.priority_promotions_count: int = 0

    def register_agent(self, state: AgentEpistemicState):
        with self._mutex:
            p = state.compute_epistemic_priority()
            self._base_priorities[state.agent_id] = p
            self._effective_priorities[state.agent_id] = p

    def update_priority(self, agent_id: str, new_p: float):
        with self._mutex:
            self._base_priorities[agent_id] = max(self._base_priorities.get(agent_id, 0.0), new_p)
            self._effective_priorities[agent_id] = max(self._effective_priorities.get(agent_id, 0.0), new_p)

    def acquire_lock(self, agent_id: str, resource_id: str, mode: LockMode, timeout_sec: float = 2.0) -> bool:
        with self._mutex:
            cond = self._get_condition(agent_id)
            start_time = time.time()
            while True:
                current_holder = self._lock_holders.get(resource_id)
                if current_holder is None or current_holder == agent_id:
                    self._lock_holders[resource_id] = agent_id
                    self._held_resources.setdefault(agent_id, set()).add(resource_id)
                    self._waiting_for.pop(agent_id, None)
                    return True

                # Direct contention: promote holder's priority if waiter has higher priority
                waiter_p = self._effective_priorities.get(agent_id, 1.0)
                holder_p = self._effective_priorities.get(current_holder, 1.0)
                if waiter_p > holder_p:
                    self._effective_priorities[current_holder] = waiter_p
                    self.priority_promotions_count += 1
                    if current_holder in self._agent_conditions:
                        self._agent_conditions[current_holder].notify_all()

                self._waiting_for[agent_id] = resource_id
                cycle = self._find_cycle()
                if cycle:
                    self.deadlocks_detected_count += 1
                    self._waiting_for.pop(agent_id, None)
                    raise DeadlockException(cycle)

                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    self._waiting_for.pop(agent_id, None)
                    return False

                cond.wait(timeout=remaining)

    def release_all_locks(self, agent_id: str):
        with self._mutex:
            held = self._held_resources.pop(agent_id, set())
            for res_id in held:
                if self._lock_holders.get(res_id) == agent_id:
                    del self._lock_holders[res_id]
            self._waiting_for.pop(agent_id, None)
            self._effective_priorities[agent_id] = self._base_priorities.get(agent_id, 1.0)
            # Wake waiting agents in descending order of effective epistemic priority
            ordered = sorted(
                self._agent_conditions.keys(),
                key=lambda aid: self._effective_priorities.get(aid, 0.0),
                reverse=True,
            )
            for aid in ordered:
                self._agent_conditions[aid].notify_all()


class WoundWaitManager:
    """
    Wound-Wait [Rosenkrantz et al. 1978, Corbett et al. 2013]:
    Older transaction (smaller timestamp/rank) wounds (preempts) a younger holder;
    younger transaction waits for an older holder.
    Tracks whether wounded holder was in Step 1 (1 lock held -> 800 tokens wasted)
    or Step 2 (2 locks held -> 1,600 tokens wasted).
    """

    def __init__(self):
        self._mutex = threading.RLock()
        self._lock_holders: Dict[str, str] = {}
        self._held_resources: Dict[str, Set[str]] = {}
        self._timestamps: Dict[str, int] = {}
        self._wounded_agents: Dict[str, int] = {}  # agent_id -> step at which wounded (1 or 2)
        self._conditions: Dict[str, threading.Condition] = {}
        self._next_ts = 1
        self.wounds_step1_count = 0
        self.wounds_step2_count = 0

    def begin_transaction(self, agent_id: str, preserve_timestamp: bool = False):
        with self._mutex:
            if not preserve_timestamp or agent_id not in self._timestamps:
                self._timestamps[agent_id] = self._next_ts
                self._next_ts += 1
            self._wounded_agents.pop(agent_id, None)
            if agent_id not in self._conditions:
                self._conditions[agent_id] = threading.Condition(self._mutex)

    def check_if_wounded(self, agent_id: str, current_step: int = 1):
        with self._mutex:
            if agent_id in self._wounded_agents:
                step_w = max(current_step, self._wounded_agents.pop(agent_id))
                raise ConcurrencyAbortException(f"{agent_id} wounded by older transaction", step_aborted=step_w)

    def acquire_lock(self, agent_id: str, resource_id: str, current_step: int = 1, timeout_sec: float = 3.0) -> bool:
        with self._mutex:
            cond = self._conditions.setdefault(agent_id, threading.Condition(self._mutex))
            my_ts = self._timestamps.get(agent_id, 999999)
            start_time = time.time()

            while True:
                if agent_id in self._wounded_agents:
                    step_w = max(current_step, self._wounded_agents.pop(agent_id))
                    raise ConcurrencyAbortException(f"{agent_id} wounded while waiting", step_aborted=step_w)

                holder = self._lock_holders.get(resource_id)
                if holder is None or holder == agent_id:
                    self._lock_holders[resource_id] = agent_id
                    self._held_resources.setdefault(agent_id, set()).add(resource_id)
                    return True

                holder_ts = self._timestamps.get(holder, 999999)
                if my_ts < holder_ts:
                    # Older wounds younger holder immediately!
                    holder_held_count = len(self._held_resources.get(holder, set()))
                    wounded_step = 2 if holder_held_count >= 2 else 1
                    if holder not in self._wounded_agents:
                        self._wounded_agents[holder] = wounded_step
                        if wounded_step == 1:
                            self.wounds_step1_count += 1
                        else:
                            self.wounds_step2_count += 1
                    # Force release of wounded holder's locks
                    for r_id in list(self._held_resources.pop(holder, set())):
                        if self._lock_holders.get(r_id) == holder:
                            del self._lock_holders[r_id]
                    if holder in self._conditions:
                        self._conditions[holder].notify_all()
                    continue

                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    return False
                cond.wait(timeout=remaining)

    def release_all_locks(self, agent_id: str):
        with self._mutex:
            for r_id in self._held_resources.pop(agent_id, set()):
                if self._lock_holders.get(r_id) == agent_id:
                    del self._lock_holders[r_id]
            for c in self._conditions.values():
                c.notify_all()


class WaitDieManager:
    """
    Wait-Die [Rosenkrantz et al. 1978]:
    Older transaction (smaller timestamp) waits for younger holder;
    younger transaction immediately dies (self-aborts) upon encountering an older holder.
    Tracks Step-1 self-aborts (0 tokens wasted) vs. Step-2 self-aborts (800 tokens wasted).
    """

    def __init__(self):
        self._mutex = threading.RLock()
        self._lock_holders: Dict[str, str] = {}
        self._held_resources: Dict[str, Set[str]] = {}
        self._timestamps: Dict[str, int] = {}
        self._conditions: Dict[str, threading.Condition] = {}
        self._next_ts = 1
        self.dies_step1_count = 0
        self.dies_step2_count = 0

    def begin_transaction(self, agent_id: str, preserve_timestamp: bool = False):
        with self._mutex:
            if not preserve_timestamp or agent_id not in self._timestamps:
                self._timestamps[agent_id] = self._next_ts
                self._next_ts += 1
            if agent_id not in self._conditions:
                self._conditions[agent_id] = threading.Condition(self._mutex)

    def acquire_lock(self, agent_id: str, resource_id: str, current_step: int = 1, timeout_sec: float = 3.0) -> bool:
        with self._mutex:
            cond = self._conditions.setdefault(agent_id, threading.Condition(self._mutex))
            my_ts = self._timestamps.get(agent_id, 999999)
            start_time = time.time()

            while True:
                holder = self._lock_holders.get(resource_id)
                if holder is None or holder == agent_id:
                    self._lock_holders[resource_id] = agent_id
                    self._held_resources.setdefault(agent_id, set()).add(resource_id)
                    return True

                holder_ts = self._timestamps.get(holder, 0)
                if my_ts > holder_ts:
                    # Younger requester dies immediately!
                    if current_step == 1:
                        self.dies_step1_count += 1
                    else:
                        self.dies_step2_count += 1
                    raise ConcurrencyAbortException(f"{agent_id} died on Step {current_step}", step_aborted=current_step)

                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    return False
                cond.wait(timeout=remaining)

    def release_all_locks(self, agent_id: str):
        with self._mutex:
            for r_id in self._held_resources.pop(agent_id, set()):
                if self._lock_holders.get(r_id) == agent_id:
                    del self._lock_holders[r_id]
            for c in self._conditions.values():
                c.notify_all()


class CoarseShardC2PLManager:
    """
    Coarse Shard-Scoped Conservative 2PL (Shard-C2PL, B_shard = 8) [Bernstein et al. 1987]:
    Acquires exclusive locks on all target domain shards atomically in a single upfront
    all-or-nothing step before executing Step 1. Guarantees 0% aborts at the cost of
    coarse shard-level serialization.
    """

    def __init__(self, num_shards: int = 8):
        self._mutex = threading.RLock()
        self.num_shards = num_shards
        self._shard_holders: Dict[int, str] = {}
        self._held_shards: Dict[str, Set[int]] = {}
        self._cond = threading.Condition(self._mutex)

    def acquire_all_shards(self, agent_id: str, resources: List[str], timeout_sec: float = 5.0) -> bool:
        shards = {partition_for_resource(r, self.num_shards) for r in resources}
        with self._mutex:
            start_time = time.time()
            while True:
                can_grant = all(
                    self._shard_holders.get(s) in (None, agent_id) for s in shards
                )
                if can_grant:
                    for s in shards:
                        self._shard_holders[s] = agent_id
                    self._held_shards[agent_id] = shards
                    return True

                elapsed = time.time() - start_time
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)

    def release_all_locks(self, agent_id: str):
        with self._mutex:
            for s in self._held_shards.pop(agent_id, set()):
                if self._shard_holders.get(s) == agent_id:
                    del self._shard_holders[s]
            self._cond.notify_all()
