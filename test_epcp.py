"""
test_epcp.py - Comprehensive Automated Test Suite for EPCP & Database Concurrency
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Zero mocks. Tests real OS threads, real SQLite database transactions,
dual-level epistemic priorities (Pi_max and tilde_Pi), 64-bit integer priority tuples,
senior saturation promotion, state-version-keyed SHA-256 inference memoization,
real deadlock detection under S2PL / 2PL-Wait Promote, and deadlock prevention under EPCP.
"""

import unittest
import threading
import time
from epcp_types import (
    LockMode,
    AgentEpistemicState,
    T_SAT_SEC,
    PI_SAT_SCORE,
    compute_normalized_shannon_entropy,
    partition_for_resource,
    footprint_to_bitmask,
)
from database import ConcurrentDatabaseEngine
from baseline_s2pl import (
    Strict2PLManager,
    TwoPLWaitPromoteManager,
    WoundWaitManager,
    WaitDieManager,
    CoarseShardC2PLManager,
    DeadlockException,
    ConcurrencyAbortException,
)
from epcp_engine import EPCPEngine, OutOfFootprintException


class TestEPCPConcurrency(unittest.TestCase):

    def setUp(self):
        self.db = ConcurrentDatabaseEngine()
        self.db.reset_and_seed(num_accounts=10, num_items=10)

    def tearDown(self):
        self.db.close()

    def test_01_epistemic_priority_and_64bit_tuples(self):
        """Verify mathematical formulation of dynamic epistemic priority and 64-bit integer tuples."""
        agent_low = AgentEpistemicState(
            agent_id="agent_low",
            task_depth=0,
            accumulated_cost_omega=0.0,
            epistemic_entropy_h=0.8,
            arrival_rank=2,
        )
        agent_high = AgentEpistemicState(
            agent_id="agent_high",
            task_depth=4,
            accumulated_cost_omega=500.0,
            epistemic_entropy_h=0.1,
            arrival_rank=1,
        )
        p_low = agent_low.compute_epistemic_priority()
        p_high = agent_high.compute_epistemic_priority()

        self.assertGreater(p_high, p_low)
        # Canonical weights: Pi_base=1.0, w_d=2.0, w_Omega=1.5, w_c=3.0
        self.assertAlmostEqual(p_low, 1.0 + 0 + 0 + 3.0 * (1.0 - 0.8), places=3)
        self.assertGreater(agent_high.upper_envelope_tuple(), agent_low.upper_envelope_tuple())
        self.assertGreater(agent_high.dynamic_priority_tuple(), agent_low.dynamic_priority_tuple())

        # Monotonic latch: increasing entropy later must NOT reduce latched priority
        p_high_after = agent_high.compute_epistemic_priority()
        agent_high.epistemic_entropy_h = 0.95
        self.assertGreaterEqual(agent_high.compute_epistemic_priority(), p_high_after)

        # Normalized Shannon entropy over uniform vs. deterministic distributions
        self.assertAlmostEqual(compute_normalized_shannon_entropy([0.25, 0.25, 0.25, 0.25]), 1.0, places=5)
        self.assertAlmostEqual(compute_normalized_shannon_entropy([1.0, 0.0, 0.0]), 0.0, places=5)

    def test_02_database_crud_acid(self):
        """Verify real SQLite database engine handles transactional balance updates."""
        bal = self.db.execute_read_account("ACC_001")
        self.assertEqual(bal, 10000.0)

        ok = self.db.execute_update_account("ACC_001", -250.0)
        self.assertTrue(ok)
        self.db.commit()

        bal_new = self.db.execute_read_account("ACC_001")
        self.assertEqual(bal_new, 9750.0)

    def test_03_s2pl_mutual_exclusion(self):
        """Verify standard S2PL enforces mutual exclusion on exclusive write locks."""
        mgr = Strict2PLManager()
        ok1 = mgr.acquire_lock("agent_1", "res_A", LockMode.EXCLUSIVE_WRITE)
        self.assertTrue(ok1)

        ok2 = mgr.acquire_lock("agent_2", "res_A", LockMode.EXCLUSIVE_WRITE, timeout_sec=0.1)
        self.assertFalse(ok2)
        mgr.release_all_locks("agent_1")

    def test_04_s2pl_deadlock_detection(self):
        """
        Verify that under S2PL, cross-resource locking (A->B and B->A)
        produces an actual Wait-For-Graph cycle and raises DeadlockException.
        """
        mgr = Strict2PLManager()
        deadlock_caught = []
        barrier = threading.Barrier(2)

        def worker_1():
            try:
                mgr.acquire_lock("A1", "res_X", LockMode.EXCLUSIVE_WRITE)
                barrier.wait()
                time.sleep(0.05)
                mgr.acquire_lock("A1", "res_Y", LockMode.EXCLUSIVE_WRITE, timeout_sec=1.0)
            except DeadlockException as e:
                deadlock_caught.append(("A1", e.cycle))
            finally:
                mgr.release_all_locks("A1")

        def worker_2():
            try:
                mgr.acquire_lock("A2", "res_Y", LockMode.EXCLUSIVE_WRITE)
                barrier.wait()
                time.sleep(0.05)
                mgr.acquire_lock("A2", "res_X", LockMode.EXCLUSIVE_WRITE, timeout_sec=1.0)
            except DeadlockException as e:
                deadlock_caught.append(("A2", e.cycle))
            finally:
                mgr.release_all_locks("A2")

        t1 = threading.Thread(target=worker_1)
        t2 = threading.Thread(target=worker_2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertGreater(len(deadlock_caught), 0)
        self.assertGreater(mgr.deadlocks_detected_count, 0)

    def test_05_epcp_deadlock_prevention(self):
        """
        Verify that under EPCP, the identical cross-resource access pattern
        executes to 100% completion with ZERO deadlocks.
        """
        epcp = EPCPEngine()
        a1_state = AgentEpistemicState(agent_id="A1", task_depth=1, accumulated_cost_omega=10.0, epistemic_entropy_h=0.5)
        a2_state = AgentEpistemicState(agent_id="A2", task_depth=2, accumulated_cost_omega=20.0, epistemic_entropy_h=0.2)
        epcp.register_agent(a1_state)
        epcp.register_agent(a2_state)

        epcp.register_resource("res_X", "Accounts", "res_X", {"A1", "A2"})
        epcp.register_resource("res_Y", "Accounts", "res_Y", {"A1", "A2"})

        success = []
        barrier = threading.Barrier(2)

        def worker_1():
            barrier.wait()
            ok_x = epcp.acquire_lock("A1", "res_X", LockMode.EXCLUSIVE_WRITE, timeout_sec=2.0)
            if ok_x:
                time.sleep(0.02)
                ok_y = epcp.acquire_lock("A1", "res_Y", LockMode.EXCLUSIVE_WRITE, timeout_sec=2.0)
                if ok_y:
                    success.append("A1")
                    epcp.release_all_locks("A1")

        def worker_2():
            barrier.wait()
            ok_y = epcp.acquire_lock("A2", "res_Y", LockMode.EXCLUSIVE_WRITE, timeout_sec=2.0)
            if ok_y:
                time.sleep(0.02)
                ok_x = epcp.acquire_lock("A2", "res_X", LockMode.EXCLUSIVE_WRITE, timeout_sec=2.0)
                if ok_x:
                    success.append("A2")
                    epcp.release_all_locks("A2")

        t1 = threading.Thread(target=worker_1)
        t2 = threading.Thread(target=worker_2)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(success), 2)
        self.assertIn("A1", success)
        self.assertIn("A2", success)

    def test_06_epcp_priority_inheritance(self):
        """Verify priority inheritance dynamically elevates priority of blocking low-priority agent."""
        epcp = EPCPEngine()
        low = AgentEpistemicState(agent_id="low", task_depth=0, accumulated_cost_omega=0.0, epistemic_entropy_h=0.9)
        high = AgentEpistemicState(agent_id="high", task_depth=5, accumulated_cost_omega=100.0, epistemic_entropy_h=0.1)
        epcp.register_agent(low)
        epcp.register_agent(high)

        epcp.register_resource("shared_R", "Accounts", "shared_R", {"low", "high"})

        epcp.acquire_lock("low", "shared_R", LockMode.EXCLUSIVE_WRITE)
        self.assertEqual(epcp._active_priorities["low"], low.compute_epistemic_priority())

        def high_requester():
            epcp.acquire_lock("high", "shared_R", LockMode.EXCLUSIVE_WRITE, timeout_sec=0.5)

        t = threading.Thread(target=high_requester)
        t.start()
        time.sleep(0.05)

        self.assertGreaterEqual(epcp._active_priorities["low"], high.compute_epistemic_priority())
        self.assertGreater(epcp.priority_inheritance_count, 0)

        epcp.release_all_locks("low")
        t.join()

    def test_07_senior_saturation_and_versioned_memoization(self):
        """Verify Theorem 3 senior saturation promotion and state-version-keyed SHA-256 memoization."""
        epcp = EPCPEngine(enforce_strict_footprint=True)
        p_x = partition_for_resource("ACC_X")
        p_y = partition_for_resource("ITEM_Y")
        st = AgentEpistemicState(
            agent_id="ag_memo",
            task_depth=1,
            accumulated_cost_omega=5.0,
            epistemic_entropy_h=0.4,
            start_time_sec=time.perf_counter() - (T_SAT_SEC + 10.0),
            declared_partitions={p_x},
        )
        epcp.register_agent(st, footprint_partitions={p_x})
        orig_rank = epcp._agents["ag_memo"].arrival_rank

        # Memoize Step 1 inference on row state v1
        state_v1 = {"AccountId": "ACC_X", "Balance": 5000.0, "Version": 1}
        epcp.memoize_inference("ag_memo", "step1", state_v1, {"patch": "APPROVED"})
        self.assertEqual(epcp.lookup_memoized_inference("ag_memo", "step1", state_v1), {"patch": "APPROVED"})

        # Mutated row state v2 must miss the cache so stale inference is never replayed
        state_v2 = {"AccountId": "ACC_X", "Balance": 4200.0, "Version": 2}
        self.assertIsNone(epcp.lookup_memoized_inference("ag_memo", "step1", state_v2))

        # Requesting out-of-footprint partition p_y triggers safe expansion while preserving rank
        if p_y != p_x:
            with self.assertRaises(OutOfFootprintException):
                epcp.acquire_lock("ag_memo", "ITEM_Y", LockMode.EXCLUSIVE_WRITE)
            epcp.expand_footprint_and_reregister("ag_memo", p_y)
            self.assertIn("ag_memo", epcp._active_agents)
            self.assertEqual(epcp._agents["ag_memo"].arrival_rank, orig_rank)
            self.assertTrue(epcp.acquire_lock("ag_memo", "ITEM_Y", LockMode.EXCLUSIVE_WRITE))
            epcp.release_all_locks("ag_memo", deregister_active=True)

    def test_08_dynamic_footprint_recovery_helper_cache_hit_and_miss(self):
        """
        Verify execute_with_dynamic_footprint_recovery handles out-of-footprint Step-2
        discovery by rolling back staged writes, expanding F(T_i), re-acquiring Step-1 lock,
        and replaying SHA-256 memoized inference on cache hit (or re-evaluating on state drift).
        """
        epcp = EPCPEngine(enforce_strict_footprint=True)
        # Find two resource keys mapping to distinct 64-bit partitions
        res_a = "ACC_001"
        p_a = partition_for_resource(res_a)
        res_b = "ITEM_001"
        idx = 1
        while partition_for_resource(res_b) == p_a:
            idx += 1
            res_b = f"ITEM_{idx:03d}"
        p_b = partition_for_resource(res_b)

        # Case 1: Unmodified R_a during expansion -> SHA-256 digest hits (1 inference call total)
        st1 = AgentEpistemicState(
            agent_id="ag_recover_hit",
            task_depth=2,
            accumulated_cost_omega=20.0,
            epistemic_entropy_h=0.3,
            declared_partitions={p_a},
        )
        epcp.register_agent(st1, footprint_partitions={p_a})
        row_store = {res_a: {"AccountId": res_a, "Balance": 5000.0, "Version": 1}}
        committed_log = []

        ok, infer_calls, replayed = epcp.execute_with_dynamic_footprint_recovery(
            agent_id="ag_recover_hit",
            step1_resource_id=res_a,
            read_step1_state_fn=lambda r: dict(row_store[r]),
            step1_infer_fn=lambda state: {"target_item": res_b, "debit": 100.0, "ver": state["Version"]},
            discover_step2_resource_fn=lambda out: out["target_item"],
            commit_fn=lambda r1, r2, staged, out: committed_log.append((r1, r2, len(staged), out["ver"])),
        )
        self.assertTrue(ok)
        self.assertEqual(infer_calls, 1)
        self.assertTrue(replayed)
        self.assertIn(p_b, epcp._agents["ag_recover_hit"].declared_partitions)
        self.assertEqual(committed_log, [(res_a, res_b, 1, 1)])

        # Case 2: Concurrent mutation to R_a during expansion -> SHA-256 digest misses (2 inference calls)
        st2 = AgentEpistemicState(
            agent_id="ag_recover_miss",
            task_depth=2,
            accumulated_cost_omega=20.0,
            epistemic_entropy_h=0.3,
            declared_partitions={p_a},
        )
        epcp.register_agent(st2, footprint_partitions={p_a})
        read_counter = {"count": 0}

        def read_with_drift(r: str):
            read_counter["count"] += 1
            if read_counter["count"] == 1:
                return {"AccountId": r, "Balance": 5000.0, "Version": 1}
            return {"AccountId": r, "Balance": 4800.0, "Version": 2}

        ok2, infer_calls2, replayed2 = epcp.execute_with_dynamic_footprint_recovery(
            agent_id="ag_recover_miss",
            step1_resource_id=res_a,
            read_step1_state_fn=read_with_drift,
            step1_infer_fn=lambda state: {"target_item": res_b, "debit": 100.0, "ver": state["Version"]},
            discover_step2_resource_fn=lambda out: out["target_item"],
            commit_fn=lambda r1, r2, staged, out: committed_log.append((r1, r2, len(staged), out["ver"])),
        )
        self.assertTrue(ok2)
        self.assertEqual(infer_calls2, 2)
        self.assertFalse(replayed2)
        self.assertEqual(committed_log[-1], (res_a, res_b, 1, 2))

    def test_09_wait_graph_saturation_propagation_and_bridge_gating(self):
        """Verify Theorem 3 T_sat promotion propagation along G_wait and multi-partition bridge gating."""
        epcp = EPCPEngine(enforce_strict_footprint=True)
        res_a = "ACC_001"
        p_a = partition_for_resource(res_a)
        res_b = "ITEM_001"
        idx = 1
        while partition_for_resource(res_b) == p_a:
            idx += 1
            res_b = f"ITEM_{idx:03d}"
        p_b = partition_for_resource(res_b)

        # Low-priority holder currently owns res_a in partition p_a
        st_holder = AgentEpistemicState(
            agent_id="ag_holder",
            task_depth=1,
            accumulated_cost_omega=1.0,
            epistemic_entropy_h=0.9,
            declared_partitions={p_a},
        )
        # Bridge agent declaring {p_a, p_b} that starts 40ms before T_SAT_SEC so it crosses T_SAT_SEC while waiting
        st_bridge = AgentEpistemicState(
            agent_id="ag_bridge",
            task_depth=1,
            accumulated_cost_omega=2.0,
            epistemic_entropy_h=0.5,
            start_time_sec=time.perf_counter() - (T_SAT_SEC - 0.04),
            declared_partitions={p_a, p_b},
        )
        # Newly arriving lower-priority single-partition agent on p_b
        st_barger = AgentEpistemicState(
            agent_id="ag_barger",
            task_depth=1,
            accumulated_cost_omega=1.0,
            epistemic_entropy_h=0.9,
            declared_partitions={p_b},
        )

        epcp.register_agent(st_holder, footprint_partitions={p_a})
        epcp.register_agent(st_bridge, footprint_partitions={p_a, p_b})
        epcp.register_agent(st_barger, footprint_partitions={p_b})

        self.assertTrue(epcp.acquire_lock("ag_holder", res_a, LockMode.EXCLUSIVE_WRITE))

        bridge_acquired_a = threading.Event()

        def bridge_worker():
            if epcp.acquire_lock("ag_bridge", res_a, LockMode.EXCLUSIVE_WRITE, timeout_sec=1.5):
                bridge_acquired_a.set()
                epcp.acquire_lock("ag_bridge", res_b, LockMode.EXCLUSIVE_WRITE, timeout_sec=1.5)
                epcp.release_all_locks("ag_bridge")

        t_bridge = threading.Thread(target=bridge_worker)
        t_bridge.start()

        # Wait long enough for ag_bridge to enter G_wait and cross T_SAT_SEC (within ~60ms)
        time.sleep(0.08)

        with epcp._mutex:
            # Verify ag_bridge was promoted to senior saturation and propagated to ag_holder
            self.assertTrue(epcp._agents["ag_bridge"].promoted_saturated)
            self.assertGreaterEqual(
                epcp._effective_upper_tuples["ag_holder"],
                epcp._agents["ag_bridge"].upper_envelope_tuple(),
            )
            # Verify intersecting ceiling on {p_b} for ag_barger sees queued ag_bridge's senior ceiling
            ceil_for_barger, blocker, all_blockers = epcp._get_intersecting_ceiling_excluding("ag_barger", res_b)
            self.assertGreaterEqual(ceil_for_barger, epcp._agents["ag_bridge"].compute_upper_envelope_priority())
            self.assertEqual(blocker, "ag_holder")
            self.assertIn("ag_holder", all_blockers)

        epcp.release_all_locks("ag_holder")
        t_bridge.join(timeout=2.0)
        self.assertTrue(bridge_acquired_a.is_set())


if __name__ == "__main__":
    unittest.main()


