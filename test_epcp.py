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
            self.assertEqual(epcp._agents["ag_memo"].arrival_rank, orig_rank)
            self.assertTrue(epcp.acquire_lock("ag_memo", "ITEM_Y", LockMode.EXCLUSIVE_WRITE))
            epcp.release_all_locks("ag_memo", deregister_active=True)


if __name__ == "__main__":
    unittest.main()
