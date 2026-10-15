"""
test_enterprise_epcp.py - Comprehensive Unit & Concurrency Test Suite for Enterprise EPCP
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Zero mocks. Tests real SQLite enterprise database ACID invariants,
epistemic priority calculations, Tarjan WFG cycle detection, and EPCP ceiling arbitration.
"""

import unittest
import os
import threading
import time
from epcp_types import LockMode, AgentEpistemicState
from database import EnterpriseDatabaseEngine
from baseline_s2pl import Strict2PLManager, DeadlockException
from epcp_engine import EPCPEngine
from semantic_reasoning_workload import execute_semantic_reasoning_step


class TestEnterpriseEPCP(unittest.TestCase):

    def setUp(self):
        self.db = EnterpriseDatabaseEngine()
        self.db.seed_from_csv()

    def tearDown(self):
        self.db.close()

    def test_01_enterprise_database_acid_invariants(self):
        """Verify multi-table ACID transaction updates balance, inventory, and audit log."""
        bal_pre, stock_pre, _ = self.db.audit_system_balance()

        ok = self.db.execute_multi_resource_transaction(
            tx_id="TX_TEST_001",
            agent_id="agent_test",
            from_acc="ACC_00001",
            to_acc="ACC_00002",
            item_id="ITEM_0001",
            amount=250.0,
            item_count=2
        )
        self.assertTrue(ok)

        bal_post, stock_post, sett_post = self.db.audit_system_balance()
        # Balance must be exactly conserved (transferred from ACC_00001 to ACC_00002)
        self.assertAlmostEqual(bal_pre, bal_post, places=2)
        # Total stock (Stock + Reserved) must be exactly conserved
        self.assertEqual(stock_pre, stock_post)
        self.assertGreaterEqual(sett_post, 1)

    def test_02_cryptographic_semantic_reasoning_execution(self):
        """Verify genuine CPU-bound HMAC-SHA256 reasoning step produces valid risk ratings."""
        res = execute_semantic_reasoning_step(
            agent_id="agent_crypto",
            from_acc="ACC_00001",
            to_acc="ACC_00002",
            item_id="ITEM_0001",
            amount=150.0,
            entropy=0.25,
            complexity_rounds=200
        )
        self.assertIn("proof_hash", res)
        self.assertEqual(len(res["proof_hash"]), 16)
        self.assertIn(res["risk_rating"], ["LOW", "MEDIUM", "ELEVATED"])
        self.assertTrue(res["compliance_approved"])

    def test_03_wfg_deadlock_detection_and_abort(self):
        """Verify Strict 2PL detects cross-resource circular waiting and aborts victim."""
        mgr = Strict2PLManager()
        errors = []

        def worker_a():
            try:
                # Locks ACC_00001, then tries ITEM_0001
                mgr.acquire_lock("agent_A", "ACC_00001", LockMode.EXCLUSIVE_WRITE)
                time.sleep(0.05)
                mgr.acquire_lock("agent_A", "ITEM_0001", LockMode.EXCLUSIVE_WRITE, timeout_sec=0.5)
            except DeadlockException as e:
                errors.append("agent_A_deadlock")
            finally:
                mgr.release_all_locks("agent_A")

        def worker_b():
            try:
                # Locks ITEM_0001, then tries ACC_00001 (Opposing sequence -> Deadlock!)
                time.sleep(0.01)
                mgr.acquire_lock("agent_B", "ITEM_0001", LockMode.EXCLUSIVE_WRITE)
                time.sleep(0.05)
                mgr.acquire_lock("agent_B", "ACC_00001", LockMode.EXCLUSIVE_WRITE, timeout_sec=0.5)
            except DeadlockException as e:
                errors.append("agent_B_deadlock")
            finally:
                mgr.release_all_locks("agent_B")

        t1 = threading.Thread(target=worker_a)
        t2 = threading.Thread(target=worker_b)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # At least one agent MUST be caught in the WFG cycle and aborted
        self.assertGreaterEqual(len(errors), 1, "Strict 2PL must detect cycle and abort victim")

    def test_04_epcp_zero_deadlock_guarantee(self):
        """Verify EPCP ceiling rule preempts circular wait and eliminates deadlocks."""
        epcp = EPCPEngine()
        # Register agents
        ag_low = AgentEpistemicState(agent_id="ag_low", task_depth=1, accumulated_cost_omega=10.0, epistemic_entropy_h=0.8)
        ag_high = AgentEpistemicState(agent_id="ag_high", task_depth=4, accumulated_cost_omega=500.0, epistemic_entropy_h=0.1)
        epcp.register_agent(ag_low)
        epcp.register_agent(ag_high)

        # Register shared resources
        epcp.register_resource("ACC_00001", "Accounts", "ACC_00001", {"ag_low", "ag_high"})
        epcp.register_resource("ITEM_0001", "Inventory", "ITEM_0001", {"ag_low", "ag_high"})

        errors = []

        def worker_low():
            try:
                epcp.acquire_lock("ag_low", "ACC_00001", LockMode.EXCLUSIVE_WRITE)
                time.sleep(0.04)
                epcp.acquire_lock("ag_low", "ITEM_0001", LockMode.EXCLUSIVE_WRITE, timeout_sec=0.5)
            except Exception as e:
                errors.append(f"low_error: {e}")
            finally:
                epcp.release_all_locks("ag_low")

        def worker_high():
            try:
                time.sleep(0.01)
                epcp.acquire_lock("ag_high", "ITEM_0001", LockMode.EXCLUSIVE_WRITE)
                time.sleep(0.04)
                epcp.acquire_lock("ag_high", "ACC_00001", LockMode.EXCLUSIVE_WRITE, timeout_sec=0.5)
            except Exception as e:
                errors.append(f"high_error: {e}")
            finally:
                epcp.release_all_locks("ag_high")

        t1 = threading.Thread(target=worker_low)
        t2 = threading.Thread(target=worker_high)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Under EPCP: EXACTLY ZERO DEADLOCK EXCEPTIONS
        self.assertEqual(len(errors), 0, f"EPCP must prevent deadlocks without exceptions: {errors}")


if __name__ == "__main__":
    unittest.main()
