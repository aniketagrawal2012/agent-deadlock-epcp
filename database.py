"""
database.py - Real Enterprise Concurrent SQLite Database Engine
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Zero mocks. Manages persistent SQLite transactional storage on macOS APFS.
Loads 10,000 enterprise accounts and 5,000 inventory items.
Operates with Write-Ahead Logging (WAL), shared-memory committed-state cache for
in-critical-section domain pre-checks (Biz-Rule Reject), and atomic batch commit flush.
"""

from __future__ import annotations
import os
import csv
import sqlite3
import threading
from typing import Optional, Dict, Any, List, Tuple

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "epcp_enterprise.db")
ACCOUNTS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets", "seed_accounts.csv")
INVENTORY_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets", "seed_inventory.csv")


class EnterpriseDatabaseEngine:
    """
    Physical multi-threaded SQLite database engine managing enterprise transactional state
    under heavy multi-agent concurrency.
    Maintains an in-memory committed row-state mirror synchronized with SQLite WAL commits
    so lock-holding workers observe 100% up-to-date committed balances/stock without
    prematurely acquiring SQLite's file-level single-writer lock during multi-step LLM inference.
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._local = threading.local()
        self._state_lock = threading.RLock()
        # In-memory mirror of committed state: acc_id -> (balance, credit_limit, version)
        self._accounts_cache: Dict[str, List[ float ]] = {}
        # item_id -> [stock_count, reserved_count, version]
        self._inventory_cache: Dict[str, List[int]] = {}
        self._init_schema()

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=60.0, check_same_thread=False)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 60000;")
            conn.execute("PRAGMA cache_size = -64000;")  # 64 MB page cache
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        with conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS Accounts (
                    AccountId TEXT PRIMARY KEY,
                    Balance REAL NOT NULL,
                    CreditLimit REAL NOT NULL,
                    CustomerTier TEXT NOT NULL,
                    AccountVersion INTEGER NOT NULL DEFAULT 1,
                    CreatedAt TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS Inventory (
                    ItemId TEXT PRIMARY KEY,
                    StockCount INTEGER NOT NULL,
                    ReservedCount INTEGER NOT NULL DEFAULT 0,
                    UnitPrice REAL NOT NULL,
                    Category TEXT NOT NULL,
                    ItemVersion INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS Settlements (
                    TxId TEXT PRIMARY KEY,
                    FromAccountId TEXT NOT NULL,
                    ToAccountId TEXT NOT NULL,
                    ItemId TEXT NOT NULL,
                    Amount REAL NOT NULL,
                    ItemCount INTEGER NOT NULL,
                    Status TEXT NOT NULL,
                    SettledAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS AuditLog (
                    LogId INTEGER PRIMARY KEY AUTOINCREMENT,
                    TxId TEXT NOT NULL,
                    AgentId TEXT NOT NULL,
                    ActionType TEXT NOT NULL,
                    EntityKey TEXT NOT NULL,
                    PreviousVersion INTEGER NOT NULL,
                    NewVersion INTEGER NOT NULL,
                    Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_settlements_acc ON Settlements (FromAccountId);
                CREATE INDEX IF NOT EXISTS idx_settlements_item ON Settlements (ItemId);
            """)
        conn.close()

    def seed_from_csv(self, scale_factor: float = 1.0):
        """
        Loads the authentic enterprise dataset into SQLite tables.
        When scale_factor > 1.0 (e.g., 100.0 for the Zero-Depletion Control),
        initial account balances and inventory stock counts are scaled accordingly
        so zero Biz-Rule Rejects occur.
        """
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        with self._state_lock, conn:
            conn.execute("DELETE FROM Accounts;")
            conn.execute("DELETE FROM Inventory;")
            conn.execute("DELETE FROM Settlements;")
            conn.execute("DELETE FROM AuditLog;")
            self._accounts_cache.clear()
            self._inventory_cache.clear()

            if os.path.exists(ACCOUNTS_CSV):
                with open(ACCOUNTS_CSV, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader)
                    acc_rows = []
                    for r in reader:
                        bal = float(r[1]) * scale_factor
                        cred = float(r[2]) * scale_factor
                        ver = int(r[4])
                        acc_rows.append((r[0], bal, cred, r[3], ver, r[5]))
                        self._accounts_cache[r[0]] = [bal, cred, float(ver)]
                    conn.executemany(
                        "INSERT INTO Accounts VALUES (?, ?, ?, ?, ?, ?);",
                        acc_rows
                    )

            if os.path.exists(INVENTORY_CSV):
                with open(INVENTORY_CSV, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader)
                    inv_rows = []
                    for r in reader:
                        stk = int(round(int(r[1]) * scale_factor))
                        res_c = int(r[2])
                        ver = int(r[5])
                        inv_rows.append((r[0], stk, res_c, float(r[3]), r[4], ver))
                        self._inventory_cache[r[0]] = [stk, res_c, ver]
                    conn.executemany(
                        "INSERT INTO Inventory VALUES (?, ?, ?, ?, ?, ?);",
                        inv_rows
                    )

        conn.close()

    def check_step1_biz_rule_sufficiency(
        self,
        from_acc: str,
        item_id: str,
        amount: float,
        item_count: int,
        check_account: bool = True,
        check_item: bool = False,
    ) -> bool:
        """
        Sub-microsecond domain pre-check (Biz-Rule Reject) executed immediately upon
        acquiring the Step-1 lock prior to invoking LLM inference Infer(phi).
        Returns True if sufficient funds/stock exist, False if depleted.
        """
        with self._state_lock:
            if check_account:
                acc = self._accounts_cache.get(from_acc)
                if acc is not None and (acc[0] + acc[1]) < amount:
                    return False
            if check_item:
                inv = self._inventory_cache.get(item_id)
                if inv is not None and inv[0] < item_count:
                    return False
        return True

    def get_row_version(self, entity_key: str) -> int:
        """Returns the current committed version integer for an Account or Inventory row."""
        with self._state_lock:
            if entity_key in self._accounts_cache:
                return int(self._accounts_cache[entity_key][2])
            if entity_key in self._inventory_cache:
                return int(self._inventory_cache[entity_key][2])
        return 1

    def execute_multi_resource_transaction(
        self,
        tx_id: str,
        agent_id: str,
        from_acc: str,
        to_acc: str,
        item_id: str,
        amount: float,
        item_count: int
    ) -> bool:
        """
        Executes a real multi-table ACID transaction across Accounts and Inventory
        at the Commit boundary while holding exclusive locks on all target rows.
        """
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT Balance, CreditLimit, AccountVersion FROM Accounts WHERE AccountId = ?;", (from_acc,))
            row_from = cur.fetchone()
            if not row_from or (row_from[0] + row_from[1]) < amount:
                return False

            v_from = row_from[2]
            cur.execute(
                "UPDATE Accounts SET Balance = Balance - ?, AccountVersion = AccountVersion + 1 WHERE AccountId = ?;",
                (amount, from_acc)
            )

            cur.execute("SELECT Balance, CreditLimit, AccountVersion FROM Accounts WHERE AccountId = ?;", (to_acc,))
            row_to = cur.fetchone()
            if not row_to:
                conn.rollback()
                return False
            v_to = row_to[2]
            cur.execute(
                "UPDATE Accounts SET Balance = Balance + ?, AccountVersion = AccountVersion + 1 WHERE AccountId = ?;",
                (amount, to_acc)
            )

            cur.execute("SELECT StockCount, ReservedCount, ItemVersion FROM Inventory WHERE ItemId = ?;", (item_id,))
            row_item = cur.fetchone()
            if not row_item or row_item[0] < item_count:
                conn.rollback()
                return False
            v_item = row_item[2]
            cur.execute(
                "UPDATE Inventory SET StockCount = StockCount - ?, ReservedCount = ReservedCount + ?, ItemVersion = ItemVersion + 1 WHERE ItemId = ?;",
                (item_count, item_count, item_id)
            )

            cur.execute(
                "INSERT INTO Settlements (TxId, FromAccountId, ToAccountId, ItemId, Amount, ItemCount, Status) VALUES (?, ?, ?, ?, ?, ?, 'COMMITTED');",
                (tx_id, from_acc, to_acc, item_id, amount, item_count)
            )

            cur.execute(
                "INSERT INTO AuditLog (TxId, AgentId, ActionType, EntityKey, PreviousVersion, NewVersion) VALUES (?, ?, 'DEBIT', ?, ?, ?);",
                (tx_id, agent_id, from_acc, v_from, v_from + 1)
            )
            cur.execute(
                "INSERT INTO AuditLog (TxId, AgentId, ActionType, EntityKey, PreviousVersion, NewVersion) VALUES (?, ?, 'RESERVE', ?, ?, ?);",
                (tx_id, agent_id, item_id, v_item, v_item + 1)
            )

            conn.commit()
            with self._state_lock:
                self._accounts_cache[from_acc] = [row_from[0] - amount, row_from[1], float(v_from + 1)]
                self._accounts_cache[to_acc] = [row_to[0] + amount, row_to[1], float(v_to + 1)]
                self._inventory_cache[item_id] = [row_item[0] - item_count, row_item[1] + item_count, v_item + 1]
            return True

        except sqlite3.OperationalError:
            conn.rollback()
            return False

    def rollback(self):
        conn = self._get_connection()
        conn.rollback()

    def audit_system_balance(self) -> Tuple[float, int, int]:
        """
        Audits physical database state for invariant verification:
        Returns: (total_balance, total_stock, total_settlements)
        """
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        cur = conn.cursor()
        cur.execute("SELECT SUM(Balance) FROM Accounts;")
        total_bal = cur.fetchone()[0] or 0.0

        cur.execute("SELECT SUM(StockCount + ReservedCount) FROM Inventory;")
        total_stock = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM Settlements WHERE Status = 'COMMITTED';")
        total_settlements = cur.fetchone()[0] or 0
        conn.close()
        return (total_bal, total_stock, total_settlements)

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn is not None:
            self._local.conn.close()
            self._local.conn = None


class ConcurrentDatabaseEngine(EnterpriseDatabaseEngine):
    """
    Lightweight database engine wrapper supporting unit-test seeding and direct CRUD helpers
    used in test_epcp.py alongside EnterpriseDatabaseEngine.
    """

    def __init__(self, db_path: Optional[str] = None):
        unit_db = db_path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "epcp_unit_test.db")
        super().__init__(db_path=unit_db)

    def reset_and_seed(self, num_accounts: int = 10, num_items: int = 10):
        conn = self._get_connection()
        with conn:
            conn.execute("DELETE FROM Accounts;")
            conn.execute("DELETE FROM Inventory;")
            conn.execute("DELETE FROM Settlements;")
            conn.execute("DELETE FROM AuditLog;")
            for i in range(1, num_accounts + 1):
                acc_id = f"ACC_{i:03d}"
                conn.execute(
                    "INSERT INTO Accounts VALUES (?, ?, ?, ?, ?, ?);",
                    (acc_id, 10000.0, 2000.0, "GOLD", 1, "2026-01-01T00:00:00Z"),
                )
            for j in range(1, num_items + 1):
                item_id = f"ITEM_{j:03d}"
                conn.execute(
                    "INSERT INTO Inventory VALUES (?, ?, ?, ?, ?, ?);",
                    (item_id, 500, 0, 50.0, "ELECTRONICS", 1),
                )

    def execute_read_account(self, account_id: str) -> Optional[float]:
        cur = self._get_connection().cursor()
        cur.execute("SELECT Balance FROM Accounts WHERE AccountId = ?;", (account_id,))
        row = cur.fetchone()
        return float(row[0]) if row else None

    def execute_update_account(self, account_id: str, delta: float) -> bool:
        cur = self._get_connection().cursor()
        cur.execute(
            "UPDATE Accounts SET Balance = Balance + ?, AccountVersion = AccountVersion + 1 WHERE AccountId = ?;",
            (delta, account_id),
        )
        return cur.rowcount > 0

    def commit(self):
        self._get_connection().commit()
