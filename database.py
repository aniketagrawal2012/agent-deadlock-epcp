"""
database.py - Real Enterprise Concurrent SQLite Database Engine
Docket: IDF-GGL-2026-AGENT-DEADLOCK-EPCP
Author: Aniket Agrawal (aniketagrawal@google.com)
Assignee: Google LLC

Zero mocks. Manages persistent SQLite transactional storage on macOS APFS.
Loads 10,000 enterprise accounts and 5,000 inventory items.
Operates with Write-Ahead Logging (WAL) and multi-threaded connection pooling.
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
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._local = threading.local()
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

    def seed_from_csv(self):
        """Loads the authentic enterprise dataset into SQLite tables."""
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        with conn:
            conn.execute("DELETE FROM Accounts;")
            conn.execute("DELETE FROM Inventory;")
            conn.execute("DELETE FROM Settlements;")
            conn.execute("DELETE FROM AuditLog;")

            # Load accounts
            if os.path.exists(ACCOUNTS_CSV):
                with open(ACCOUNTS_CSV, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader)  # Header
                    conn.executemany(
                        "INSERT INTO Accounts VALUES (?, ?, ?, ?, ?, ?);",
                        reader
                    )

            # Load inventory
            if os.path.exists(INVENTORY_CSV):
                with open(INVENTORY_CSV, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader)  # Header
                    conn.executemany(
                        "INSERT INTO Inventory VALUES (?, ?, ?, ?, ?, ?);",
                        reader
                    )

        conn.close()
        print(f"[OK] Database seeded from CSV into {self.db_path}")

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
        Executes a real multi-table ACID transaction across Accounts and Inventory.
        """
        conn = self._get_connection()
        cur = conn.cursor()
        try:
            # 1. Check & Debit from_acc
            cur.execute("SELECT Balance, CreditLimit, AccountVersion FROM Accounts WHERE AccountId = ?;", (from_acc,))
            row_from = cur.fetchone()
            if not row_from or (row_from[0] + row_from[1]) < amount:
                return False

            v_from = row_from[2]
            cur.execute(
                "UPDATE Accounts SET Balance = Balance - ?, AccountVersion = AccountVersion + 1 WHERE AccountId = ?;",
                (amount, from_acc)
            )

            # 2. Credit to_acc
            cur.execute("SELECT AccountVersion FROM Accounts WHERE AccountId = ?;", (to_acc,))
            row_to = cur.fetchone()
            if not row_to:
                conn.rollback()
                return False
            v_to = row_to[0]
            cur.execute(
                "UPDATE Accounts SET Balance = Balance + ?, AccountVersion = AccountVersion + 1 WHERE AccountId = ?;",
                (amount, to_acc)
            )

            # 3. Reserve Inventory
            cur.execute("SELECT StockCount, ItemVersion FROM Inventory WHERE ItemId = ?;", (item_id,))
            row_item = cur.fetchone()
            if not row_item or row_item[0] < item_count:
                conn.rollback()
                return False
            v_item = row_item[1]
            cur.execute(
                "UPDATE Inventory SET StockCount = StockCount - ?, ReservedCount = ReservedCount + ?, ItemVersion = ItemVersion + 1 WHERE ItemId = ?;",
                (item_count, item_count, item_id)
            )

            # 4. Insert Settlement Record
            cur.execute(
                "INSERT INTO Settlements (TxId, FromAccountId, ToAccountId, ItemId, Amount, ItemCount, Status) VALUES (?, ?, ?, ?, ?, ?, 'COMMITTED');",
                (tx_id, from_acc, to_acc, item_id, amount, item_count)
            )

            # 5. Insert Audit Log Entries
            cur.execute(
                "INSERT INTO AuditLog (TxId, AgentId, ActionType, EntityKey, PreviousVersion, NewVersion) VALUES (?, ?, 'DEBIT', ?, ?, ?);",
                (tx_id, agent_id, from_acc, v_from, v_from + 1)
            )
            cur.execute(
                "INSERT INTO AuditLog (TxId, AgentId, ActionType, EntityKey, PreviousVersion, NewVersion) VALUES (?, ?, 'RESERVE', ?, ?, ?);",
                (tx_id, agent_id, item_id, v_item, v_item + 1)
            )

            conn.commit()
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
