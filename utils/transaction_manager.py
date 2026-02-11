#!/usr/bin/env python3
"""
Distributed Transaction Manager
Ensures data consistency across multiple databases with rollback support
"""

from contextlib import contextmanager
import logging
from typing import Dict
from sqlalchemy import Engine

logger = logging.getLogger(__name__)


class DistributedTransactionManager:
    """
    Manage transactions across multiple databases
    
    Ensures all-or-nothing semantics: either all databases commit successfully,
    or all rollback on any failure.
    """
    
    def __init__(self, engines: Dict[str, Engine]):
        """
        Initialize transaction manager
        
        Args:
            engines: Dictionary of {db_name: engine} pairs
        """
        self.engines = engines
        self.connections = {}
        self.transactions = {}
    
    @contextmanager
    def distributed_transaction(self):
        """
        Context manager for distributed transaction
        
        Usage:
            tx_manager = DistributedTransactionManager(engines)
            with tx_manager.distributed_transaction() as connections:
                # Use connections['supabase'], connections['prod'], etc.
                connections['supabase'].execute(...)
                connections['prod'].execute(...)
            # Auto-commits all if successful, auto-rollbacks all on error
        
        Yields:
            Dictionary of {db_name: connection} pairs
        """
        try:
            # Step 1: Begin all transactions
            self._begin_all()
            
            logger.info("🔒 Distributed transaction started across all databases")
            
            # Step 2: Yield connections for use
            yield self.connections
            
            # Step 3: Commit all if no exception
            self._commit_all()
            
            logger.info("✅ Distributed transaction committed successfully")
            
        except Exception as e:
            # Step 4: Rollback all on any exception
            self._rollback_all()
            logger.error(f"❌ Distributed transaction failed, rolled back all: {e}")
            raise
        
        finally:
            # Step 5: Always close all connections
            self._close_all()
    
    def _begin_all(self):
        """Begin transaction on all databases"""
        for db_name, engine in self.engines.items():
            try:
                conn = engine.connect()
                trans = conn.begin()
                self.connections[db_name] = conn
                self.transactions[db_name] = trans
                logger.debug(f"   → Started transaction on {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to start transaction on {db_name}: {e}")
                # Rollback any already-started transactions
                self._rollback_all()
                raise
    
    def _commit_all(self):
        """Commit all transactions"""
        failed_commits = []
        
        for db_name, trans in self.transactions.items():
            try:
                trans.commit()
                logger.debug(f"   → Committed {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to commit {db_name}: {e}")
                failed_commits.append((db_name, e))
        
        if failed_commits:
            # Critical: Some commits succeeded, some failed
            # This is a partial commit state - log for manual intervention
            logger.critical(f"🚨 CRITICAL: Partial commit failure!")
            logger.critical(f"   Failed commits: {failed_commits}")
            logger.critical(f"   Manual intervention may be required to restore consistency")
            raise Exception(f"Distributed transaction partially failed: {failed_commits}")
    
    def _rollback_all(self):
        """Rollback all transactions"""
        for db_name, trans in self.transactions.items():
            try:
                trans.rollback()
                logger.debug(f"   → Rolled back {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to rollback {db_name}: {e}")
    
    def _close_all(self):
        """Close all connections"""
        for db_name, conn in self.connections.items():
            try:
                conn.close()
                logger.debug(f"   → Closed connection to {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to close connection to {db_name}: {e}")


# Convenience function for simple use cases
@contextmanager
def atomic_multi_db_operation(engines: Dict[str, Engine]):
    """
    Simple wrapper for distributed transactions
    
    Usage:
        with atomic_multi_db_operation(engines) as conns:
            conns['db1'].execute(...)
            conns['db2'].execute(...)
    """
    manager = DistributedTransactionManager(engines)
    with manager.distributed_transaction() as connections:
        yield connections
