#!/usr/bin/env python3
import logging
from contextlib import contextmanager
from typing import Dict
from sqlalchemy import Engine

logger = logging.getLogger(__name__)


class DistributedTransactionManager:
    
    def __init__(self, engines: Dict[str, Engine]):
        self.engines = engines
        self.connections = {}
        self.transactions = {}
    
    @contextmanager
    def distributed_transaction(self):
        try:
            self._begin_all()
            
            logger.info("🔒 Distributed transaction started across all databases")
            
            yield self.connections
            
            self._commit_all()
            
            logger.info("✅ Distributed transaction committed successfully")
            
        except Exception as e:
            self._rollback_all()
            logger.error(f"❌ Distributed transaction failed, rolled back all: {e}")
            raise
        
        finally:
            self._close_all()
    
    def _begin_all(self):
        for db_name, engine in self.engines.items():
            try:
                conn = engine.connect()
                trans = conn.begin()
                self.connections[db_name] = conn
                self.transactions[db_name] = trans
                logger.debug(f"   → Started transaction on {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to start transaction on {db_name}: {e}")
                self._rollback_all()
                raise
    
    def _commit_all(self):
        failed_commits = []
        
        for db_name, trans in self.transactions.items():
            try:
                trans.commit()
                logger.debug(f"   → Committed {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to commit {db_name}: {e}")
                failed_commits.append((db_name, e))
        
        if failed_commits:
            logger.critical(f"🚨 CRITICAL: Partial commit failure!")
            logger.critical(f"   Failed commits: {failed_commits}")
            logger.critical(f"   Manual intervention may be required to restore consistency")
            raise Exception(f"Distributed transaction partially failed: {failed_commits}")
    
    def _rollback_all(self):
        for db_name, trans in self.transactions.items():
            try:
                trans.rollback()
                logger.debug(f"   → Rolled back {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to rollback {db_name}: {e}")
    
    def _close_all(self):
        for db_name, conn in self.connections.items():
            try:
                conn.close()
                logger.debug(f"   → Closed connection to {db_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to close connection to {db_name}: {e}")


@contextmanager
def atomic_multi_db_operation(engines: Dict[str, Engine]):
    manager = DistributedTransactionManager(engines)
    with manager.distributed_transaction() as connections:
        yield connections
