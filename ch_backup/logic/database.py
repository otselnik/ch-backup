"""
ClickHouse backup logic for databases.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Sequence

from kazoo.exceptions import NoNodeError
from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_not_exception_message,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential,
)

from ch_backup import logging
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.metadata_cleaner import MetadataCleaner
from ch_backup.clickhouse.models import Database
from ch_backup.clickhouse.schema import (
    embedded_schema_db_sql,
    rewrite_database_schema,
    to_attach_query,
    to_create_query,
)
from ch_backup.logic.backup_manager import BackupManager
from ch_backup.util import replace_macros
from ch_backup.zookeeper.zookeeper import ZookeeperCTL

REPLICATED_DB_ENGINE_RE = re.compile(
    r"""Replicated\('(?P<zk_path>[^']+)',\s*'(?P<shard>[^']+)',\s*'(?P<replica>[^']+)'\)"""
)
TIMEOUT_EXCEEDED_EXCEPTION_RE = r".*Timeout exceeded.*"


class SyncStatus(Enum):
    """Status of replicated database sync operation."""

    COMPLETED = "completed"
    STUCK = "stuck"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class DatabaseSyncConfig:
    """Sync-related configuration extracted from BackupContext."""

    timeout: int
    poll_interval: float
    stall_threshold: int
    max_retries: int
    max_backoff: int

    @classmethod
    def from_context(cls, context: BackupContext) -> "DatabaseSyncConfig":
        """Create DatabaseSyncConfig from BackupContext configuration."""
        cfg = context.ch_ctl_conf
        return cls(
            timeout=cfg["sync_database_replica_timeout"],
            poll_interval=cfg["sync_database_replica_poll_interval"],
            stall_threshold=cfg["sync_database_replica_stall_threshold"],
            max_retries=cfg["sync_database_replica_max_retries"],
            max_backoff=cfg["sync_database_replica_max_backoff"],
        )


@dataclass
class DatabaseZookeeperData:
    """
    Resolved ZooKeeper information for a replicated database and helpers
    to read its sync state.
    """

    zk_ctl: ZookeeperCTL = field(repr=False)
    zk_path: str = ""
    shard: str = ""
    replica: str = ""
    max_log_ptr: int = 0

    @property
    def _zk_client(self):
        return self.zk_ctl.zk_client

    @property
    def _zk_root_path(self) -> str:
        return self.zk_ctl.zk_root_path

    def _read_int_node(self, path: str) -> Optional[int]:
        """Read an integer value from ZooKeeper. Return None if node does not exist."""
        with self._zk_client as zk_client:
            try:
                data, _ = zk_client.get(path)
                return int(data.decode("utf-8").strip())
            except NoNodeError:
                return None

    def _full_path(self, suffix: str) -> str:
        return f"{self._zk_root_path}{self.zk_path}{suffix}"

    def get_log_ptr(self) -> Optional[int]:
        """Read log_ptr for this database replica."""
        replica_key = f"{self.shard}|{self.replica}"
        return self._read_int_node(self._full_path(f"/replicas/{replica_key}/log_ptr"))

    def get_max_log_ptr(self) -> Optional[int]:
        """Read global max_log_ptr for this replicated database."""
        return self._read_int_node(self._full_path("/max_log_ptr"))

    @staticmethod
    def parse_replicated_db_zk_info(
        db: Database, macros: dict[str, str]
    ) -> Optional[tuple[str, str, str]]:
        """
        Parse ZooKeeper path, shard, and replica from Replicated database engine_full.
        """
        if not db.engine_full:
            return None

        match = REPLICATED_DB_ENGINE_RE.search(db.engine_full)
        if not match:
            return None

        return (
            replace_macros(match.group("zk_path"), macros),
            replace_macros(match.group("shard"), macros),
            replace_macros(match.group("replica"), macros),
        )

    @classmethod
    def resolve(
        cls, context: BackupContext, db: Database
    ) -> Optional["DatabaseZookeeperData"]:
        """
        Resolve ZooKeeper parameters for a replicated database and load max_log_ptr.
        """
        db_name = db.name

        try:
            macros = context.ch_ctl.get_macros()
            parsed = cls.parse_replicated_db_zk_info(db, macros)
            if parsed is None:
                logging.warning(
                    f"Database {db_name}: cannot parse ZK path from "
                    f"engine_full={db.engine_full}."
                )
                return None

            zk_path, shard, replica = parsed
            logging.debug(
                f"Database {db_name}: ZK path={zk_path}, shard={shard}, replica={replica}."
            )

            zk_data = cls(
                zk_ctl=context.zk_ctl,
                zk_path=zk_path,
                shard=shard,
                replica=replica,
            )
            max_log_ptr = zk_data.get_max_log_ptr()
            if max_log_ptr is None:
                logging.warning(
                    f"Database {db_name}: max_log_ptr node not found in ZK at path {zk_path}."
                )
                return None

            logging.debug(f"Database {db_name}: max_log_ptr={max_log_ptr}")
            zk_data.max_log_ptr = max_log_ptr
            return zk_data
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning(f"Database {db_name}: failed to resolve ZK params: {exc}.")
            return None


class ReplicatedDatabaseSyncer:
    """Encapsulates synchronization logic for replicated databases."""

    def __init__(self, context: BackupContext) -> None:
        self.context = context
        self.config = DatabaseSyncConfig.from_context(context)

    def sync_databases(
        self,
        databases: Iterable[Database],
        keep_going: bool,
    ) -> dict[str, SyncStatus]:
        """
        Synchronize all replicated databases in the provided iterable.

        Returns a dict of database name -> SyncStatus.
        """
        if self.context.config["force_non_replicated"]:
            logging.info("Skipping synchronizing replicated database replicas.")
            return {}

        logging.info("Synchronizing replicated database replicas")

        deadline = time.time() + self.config.timeout
        results: dict[str, SyncStatus] = {}

        for db in databases:
            if not db.is_replicated_db_engine():
                continue

            logging.info(f"Synchronizing replicated database: {db.name}")
            status = self.sync_database(db, deadline)
            results[db.name] = status

            if status == SyncStatus.COMPLETED:
                logging.info(f"Database {db.name} sync completed successfully")
            else:
                message = (
                    f"Database {db.name} sync is stuck due to DDL errors. Will retry after table restore."
                    if status == SyncStatus.STUCK
                    else f"Database {db.name} sync timed out"
                )
                if keep_going:
                    logging.warning(message)
                else:
                    raise RuntimeError(message)

        return results

    def sync_database(self, db: Database, deadline: float) -> SyncStatus:
        """
        Synchronize a single replicated database.

        Uses ZooKeeper polling if database ZK parameters can be resolved;
        otherwise falls back to SYSTEM SYNC DATABASE REPLICA.
        """
        zk_data = DatabaseZookeeperData.resolve(self.context, db)
        if zk_data is None:
            return self._sync_with_system_command(db.name, deadline)

        return self._poll_zookeeper_until_synced(db.name, zk_data, deadline)

    def _poll_zookeeper_until_synced(
        self,
        db_name: str,
        zk_data: DatabaseZookeeperData,
        deadline: float,
    ) -> SyncStatus:
        """
        Poll ZooKeeper log_ptr until sync completes, stalls, or times out.
        """
        previous_log_ptr: Optional[int] = None
        stall_count = 0

        while self._seconds_left(deadline) > 0:
            if stall_count >= self.config.stall_threshold:
                logging.info(
                    f"Database {db_name}: log_ptr stalled at {previous_log_ptr} "
                    f"for {stall_count} checks, marking as STUCK"
                )
                has_errors = self._check_ddl_queue_for_errors(db_name)
                if not has_errors:
                    logging.debug(
                        f"Database {db_name}: no DDL errors found in queue, "
                        f"but log_ptr is stalled — marking as STUCK anyway"
                    )
                return SyncStatus.STUCK

            try:
                log_ptr = zk_data.get_log_ptr()
            except Exception as exc:  # pylint: disable=broad-except
                log_ptr = None
                logging.warning(
                    f"Database {db_name}: exception while reading log_ptr from ZK: {exc}. Will retry."
                )

            if log_ptr is None:
                stall_count += 1
            else:
                logging.debug(
                    f"Database {db_name}: log_ptr={log_ptr}, "
                    f"max_log_ptr={zk_data.max_log_ptr}"
                )

                if log_ptr >= zk_data.max_log_ptr:
                    logging.info(
                        f"Database {db_name}: replica log_ptr={log_ptr} reached "
                        f"max_log_ptr={zk_data.max_log_ptr}, sync complete"
                    )
                    return SyncStatus.COMPLETED

                if previous_log_ptr is not None and log_ptr <= previous_log_ptr:
                    stall_count += 1
                    logging.debug(
                        f"Database {db_name}: log_ptr={log_ptr} not advancing "
                        f"({stall_count}/{self.config.stall_threshold} stall checks), "
                        f"max_log_ptr={zk_data.max_log_ptr}"
                    )
                else:
                    stall_count = 0

            previous_log_ptr = log_ptr
            time.sleep(self.config.poll_interval)

        return SyncStatus.TIMEOUT

    def _sync_with_system_command(
        self,
        db_name: str,
        deadline: float,
    ) -> SyncStatus:
        """
        Synchronize database replica using SYSTEM SYNC DATABASE REPLICA with retries.
        """
        remaining = self._seconds_left(deadline)
        if remaining <= 0:
            logging.warning(
                f"Database {db_name}: deadline already exceeded before sync with retries"
            )
            return SyncStatus.TIMEOUT

        retry_decorator = retry(
            retry=(
                retry_if_exception_type(ClickhouseError)
                & retry_if_not_exception_message(match=TIMEOUT_EXCEEDED_EXCEPTION_RE)
            ),
            stop=(
                stop_after_attempt(self.config.max_retries)
                | stop_after_delay(max(remaining, 1))
            ),
            wait=wait_random_exponential(max=self.config.max_backoff),
            reraise=True,
        )

        @retry_decorator
        def execute_sync() -> None:
            current_time_left = max(self._seconds_left(deadline), 1)
            settings = {"receive_timeout": current_time_left}
            self.context.ch_ctl.system_sync_database_replica(
                db_name,
                timeout=int(current_time_left),
                settings=settings,
            )

        try:
            logging.info(
                f"Database {db_name}: running SYSTEM SYNC DATABASE REPLICA with retries "
                f"(timeout={remaining}s, max_retries={self.config.max_retries})"
            )
            execute_sync()
            logging.info(
                f"Database {db_name}: SYSTEM SYNC DATABASE REPLICA with retries completed"
            )
            return SyncStatus.COMPLETED
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning(
                f"Database {db_name}: SYSTEM SYNC DATABASE REPLICA with retries failed: {exc}"
            )
            return SyncStatus.TIMEOUT

    def _check_ddl_queue_for_errors(self, db_name: str) -> bool:
        """
        Query system.distributed_ddl_queue for entries with errors for the given database.
        """
        try:
            entries = self.context.ch_ctl.get_ddl_queue_unfinished_status(db_name)
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning(
                f"Database {db_name}: failed to query distributed_ddl_queue: {exc}"
            )
            return False

        error_entries = [
            entry for entry in entries if int(entry.get("exception_code", 0)) != 0
        ]

        for entry in error_entries:
            logging.warning(
                f"Stuck DDL entry in database {db_name}: "
                f"entry={entry.get('entry')}, "
                f"exception_code={entry.get('exception_code')}, "
                f"exception_text={str(entry.get('exception_text', ''))[:300]}"
            )

        return bool(error_entries)

    @staticmethod
    def _seconds_left(deadline: float) -> int:
        """Return non-negative integer number of seconds left until deadline."""
        return max(int(deadline - time.time()), 0)


class DatabaseBackup(BackupManager):
    """
    Database backup class
    """

    def backup(
        self, context: BackupContext, databases: Sequence[Database]
    ) -> list[Database]:
        """
        Backup database objects metadata.
        """
        db_with_create_statements = []
        db_with_embedded_metadata = []

        for db in databases:
            if db.has_embedded_metadata():
                db_with_embedded_metadata.append(db)
            else:
                db_with_create_statements.append(db)

        backed_up_databases = []
        if db_with_create_statements:
            backed_up_databases = (
                context.backup_layout.upload_database_create_statements(
                    context.backup_meta, db_with_create_statements
                )
            )

        context.backup_layout.wait()

        for db in backed_up_databases + db_with_embedded_metadata:
            context.backup_meta.add_database(db)

        context.backup_layout.upload_backup_metadata(context.backup_meta)

        return backed_up_databases + db_with_embedded_metadata

    @staticmethod
    def restore(
        context: BackupContext,
        databases: Dict[str, Database],
        keep_going: bool,
        metadata_cleaner: Optional[MetadataCleaner],
    ) -> List[Database]:
        """
        Restore database objects.
        """
        # pylint: disable=too-many-branches
        logging.debug("Retrieving list of databases")
        present_databases = {db.name: db for db in context.ch_ctl.get_databases()}

        databases_to_restore: Dict[str, Database] = {}
        for name, db in databases.items():
            if (
                name in present_databases
                and db.engine != present_databases[name].engine
            ):
                logging.debug(
                    f"Database engine mismatch({db.engine}!={present_databases[name].engine}), deleting"
                )
                context.ch_ctl.drop_database_if_exists(name)
                del present_databases[name]

            if name not in present_databases:
                databases_to_restore[name] = db
                continue

        if metadata_cleaner:
            replicated_databases = [
                database
                for database in databases_to_restore.values()
                if database.is_replicated_db_engine()
            ]
            metadata_cleaner.clean_database_metadata(replicated_databases)

        logging.debug("Downloading database create statements")
        create_statements = dict(
            context.backup_layout.get_database_create_statements(
                context.backup_meta, list(databases_to_restore.keys())
            )
        )

        logging.info("Restoring databases: {}", ", ".join(databases_to_restore.keys()))
        for db in databases_to_restore.values():
            if db.has_embedded_metadata():
                db_sql = embedded_schema_db_sql(db)
            else:
                db_sql = create_statements[db.name]
            try:
                if db.is_atomic() or db.has_embedded_metadata():
                    logging.debug(f"Going to restore database `{db.name}` using CREATE")
                    db_sql = to_create_query(db_sql)
                    db_sql = rewrite_database_schema(
                        db,
                        db_sql,
                        context.config["force_non_replicated"],
                        context.config["override_replica_name"],
                    )
                    logging.debug(f"Creating database `{db.name}`")
                    context.ch_ctl.restore_database(db_sql)
                else:
                    logging.debug(f"Going to restore database `{db.name}` using ATTACH")
                    db_sql = to_attach_query(db_sql)
                    context.backup_layout.write_database_metadata(db, db_sql)
                    logging.debug(f"Attaching database `{db.name}`")
                    context.ch_ctl.attach_database(db)
            except Exception as e:
                if keep_going:
                    logging.exception(
                        f"Restore of database {db.name} failed, skipping due to --keep-going flag. Reason {e}"
                    )
                else:
                    raise

        logging.info("All databases restored")
        return list(databases_to_restore.values())

    @staticmethod
    def wait_sync_replicated_databases(
        context: BackupContext,
        databases: Iterable[Database],
        keep_going: bool,
    ) -> dict[str, SyncStatus]:
        """
        Wait for replicated databases to sync.

        Returns a dict mapping database name to SyncStatus.
        Databases with STUCK status should be retried after table restore fixes broken tables.
        """
        syncer = ReplicatedDatabaseSyncer(context)
        return syncer.sync_databases(
            databases=databases,
            keep_going=keep_going,
        )
