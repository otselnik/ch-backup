from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.models import Database, Table
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.logic.table import TableBackup

UUID = "fa8ff291-1922-4b7f-afa7-06633d5e16ae"


@dataclass
class FakeStatResult:
    st_mtime_ns: int
    st_ctime_ns: int


@pytest.mark.parametrize(
    "fake_stats, backups_expected",
    [
        (
            [
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
            ],
            1,
        ),
        (
            [
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
                FakeStatResult(
                    st_mtime_ns=16890001958000111, st_ctime_ns=16890001958000000
                ),
            ],
            0,
        ),
        (
            [
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000000
                ),
                FakeStatResult(
                    st_mtime_ns=16890001958000000, st_ctime_ns=16890001958000111
                ),
            ],
            0,
        ),
    ],
)
def test_backup_table_skipping_if_metadata_updated_during_backup(
    fake_stats: List[FakeStatResult], backups_expected: int
) -> None:
    table_name = "table1"
    db_name = "db1"
    creation_statement = (
        f"ATTACH TABLE db1.table1 UUID '{UUID}' (date Date) ENGINE = MergeTree();"
    )

    # Prepare involved data objects
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    db = Database(
        db_name, "MergeTree", "/var/lib/clickhouse/metadata/db1.sql", None, None
    )
    table_backup = TableBackup()
    backup_meta = BackupMetadata(
        name="20181017T210300",
        version="1.0.100",
        ch_version="19.1.16",
        time_format="%Y-%m-%dT%H:%M:%S%Z",
        hostname="clickhouse01.test_net_711",
    )

    backup_meta.add_database(db)
    context.backup_meta = backup_meta

    # Mock external interactions
    clickhouse_ctl_mock = Mock()
    clickhouse_ctl_mock.get_tables.return_value = [
        Table(
            db_name,
            table_name,
            "MergeTree",
            [],
            [],
            "/var/lib/clickhouse/metadata/db1/table1.sql",
            "",
            UUID,
        ),
    ]
    clickhouse_ctl_mock.get_disks.return_value = {}
    context.ch_ctl = clickhouse_ctl_mock

    context.backup_layout = Mock()

    read_bytes_mock = Mock(return_value=creation_statement.encode())

    # Backup table
    with (
        patch("os.stat", side_effect=fake_stats),
        patch("ch_backup.logic.table.Path", read_bytes=read_bytes_mock),
    ):
        table_backup.backup(
            context,
            [db],
            {db_name: [table_name]},
            schema_only=False,
            multiprocessing_config=DEFAULT_CONFIG["multiprocessing"],  # type: ignore
        )

    assert len(context.backup_meta.get_tables(db_name)) == backups_expected
    # One call after each table and one after database is backuped
    assert clickhouse_ctl_mock.remove_freezed_data.call_count == 2


class TestValidateUploadedParts:
    """
    Tests for TableBackup._validate_uploaded_parts.
    """

    # pylint: disable=protected-access

    _BACKUP_NAME = "20181017T210300"

    def _make_part(self, name: str, link: Optional[str] = None) -> PartMetadata:
        return PartMetadata(
            database="db1",
            table="table1",
            name=name,
            checksum="abc123",
            size=1024,
            files=["data.bin"],
            tarball=True,
            link=link,
        )

    def _make_context(
        self, validate: bool, check_returns: bool
    ) -> tuple[BackupContext, MagicMock]:
        context = Mock(spec=BackupContext)
        context.config = {"validate_part_after_upload": validate}
        context.backup_meta = MagicMock()
        context.backup_meta.name = self._BACKUP_NAME
        check_data_part_mock = MagicMock(return_value=check_returns)
        layout_mock = MagicMock()
        layout_mock.check_data_part = check_data_part_mock
        context.backup_layout = layout_mock
        return context, check_data_part_mock

    def test_validate_disabled_skips_check(self):
        """When validate_part_after_upload is False, check_data_part is never called."""
        part = self._make_part("all_1_1_0")
        context, check_mock = self._make_context(validate=False, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_not_called()

    def test_validate_calls_check_with_backup_name(self):
        """check_data_part must receive the backup *name* (not a path)."""
        part = self._make_part("all_1_1_0")
        context, check_mock = self._make_context(validate=True, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_called_once_with(self._BACKUP_NAME, part)

    def test_validate_raises_on_broken_part(self):
        """RuntimeError is raised when check_data_part returns False."""
        part = self._make_part("all_1_1_0")
        context, _ = self._make_context(validate=True, check_returns=False)

        with pytest.raises(RuntimeError, match="all_1_1_0"):
            TableBackup._validate_uploaded_parts(context, [part])

    def test_validate_deduplicated_part_uses_backup_name(self):
        """
        For a deduplicated part (link set to a source backup name),
        _validate_uploaded_parts still passes the *current* backup name to
        check_data_part — the layout itself resolves the link internally.
        """
        source_backup = "20181010T120000"
        part = self._make_part("all_1_1_0", link=source_backup)
        context, check_mock = self._make_context(validate=True, check_returns=True)

        TableBackup._validate_uploaded_parts(context, [part])

        check_mock.assert_called_once_with(self._BACKUP_NAME, part)

    def test_validate_all_parts_checked_before_raising(self):
        """All invalid parts are collected before RuntimeError is raised."""
        parts = [self._make_part(f"all_{i}_1_0") for i in range(3)]
        context, check_mock = self._make_context(validate=True, check_returns=False)

        with pytest.raises(RuntimeError):
            TableBackup._validate_uploaded_parts(context, parts)

        assert check_mock.call_count == 3
