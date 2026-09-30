"""Lock contention, error handling, and platform filesystem behavior."""

import errno
import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from feed import _filesystem as fs


@pytest.fixture
def windows_locks(monkeypatch):
    backend = SimpleNamespace(LK_NBLCK=2, locking=Mock())
    monkeypatch.setattr(fs, "msvcrt", backend)
    monkeypatch.setattr(fs, "fcntl", None)
    return backend


def test_windows_lock_retries_contention_without_ten_attempt_limit(
    tmp_path, monkeypatch, windows_locks
):
    windows_locks.locking.side_effect = [
        OSError(errno.EACCES, "locked") for _ in range(12)
    ] + [None]
    monkeypatch.setattr(fs.time, "sleep", Mock())
    fd = os.open(tmp_path / "lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.lseek(fd, 5, os.SEEK_SET)
        fs.lock(fd)
        assert os.lseek(fd, 0, os.SEEK_CUR) == 0
        assert windows_locks.locking.call_count == 13
        windows_locks.locking.assert_called_with(fd, windows_locks.LK_NBLCK, 1)
    finally:
        os.close(fd)


def test_windows_nonblocking_contention_is_reported(tmp_path, windows_locks):
    windows_locks.locking.side_effect = OSError(errno.EACCES, "locked")
    fd = os.open(tmp_path / "lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        with pytest.raises(BlockingIOError):
            fs.lock(fd, blocking=False)
        assert windows_locks.locking.call_count == 1
    finally:
        os.close(fd)


@pytest.mark.parametrize("blocking", [True, False])
def test_windows_lock_propagates_real_errors(tmp_path, windows_locks, blocking):
    windows_locks.locking.side_effect = OSError(errno.EBADF, "invalid descriptor")
    fd = os.open(tmp_path / "lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        with pytest.raises(OSError) as error:
            fs.lock(fd, blocking=blocking)
        assert error.value.errno == errno.EBADF
        assert windows_locks.locking.call_count == 1
    finally:
        os.close(fd)


def test_windows_directory_sync_does_not_open_directory(tmp_path, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(fs, "msvcrt", object())
        patch.setattr(fs.os, "open", Mock(side_effect=AssertionError("directory open")))
        fs.sync_directory(tmp_path)


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory sync")
def test_directory_sync_propagates_io_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(fs.os, "fsync", Mock(side_effect=OSError(errno.EIO, "sync")))
    with pytest.raises(OSError) as error:
        fs.sync_directory(tmp_path)
    assert error.value.errno == errno.EIO
