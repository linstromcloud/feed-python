"""Native process locks and directory syncing for local Feed storage."""

import errno
import os
import time

if os.name == "nt":
    import msvcrt

    fcntl = None
else:
    import fcntl

    msvcrt = None


def lock(fd, *, blocking=True):
    """Hold an exclusive lock until the descriptor is closed.

    Windows locks byte zero, including on empty files. Retry contention ourselves
    because LK_LOCK stops waiting after ten attempts; credential refreshes can
    hold the lock longer. Each caller owns a separate descriptor.
    """
    if msvcrt is None:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        fcntl.flock(fd, flags)
        return
    os.lseek(fd, 0, os.SEEK_SET)
    while True:
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return
        except OSError as exc:
            if exc.errno != errno.EACCES:
                raise
            if not blocking:
                raise BlockingIOError(errno.EAGAIN, "Feed storage is locked") from exc
            time.sleep(0.05)


def sync_directory(path):
    """Sync directory entries on POSIX; Windows only supports file fsync here.

    Windows writes still flush file contents before replacement. They do not
    provide a directory durability guarantee across power loss.
    """
    if msvcrt is not None:
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
