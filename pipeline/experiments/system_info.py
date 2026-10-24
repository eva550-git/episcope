"""Small, dependency-free helpers for recording benchmark host details."""

from __future__ import annotations

import ctypes
import os
import platform
import shutil
from pathlib import Path
from typing import Any

from pipeline.common import ROOT


def installed_memory_bytes() -> int | None:
    """Return installed physical memory when the operating system exposes it."""
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.dwLength = ctypes.sizeof(MemoryStatus)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return int(status.ullTotalPhys)
        return None

    try:
        return int(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, OSError, ValueError):
        return None


def collect_system_info(root: Path = ROOT) -> dict[str, Any]:
    """Collect stable, non-sensitive host information for experiment records."""
    memory_bytes = installed_memory_bytes()
    return {
        "os": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "installed_memory_bytes": memory_bytes,
        "free_disk_bytes": shutil.disk_usage(root).free,
    }
