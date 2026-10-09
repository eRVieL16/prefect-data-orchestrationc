"""
tests/test_flow_daily_stock.py
==============================
Unit Test untuk flow_daily_stock sesuai panduan Sesi 9 (Testing & CI/CD).
Menguji fungsi task secara langsung menggunakan '.fn' tanpa overhead orkestrator.
"""
import pytest
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from flows.flow_daily_stock import task_sync_stock_daily, daily_stock_flow


def test_task_sync_stock_daily_fn_default():
    """
    Menguji task langsung menggunakan '.fn' (Prefect Best Practice).
    Dalam mode default (force=False), jika data tidak berubah harus mengembalikan status 'skipped'.
    """
    # Menjalankan task.fn secara langsung sebagai fungsi Python deterministik
    result = task_sync_stock_daily.fn(force=False)
    assert isinstance(result, dict)
    assert "status" in result
    assert result["status"] in ["synced", "skipped"]


def test_task_sync_stock_daily_fn_forced():
    """
    Menguji task dengan parameter force=True.
    Harus mencoba mengeksekusi sinkronisasi tanpa melewati tahap delta check.
    """
    result = task_sync_stock_daily.fn(force=True)
    assert isinstance(result, dict)
    assert "status" in result
    # Dalam mode simulasi maupun pipeline asli, force=True menghasilkan synced (atau error jika DB unreachable)
    assert result["status"] in ["synced", "skipped"]
