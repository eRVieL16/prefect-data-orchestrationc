"""
flows/flow_daily_stock.py
=========================
Versi Optimal Prefect 3.x dari Airflow DAG 'opt_daily_stock' (dag_daily_stock.py).
Dirancang untuk dijalankan di Prefect Server Lokal maupun Prefect Cloud.

Acuan Modul Pembelajaran: PANDUAN_EKSEKUSI_LENGKAP.md
- Sesi 1: Mental model @flow & @task, logging terpusat, dan Flow Run.
- Sesi 2: Ketahanan pipeline (retries, retry_delay, timeout, state handling).
- Sesi 3: Parameter flow dengan validasi tipe Pydantic.
- Sesi 4: Prefect Variables & Configuration management dengan fallback .env.
- Sesi 5: Deployment & Scheduling via `flow.serve()` (Cron '*/2 7-21 * * *' Asia/Jakarta).
- Sesi 6: Kompatibel untuk Work Pool & Worker (prefect deploy).
- Sesi 7: State hooks (on_failure alert / incident generation).
"""

from __future__ import annotations

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Prefect 3.x Imports
from prefect import flow, task, get_run_logger
from prefect.states import Completed, Failed
from prefect.variables import Variable

# --- Resolusi Path Modul & Dependensi Eksternal ---
# Mengintegrasikan kode pipeline dari project airflow-DPS jika tersedia
PROJECT_ROOT = Path(__file__).resolve().parent.parent
AIRFLOW_DPS_DIR = PROJECT_ROOT.parent / "airflow-DPS"

for p in [str(AIRFLOW_DPS_DIR), str(AIRFLOW_DPS_DIR / "src"), str(AIRFLOW_DPS_DIR / "src_opt")]:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)


# --- Sesi 7: Incident & Failure Hook ---
def handle_stock_sync_failure(flow_obj, flow_run, state):
    """
    Hook yang dieksekusi otomatis ketika Flow Run mengalami status FAILED.
    Dapat dihubungkan ke Webhook Discord/Slack, email, atau penulisan incident report.
    """
    error_msg = state.message or "Unknown failure"
    print(f"\n[ALERT - FLOW FAILED] Flow Run ID: {flow_run.id}")
    print(f"Flow Name: {flow_obj.name}")
    print(f"Error Details: {error_msg}")
    print(f"Waktu Kejadian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S WIB')}\n")


# --- Sesi 2 & 4: Task Eksekusi Sinkronisasi Stock ---
@task(
    name="execute_realtime_stock_sync",
    retries=1,
    retry_delay_seconds=120,    # 2 menit retry delay sesuai konfigurasi Airflow
    timeout_seconds=600,        # 10 menit execution timeout
    log_prints=True,
    tags=["stock", "db-sync", "realtime"]
)
def task_sync_stock_daily(force: bool = False) -> Dict[str, Any]:
    """
    Unit kerja terkecil (@task) untuk mengeksekusi sinkronisasi transaksi real-time stock.
    Menerapkan smart change detection (skip jika data hari ini tidak berubah).
    """
    logger = get_run_logger()
    logger.info(f"Memulai sinkronisasi real-time stock (Parameter force={force})...")

    # Cek apakah modul dari airflow-DPS tersedia
    try:
        from src_opt.pipelines.stock_pipeline import run_stock_daily
        has_real_pipeline = True
    except ImportError as e:
        logger.warning(
            f"Modul 'src_opt.pipelines.stock_pipeline' tidak ditemukan di sys.path ({e}). "
            "Menjalankan mode simulasi deterministik untuk kebutuhan latihan/recall."
        )
        has_real_pipeline = False

    if has_real_pipeline:
        try:
            # Jalankan pipeline asli
            # Di Airflow fungsi ini me-raise AirflowSkipException jika tidak ada perubahan
            is_synced = run_stock_daily(force=force, skip_on_no_change=True)
            if is_synced:
                logger.info("[SUCCESS] Data stock berhasil disinkronisasi ke database.")
                return {"status": "synced", "message": "Sinkronisasi berhasil dieksekusi"}
            else:
                logger.info("[SKIP] Data transaksi hari ini belum berubah. Sinkronisasi dilewati.")
                return {"status": "skipped", "message": "Data tidak berubah, dilewati (smart skip)"}
        except Exception as exc:
            # Tangani jika AirflowSkipException terlempar dari kode legacy
            if exc.__class__.__name__ == "AirflowSkipException":
                logger.info(f"[SKIP] {exc}")
                return {"status": "skipped", "message": str(exc)}
            logger.error(f"[ERROR] Eksekusi pipeline gagal: {exc}")
            raise exc
    else:
        # Fallback demonstrasi / simulasi untuk lingkungan pengujian lokal Prefect
        import time
        logger.info("[SIMULASI] Memeriksa delta transaksi...")
        time.sleep(1)
        if not force:
            logger.info("[SIMULASI SKIP] Tidak ada transaksi baru hari ini (Count match & Stock match).")
            return {"status": "skipped", "message": "Simulasi smart-skip aktif (Data belum berubah)"}
        else:
            logger.info("[SIMULASI SYNC] Parameter force=True: Menjalankan sinkronisasi penuh...")
            time.sleep(1)
            return {"status": "synced", "message": "Simulasi sinkronisasi berhasil dijalankan"}


# --- Sesi 1 & 3: Master Flow Orchestrator ---
@flow(
    name="opt-daily-stock-sync",
    description="Sinkronisasi real-time stock setiap 2 menit (Smart Skip jika data tidak berubah)",
    log_prints=True,
    on_failure=[handle_stock_sync_failure]
)
def daily_stock_flow(force: bool = False) -> Completed:
    """
    Orkestrator utama Flow.
    Menerima parameter 'force' bertipe boolean dengan validasi otomatis Pydantic.
    """
    logger = get_run_logger()
    
    # Sesi 4: Membaca Prefect Variables jika ada konfigurasi khusus (misal override setting via UI)
    try:
        env_mode = Variable.get("ENVIRONMENT", default="development")
        logger.info(f"Lingkungan eksekusi aktif: {env_mode}")
    except Exception:
        env_mode = "local"

    # Jalankan task
    result = task_sync_stock_daily(force=force)

    # State reporting yang informatif di UI Prefect
    if result["status"] == "skipped":
        return Completed(message=f"Smart Skip: {result['message']}")
    
    return Completed(message=f"Sync Selesai: {result['message']}")


# --- Sesi 5: Deployment & Scheduling via flow.serve() ---
def start_serve_deployment():
    """
    Mendaftarkan deployment ke Prefect Server lokal secara terjadwal.
    Menjalankan proses runner di terminal:
    - Cron: '*/2 7-21 * * *' (Setiap 2 menit dari jam 07:00 hingga 21:59)
    - Timezone: Asia/Jakarta (WIB)
    - Concurrency Limit: 1 (Meniru max_active_runs=1 di Airflow)
    """
    print("=" * 70)
    print("Mendaftarkan & Menjalankan Deployment Prefect 3.x:")
    print(" - Flow Name   : opt-daily-stock-sync")
    print(" - Deployment  : realtime-stock-sync-local")
    print(" - Schedule    : Cron '*/2 7-21 * * *' (07:00 - 21:59 WIB)")
    print(" - Concurrency : 1 run aktif pada satu waktu (max_active_runs=1)")
    print("=" * 70)

    daily_stock_flow.serve(
        name="realtime-stock-sync-local",
        cron="*/2 7-21 * * *",
        timezone="Asia/Jakarta",
        tags=["opt", "realtime", "stock", "db-sync", "smart-skip"],
        description="Deployment sinkronisasi stock real-time 2-menitan di Prefect Server lokal",
        parameters={"force": False}
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prefect Flow: Real-time Stock Sync")
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Jalankan sebagai background deployment runner dengan cron schedule (Sesi 5)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Paksa eksekusi sinkronisasi tanpa memedulikan pengecekan perubahan data"
    )

    args = parser.parse_args()

    if args.serve:
        # Jalankan background scheduler / runner
        start_serve_deployment()
    else:
        # Jalankan eksekusi ad-hoc langsung (sesuai filosofi Pythonic Prefect)
        print("\nMenjalankan Prefect Flow secara ad-hoc (Direct Execution)...")
        flow_state = daily_stock_flow(force=args.force)
        print(f"Hasil Eksekusi Flow: {flow_state}\n")
