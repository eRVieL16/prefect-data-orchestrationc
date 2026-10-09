"""
prefect-tutorial/sesi-06/02_daily_stock_worker.py
=================================================
Implementasi Sesi 6: Arsitektur Work Pool & Worker
Studi Kasus Nyata: Deployment 'daily_stock_flow' ke Work Pool untuk Persiapan Cloud / Multi-Server.

Alur Kerja:
1. Buat Work Pool: `prefect work-pool create local-process-pool --type process`
2. Jalankan skrip ini untuk mendaftarkan deployment ke pool tersebut via API Python.
3. Jalankan Worker: `prefect worker start --pool local-process-pool`
"""
import sys
from pathlib import Path
from prefect.client.schemas.schedules import CronSchedule

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from flows.flow_daily_stock import daily_stock_flow

def deploy_to_work_pool():
    print("Mendaftarkan deployment ke Work Pool 'local-process-pool'...")
    deployment_id = daily_stock_flow.to_deployment(
        name="daily-stock-worker-deployment",
        work_pool_name="local-process-pool",
        schedule=CronSchedule(cron="*/2 7-21 * * *", timezone="Asia/Jakarta"),
        tags=["sesi-06", "worker-based", "cloud-ready", "stock"],
        parameters={"force": False},
        description="Deployment stock pipeline berbasis worker pool (arsitektur data plane & control plane)."
    ).apply()

    print(f"Deployment berhasil didaftarkan! ID: {deployment_id}")
    print("\nUntuk mengeksekusi antrean run ini, jalankan worker di terminal terpisah:")
    print("  prefect worker start --pool local-process-pool\n")

if __name__ == "__main__":
    deploy_to_work_pool()
