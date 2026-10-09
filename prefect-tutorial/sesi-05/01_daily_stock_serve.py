"""
prefect-tutorial/sesi-05/01_daily_stock_serve.py
================================================
Implementasi Sesi 5: Deployment & Scheduling via flow.serve()
Studi Kasus Nyata: Migrasi Airflow 'dag_daily_stock' ke Prefect 3.x
"""
import sys
from pathlib import Path

# Impor flow dari direktori flows
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from flows.flow_daily_stock import daily_stock_flow

if __name__ == "__main__":
    print("Mendaftarkan dan menjalankan listener deployment (Sesi 5)...")
    daily_stock_flow.serve(
        name="daily-stock-cron-serve",
        cron="*/2 7-21 * * *",
        timezone="Asia/Jakarta",
        tags=["sesi-05", "stock", "realtime", "production"],
        description="Deployment sinkronisasi stock setiap 2 menit (07:00-21:59 WIB)",
        parameters={"force": False}
    )
