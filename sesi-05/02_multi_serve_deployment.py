"""
Sesi 5 - Latihan 2: Multi-Deployment Service dengan prefect.serve()
-------------------------------------------------------------------
Konsep Inti:
1. Satu Runner untuk Banyak Flow:
   Dalam operasional nyata, kita sering memiliki beberapa pipeline berbeda
   (misal: data audit harian dan healthcheck setiap menit) yang ingin kita jalankan
   dalam satu proses server worker yang sama tanpa perlu membuka banyak terminal.
2. Kombinasi Schedule:
   - Pipeline 1: Menggunakan 'interval' (setiap 60 detik) untuk health check sistem.
   - Pipeline 2: Menggunakan 'cron' ("0 1 * * *", jam 01:00 pagi) untuk audit keuangan.
3. Metode 'flow.to_deployment()':
   Menyiapkan spesifikasi deployment terlebih dahulu tanpa langsung menjalankannya,
   lalu dipaketkan bersama fungsi 'prefect.serve(dep1, dep2)'.
"""

from datetime import datetime
import random
from prefect import flow, task, serve, get_run_logger


# =====================================================================
# FLOW 1: SYSTEM HEALTHCHECK (Interval Schedule - Tiap 60 Detik)
# =====================================================================
@task(name="ping-database-service")
def ping_database_service() -> bool:
    logger = get_run_logger()
    logger.info("[PING] Memeriksa latensi koneksi PostgreSQL Data Warehouse...")
    latency_ms = random.randint(5, 25)
    logger.info(f"[PING] Database online - RTT Latensi: {latency_ms} ms.")
    return True


@task(name="check-disk-usage")
def check_disk_usage() -> float:
    logger = get_run_logger()
    logger.info("[DISK] Memeriksa kapasitas sisa penyimpanan Data Lake...")
    usage_percent = 42.5
    logger.info(f"[DISK] Data Lake Storage terisi: {usage_percent}% (Status Aman).")
    return usage_percent


@flow(
    name="system-healthcheck-flow",
    description="Flow ringan untuk memantau status kesehatan infrastruktur data setiap 60 detik."
)
def system_healthcheck_flow():
    logger = get_run_logger()
    logger.info(f"--- [PULSE] Menjalankan Healthcheck pada {datetime.now().strftime('%H:%M:%S')} ---")
    db_ok = ping_database_service()
    disk_usage = check_disk_usage()
    logger.info(f"--- [PULSE] Healthcheck Sukses (DB: {db_ok}, Disk: {disk_usage}%) ---")


# =====================================================================
# FLOW 2: NIGHTLY AUDIT RECONCILIATION (Cron Schedule - Tiap Jam 01:00 Pagi)
# =====================================================================
@task(name="fetch-ledger-discrepancies")
def fetch_ledger_discrepancies(audit_mode: str) -> int:
    logger = get_run_logger()
    logger.info(f"[AUDIT] Mencocokkan buku besar bank dengan internal ledger (Mode: {audit_mode})...")
    # Simulasi perhitungan selisih rekonsiliasi
    selisih = 0
    logger.info(f"[AUDIT] Rekonsiliasi selesai: {selisih} selisih transaksi ditemukan.")
    return selisih


@flow(
    name="nightly-financial-audit-flow",
    description="Flow rekonsiliasi keuangan malam hari terjadwal dengan cron UNIX."
)
def nightly_financial_audit_flow(audit_mode: str = "STRICT"):
    logger = get_run_logger()
    logger.info("==================================================")
    logger.info(f"Memulai Audit Rekonsiliasi Keuangan Malam Hari")
    logger.info(f"Waktu Eksekusi: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("==================================================")

    discrepancies = fetch_ledger_discrepancies(audit_mode=audit_mode)
    if discrepancies == 0:
        logger.info("[AUDIT RESULT] 100% Selaras (Clean Audit).")
    else:
        logger.warning(f"[AUDIT RESULT] Perhatian! Terdeteksi {discrepancies} anomali.")


# =====================================================================
# MENDAFTARKAN & MELAYANI KEDUA DEPLOYMENT BERSAMAAN
# =====================================================================
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Mempersiapkan Multi-Deployment Runner (Prefect 3.x)...")
    print("=" * 70)

    # 1. Konfigurasi Deployment 1: Healthcheck (Interval Schedule)
    healthcheck_dep = system_healthcheck_flow.to_deployment(
        name="infra-heartbeat-60s",
        interval=60,  # Berjalan setiap 60 detik
        tags=["monitoring", "infra", "sesi-05"],
        description="Deployment pemantauan berkala kesehatan database dan storage."
    )

    # 2. Konfigurasi Deployment 2: Audit Keuangan (Cron Schedule)
    # Cron format: Menit Jam Tanggal Bulan Hari-dalam-pekan
    # Contoh di bawah: "0 1 * * *" (Jam 01:00 pagi setiap hari)
    audit_dep = nightly_financial_audit_flow.to_deployment(
        name="nightly-reconciliation-cron",
        cron="0 1 * * *",
        tags=["audit", "finance", "sesi-05"],
        parameters={"audit_mode": "STRICT"},
        description="Deployment rekonsiliasi akuntansi otomatis setiap jam 01:00 pagi."
    )

    print("\nKedua deployment berhasil disiapkan:")
    print("  1. infra-heartbeat-60s       --> Interval: 60 detik")
    print("  2. nightly-reconciliation-cron --> Cron: '0 1 * * *' (01:00 AM setiap hari)")
    print("\nMenjalankan Prefect Serve Runner...")
    print("Tekan Ctrl + C untuk berhenti.\n")

    # Jalankan kedua deployment dalam satu proses runner!
    serve(healthcheck_dep, audit_dep)
