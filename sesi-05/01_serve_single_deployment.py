"""
Sesi 5 - Latihan 1: Single Deployment & Scheduling dengan flow.serve()
----------------------------------------------------------------------
Konsep Inti:
1. Dari Script Manual ke Background Service:
   Sebelumnya kita menjalankan flow dengan 'python script.py' (sekali jalan lalu berhenti).
   Dengan 'flow.serve()', Prefect mendaftarkan Deployment ke server dan menyalakan
   Runner lokal yang terus aktif (listening) untuk mengeksekusi run sesuai jadwal atau trigger manual.
2. Schedule:
   - interval : Menjalankan flow setiap N detik/menit (contoh: interval=60 atau timedelta(minutes=2)).
   - cron     : Standar waktu UNIX (contoh: "0 2 * * *" untuk tiap jam 02:00 pagi).
3. Parameterisasi UI:
   Parameter fungsi flow otomatis menjadi form input interaktif di Prefect UI.
"""

from datetime import datetime, timezone
import os
from prefect import flow, task, get_run_logger


@task(name="extract-sales-records")
def extract_sales_records(report_date: str) -> list[dict]:
    """Mengambil data transaksi penjualan untuk tanggal tertentu."""
    logger = get_run_logger()
    logger.info(f"[EXTRACT] Mengambil data penjualan untuk tanggal: {report_date}")

    # Simulasi data penjualan mentah
    raw_data = [
        {"trx_id": "TRX-501", "customer": "Budi Santoso", "nominal": 250000, "status": "SUCCESS"},
        {"trx_id": "TRX-502", "customer": "Siti Aminah", "nominal": 75000, "status": "SUCCESS"},
        {"trx_id": "TRX-503", "customer": "CV Maju Jaya", "nominal": 1200000, "status": "SUCCESS"},
        {"trx_id": "TRX-504", "customer": "Ahmad Dani", "nominal": 50000, "status": "FAILED"},
        {"trx_id": "TRX-505", "customer": "PT Berkah", "nominal": 4500000, "status": "SUCCESS"},
    ]
    logger.info(f"[EXTRACT] Berhasil mengunduh {len(raw_data)} record transaksi.")
    return raw_data


@task(name="filter-high-value-sales")
def filter_high_value_sales(records: list[dict], min_amount: float) -> list[dict]:
    """Menyaring transaksi sukses dengan nilai minimal tertentu."""
    logger = get_run_logger()
    logger.info(f"[TRANSFORM] Memfilter transaksi SUCCESS dengan nominal >= Rp {min_amount:,.0f}...")

    filtered = [
        r for r in records
        if r["status"] == "SUCCESS" and r["nominal"] >= min_amount
    ]

    total_omset = sum(r["nominal"] for r in filtered)
    logger.info(f"[TRANSFORM] Ditemukan {len(filtered)} transaksi prioritas. Total omset: Rp {total_omset:,.0f}")
    return filtered


@task(name="load-summary-report")
def load_summary_report(filtered_records: list[dict], report_date: str, dry_run: bool = False) -> str:
    """Menyimpan ringkasan laporan ke penyimpanan data lake lokal."""
    logger = get_run_logger()

    if dry_run:
        logger.warning("[LOAD] DRY RUN AKTIF - Data ringkasan tidak ditulis ke file disk.")
        return "DRY_RUN_SUCCESS"

    output_dir = "./data/reports"
    os.makedirs(output_dir, exist_ok=True)
    report_file = os.path.join(output_dir, f"sales_summary_{report_date.replace('-', '')}.txt")

    total_omset = sum(r["nominal"] for r in filtered_records)
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(f"=== LAPORAN PENJUALAN HARIAN ({report_date}) ===\n")
        f.write(f"Waktu Generate: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Jumlah Transaksi Prioritas: {len(filtered_records)}\n")
        f.write(f"Total Nilai Omset: Rp {total_omset:,.0f}\n\n")
        f.write("Detail Transaksi:\n")
        for r in filtered_records:
            f.write(f"- [{r['trx_id']}] {r['customer']} : Rp {r['nominal']:,.0f}\n")

    logger.info(f"[LOAD] Laporan berhasil disimpan ke: {report_file}")
    return report_file


@flow(
    name="daily-sales-pipeline",
    description="Pipeline ETL penjualan harian yang berjalan otomatis via Prefect Deployment."
)
def daily_sales_pipeline(
    report_date: str = "",
    min_amount: float = 100000.0,
    dry_run: bool = False
):
    """
    Main Flow: Orkestrasi penarikan data penjualan, filtering transaksi bernilai tinggi,
    dan penyimpanan laporan harian.

    Parameter di bawah otomatis muncul sebagai form input di Prefect UI:
    - report_date : Tanggal laporan (YYYY-MM-DD), default hari ini
    - min_amount  : Ambang batas minimal nominal transaksi (default 100,000)
    - dry_run     : Jika True, proses simulasi tanpa menulis ke disk
    """
    logger = get_run_logger()

    # Jika report_date tidak diisi, gunakan tanggal hari ini
    if not report_date:
        report_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("==================================================")
    logger.info(f"Memulai Daily Sales Pipeline | Tanggal: {report_date}")
    logger.info(f"Filter Nominal: Rp {min_amount:,.0f} | Mode Dry-Run: {dry_run}")
    logger.info("==================================================")

    # Eksekusi task pipeline
    raw_data = extract_sales_records(report_date=report_date)
    filtered = filter_high_value_sales(records=raw_data, min_amount=min_amount)
    status_file = load_summary_report(
        filtered_records=filtered,
        report_date=report_date,
        dry_run=dry_run
    )

    logger.info(f"Selesai! Status Output: {status_file}")


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("Mendaftarkan Deployment ke Prefect Server...")
    print("Tekan Ctrl + C di terminal ini kapan saja untuk menghentikan runner.")
    print("=" * 65 + "\n")

    # Mendaftarkan flow sebagai deployment yang terus standby di terminal ini
    # Jadwal diatur dengan interval 120 detik (2 menit) untuk kemudahan observasi langsung di UI.
    # Di dunia nyata untuk harian, Anda bisa gunakan cron="0 2 * * *" (tiap 02:00 pagi).
    daily_sales_pipeline.serve(
        name="sales-scheduled-2min",
        interval=120,  # Berjalan otomatis setiap 120 detik (2 menit)
        tags=["sales", "finance", "sesi-05", "automated"],
        description=(
            "### Deployment Penjualan Harian (Sesi 5)\n"
            "- **Jadwal**: Berjalan otomatis setiap 2 menit.\n"
            "- **Fitur**: Mendukung Quick Run & Custom Run parameter via UI.\n"
        ),
        parameters={
            "min_amount": 100000.0,
            "dry_run": False
        }
    )
