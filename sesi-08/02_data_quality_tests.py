"""
Sesi 8 - Latihan 2: Data Quality Gatekeeping (dbt-Style Assertion Tests)
-----------------------------------------------------------------------
Konsep Inti:
1. Silent Data Corruption (Bahaya Terbesar Data Engineer):
   Jika ada data rusak (misal nominal transaksi negatif atau ID duplikat) yang lolos
   ke tabel Mart, laporan eksekutif perusahaan akan salah dan merugikan bisnis!
2. Data Quality Gatekeeping (dbt Testing Pattern):
   Sebelum mempublikasikan data ke tabel produksi ('prod_mart_daily_revenue'),
   kita wajib menjalankan serangkaian Assertion Test (Unique, Not Null, Value Range).
3. Circuit Breaker saat Quality Breach:
   Jika 1 saja test gagal -> Hentikan pipeline seketika!
   Data produksi tetap bersih, dan kirim alert insiden ke Discord / Slack!
"""

from datetime import datetime, timezone
import json
import os
import urllib.request
from dotenv import load_dotenv
import duckdb
from prefect import flow, task, get_run_logger

# Muat environment variable (.env) untuk Discord Webhook
load_dotenv()

DB_PATH = "./data/warehouse/analytics.duckdb"


class DataQualityBreachException(Exception):
    """Exception khusus yang dilempar ketika data melanggar aturan integritas."""
    pass


# =====================================================================
# ON_FAILURE HOOK UNTUK DATA QUALITY BREACH
# =====================================================================
def alert_data_quality_failure(flow, flow_run, state):
    """Mengirim alert Discord instan saat terjadi kegagalan integritas data."""
    ui_url = f"http://127.0.0.1:4200/runs/flow-run/{flow_run.id}"
    error_msg = str(state.message)

    print("\n" + "!" * 75)
    print("🛡️ [DATA QUALITY BREACH CAUGHT - PRODUCTION PROTECTED] 🛡️")
    print(f"Flow Name    : {flow.name}")
    print(f"Penyebab     : {error_msg}")
    print(f"Dashboard UI : {ui_url}")
    print("Tindakan     : Publikasi data ke Production Mart DIBATALKAN!")
    print("!" * 75 + "\n")

    discord_url = os.getenv("DISCORD_WEBHOOK_URL")
    if discord_url:
        payload = {
            "content": "⚠️ **[DATA QUALITY BREACH ALERT]**",
            "embeds": [
                {
                    "title": "Data Quality Assertion Failed!",
                    "description": error_msg,
                    "color": 16753920,  # Oranye Peringatan
                    "fields": [
                        {"name": "Flow Run", "value": f"`{flow_run.name}`", "inline": True},
                        {"name": "Status Warehouse", "value": "Tabel Produksi Tetap Aman (Rollback/Aborted)", "inline": False},
                        {"name": "Investigasi Log", "value": f"[Buka di Prefect UI]({ui_url})", "inline": False}
                    ],
                    "footer": {"text": "Prefect + DuckDB Data Quality Sentinel"}
                }
            ]
        }
        try:
            req = urllib.request.Request(
                discord_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"🚀 [DISCORD ALERT SENT] HTTP Status: {resp.status} - Notifikasi integritas terkirim!")
        except Exception as e:
            print(f"[DISCORD ERROR] {e}")


# =====================================================================
# TASK 1: INGEST DATA DENGAN OPSI ANOMALI
# =====================================================================
@task(name="ingest-orders-with-anomaly-flag")
def ingest_orders(db_path: str, inject_bad_data: bool = False) -> int:
    """Memasukkan data transaksi ke tabel staging dengan opsi injeksi data anomali."""
    logger = get_run_logger()
    logger.info(f"[INGEST] Memuat data transaksi (Mode Injeksi Anomali: {inject_bad_data})...")

    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS staging_raw_orders (order_id VARCHAR, customer_id VARCHAR, amount DOUBLE, status VARCHAR);")
    con.execute("TRUNCATE TABLE staging_raw_orders;")

    # Data bersih normal
    rows = [
        ("ORD-101", "CUST-10", 150000.0, "SUCCESS"),
        ("ORD-102", "CUST-11", 275000.0, "SUCCESS"),
        ("ORD-103", "CUST-12", 450000.0, "SUCCESS"),
        ("ORD-104", "CUST-13", 80000.0, "SUCCESS"),
    ]

    if inject_bad_data:
        logger.warning("[WARNING] Menginjeksikan baris anomali (Data Rusak) untuk menguji gatekeeper...")
        # Anomali 1: Order ID duplikat ("ORD-101")
        # Anomali 2: Nominal negatif (-500,000)
        # Anomali 3: Customer ID NULL
        rows.append(("ORD-101", "CUST-99", -500000.0, "SUCCESS"))
        rows.append(("ORD-105", None, 320000.0, "SUCCESS"))

    con.executemany("INSERT INTO staging_raw_orders VALUES (?, ?, ?, ?)", rows)
    total = con.execute("SELECT COUNT(*) FROM staging_raw_orders").fetchone()[0]
    con.close()

    logger.info(f"[INGEST] Total {total} baris dimasukkan ke staging.")
    return total


# =====================================================================
# TASK 2: DATA QUALITY ASSERTION TESTS (dbt PATTERN)
# =====================================================================
@task(name="run-data-quality-tests")
def run_data_quality_tests(db_path: str) -> bool:
    """
    Menjalankan serangkaian uji kualitas data (SQL Assertion Tests).
    Jika ditemukan pelanggaran, melempar DataQualityBreachException!
    """
    logger = get_run_logger()
    logger.info("[QUALITY GATE] Memulai audit kualitas data (dbt Testing Assertions)...")

    con = duckdb.connect(db_path)
    violations = []

    # Test 1: UNIQUE KEY ASSERTION (order_id tidak boleh duplikat)
    dup_query = """
        SELECT order_id, COUNT(*) AS cnt
        FROM staging_raw_orders
        GROUP BY order_id
        HAVING COUNT(*) > 1;
    """
    dup_results = con.execute(dup_query).fetchall()
    if dup_results:
        violations.append(f"UNIQUE VIOLATION: Ditemukan {len(dup_results)} order_id duplikat: {dup_results}")

    # Test 2: NOT NULL ASSERTION (customer_id tidak boleh kosong)
    null_query = "SELECT COUNT(*) FROM staging_raw_orders WHERE customer_id IS NULL;"
    null_count = con.execute(null_query).fetchone()[0]
    if null_count > 0:
        violations.append(f"NOT_NULL VIOLATION: Ditemukan {null_count} baris dengan customer_id NULL!")

    # Test 3: VALUE THRESHOLD ASSERTION (nominal tidak boleh <= 0)
    negative_query = "SELECT COUNT(*) FROM staging_raw_orders WHERE amount <= 0;"
    negative_count = con.execute(negative_query).fetchone()[0]
    if negative_count > 0:
        violations.append(f"THRESHOLD VIOLATION: Ditemukan {negative_count} transaksi dengan nominal negatif/nol!")

    con.close()

    if violations:
        error_detail = " | ".join(violations)
        logger.error(f"[GATEKEEPER BLOCKED] {error_detail}")
        raise DataQualityBreachException(f"Kegagalan Audit Data Quality: {error_detail}")

    logger.info("[QUALITY GATE PASSED] Seluruh assertion test lolos dengan sempurna (100% Valid)!")
    return True


# =====================================================================
# TASK 3: PROMOTE KE PRODUCTION MART (HANYA JIKA TEST LOLOS)
# =====================================================================
@task(name="promote-to-production-mart")
def promote_to_production_mart(db_path: str):
    """
    Task ini HANYA akan dieksekusi jika test kualitas di atas berhasil.
    Mencegah data tercemar merusak dashboard bisnis produksi.
    """
    logger = get_run_logger()
    logger.info("[PRODUCTION PROMOTION] Mempromosikan data yang telah teruji ke 'prod_mart_orders'...")

    con = duckdb.connect(db_path)
    con.execute("""
        CREATE OR REPLACE TABLE prod_mart_orders AS
        SELECT * FROM staging_raw_orders;
    """)
    count = con.execute("SELECT COUNT(*) FROM prod_mart_orders;").fetchone()[0]
    con.close()

    logger.info(f"[SUCCESS] {count} baris data tersertifikasi bersih kini tersedia di 'prod_mart_orders'.")
    return count


# =====================================================================
# MAIN FLOW
# =====================================================================
@flow(
    name="data-quality-gatekeeper-flow",
    on_failure=[alert_data_quality_failure],
    description="Pipeline data dengan pengujian integritas ketat (dbt testing pattern)."
)
def data_quality_pipeline(inject_bad_data: bool = False):
    logger = get_run_logger()
    logger.info("==================================================")
    logger.info("Memulai Data Quality Gatekeeper Pipeline")
    logger.info(f"Mode Injeksi Data Rusak: {inject_bad_data}")
    logger.info("==================================================")

    # 1. Ingest
    ingest_orders(db_path=DB_PATH, inject_bad_data=inject_bad_data)

    # 2. Quality Gatekeeper (Jika gagal, flow berhenti di sini!)
    run_data_quality_tests(db_path=DB_PATH)

    # 3. Publish ke Produksi (Hanya berjalan jika lolos!)
    promote_to_production_mart(db_path=DB_PATH)

    logger.info("Pipeline Selesai! Data Produksi Berhasil Diperbarui.")


if __name__ == "__main__":
    import sys

    # Cek apakah user memberikan argumen CLI '--bad-data'
    bad_data = "--bad-data" in sys.argv

    print("\n" + "=" * 65)
    print(f"MENJALANKAN PIPELINE DENGAN MODE: {'INJEKSI DATA RUSAK' if bad_data else 'DATA BERSIH'}")
    print("=" * 65)

    try:
        data_quality_pipeline(inject_bad_data=bad_data)
    except Exception:
        print("\n[INFO] Pipeline berhasil dihentikan oleh Quality Gatekeeper!")
        print("Tabel produksi tetap terlindungi dari data korup.")
