"""
Sesi 8 - Latihan 1: Modern ELT Pipeline dengan DuckDB & Prefect
----------------------------------------------------------------
Konsep Inti:
1. Pergeseran ETL ke ELT:
   - ETL Tradisional: Data di-extract, di-transform di memory Python, lalu di-load.
   - Modern ELT: Data mentah di-load langsung ke Data Warehouse (DuckDB),
     lalu transformasi berat dilakukan secara in-database menggunakan kekuatan SQL.
2. Layering Arsitektur Data Warehouse (Medallion / dbt pattern):
   - Raw Layer     : 'raw_orders' (data mentah asli dari sumber eksternal).
   - Staging Layer : 'stg_orders' (standardisasi tipe data, format string, filtering test data).
   - Mart Layer    : 'mart_daily_revenue' (tabel bisnis teragregasi untuk dashboard BI).
"""

import os
from datetime import datetime, timedelta
import random
import duckdb
from prefect import flow, task, get_run_logger


DB_PATH = "./data/warehouse/analytics.duckdb"


# =====================================================================
# TASK 1: EXTRACT & LOAD (EL) KE RAW LAYER
# =====================================================================
@task(name="extract-load-raw-orders")
def extract_and_load_raw_orders(db_path: str, record_count: int = 100) -> int:
    """Mengambil data transaksi mentah dan memuatnya langsung ke Raw Table DuckDB."""
    logger = get_run_logger()
    logger.info(f"[EL LAYER] Menyiapkan raw database di: {db_path}...")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)

    # Buat tabel raw_orders
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            category VARCHAR,
            amount DOUBLE,
            status VARCHAR,
            created_at TIMESTAMP
        );
    """)

    # Hapus data sebelumnya agar run ini idempoten
    con.execute("TRUNCATE TABLE raw_orders;")

    # Simulasi data transaksi mentah
    categories = ["ELECTRONICS", "FASHION", "GROCERY", "HOME_LIVING"]
    statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "PENDING", "CANCELLED"]
    base_time = datetime.now() - timedelta(days=2)

    rows = []
    for i in range(1, record_count + 1):
        order_id = f"ORD-{20260000 + i}"
        customer_id = f"CUST-{random.randint(100, 150)}"
        category = random.choice(categories)
        amount = round(random.uniform(50000, 2500000), 2)
        status = random.choice(statuses)
        created_at = base_time + timedelta(hours=random.randint(1, 48))
        rows.append((order_id, customer_id, category, amount, status, created_at))

    # Bulk insert berkecepatan tinggi ke DuckDB
    con.executemany("INSERT INTO raw_orders VALUES (?, ?, ?, ?, ?, ?)", rows)
    total_raw = con.execute("SELECT COUNT(*) FROM raw_orders;").fetchone()[0]

    con.close()
    logger.info(f"[EL LAYER] Sukses memuat {total_raw} baris ke tabel 'raw_orders'.")
    return total_raw


# =====================================================================
# TASK 2: TRANSFORMASI - STAGING LAYER (SQL)
# =====================================================================
@task(name="transform-staging-orders")
def transform_staging_layer(db_path: str) -> int:
    """Melakukan pembersihan data, casting tipe, dan filtering ke tabel stg_orders."""
    logger = get_run_logger()
    logger.info("[TRANSFORM LAYER] Membangun model 'stg_orders' via SQL in-database...")

    con = duckdb.connect(db_path)

    # Transformasi SQL: Hapus status CANCELLED, rapikan lowercase, dan ekstraksi date
    con.execute("""
        CREATE OR REPLACE TABLE stg_orders AS
        SELECT
            order_id,
            customer_id,
            UPPER(category) AS product_category,
            amount AS order_amount_idr,
            status AS order_status,
            created_at::DATE AS order_date,
            created_at AS order_timestamp
        FROM raw_orders
        WHERE status != 'CANCELLED';
    """)

    stg_count = con.execute("SELECT COUNT(*) FROM stg_orders;").fetchone()[0]
    con.close()

    logger.info(f"[TRANSFORM LAYER] Staging selesai: {stg_count} order valid disimpan ke 'stg_orders'.")
    return stg_count


# =====================================================================
# TASK 3: TRANSFORMASI - ANALYTICS MART LAYER (SQL)
# =====================================================================
@task(name="build-daily-revenue-mart")
def build_daily_revenue_mart(db_path: str) -> int:
    """Membangun tabel Mart agregasi bisnis (Daily Revenue & AOV) untuk BI Dashboard."""
    logger = get_run_logger()
    logger.info("[MART LAYER] Menghitung agregasi mart_daily_revenue...")

    con = duckdb.connect(db_path)

    # Agregasi bisnis: Total omset, jumlah transaksi, AOV, dan jumlah unik pelanggan per hari & kategori
    con.execute("""
        CREATE OR REPLACE TABLE mart_daily_revenue AS
        SELECT
            order_date,
            product_category,
            COUNT(order_id) AS total_successful_orders,
            COUNT(DISTINCT customer_id) AS unique_active_customers,
            ROUND(SUM(order_amount_idr), 2) AS total_revenue_idr,
            ROUND(AVG(order_amount_idr), 2) AS average_order_value_idr
        FROM stg_orders
        WHERE order_status = 'SUCCESS'
        GROUP BY order_date, product_category
        ORDER BY order_date DESC, total_revenue_idr DESC;
    """)

    mart_count = con.execute("SELECT COUNT(*) FROM mart_daily_revenue;").fetchone()[0]
    con.close()

    logger.info(f"[MART LAYER] Mart bisnis selesai: {mart_count} baris agregasi dibuat.")
    return mart_count


# =====================================================================
# TASK 4: VERIFIKASI & PELAPORAN HASIL
# =====================================================================
@task(name="inspect-analytics-results")
def inspect_analytics_results(db_path: str):
    """Membaca sampel data mart dan mencetak ringkasan ke Prefect Logger."""
    logger = get_run_logger()
    con = duckdb.connect(db_path)

    query = """
        SELECT
            order_date,
            product_category,
            total_successful_orders,
            total_revenue_idr,
            average_order_value_idr
        FROM mart_daily_revenue
        LIMIT 5;
    """
    rows = con.execute(query).fetchall()
    con.close()

    logger.info("==================================================")
    logger.info("RINGKASAN ANALITIK MART DARI DUCKDB WAREHOUSE:")
    logger.info("Date       | Category       | Orders | Revenue (Rp)   | AOV (Rp)")
    logger.info("-----------+----------------+--------+----------------+---------")
    for r in rows:
        logger.info(f"{r[0]} | {r[1]:<14} | {r[2]:<6} | Rp {r[3]:>12,.0f} | Rp {r[4]:>9,.0f}")
    logger.info("==================================================")


# =====================================================================
# MAIN FLOW ORCHESTRATOR
# =====================================================================
@flow(
    name="duckdb-modern-elt-pipeline",
    description="Pipeline ELT data warehouse orkestrasi Prefect + DuckDB in-process SQL engine."
)
def duckdb_modern_elt_pipeline(db_path: str = DB_PATH, record_count: int = 120):
    logger = get_run_logger()
    logger.info(f"=== Memulai Pipeline ELT DuckDB Warehouse (Target: {record_count} Records) ===")

    # 1. Ingest (Extract & Load)
    raw_rows = extract_and_load_raw_orders(db_path=db_path, record_count=record_count)

    # 2. Transform Staging
    stg_rows = transform_staging_layer(db_path=db_path)

    # 3. Transform Marts (Business Aggregations)
    mart_rows = build_daily_revenue_mart(db_path=db_path)

    # 4. Reporting
    inspect_analytics_results(db_path=db_path)

    logger.info(f"=== Pipeline ELT Sukses! Raw: {raw_rows} -> Staging: {stg_rows} -> Mart: {mart_rows} ===")


if __name__ == "__main__":
    duckdb_modern_elt_pipeline()
