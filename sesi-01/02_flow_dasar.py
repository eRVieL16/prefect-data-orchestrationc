"""
Sesi 1 - Langkah 2: Migrasi ke Prefect Flow & Task
--------------------------------------------------
Konsep Inti Prefect 3.x:
1. @task: Unit kerja terkecil (fungsi spesifik seperti Extract, Transform, Load).
   - Task bisa memiliki retry, timeout, tags, dan caching.
2. @flow: Orkestrator workflow (kontainer pemanggil task-task).
   - Flow melacak status keseluruhan (Completed, Failed, Running).
3. Flow Run: Satu instansi eksekusi flow yang tercatat dalam Prefect UI.
"""

import sqlite3
from datetime import datetime
from prefect import flow, task, get_run_logger

# 1. TASK EXTRACT
@task(name="extract-transaksi", description="Mengambil data transaksi mentah")
def extract_transactions():
    logger = get_run_logger()
    logger.info("Mengambil data transaksi mentah dari sumber data...")
    raw_data = [
        {"order_id": 101, "customer": "Andi", "amount": 150000, "status": "COMPLETED"},
        {"order_id": 102, "customer": "Budi", "amount": 250000, "status": "COMPLETED"},
        {"order_id": 103, "customer": "Citra", "amount": 50000, "status": "CANCELLED"},
        {"order_id": 104, "customer": "Dewi", "amount": 420000, "status": "COMPLETED"},
        {"order_id": 105, "customer": "Eko", "amount": -10000, "status": "ERROR"},
    ]
    logger.info(f"Ditemukan {len(raw_data)} record mentah.")
    return raw_data

# 2. TASK TRANSFORM
@task(name="transform-transaksi", description="Menyaring & kalkulasi pajak")
def transform_transactions(transactions: list):
    logger = get_run_logger()
    logger.info("Memulai transformasi & kalkulasi bisnis...")
    clean_records = []
    
    for item in transactions:
        if item.get("status") == "COMPLETED" and item.get("amount", 0) > 0:
            clean_records.append({
                "order_id": item["order_id"],
                "customer": item["customer"],
                "amount": item["amount"],
                "tax": int(item["amount"] * 0.11),
                "total": int(item["amount"] * 1.11),
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
    logger.info(f"Filter selesai: {len(clean_records)} transaksi valid diproses.")
    return clean_records

# 3. TASK LOAD
@task(name="load-ke-sqlite", description="Menyimpan record ke database SQLite")
def load_to_database(clean_records: list, db_path: str = "transaksi_prefect.db"):
    logger = get_run_logger()
    logger.info(f"Membuka koneksi database {db_path}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer TEXT,
            amount INTEGER,
            tax INTEGER,
            total INTEGER,
            processed_at TEXT
        )
    """)
    
    for row in clean_records:
        cursor.execute("""
            INSERT OR REPLACE INTO orders (order_id, customer, amount, tax, total, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row["order_id"], row["customer"], row["amount"], row["tax"], row["total"], row["processed_at"]))
        
    conn.commit()
    conn.close()
    logger.info("Penyimpanan selesai ke database.")

# FLOW UTAMA
@flow(name="etl-penjualan-harian", log_prints=True)
def etl_flow():
    logger = get_run_logger()
    logger.info(">>> Memulai Prefect Flow: etl-penjualan-harian <<<")
    
    raw = extract_transactions()
    clean = transform_transactions(raw)
    load_to_database(clean)
    
    logger.info(">>> Flow berhasil dieksekusi dengan sempurna! <<<")

if __name__ == "__main__":
    # Jalankan langsung layaknya script python biasa
    etl_flow()
