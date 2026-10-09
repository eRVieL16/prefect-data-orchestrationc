"""
Sesi 1 - Langkah 1: Script ETL Biasa (Tanpa Orkestrator)
--------------------------------------------------------
Tujuan: Mengetahui bentuk dasar script data sebelum diorkestrasi.
Masalah yang sering terjadi di script biasa:
1. Tidak ada visibilitas status langkah (step) yang gagal.
2. Sulit memantau history eksekusi secara visual.
3. Tidak ada mekanisme retry otomatis bawaan.
"""

import json
import sqlite3
from datetime import datetime

# 1. EXTRACT: Simulasi mengambil data transaksi mentah
def extract_transactions():
    print("[Extract] Mengambil data transaksi mentah...")
    raw_data = [
        {"order_id": 101, "customer": "Andi", "amount": 150000, "status": "COMPLETED"},
        {"order_id": 102, "customer": "Budi", "amount": 250000, "status": "COMPLETED"},
        {"order_id": 103, "customer": "Citra", "amount": 50000, "status": "CANCELLED"},
        {"order_id": 104, "customer": "Dewi", "amount": 420000, "status": "COMPLETED"},
        {"order_id": 105, "customer": "Eko", "amount": -10000, "status": "ERROR"}, # data anomali
    ]
    return raw_data

# 2. TRANSFORM: Membersihkan & menyaring data valid
def transform_transactions(transactions):
    print(f"[Transform] Memproses {len(transactions)} data transaksi...")
    clean_records = []
    for item in transactions:
        # Hanya ambil yang COMPLETED dan nilai amount positif
        if item.get("status") == "COMPLETED" and item.get("amount", 0) > 0:
            clean_records.append({
                "order_id": item["order_id"],
                "customer": item["customer"],
                "amount": item["amount"],
                "tax": int(item["amount"] * 0.11), # PPN 11%
                "total": int(item["amount"] * 1.11),
                "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    print(f"[Transform] Berhasil menyaring {len(clean_records)} transaksi valid.")
    return clean_records

# 3. LOAD: Menyimpan ke database SQLite lokal
def load_to_database(clean_records, db_path="transaksi.db"):
    print(f"[Load] Menyimpan ke database {db_path}...")
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
    print("[Load] Data tersimpan dengan sukses!")

# Fungsi utama (Runner)
def run_pipeline():
    print("=== Memulai Pipeline ETL Tradisional ===")
    data = extract_transactions()
    clean = transform_transactions(data)
    load_to_database(clean)
    print("=== Pipeline Selesai ===")

if __name__ == "__main__":
    run_pipeline()
