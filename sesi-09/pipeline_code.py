"""
Sesi 9: Pipeline Finansial & Pajak Siap Produksi (Idempotent & Testable)
-----------------------------------------------------------------------
Konsep Inti:
1. Pure Function Tasks:
   Setiap task dirancang modular dengan input & output terdefinisi jelas,
   sehingga logika bisnisnya dapat diuji secara terisolasi tanpa server Prefect.
2. Idempotency (Sifat Wajib Data Engineering):
   Pipeline yang idempoten menghasilkan status data yang IDENTIK meskipun dijalankan
   berulang kali pada partisi tanggal yang sama (misal saat retry otomatis atau manual backfill).
"""

from datetime import datetime
import os
import logging
import duckdb
from prefect import flow, task, get_run_logger


def get_safe_logger():
    """Mengambil Prefect logger jika di dalam run context, atau standard logger jika di unit test."""
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger("pipeline")


DB_PATH = "./data/warehouse/financial_reports.duckdb"


# =====================================================================
# TASK 1: DATA NORMALIZATION (UNIT TESTABLE VIA .fn)
# =====================================================================
@task(name="normalize-transaction")
def normalize_transaction(raw: dict) -> dict:
    """Membersihkan dan menstandarisasi field data mentah."""
    trx_id = str(raw.get("id", "")).strip().upper()
    if not trx_id.startswith("TRX-"):
        trx_id = f"TRX-{trx_id}"

    # Parsing nominal aman
    raw_amount = raw.get("amount", 0)
    try:
        amount = float(raw_amount)
    except (ValueError, TypeError):
        raise ValueError(f"Nominal transaksi tidak valid: {raw_amount}")

    customer_tier = str(raw.get("tier", "REGULAR")).strip().upper()
    if customer_tier not in ["REGULAR", "GOLD", "PLATINUM"]:
        customer_tier = "REGULAR"

    return {
        "trx_id": trx_id,
        "amount": amount,
        "customer_tier": customer_tier
    }


# =====================================================================
# TASK 2: TAX & DISCOUNT CALCULATION (BUSINESS LOGIC)
# =====================================================================
@task(name="calculate-tax-and-discount")
def calculate_tax_and_discount(amount: float, customer_tier: str) -> dict:
    """Menghitung diskon berdasarkan tier dan PPN 11%."""
    if amount < 0:
        raise ValueError("Nominal transaksi tidak boleh bernilai negatif.")

    # Diskon member
    discount_rates = {"REGULAR": 0.0, "GOLD": 0.05, "PLATINUM": 0.10}
    discount_rate = discount_rates.get(customer_tier, 0.0)
    discount_amount = round(amount * discount_rate, 2)
    after_discount = amount - discount_amount

    # PPN 11%
    tax_rate = 0.5
    tax_amount = round(after_discount * tax_rate, 2)
    final_amount = round(after_discount + tax_amount, 2)

    return {
        "original_amount": amount,
        "discount_amount": discount_amount,
        "tax_amount": tax_amount,
        "final_amount": final_amount
    }


# =====================================================================
# TASK 3: IDEMPOTENT STORAGE (PARTITION-OVERWRITE PATTERN)
# =====================================================================
@task(name="save-partitioned-transactions")
def save_partitioned_transactions(records: list[dict], partition_date: str, db_path: str = DB_PATH) -> int:
    """
    Menyimpan transaksi ke partisi tanggal tertentu secara IDEMPOTEN.
    Jika partisi sudah berisi data lama, data partisi tersebut dihapus terlebih dahulu
    sebelum data baru dimasukkan (Delete-then-Insert partition pattern).
    """
    logger = get_safe_logger()

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)

    # Buat tabel jika belum ada
    con.execute("""
        CREATE TABLE IF NOT EXISTS tax_settlement_ledger (
            trx_id VARCHAR,
            partition_date DATE,
            original_amount DOUBLE,
            discount_amount DOUBLE,
            tax_amount DOUBLE,
            final_amount DOUBLE,
            created_at TIMESTAMP
        );
    """)

    # IDEMPOTENCY KEY: Hapus partisi tanggal terkait agar tidak duplikat saat re-run!
    con.execute("DELETE FROM tax_settlement_ledger WHERE partition_date = ?;", [partition_date])

    # Insert data partisi yang baru
    now_ts = datetime.now()
    insert_rows = [
        (
            r["trx_id"],
            partition_date,
            r["original_amount"],
            r["discount_amount"],
            r["tax_amount"],
            r["final_amount"],
            now_ts
        )
        for r in records
    ]

    con.executemany("INSERT INTO tax_settlement_ledger VALUES (?, ?, ?, ?, ?, ?, ?);", insert_rows)
    total_in_partition = con.execute(
        "SELECT COUNT(*) FROM tax_settlement_ledger WHERE partition_date = ?;",
        [partition_date]
    ).fetchone()[0]

    con.close()
    if logger:
        logger.info(f"[IDEMPOTENT LOAD] Partisi {partition_date} berhasil diisi {total_in_partition} baris tanpa duplikasi.")

    return total_in_partition


# =====================================================================
# MAIN FLOW ORCHESTRATOR
# =====================================================================
@flow(name="financial-tax-settlement-flow")
def financial_tax_pipeline(raw_items: list[dict], partition_date: str, db_path: str = DB_PATH) -> int:
    """Orkestrasi pemrosesan dan penyimpanan pajak idempoten."""
    processed = []
    for item in raw_items:
        norm = normalize_transaction(raw=item)
        calc = calculate_tax_and_discount(amount=norm["amount"], customer_tier=norm["customer_tier"])
        processed.append({
            "trx_id": norm["trx_id"],
            **calc
        })

    saved_count = save_partitioned_transactions(
        records=processed,
        partition_date=partition_date,
        db_path=db_path
    )
    return saved_count


if __name__ == "__main__":
    sample_data = [
        {"id": "501", "amount": 250000, "tier": "REGULAR"},
        {"id": "502", "amount": 1000000, "tier": "GOLD"},
        {"id": "503", "amount": 3500000, "tier": "PLATINUM"},
    ]
    today = datetime.now().strftime("%Y-%m-%d")
    financial_tax_pipeline(raw_items=sample_data, partition_date=today)
