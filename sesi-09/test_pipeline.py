"""
Sesi 9: Unit & Integration Test Suite menggunakan pytest
---------------------------------------------------------
Konsep Inti:
1. Pengujian Task via '.fn':
   Memanggil atribut '.fn' pada Prefect Task memungkinkan pengujian logika murni Python
   dalam hitungan milidetik tanpa memerlukan Prefect Server atau database aktif!
2. Verifikasi Idempotency:
   Memastikan bahwa menjalankan task penyimpanan 2 kali pada partisi yang sama
   menghasilkan jumlah baris yang konsisten (tidak terjadi data duplikat).
"""

import os
import sys
from pathlib import Path
import pytest
import duckdb

# Tambahkan direktori sesi-09 ke sys.path
sys.path.insert(0, str(Path(__file__).parent))

from pipeline_code import (
    normalize_transaction,
    calculate_tax_and_discount,
    save_partitioned_transactions,
    financial_tax_pipeline
)


# =====================================================================
# 1. UNIT TEST: NORMALIZATION TASK (MENGGUNAKAN .fn)
# =====================================================================
def test_normalize_transaction_valid():
    raw_input = {"id": "991", "amount": "150000", "tier": "gold"}
    # Panggil langsung via .fn tanpa engine orkestrasi
    result = normalize_transaction.fn(raw=raw_input)

    assert result["trx_id"] == "TRX-991"
    assert result["amount"] == 150000.0
    assert result["customer_tier"] == "GOLD"


def test_normalize_transaction_invalid_amount():
    raw_input = {"id": "992", "amount": "BukanAngka", "tier": "REGULAR"}
    with pytest.raises(ValueError, match="Nominal transaksi tidak valid"):
        normalize_transaction.fn(raw=raw_input)


# =====================================================================
# 2. UNIT TEST: BUSINESS LOGIC TAX & DISCOUNT
# =====================================================================
@pytest.mark.parametrize("tier,discount_rate", [
    ("REGULAR", 0.0),
    ("GOLD", 0.05),
    ("PLATINUM", 0.10)
])
def test_calculate_tax_and_discount_tiers(tier, discount_rate):
    amount = 1000000.0
    res = calculate_tax_and_discount.fn(amount=amount, customer_tier=tier)

    expected_discount = amount * discount_rate
    after_discount = amount - expected_discount
    expected_tax = round(after_discount * 0.11, 2)
    expected_final = round(after_discount + expected_tax, 2)

    assert res["discount_amount"] == expected_discount
    assert res["tax_amount"] == expected_tax
    assert res["final_amount"] == expected_final


def test_calculate_tax_negative_amount_rejected():
    with pytest.raises(ValueError, match="tidak boleh bernilai negatif"):
        calculate_tax_and_discount.fn(amount=-50000.0, customer_tier="REGULAR")


# =====================================================================
# 3. TEST IDEMPOTENCY: STORAGE TASK
# =====================================================================
def test_idempotent_partition_storage(tmp_path):
    """
    Uji coba sifat IDEMPOTEN:
    Menjalankan proses load berkali-kali pada tanggal yang sama
    TIDAK BOLEH menghasilkan data duplikat!
    """
    test_db = str(tmp_path / "test_tax.duckdb")
    partition_date = "2026-10-09"

    records = [
        {"trx_id": "TRX-1", "original_amount": 100000, "discount_amount": 0, "tax_amount": 11000, "final_amount": 111000},
        {"trx_id": "TRX-2", "original_amount": 200000, "discount_amount": 10000, "tax_amount": 20900, "final_amount": 210900},
    ]

    # Eksekusi Pertama: Masukkan data partisi
    count_run_1 = save_partitioned_transactions.fn(records=records, partition_date=partition_date, db_path=test_db)
    assert count_run_1 == 2

    # Eksekusi Kedua (Simulasi Retry / Backfill ulang dengan data yang sama)
    count_run_2 = save_partitioned_transactions.fn(records=records, partition_date=partition_date, db_path=test_db)
    assert count_run_2 == 2

    # Verifikasi langsung ke database
    con = duckdb.connect(test_db)
    total_rows = con.execute("SELECT COUNT(*) FROM tax_settlement_ledger WHERE partition_date = ?;", [partition_date]).fetchone()[0]
    con.close()

    # TOTAL HARUS TETAP 2, BUKAN 4! (Bukti Idempotency)
    assert total_rows == 2, f"Idempotency breach! Ditemukan {total_rows} baris alih-alih 2 baris."


# =====================================================================
# 4. INTEGRATION TEST: END-TO-END FLOW
# =====================================================================
def test_financial_tax_pipeline_end_to_end(tmp_path):
    test_db = str(tmp_path / "integration_tax.duckdb")
    sample_items = [
        {"id": "A1", "amount": 100000, "tier": "REGULAR"},
        {"id": "A2", "amount": 500000, "tier": "GOLD"},
        {"id": "A3", "amount": 2000000, "tier": "PLATINUM"},
    ]

    # Jalankan flow secara langsung
    result = financial_tax_pipeline(
        raw_items=sample_items,
        partition_date="2026-10-09",
        db_path=test_db
    )

    assert result == 3
