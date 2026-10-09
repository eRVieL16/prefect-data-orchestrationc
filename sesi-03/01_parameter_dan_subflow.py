"""
Sesi 3 - Latihan 1: Parameter Flow & Subflow Modular
---------------------------------------------------
Konsep Inti:
1. Flow Parameters:
   - Validasi otomatis menggunakan type hints Python (Pydantic).
   - Nilai default dapat disediakan.
   - Di Prefect UI, parameter ini otomatis memiliki form input interaktif saat deployment dibuat.
2. Subflow:
   - Sebuah flow (@flow) yang dipanggil di dalam flow lain.
   - Menghasilkan Flow Run tersendiri di UI dan terhubung dengan parent flow run.
   - Sangat baik untuk modularitas kode (memisahkan domain bisnis).
"""

from datetime import date
from typing import List, Optional
from prefect import flow, task, get_run_logger

# -----------------
# TASKS
# -----------------
@task(name="ekstrak-transaksi-toko")
def extract_store_sales(store_name: str, target_date: str) -> list:
    logger = get_run_logger()
    logger.info(f"Mengambil data penjualan toko: '{store_name}' pada tanggal {target_date}...")
    
    # Mock data penjualan per toko
    mock_data = {
        "Jakarta-Pusat": [{"item": "Laptop", "price": 12000000}, {"item": "Mouse", "price": 250000}],
        "Bandung-Dago": [{"item": "Keyboard", "price": 750000}, {"item": "Monitor", "price": 3200000}],
        "Surabaya-Barat": [{"item": "Printer", "price": 2100000}]
    }
    return mock_data.get(store_name, [{"item": "Item-Umum", "price": 100000}])

@task(name="hitung-omzet-toko")
def aggregate_store_sales(sales: list) -> int:
    logger = get_run_logger()
    omzet = sum(item["price"] for item in sales)
    logger.info(f"Total omzet terhitung: Rp {omzet:,.0f} dari {len(sales)} item.")
    return omzet


# -----------------
# SUBFLOW: Menangani logika khusus 1 Toko
# -----------------
@flow(name="subflow-proses-toko", log_prints=True)
def process_single_store_subflow(store_name: str, target_date: str) -> dict:
    """
    Subflow ini memiliki siklus hidup (lifecycle) tersendiri.
    Di UI Prefect, ini akan tercatat sebagai Flow Run anak (child run).
    """
    logger = get_run_logger()
    logger.info(f">>> [SUBFLOW START] Mulai proses analitik untuk: {store_name} <<<")
    
    sales = extract_store_sales(store_name, target_date)
    omzet = aggregate_store_sales(sales)
    
    logger.info(f">>> [SUBFLOW END] Selesai: {store_name} | Omzet: Rp {omzet:,.0f} <<<")
    return {"store": store_name, "date": target_date, "omzet": omzet}


# -----------------
# PARENT FLOW (ORKESTRATOR UTAMA)
# -----------------
@flow(name="sesi-03-parent-orchestrator", log_prints=True)
def retail_daily_orchestrator(
    target_date: Optional[str] = None,
    stores: List[str] = ["Jakarta-Pusat", "Bandung-Dago", "Surabaya-Barat"]
):
    """
    Parent flow dengan validasi tipe data parameter.
    """
    logger = get_run_logger()
    
    # Gunakan tanggal hari ini jika target_date kosong
    if not target_date:
        target_date = date.today().isoformat()
        
    logger.info(f"=== Memulai Retail Orchestrator untuk Tanggal: {target_date} ===")
    logger.info(f"Daftar toko yang akan diproses: {stores}")
    
    summary_results = []
    
    # Memanggil Subflow untuk setiap toko
    for store in stores:
        hasil_toko = process_single_store_subflow(store_name=store, target_date=target_date)
        summary_results.append(hasil_toko)
        
    total_seluruh_toko = sum(r["omzet"] for r in summary_results)
    logger.info(f"=== SEMUA SUBFLOW SELESAI ===")
    logger.info(f"Total Omzet Seluruh Toko: Rp {total_seluruh_toko:,.0f}")
    return summary_results

if __name__ == "__main__":
    # Menjalankan parent flow dengan parameter default
    retail_daily_orchestrator()
