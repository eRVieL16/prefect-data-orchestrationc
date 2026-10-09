"""
Sesi 2 - Latihan 2: Caching Task Berdasarkan Input
--------------------------------------------------
Konsep Inti Caching:
1. cache_key_fn: Fungsi pembuat identitas (key) cache.
   task_input_hash menghasilkan hash dari nama task + nilai argumen input.
2. cache_expiration: Durasi masa berlaku cache (contoh: 10 menit).
3. Keuntungan:
   - Menghemat kuota API / rate-limit pihak ketiga.
   - Menghemat biaya komputasi BigQuery/Snowflake/Warehouse.
   - Mempercepat re-run flow jika langkah tertentu sudah pernah selesai.
"""

import time
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# TASK KOMPUTASI BERAT DENGAN CACHE
@task(
    name="query-laporan-keuangan-berat",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=10),
    description="Simulasi query agregasi 1 juta baris data finansial"
)
def compute_heavy_daily_report(report_date: str) -> dict:
    logger = get_run_logger()
    logger.info(f"===> [CACHE MISS] Menjalankan query berat di database untuk tanggal: {report_date}...")
    
    # Simulasi latency pemrosesan / database query selama 3 detik
    time.sleep(3)
    
    result = {
        "report_date": report_date,
        "total_revenue": 145_000_000,
        "total_orders": 3_420,
        "calculated_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    logger.info(f"===> Perhitungan selesai untuk tanggal {report_date}.")
    return result


@flow(name="sesi-02-caching-demo", log_prints=True)
def caching_flow():
    logger = get_run_logger()
    logger.info("=== Demonstrasi Caching Prefect 3.x ===")
    
    # Panggilan 1: Pertama kali untuk tanggal '2026-10-01'
    start_time = time.time()
    logger.info("--- Panggilan 1: Tanggal 2026-10-01 (Harus Cache Miss / Eksekusi Penuh) ---")
    data_1 = compute_heavy_daily_report("2026-10-01")
    durasi_1 = time.time() - start_time
    logger.info(f"Durasi Panggilan 1: {durasi_1:.2f} detik | Waktu kalkulasi: {data_1['calculated_at']}")
    
    # Panggilan 2: Input sama persis ('2026-10-01')
    start_time = time.time()
    logger.info("--- Panggilan 2: Tanggal 2026-10-01 (Harus Cache Hit / Instan) ---")
    data_2 = compute_heavy_daily_report("2026-10-01")
    durasi_2 = time.time() - start_time
    logger.info(f"Durasi Panggilan 2: {durasi_2:.2f} detik | Waktu kalkulasi: {data_2['calculated_at']}")
    
    # Panggilan 3: Input berbeda ('2026-10-02')
    start_time = time.time()
    logger.info("--- Panggilan 3: Tanggal 2026-10-02 (Input Baru -> Cache Miss / Eksekusi Penuh) ---")
    data_3 = compute_heavy_daily_report("2026-10-02")
    durasi_3 = time.time() - start_time
    logger.info(f"Durasi Panggilan 3: {durasi_3:.2f} detik | Waktu kalkulasi: {data_3['calculated_at']}")

    logger.info(f"\n[RINGKASAN KECEPATAN]")
    logger.info(f"- Panggilan 1 (Eksekusi Asli) : {durasi_1:.2f} detik")
    logger.info(f"- Panggilan 2 (Prefect Cache) : {durasi_2:.4f} detik (Hemat {((durasi_1 - durasi_2)/durasi_1)*100:.1f}%)")
    logger.info(f"- Panggilan 3 (Input Baru)    : {durasi_3:.2f} detik")

if __name__ == "__main__":
    caching_flow()
