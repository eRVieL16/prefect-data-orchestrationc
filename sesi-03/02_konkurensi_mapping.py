"""
Sesi 3 - Latihan 2: Konkurensi & Paralelisme dengan .map()
---------------------------------------------------------
Konsep Inti:
1. .map(): Menjalankan task secara bersamaan (konkruen) untuk setiap elemen dalam list/iterable.
   Mirip map() bawaan Python, tetapi dieksekusi sebagai asynchronous Prefect Task Run!
2. unmapped(): Wrapper untuk argumen yang ingin dipertahankan nilainya secara konstan
   (tidak ingin di-iterasi bersama list).
3. Perbedaan Sekuensial vs Konkruen:
   - 4 item x 1.5 detik:
     - Sekuensial: ~6.0 detik
     - Konkruen:   ~1.5 detik (Hemat ~75% waktu eksekusi!)
"""

import time
from prefect import flow, task, unmapped, get_run_logger

# TASK DENGAN LATENSI I/O (SIMULASI KONEKSI DB / API EKSTERNAL)
@task(name="proses-partisi-cabang")
def process_branch_partition(branch_id: str, batch_code: str) -> dict:
    logger = get_run_logger()
    logger.info(f"===> [START] Memproses Cabang: {branch_id} (Batch: {batch_code})...")
    
    # Simulasi latency I/O 1.5 detik per cabang
    time.sleep(1.5)
    
    rows_processed = 5_000
    logger.info(f"<=== [DONE] Cabang: {branch_id} selesai ({rows_processed:,} baris).")
    return {"branch": branch_id, "rows": rows_processed}


# -----------------
# 1. FLOW SEKUENSIAL (SATU PER SATU)
# -----------------
@flow(name="demo-eksekusi-sekuensial", log_prints=True)
def sequential_flow(branches: list[str], batch_code: str):
    logger = get_run_logger()
    logger.info(f"--- [MODE SEKUENSIAL] Mulai memproses {len(branches)} cabang berurutan ---")
    start = time.time()
    
    results = []
    for branch in branches:
        # Panggilan biasa: blocking hingga task selesai
        res = process_branch_partition(branch, batch_code)
        results.append(res)
        
    duration = time.time() - start
    logger.info(f"--- [MODE SEKUENSIAL SELESAI] Durasi: {duration:.2f} detik ---")
    return duration


# -----------------
# 2. FLOW KONKRUEN MENGGUNAKAN .map()
# -----------------
@flow(name="demo-eksekusi-konkruen-map", log_prints=True)
def concurrent_flow(branches: list[str], batch_code: str):
    logger = get_run_logger()
    logger.info(f"--- [MODE KONKRUEN .map()] Mulai memproses {len(branches)} cabang serentak ---")
    start = time.time()
    
    # .map() mengeksekusi semua elemen branches secara non-blocking!
    # unmapped(batch_code) memastikan argumen kedua konstan untuk setiap iterasi.
    futures = process_branch_partition.map(branches, unmapped(batch_code))
    
    # Menunggu dan mengambil hasil dari semua task futures
    results = [f.result() for f in futures]
    
    duration = time.time() - start
    logger.info(f"--- [MODE KONKRUEN SELESAI] Durasi: {duration:.2f} detik ---")
    return duration


# -----------------
# BENCHMARK COMPARISON FLOW
# -----------------
@flow(name="sesi-03-benchmark-concurrency", log_prints=True)
def main_benchmark():
    logger = get_run_logger()
    daftar_cabang = ["CABANG-JKT", "CABANG-BDG", "CABANG-SBY", "CABANG-MDN"]
    batch_code = "BATCH-2026-Q4"
    
    logger.info("==================================================")
    logger.info("   MEMULAI BENCHMARK: SEKUENSIAL VS KONKRUEN      ")
    logger.info("==================================================")
    
    # Uji 1: Sekuensial
    t_seq = sequential_flow(daftar_cabang, batch_code)
    
    # Uji 2: Konkruen
    t_con = concurrent_flow(daftar_cabang, batch_code)
    
    hemat_persen = ((t_seq - t_con) / t_seq) * 100
    
    logger.info("\n" + "="*50)
    logger.info("              HASIL BENCHMARK AKHIR               ")
    logger.info("="*50)
    logger.info(f"- Durasi Mode Sekuensial : {t_seq:.2f} detik")
    logger.info(f"- Durasi Mode Konkruen   : {t_con:.2f} detik")
    logger.info(f"- Efisiensi Waktu        : Lebih cepat {hemat_persen:.1f}%!")
    logger.info("="*50)

if __name__ == "__main__":
    main_benchmark()
