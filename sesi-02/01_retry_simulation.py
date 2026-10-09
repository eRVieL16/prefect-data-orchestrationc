"""
Sesi 2 - Latihan 1: Ketahanan Pipeline (Retries, Timeout, & Failure Handling)
----------------------------------------------------------------------------
Konsep Inti:
1. retries: Jumlah percobaan ulang jika task melempar exception/error.
2. retry_delay_seconds: Jeda waktu (detik) sebelum mencoba kembali.
   Bisa berupa angka integer (misal: 3), list backoff (misal: [2, 5, 10]).
3. retry_jitter_factor: Variasi acak pada jeda retry untuk mencegah thundering herd.
4. timeout_seconds: Batas waktu maksimal eksekusi task sebelum dianggap timeout/gagal.
5. States: Perhatikan transisi state di UI (Running -> Retrying -> Completed / Failed).
"""

import time
import random
from prefect import flow, task, get_run_logger

# Counter global simulasi percobaan
ATTEMPT_COUNTER = 0

# 1. TASK DENGAN RETRY OTOMATIS
@task(
    name="fetch-exchange-rate-api",
    retries=3,
    retry_delay_seconds=3,
    retry_jitter_factor=0.1,
    timeout_seconds=10,
    description="Simulasi pemanggilan API kurs yang rentan flaky network"
)
def fetch_exchange_rate(currency: str = "USD") -> float:
    global ATTEMPT_COUNTER
    ATTEMPT_COUNTER += 1
    logger = get_run_logger()
    
    logger.info(f"[Percobaan ke-{ATTEMPT_COUNTER}] Menghubungi API kurs untuk {currency}...")
    
    # Simulasi: Percobaan 1 & 2 gagal (Network Timeout / HTTP 503), Percobaan 3 berhasil
    if ATTEMPT_COUNTER < 3:
        logger.warning(f"Koneksi ke server gateway {currency} terputus (HTTP 503)! Melempar ConnectionError...")
        raise ConnectionError("Gateway Timeout 503: Layanan kurs sementara tidak merespons.")
    
    # Percobaan ke-3: Berhasil
    rate = 15850.0
    logger.info(f"Koneksi berhasil! Kurs {currency}/IDR: {rate:,.2f}")
    return rate


# 2. TASK YANG SENGAJA GAGAL TOTAL (Melebihi batas retry)
@task(
    name="task-kritis-rusak",
    retries=2,
    retry_delay_seconds=2,
    description="Task yang terus gagal untuk mengamati state Failed di UI"
)
def unrecoverable_task():
    logger = get_run_logger()
    logger.error("Data source korup: Format payload tidak valid (Fatal Error)!")
    raise ValueError("Payload rusak: Missing primary key 'transaction_id'")


# 3. TASK DENGAN TIMEOUT
@task(
    name="task-uji-timeout",
    timeout_seconds=2,
    description="Task yang melampaui batas timeout yang diizinkan"
)
def slow_task():
    logger = get_run_logger()
    logger.info("Task lambat mulai bekerja...")
    time.sleep(5)  # Lebih lama dari timeout_seconds=2
    return "Selesai"


# FLOW UTAMA
@flow(name="sesi-02-resilience-demo", log_prints=True)
def resilience_flow(test_crash: bool = False, test_timeout: bool = False):
    logger = get_run_logger()
    logger.info("=== Memulai Pipeline Ketahanan (Sesi 2) ===")
    
    # Bagian 1: Uji Retry (Akan sukses pada percobaan ke-3)
    logger.info("--- 1. Menguji Mekanisme Retries ---")
    rate = fetch_exchange_rate("USD")
    logger.info(f"Hasil rate yang diterima: {rate}")

    # Bagian 2: Uji Timeout (Opsional)
    if test_timeout:
        logger.info("--- 2. Menguji Timeout (Akan Fail karena Timeout) ---")
        slow_task()

    # Bagian 3: Uji Fatal Failure (Opsional)
    if test_crash:
        logger.info("--- 3. Menguji Kegagalan Fatal (Akan Fail setelah 2 Retries) ---")
        unrecoverable_task()

    logger.info("=== Pipeline Ketahanan Selesai ===")

if __name__ == "__main__":
    # Menjalankan simulasi retry yang berhasil sembuh di percobaan ke-3
    print("\n--- Menjalankan simulasi self-healing retry ---")
    resilience_flow(test_crash=False)
