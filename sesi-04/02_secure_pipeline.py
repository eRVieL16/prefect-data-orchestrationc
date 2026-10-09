"""
Sesi 4 - Langkah 2: Pipeline Aman dengan Prefect Variables & Blocks
-------------------------------------------------------------------
Konsep Inti:
1. Zero Hardcoded Credentials: Kode dapat di-commit ke Git publik dengan aman.
2. Pemisahan Dev vs Prod: Cukup ubah nilai 'target_environment' di UI Prefect,
   pipeline otomatis menyesuaikan tingkah lakunya tanpa mengubah kode!
3. Secret Masking: Jangan pernah mencetak secret utuh ke dalam Prefect Logs.
"""

import os
from prefect import flow, task, get_run_logger
from prefect.variables import Variable
from prefect.blocks.system import Secret

# 1. TASK MEMBACA KONFIGURASI DINAMIS DARI PREFECT SERVER
@task(name="load-dynamic-config")
def load_runtime_configuration() -> dict:
    logger = get_run_logger()
    logger.info("Mengambil konfigurasi runtime dari Prefect Server...")

    # Membaca variables (non-sensitif) dengan nilai fallback aman
    env = Variable.get("target_environment", default="development")
    data_lake = Variable.get("data_lake_path", default="./data/dev_lake")
    batch_limit = Variable.get("batch_process_limit", default=100)

    # Membaca secret blocks (kredensial terenkripsi)
    payment_secret = Secret.load("payment-gateway-token")
    dwh_secret = Secret.load("dwh-database-password")

    api_token = payment_secret.get()
    db_pass = dwh_secret.get()

    # Praktik Terbaik Keamanan: MASKING CREDENTIALS DI LOG!
    masked_token = f"{api_token[:4]}****{api_token[-4:]}"
    masked_db = f"{db_pass[:2]}****"

    logger.info(f"Target Environment : [{env.upper()}]")
    logger.info(f"Data Lake Path     : {data_lake}")
    logger.info(f"Batch Limit        : {batch_limit} records")
    logger.info(f"API Token Terbaca  : {masked_token} (Masked)")
    logger.info(f"DB Pass Terbaca    : {masked_db} (Masked)")

    return {
        "env": env,
        "lake_path": data_lake,
        "batch_limit": int(batch_limit),
        "api_token": api_token,
        "db_password": db_pass
    }


# 2. TASK EXTRACT DENGAN AUTHENTICATION TOKEN
@task(name="extract-external-api-secure")
def extract_transactions(api_token: str, limit: int) -> list:
    logger = get_run_logger()
    logger.info(f"Melakukan autentikasi API dengan bearer token valid...")
    
    # Simulasi memanggil payment gateway menggunakan token rahasia
    data = [
        {"tx_id": f"TX-2026-00{i}", "amount": (i + 1) * 75000, "status": "SETTLED"}
        for i in range(1, 4)
    ]
    logger.info(f"Berhasil mengunduh {len(data)} transaksi (Limit: {limit}).")
    return data


# 3. TASK LOAD KE DESTINASI SESUAI ENVIRONMENT
@task(name="load-to-warehouse-secure")
def load_to_destination(records: list, config: dict):
    logger = get_run_logger()
    env = config["env"]
    dest_path = config["lake_path"]

    # Pemisahan Dev vs Prod secara dinamis
    if env.lower() == "production":
        logger.info(f"[PROD MODE] Mengirim {len(records)} baris ke Data Warehouse Cloud (Port 5432)...")
        logger.info(f"[PROD MODE] Menggunakan kredensial database tersertifikasi.")
    else:
        logger.info(f"[DEV MODE] Menyimpan {len(records)} baris secara lokal ke directory: {dest_path}")
        os.makedirs(dest_path, exist_ok=True)
        out_file = os.path.join(dest_path, "transactions_dev.txt")
        with open(out_file, "w") as f:
            for r in records:
                f.write(str(r) + "\n")
        logger.info(f"[DEV MODE] Berhasil menulis file lokal: {out_file}")


# FLOW UTAMA
@flow(name="sesi-04-secure-pipeline", log_prints=True)
def secure_data_pipeline():
    logger = get_run_logger()
    logger.info("=== Memulai Pipeline Aman (Sesi 4: Zero Hardcode) ===")

    # 1. Ambil config & secret dari Prefect Server
    config = load_runtime_configuration()

    # 2. Ekstraksi data menggunakan kredensial
    records = extract_transactions(api_token=config["api_token"], limit=config["batch_limit"])

    # 3. Muat data ke tujuan sesuai environment aktif
    load_to_destination(records=records, config=config)

    logger.info("=== Pipeline Aman Selesai dengan Sukses ===")

if __name__ == "__main__":
    secure_data_pipeline()
