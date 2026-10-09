"""
Sesi 6 - Latihan 2: Mendaftarkan Deployment ke Work Pool
-------------------------------------------------------
Konsep Inti:
1. 'flow.from_source(...).deploy(...)':
   Di Prefect 3.x, kita mendaftarkan metadata deployment dan lokasi kode sumber
   ke Work Pool tanpa perlu menjalankan proses runner yang membebani script ini.
2. Peran Work Pool & Worker:
   Setelah deployment didaftarkan, tugas eksekusi diserahkan sepenuhnya ke Worker
   yang mendengarkan antrean Work Pool 'local-process-pool'.
"""

import os
from datetime import datetime
import random
from prefect import flow, task, get_run_logger


# =====================================================================
# TASK PIPELINE: ANALISIS CHURN PELANGGAN
# =====================================================================
@task(name="extract-customer-activity")
def extract_customer_activity(date_str: str, chunk_size: int) -> list[dict]:
    logger = get_run_logger()
    logger.info(f"[EXTRACT] Mengambil aktivitas {chunk_size} pelanggan per tanggal {date_str}...")

    # Simulasi data aktivitas pelanggan
    customers = []
    for i in range(1, chunk_size + 1):
        days_inactive = random.randint(1, 90)
        total_tx = random.randint(1, 40)
        customers.append({
            "cust_id": f"CUST-{1000 + i}",
            "days_inactive": days_inactive,
            "total_transactions": total_tx,
            "has_support_ticket": random.choice([True, False])
        })

    logger.info(f"[EXTRACT] Berhasil mengunduh data {len(customers)} pelanggan.")
    return customers


@task(name="compute-churn-risk")
def compute_churn_risk(customers: list[dict]) -> list[dict]:
    logger = get_run_logger()
    logger.info(f"[TRANSFORM] Menghitung skor churn risk untuk {len(customers)} akun...")

    high_risk_count = 0
    predictions = []
    for c in customers:
        # Logika scoring sederhana: inaktif > 30 hari & ada tiket komplain
        is_high_risk = c["days_inactive"] > 30 and c["has_support_ticket"]
        risk_score = round(min(1.0, (c["days_inactive"] / 60.0) + (0.3 if c["has_support_ticket"] else 0.0)), 2)

        if is_high_risk:
            high_risk_count += 1

        predictions.append({
            "cust_id": c["cust_id"],
            "days_inactive": c["days_inactive"],
            "risk_score": risk_score,
            "is_high_risk": is_high_risk
        })

    logger.info(f"[TRANSFORM] Analisis selesai: Terdeteksi {high_risk_count} pelanggan berisiko tinggi churn!")
    return predictions


@task(name="save-churn-predictions")
def save_churn_predictions(predictions: list[dict], export_format: str) -> str:
    logger = get_run_logger()
    logger.info(f"[LOAD] Menyimpan hasil prediksi churn dalam format {export_format.upper()}...")

    output_dir = "./data/predictions"
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"churn_scoring_{today}.csv")

    high_risks = [p for p in predictions if p["is_high_risk"]]
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("cust_id,days_inactive,risk_score,is_high_risk\n")
        for p in predictions:
            f.write(f"{p['cust_id']},{p['days_inactive']},{p['risk_score']},{p['is_high_risk']}\n")

    logger.info(f"[LOAD] Sukses! File prediksi ({len(high_risks)} akun high-risk) tersimpan di: {file_path}")
    return file_path


# =====================================================================
# MAIN FLOW
# =====================================================================
@flow(
    name="customer-churn-etl",
    description="Pipeline prediksi Churn Pelanggan yang dieksekusi secara otomatis oleh Worker."
)
def customer_churn_etl(
    target_date: str = "",
    chunk_size: int = 50,
    export_format: str = "CSV"
):
    logger = get_run_logger()
    if not target_date:
        target_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("==================================================")
    logger.info(f"Memulai Customer Churn Pipeline (Worker Execution)")
    logger.info(f"Tanggal: {target_date} | Chunk Size: {chunk_size} | Format: {export_format}")
    logger.info("==================================================")

    data = extract_customer_activity(date_str=target_date, chunk_size=chunk_size)
    predictions = compute_churn_risk(customers=data)
    result_path = save_churn_predictions(predictions=predictions, export_format=export_format)

    logger.info(f"Pipeline Selesai! Output: {result_path}")
    return result_path


# =====================================================================
# REGISTRASI KE WORK POOL
# =====================================================================
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Mendaftarkan Deployment ke Work Pool 'local-process-pool'...")
    print("=" * 70)

    # Lokasi root folder project saat ini
    project_root = os.path.abspath(os.getcwd())
    entrypoint_path = "sesi-06/02_deploy_to_worker.py:customer_churn_etl"

    # Definisikan flow dari source path
    deployed_flow = flow.from_source(
        source=project_root,
        entrypoint=entrypoint_path
    )

    # Daftarkan ke work pool
    deployment_id = deployed_flow.deploy(
        name="churn-prediction-worker-pipeline",
        work_pool_name="local-process-pool",
        cron="0 3 * * *",  # Jam 03:00 pagi setiap hari
        tags=["churn", "ml", "sesi-06", "worker-based"],
        parameters={
            "chunk_size": 40,
            "export_format": "CSV"
        },
        description=(
            "### Customer Churn ETL & Scoring Pipeline\n"
            "- **Work Pool**: `local-process-pool`\n"
            "- **Tipe Worker**: Process (Local Subprocess Isolation)\n"
            "- **Schedule**: Cron jam 03:00 pagi setiap hari.\n"
        ),
        ignore_warnings=True
    )

    print("\n[SUKSES] Deployment berhasil didaftarkan!")
    print(f"Deployment ID: {deployment_id}")
    print("\nLangkah selanjutnya untuk mengeksekusi pipeline ini:")
    print("-----------------------------------------------------------------")
    print("1. Nyalakan Worker di terminal baru:")
    print("   prefect worker start --pool 'local-process-pool'")
    print("2. Picu flow run secara manual via CLI:")
    print("   prefect deployment run 'customer-churn-etl/churn-prediction-worker-pipeline'")
    print("   atau klik tombol 'Run' di browser: http://127.0.0.1:4200/deployments")
    print("=" * 70 + "\n")
