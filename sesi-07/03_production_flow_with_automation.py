"""
Sesi 7 - Latihan 3: Server-Level Automations & Production Alerting
-----------------------------------------------------------------
Konsep Inti:
1. Server Automations vs Code Hooks:
   - Code Hooks ('on_failure'): Didefinisikan di dalam kode Python per flow/task.
   - Server Automations: Dikelola terpusat di Prefect Server (UI tab 'Automations').
     Memonitor SELURUH flow di sistem tanpa perlu mengubah satu baris pun kode flow!
2. Event Trigger:
   Server mendeteksi event: "Flow run dengan tag 'critical' memasuki state 'Failed'".
3. Action:
   Server mengeksekusi aksi: Pause schedule deployment (agar kegagalan tidak berulang),
   atau memicu webhook/notifikasi email.
"""

import os
from prefect import flow, task, get_run_logger


@task(name="extract-core-banking-transactions")
def extract_core_banking(trigger_failure: bool = True):
    logger = get_run_logger()
    logger.info("[CRITICAL TASK] Menghubungi Core Banking System...")

    if trigger_failure:
        logger.error("[FATAL] Core Banking Database mengembalikan error: DB_CONN_REFUSED!")
        raise RuntimeError("Database Core Banking tidak dapat dihubungi (Connection Refused).")

    logger.info("[CRITICAL TASK] 5,000 transaksi berhasil ditarik.")
    return {"total_records": 5000}


@flow(
    name="critical-banking-settlement",
    description="Pipeline transaksi keuangan kritis dengan tag 'critical' dan 'production'."
)
def critical_banking_settlement(trigger_failure: bool = True):
    logger = get_run_logger()
    logger.info("==================================================")
    logger.info("Memulai Critical Banking Settlement Pipeline")
    logger.info("Tag: ['production', 'critical', 'sesi-07']")
    logger.info("==================================================")

    data = extract_core_banking(trigger_failure=trigger_failure)
    logger.info(f"Pipeline Sukses: {data}")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Mendaftarkan Deployment Kritis ke Work Pool 'local-process-pool'...")
    print("=" * 70)

    project_root = os.path.abspath(os.getcwd())
    entrypoint_path = "sesi-07/03_production_flow_with_automation.py:critical_banking_settlement"

    deployed = flow.from_source(
        source=project_root,
        entrypoint=entrypoint_path
    )

    dep_id = deployed.deploy(
        name="critical-banking-production",
        work_pool_name="local-process-pool",
        tags=["production", "critical", "sesi-07"],
        parameters={"trigger_failure": True},
        description=(
            "### Critical Banking Settlement (Production Pipeline)\n"
            "- Dilengkapi tag `critical` untuk pengujian Prefect Automations.\n"
            "- Dieksekusi via Worker di `local-process-pool`.\n"
        ),
        ignore_warnings=True
    )

    print("\n[SUKSES] Deployment berhasil didaftarkan!")
    print(f"Deployment ID: {dep_id}")
    print("\nLangkah Eksplorasi Server Automations:")
    print("-----------------------------------------------------------------")
    print("1. Buka browser: http://127.0.0.1:4200/automations")
    print("2. Buat Automation baru:")
    print("   - Trigger : Flow run state -> Failed")
    print("   - Match   : Tag 'critical'")
    print("   - Actions : Pause deployment 'critical-banking-production'")
    print("3. Picu flow run melalui CLI:")
    print("   prefect deployment run 'critical-banking-settlement/critical-banking-production'")
    print("4. Amati di UI: Begitu run gagal, deployment otomatis berstatus PAUSED!")
    print("=" * 70 + "\n")
