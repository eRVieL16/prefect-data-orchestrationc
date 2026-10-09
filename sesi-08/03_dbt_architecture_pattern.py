"""
Sesi 8 - Latihan 3: Arsitektur Orkestrasi dbt (Data Build Tool) dengan Prefect
-----------------------------------------------------------------------------
Konsep Inti:
1. Mengapa Menggabungkan Prefect & dbt?
   - dbt hanya mengelola transformasi SQL di dalam Data Warehouse.
   - dbt TIDAK BISA:
     * Mengambil data dari REST API atau SFTP eksternal.
     * Mengorkestrasikan retraining model Machine Learning Python.
     * Mengirim payload webhook khusus ke internal notification engine.
   - PREFECT adalah konduktor master:
     [Ingest Python] -> [dbt build (SQL Transform & Test)] -> [ML / Reverse ETL] -> [Discord Alert]
2. Menjalankan & Memvalidasi Hasil dbt di Prefect:
   Prefect mengeksekusi 'dbt build', memantau status setiap model & test,
   dan menghentikan DAG jika terdapat test yang gagal (*data breach*).
"""

import time
from prefect import flow, task, get_run_logger


# =====================================================================
# TASK 1: UPSTREAM INGESTION (DI LUAR JANGKAUAN DBT)
# =====================================================================
@task(name="upstream-api-ingest")
def upstream_api_ingest() -> dict:
    logger = get_run_logger()
    logger.info("[UPSTREAM] Mengunduh dump data harian dari Payment API & Salesforce CRM...")
    return {"status": "SUCCESS", "records_ingested": 15000}


# =====================================================================
# TASK 2: DBT ORCHESTRATION (TRANSFORM & TEST)
# =====================================================================
@task(name="run-dbt-build")
def run_dbt_build(select_tag: str = "tag:finance", fail_test: bool = False) -> dict:
    """
    Mensimulasikan eksekusi 'dbt build' (menjalankan model SQL + schema test).
    Di produksi, ini dapat memanggil 'prefect-dbt' atau subprocess CLI 'dbt build'.
    """
    logger = get_run_logger()
    logger.info(f"[DBT RUNNER] Menjalankan: dbt build --select {select_tag}...")

    # Simulasi eksekusi model dbt
    models = ["stg_payments", "stg_crm_accounts", "int_customer_orders", "fct_monthly_revenue"]
    for m in models:
        logger.info(f"  └── [dbt model] Compiling & executing model: {m} (OK)")
        time.sleep(0.1)

    # Simulasi eksekusi schema tests dbt (not_null, unique, relationships)
    logger.info("[DBT TEST] Menjalankan automated schema assertions...")
    tests = [
        "unique_fct_monthly_revenue_account_id",
        "not_null_fct_monthly_revenue_amount",
        "relationships_fct_monthly_revenue_customer_id"
    ]
    for t in tests:
        logger.info(f"  └── [dbt test] Running assertion: {t} (PASS)")

    if fail_test:
        test_fail_name = "accepted_values_fct_monthly_revenue_currency"
        logger.error(f"  └── [dbt test] CRITICAL ERROR: {test_fail_name} (FAIL: expected ['IDR', 'USD'], found ['INVALID'])")
        raise RuntimeError(f"dbt test assertion gagal: {test_fail_name}")

    return {
        "models_built": len(models),
        "tests_passed": len(tests),
        "dbt_status": "SUCCESS"
    }


# =====================================================================
# TASK 3: DOWNSTREAM CONSUMER (REVERSE ETL / BI SYNC)
# =====================================================================
@task(name="downstream-reverse-etl-sync")
def downstream_reverse_etl_sync(dbt_result: dict):
    logger = get_run_logger()
    logger.info(f"[DOWNSTREAM] dbt berhasil menyelesaikan {dbt_result['models_built']} model & {dbt_result['tests_passed']} test.")
    logger.info("[DOWNSTREAM] Mensinkronisasi mart terverifikasi ke CRM Salesforce & PowerBI...")
    return "SYNC_COMPLETED"


# =====================================================================
# MASTER ELT FLOW
# =====================================================================
@flow(
    name="master-elt-dbt-orchestration",
    description="Master pipeline orkestrasi hulu-ke-hilir: Ingestion -> dbt Build -> Reverse ETL."
)
def master_elt_dbt_flow(fail_dbt_test: bool = False):
    logger = get_run_logger()
    logger.info("==================================================")
    logger.info("Memulai Master ELT Orchestration (Prefect + dbt)")
    logger.info("==================================================")

    # 1. Hulu: Ekstraksi & Ingesti API (Python)
    ingest_result = upstream_api_ingest()

    # 2. Tengah: Transformasi & Test SQL Warehouse (dbt)
    dbt_result = run_dbt_build(select_tag="tag:finance", fail_test=fail_dbt_test)

    # 3. Hilir: Reverse ETL & BI Refresh (Hanya jika dbt 100% lolos test!)
    sync_status = downstream_reverse_etl_sync(dbt_result=dbt_result)

    logger.info(f"Seluruh siklus data warehouse selesai dengan status: {sync_status}!")


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("MENGUJI ORKESTRASI MASTER ELT (PREFECT + DBT PATTERN)")
    print("=" * 65)

    master_elt_dbt_flow(fail_dbt_test=False)
