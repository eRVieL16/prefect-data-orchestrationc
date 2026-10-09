import sys
import os
from prefect import flow, task, get_run_logger

@task(name="fetch-warehouse-data")
def fetch_warehouse_data(limit: int) -> list[str]:
    logger = get_run_logger()
    logger.info(f"[EXTRACT] Worker mengambil {limit} baris data warehouse...")
    return [f"record_{i}" for i in range(limit)]

@flow(name="worker-pipeline-flow")
def worker_pipeline_flow(batch_size: int = 50):
    logger = get_run_logger()
    logger.info("=== Menjalankan worker_pipeline_flow di dalam Worker Isolated Process ===")
    data = fetch_warehouse_data(limit=batch_size)
    logger.info(f"=== Selesai memproses {len(data)} record via Worker ===")

if __name__ == "__main__":
    # Registrasi deployment ke work pool
    deploy_id = worker_pipeline_flow.deploy(
        name="worker-process-deployment",
        work_pool_name="local-process-pool",
        build=False,
        push=False,
        parameters={"batch_size": 25},
        tags=["sesi-06", "worker", "process"],
        description="Deployment yang dieksekusi oleh Prefect Worker terisolasi."
    )
    print(f"Deployment berhasil didaftarkan ke work pool 'local-process-pool' dengan ID: {deploy_id}")
