"""
Sesi 6 - Latihan 3: Trigger Flow Run & Observasi Siklus Hidup Worker
-------------------------------------------------------------------
Konsep Inti:
1. Pengiriman Tugas ke Antrean (Decoupled Queue):
   Saat kita memicu 'run_deployment', tugas dimasukkan ke Work Pool 'local-process-pool'.
2. Apa yang Terjadi Jika Worker Belum Menyala / Sedang Mati?
   Tugas TIDAK AKAN HILANG. Prefect Server menyimpan status run sebagai 'Scheduled'
   (atau 'Late' jika lewat waktu).
   Begitu Worker dinyalakan ('prefect worker start --pool local-process-pool'),
   Worker akan langsung mengambil antrean yang tertunda tersebut dan mengeksekusinya!
"""

import sys
from prefect.deployments import run_deployment


def trigger_churn_pipeline():
    print("=" * 70)
    print("Memicu Eksekusi Deployment ke Work Pool (Decoupled Execution)...")
    print("=" * 70)

    deployment_name = "customer-churn-etl/churn-prediction-worker-pipeline"

    print(f"Target Deployment: {deployment_name}")
    print("Mengirim perintah run dengan parameter custom: chunk_size=20...\n")

    # timeout=0 artinya non-blocking: Perintah kembali segera setelah flow run dibuat di server
    flow_run = run_deployment(
        name=deployment_name,
        parameters={
            "chunk_size": 20,
            "export_format": "CSV"
        },
        timeout=0
    )

    print("-----------------------------------------------------------------")
    print(f"[SUKSES DIKIRIM KE ANTAR-MUKA WORK POOL]")
    print(f"Flow Run ID   : {flow_run.id}")
    print(f"Flow Run Name : {flow_run.name}")
    print(f"Initial State : {flow_run.state.name} ({flow_run.state.type.value})")
    print("-----------------------------------------------------------------")
    print(f"\nLihat detail run di browser:")
    print(f"http://127.0.0.1:4200/runs/flow-run/{flow_run.id}")
    print("\nSKENARIO OBSERVASI:")
    print("1. Jika Worker BELUM menyala:")
    print("   - Run akan berstatus 'Scheduled' atau 'Late'.")
    print("2. Nyalakan Worker di terminal Anda:")
    print("   prefect worker start --pool 'local-process-pool'")
    print("   - Perhatikan bagaimana Worker langsung mengambil run ini dan mengeksekusinya!")
    print("=================================================================\n")


if __name__ == "__main__":
    trigger_churn_pipeline()
