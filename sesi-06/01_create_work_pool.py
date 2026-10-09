"""
Sesi 6 - Latihan 1: Membuat & Mengelola Work Pool
------------------------------------------------
Konsep Inti:
1. Control Plane vs Data Plane:
   - Control Plane (Prefect Server) mengatur jadwal, metadata, dan state.
   - Data Plane (Worker di mesin Anda) bertugas mengeksekusi kode aktual.
2. Work Pool:
   Saluran (routing bridge) di Prefect Server tempat flow run ditempatkan.
   Tipe-tipe Work Pool:
   - 'process'    : Menjalankan flow sebagai subprocess lokal di sistem operasi.
   - 'docker'     : Menjalankan flow di dalam container Docker terisolasi.
   - 'kubernetes' : Menjalankan flow sebagai pod / Kubernetes job dinamis.
"""

import sys
import asyncio
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate


async def ensure_work_pool(pool_name: str = "local-process-pool"):
    """
    Memastikan Work Pool dengan tipe 'process' tersedia di Prefect Server.
    Fungsi ini ekuivalen dengan perintah CLI:
    prefect work-pool create local-process-pool --type process
    """
    print("=" * 65)
    print("Memeriksa status Work Pool di Prefect Server...")
    print("=" * 65)

    async with get_client() as client:
        # 1. Cek apakah work pool sudah pernah dibuat
        try:
            pool = await client.read_work_pool(work_pool_name=pool_name)
            print(f"[STATUS] Work Pool '{pool_name}' SUDAH TERSEDIA.")
            print(f"         - ID   : {pool.id}")
            print(f"         - Type : {pool.type}")
            print(f"         - State: {'Paused' if pool.is_paused else 'Active'}")
            return pool
        except Exception:
            print(f"[INFO] Work Pool '{pool_name}' belum ada. Membuat pool baru...")

        # 2. Jika belum ada, buat work pool baru bertipe 'process'
        work_pool_data = WorkPoolCreate(
            name=pool_name,
            type="process",
            description="Work pool lokal untuk mengeksekusi flow via background worker process."
        )
        pool = await client.create_work_pool(work_pool=work_pool_data)
        print(f"[SUKSES] Berhasil membuat Work Pool '{pool.name}'!")
        print(f"         - ID   : {pool.id}")
        print(f"         - Type : {pool.type}")
        return pool


if __name__ == "__main__":
    pool = asyncio.run(ensure_work_pool("local-process-pool"))
    print("\nLangkah berikutnya:")
    print("1. Anda dapat melihat Work Pool ini di browser: http://127.0.0.1:4200/work-pools")
    print("2. Jalankan script '02_deploy_to_worker.py' untuk mendaftarkan deployment ke pool ini.")
    print("=" * 65)
