"""
Sesi 4 - Langkah 1: Setup Variables & Secret Blocks
---------------------------------------------------
Tujuan: Menyimpan konfigurasi dan kredensial sensitif ke server Prefect.
Dengan cara ini:
1. Kode Python tidak lagi menyimpan password atau API key secara hardcoded.
2. Konfigurasi dapat diubah sewaktu-waktu langsung dari Prefect UI tanpa deploy ulang kode.
"""

from prefect.variables import Variable
from prefect.blocks.system import Secret

def setup_prefect_configurations():
    print("=== [1/2] Menyiapkan Prefect Variables (Konfigurasi Non-Sensitif) ===")
    
    # 1. Target Environment (development / staging / production)
    Variable.set(
        name="target_environment",
        value="development",
        overwrite=True
    )
    print("[OK] Variable 'target_environment' diset ke: 'development'")

    # 2. Path penyimpanan data lake lokal/cloud
    Variable.set(
        name="data_lake_path",
        value="./data/lake",
        overwrite=True
    )
    print("[OK] Variable 'data_lake_path' diset ke: './data/lake'")

    # 3. Limit pemrosesan per batch
    Variable.set(
        name="batch_process_limit",
        value=500,
        overwrite=True
    )
    print("[OK] Variable 'batch_process_limit' diset ke: 500")


    print("\n=== [2/2] Menyiapkan Prefect Blocks (Kredensial Sensitif / Encrypted) ===")
    
    # 1. API Token Gateway Pembayaran
    payment_secret = Secret(value="sk_live_mock_super_secret_token_8899")
    payment_secret.save("payment-gateway-token", overwrite=True)
    print("[OK] Secret Block 'payment-gateway-token' berhasil disimpan.")

    # 2. Password Database Data Warehouse
    db_secret = Secret(value="PostgreP@ssw0rdProd2026!")
    db_secret.save("dwh-database-password", overwrite=True)
    print("[OK] Secret Block 'dwh-database-password' berhasil disimpan.")

    print("\n=== SETUP SELESAI ===")
    print("Buka browser di http://127.0.0.1:4200:")
    print("- Menu 'Variables' untuk melihat variabel yang terdaftar.")
    print("- Menu 'Blocks' untuk melihat Secret Block yang terenkripsi.")

if __name__ == "__main__":
    setup_prefect_configurations()
