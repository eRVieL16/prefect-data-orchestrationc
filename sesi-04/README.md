# Sesi 4: Konfigurasi & Secret Management (Variables & Blocks)

## Target Pembelajaran
1. **Zero-Hardcoded Credentials:** Menghilangkan semua password database dan API token dari kode sumber.
2. **Prefect Variables:** Mengelola konfigurasi non-sensitif (`ENVIRONMENT`, `PATH`, `BATCH_SIZE`) yang bisa diubah dinamis via UI atau CLI.
3. **Prefect Blocks (Secret):** Menyimpan nilai rahasia terenkripsi di server Prefect.
4. **Pemisahan Dev vs Prod:** Mengatur alur pipeline agar otomatis beradaptasi dengan target environment tanpa modifikasi kode.

---

## Konsep Inti (80/20)

### Perbedaan Variables vs Blocks
| Fitur | Prefect Variables | Prefect Blocks (Secret) |
|---|---|---|
| **Tujuan** | Konfigurasi non-sensitif (string, int, JSON) | Data kredensial sensitif terenkripsi |
| **Visibilitas UI** | Terbaca langsung di tab *Variables* | Ditutupi asterisk (`********`) di tab *Blocks* |
| **Cara Ambil di Kode** | `Variable.get("target_environment")` | `Secret.load("db-password").get()` |
| **Contoh Nilai** | `"development"`, `500`, `"s3://my-bucket"` | Token API, SSH Key, DB Password |

---

## Langkah Praktik

Pastikan Prefect Server Anda aktif di terminal (`prefect server start`) dan dashboard terbuka di `http://127.0.0.1:4200`.

### Langkah 1: Daftarkan Variables & Secrets ke Prefect Server
Jalankan file [01_setup_config.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-04/01_setup_config.py):
```powershell
python sesi-04/01_setup_config.py
```
**Cek di Browser (Prefect UI):**
1. Buka [http://127.0.0.1:4200/variables](http://127.0.0.1:4200/variables) ➔ Anda akan melihat `target_environment`, `data_lake_path`, dan `batch_process_limit`.
2. Buka [http://127.0.0.1:4200/blocks](http://127.0.0.1:4200/blocks) ➔ Anda akan melihat `payment-gateway-token` dan `dwh-database-password`.

### Langkah 2: Jalankan Pipeline Aman (Zero-Hardcode)
Jalankan file [02_secure_pipeline.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-04/02_secure_pipeline.py):
```powershell
python sesi-04/02_secure_pipeline.py
```
**Perhatikan output log:**
- Pipeline membaca config dari server.
- API Token dan DB password dimuat secara terenkripsi lalu di-masking saat dicatat ke log (`sk_l****8899`).
- Karena `target_environment` bernilai `development`, data disimpan ke folder lokal `./data/lake/transactions_dev.txt`.

### Langkah 3: Uji Coba Ubah ke Production via UI (Tanpa Edit Kode!)
1. Buka menu **Variables** di browser UI: [http://127.0.0.1:4200/variables](http://127.0.0.1:4200/variables).
2. Klik variabel `target_environment`, edit nilainya dari `"development"` menjadi `"production"`, lalu klik **Save**.
3. Jalankan kembali script tanpa mengubah 1 baris kode pun:
   ```powershell
   python sesi-04/02_secure_pipeline.py
   ```
4. Perhatikan log: Pipeline otomatis beralih ke mode `[PROD MODE]`!

---

## Review 15 Menit (Lembar Refleksi Sesi 4)
- [ ] **Apakah masih ada kredensial yang tertulis di dalam file kode?**
  *(Tidak ada! Seluruh kredensial tersimpan di Prefect Blocks server).*
- [ ] **Bisakah config Dev dan Prod diganti tanpa mengubah kode?**
  *(Bisa, cukup mengganti nilai variable `target_environment` di UI atau CLI).*
- [ ] **Catat apa yang masih hardcoded di kode Anda?**
  *(Biasanya hanya nama referensi variable/block seperti `"payment-gateway-token"`, yang memang aman untuk di-commit).*
- [ ] **Satu pertanyaan untuk Sesi 5:** *"Bagaimana cara membuat pipeline ini berjalan otomatis setiap jam 02:00 pagi setiap hari tanpa perlu kita jalankan manual?"* (Jawabannya: Deployment & Scheduling via `flow.serve()` di Sesi 5!).
