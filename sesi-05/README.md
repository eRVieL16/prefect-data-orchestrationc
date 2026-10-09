# Sesi 5: Deployment & Scheduling (`flow.serve`)

Selamat datang di **Sesi 5**! Di sesi sebelumnya (Sesi 1–4), kita selalu menjalankan pipeline secara manual di terminal menggunakan perintah `python script.py`. Setelah selesai eksekusi, proses Python langsung berhenti.

Namun di lingkungan produksi data engineering, pipeline harus berjalan **secara otomatis sesuai jadwal** (misal setiap 2 menit, setiap jam 02:00 pagi, atau setiap hari Minggu) serta bisa dipicu sesuai permintaan (**on-demand / Quick Run**) melalui UI atau API tanpa perlu seseorang login ke server secara manual.

---

## 1. Target Pembelajaran Sesi 5
1. **Memahami Konsep Deployment:** Perbedaan mendasar antara *Flow*, *Flow Run*, dan *Deployment*.
2. **Transformasi Script ke Background Service:** Menggunakan `flow.serve()` di Prefect 3.x untuk mendaftarkan dan menjalankan flow runner secara lokal.
3. **Mekanisme Scheduling:** Menentukan jadwal dengan **Interval** (detik/menit) dan format standar **Cron UNIX** (`* * * * *`).
4. **Multi-Deployment Runner:** Melayani beberapa deployment sekaligus dalam 1 proses dengan `prefect.serve()`.
5. **Observabilitas di Prefect UI:**
   - Memantau status *Scheduled Runs* yang akan datang.
   - Melakukan *Quick Run* dan *Custom Run* (mengganti parameter saat eksekusi via UI).
   - Melakukan jeda (*Pause*) dan melanjutkan (*Resume*) jadwal otomatis.

---

## 2. Konsep Inti (80/20 Mental Model)

### Hubungan: Flow vs Flow Run vs Deployment

```mermaid
graph TD
    F[<b>Flow (Python Code)</b><br/>Definisi logika pipeline: @flow] --> D[<b>Deployment (Prefect Server)</b><br/>Metadata: Kapan dijalankan, parameter default, schedule, tag]
    D -->|Otomatis sesuai Schedule| FR1[<b>Flow Run #1</b> (02:00)]
    D -->|Otomatis sesuai Schedule| FR2[<b>Flow Run #2</b> (02:02)]
    D -->|Manual via UI: Quick Run| FR3[<b>Flow Run #3</b> (Ad-hoc)]
    
    style F fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
    style D fill:#ede7f6,stroke:#512da8,stroke-width:2px
    style FR1 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style FR2 fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style FR3 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
```

- **Flow:** Fungsi Python yang dihias `@flow`. Hanya berupa *blue-print* logika data.
- **Deployment:** Pembungkus (*wrapper*) di Prefect Server yang menentukan **bagaimana**, **kapan**, dan **dengan parameter apa** flow tersebut harus dijalankan.
- **Flow Run:** Satu kali instansiasi eksekusi nyata dari flow tersebut.

---

### Cara Penjadwalan: Interval vs Cron

| Tipe Jadwal | Format Contoh | Arti & Kapan Digunakan |
|---|---|---|
| **Interval** | `interval=120` | Berjalan setiap 120 detik (2 menit). Cocok untuk task periodik yang konsisten seperti sensor polling, healthcheck, atau ingest data streaming. |
| **Interval (Timedelta)** | `interval=timedelta(hours=6)` | Berjalan tepat setiap 6 jam sekali. |
| **Cron UNIX** | `cron="0 2 * * *"` | Berjalan tepat pada jam 02:00 pagi setiap hari. Standar industri untuk batch ETL malam hari. |
| **Cron UNIX** | `cron="*/15 * * * *"` | Berjalan setiap 15 menit (menit ke 0, 15, 30, 45). |
| **Cron UNIX** | `cron="0 8 * * 1"` | Berjalan tepat jam 08:00 pagi setiap hari Senin (*1 = Monday*). |

#### Cheat Sheet 5 Kolom Cron UNIX:
```text
┌───────────── menit (0 - 59)
│ ┌─────────── jam (0 - 23)
│ │ ┌───────── hari dalam bulan (1 - 31)
│ │ │ ┌─────── bulan (1 - 12)
│ │ │ │ ┌───── hari dalam sepekan (0 - 6, 0 = Minggu, 1 = Senin)
│ │ │ │ │
* * * * *
```

---

## 3. Langkah Praktik Terpandu

Pastikan terminal pertama Anda tetap menjalankan server Prefect:
```powershell
prefect server start
```
Dan pastikan dashboard UI dapat diakses di: [http://127.0.0.1:4200](http://127.0.0.1:4200).

---

### Praktik 1: Menjalankan Single Deployment (`01_serve_single_deployment.py`)

Buka terminal kedua (pastikan virtual environment `.venv` aktif), lalu jalankan:

```powershell
python sesi-05/01_serve_single_deployment.py
```

#### Apa yang terjadi di Terminal?
1. Script mendaftarkan deployment bernama `sales-scheduled-2min` ke Prefect Server.
2. Terminal akan menampilkan pesan:
   ```text
   Your flow is now being served with the schedule: every 120 seconds.
   ```
3. Terminal ini sekarang berperan sebagai **Prefect Runner** aktif. **Biarkan terminal ini tetap menyala!**

#### Eksplorasi di Prefect UI:
1. Buka browser: [http://127.0.0.1:4200/deployments](http://127.0.0.1:4200/deployments)
2. Anda akan melihat deployment `daily-sales-pipeline/sales-scheduled-2min` dengan status **Ready / Active**.
3. Klik nama deployment tersebut untuk masuk ke halaman detailnya:
   - **Upcoming Runs:** Lihat daftar antrean run yang dijadwalkan secara otomatis setiap 2 menit ke depan.
   - **Parameters:** Nilai default `min_amount = 100,000` dan `dry_run = false`.
   - **Tags:** Label `sales`, `finance`, `sesi-05`.

#### Uji Coba Quick Run & Parameter Override via UI:
1. Di halaman detail deployment di UI, klik tombol **Run** di pojok kanan atas, lalu pilih **Custom run**.
2. Ubah parameter:
   - `min_amount`: Ubah menjadi `1000000` (Rp 1.000.000).
   - `dry_run`: Centang menjadi `True`.
3. Klik tombol **Save and Run**.
4. Lihat terminal runner Anda: runner langsung mendeteksi permintaan eksekusi dan menjalankannya secara instan!
5. Buka tab **Flow Runs** di UI untuk melihat log hasil run tersebut.

#### Uji Coba Pause / Resume Schedule:
1. Di halaman deployment di UI, perhatikan tombol toggle **Schedule** di bagian atas.
2. Klik toggle untuk menonaktifkan (**Pause**). Perhatikan antrean *Upcoming runs* langsung hilang/terjeda.
3. Klik toggle kembali untuk mengaktifkan (**Resume**). Jadwal otomatis langsung aktif kembali.

---

### Praktik 2: Multi-Deployment Service (`02_multi_serve_deployment.py`)

Setelah mencoba Praktik 1, hentikan script dengan menekan `Ctrl + C` di terminal. Sekarang jalankan script multi-deployment:

```powershell
python sesi-05/02_multi_serve_deployment.py
```

#### Konsep yang Diuji:
1. Dua pipeline dengan karakteristik berbeda berjalan dalam **satu proses runner**:
   - `system-healthcheck-flow/infra-heartbeat-60s` (Interval 60 detik)
   - `nightly-financial-audit-flow/nightly-reconciliation-cron` (Cron jam 01:00 AM)
2. Buka tab **Deployments** di UI: Anda akan melihat kedua deployment terdaftar rapi!
3. Anda dapat memicu run ad-hoc untuk salah satu atau keduanya kapan saja.

---

## 4. Review 15 Menit (Lembar Refleksi Sesi 5)

Luangkan waktu 15 menit untuk menjawab pertanyaan refleksi berikut:

- [ ] **Apa perbedaan utama antara menjalankan `my_flow()` langsung vs `my_flow.serve()`?**
  *(Jawaban: `my_flow()` berjalan ad-hoc sekali jalan lalu keluar. `my_flow.serve()` mendaftarkan deployment ke server dan membuka runner service berkesinambungan yang mendengarkan jadwal dan permintaan UI).*
- [ ] **Kapan sebaiknya menggunakan `interval` dan kapan menggunakan `cron`?**
  *(Jawaban: Gunakan `interval` untuk jeda waktu relatif seperti polling tiap 30 detik atau 5 menit. Gunakan `cron` untuk waktu kalender absolut seperti jam 02:00 pagi WIB setiap hari atau hari kerja Senin-Jumat).*
- [ ] **Bagaimana cara mengubah parameter flow tanpa menyentuh kode sama sekali?**
  *(Jawaban: Melalui tombol "Custom run" di Prefect UI atau melalui CLI `prefect deployment run`).*
- [ ] **Pertanyaan Transisi Menuju Sesi 6:**
  *"Jika script `flow.serve()` berjalan di laptop lokal saya, apa yang terjadi jika laptop saya dimatikan atau sleep? Bagaimana jika di production kita punya cluster Docker / Kubernetes terpisah untuk mengeksekusi pipeline?"*
  *(Jawabannya: Inilah alasan kita membutuhkan **Work Pools & Workers** di **Sesi 6**, yang memisahkan orchestration control plane dari distributed execution environment!).*
