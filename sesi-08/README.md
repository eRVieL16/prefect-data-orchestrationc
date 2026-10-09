# Sesi 8: Integrasi Data Stack (DuckDB & dbt)

Selamat datang di **Sesi 8**! Di sesi sebelumnya (Sesi 1–7), kita telah menguasai seluruh aspek orkestrasi: mulai dari task & flow dasar, retry, parameter, secrets, scheduling, worker pools, hingga automations & alerting.

Sekarang, kita mengintegrasikan Prefect dengan **Modern Data Stack (MDS)** sesungguhnya:
- **DuckDB**: *High-performance in-process OLAP analytical database* (standar emas pemrosesan analitik cepat).
- **dbt (Data Build Tool) Pattern**: Transformasi data berbasis SQL dengan pengujian kualitas data (*Data Quality Assertions*) otomatis.

---

## 1. Target Pembelajaran Sesi 8
1. **Pergeseran Paradigma ETL ke ELT:** Memahami mengapa tim data modern memindahkan data mentah terlebih dahulu ke warehouse (*Extract & Load*), lalu melakukan transformasi di dalam database menggunakan SQL (*Transform*).
2. **Arsitektur Medallion / Layering Data Warehouse:**
   - **Raw Layer:** Data mentah persis seperti aslinya dari API/sumber.
   - **Staging Layer:** Standardisasi tipe data, pembersihan nama kolom, dan filter dasar.
   - **Marts Layer:** Tabel agregasi bisnis (Daily Revenue, AOV, User Lifetime Value) yang siap dikonsumsi dashboard BI.
3. **Data Quality Gatekeeping (dbt Testing Pattern):** Menghentikan pipeline secara otomatis jika ditemukan data rusak (*Unique key duplicate, NULL value, transaksi negatif*).
4. **Prefect sebagai Master Conductor vs dbt Cloud Scheduler:** Mengetahui keunggulan Prefect dalam mengorkestrasikan siklus data hulu-ke-hilir.

---

## 2. Konsep Inti (80/20 Mental Model)

### Diagram Alur Modern ELT dengan Data Quality Sentinel

```mermaid
graph TD
    subgraph Fase 1: Extract & Load (Python / Prefect)
        API[External Source / API] -->|Extract| P1[Task: extract_and_load]
        P1 -->|Bulk Insert| RAW[(DuckDB: raw_orders)]
    end

    subgraph Fase 2: Transform (In-Warehouse SQL)
        RAW -->|SQL Cleaning| STG[(DuckDB: stg_orders)]
    end

    subgraph Fase 3: Data Quality Sentinel (dbt Test Assertions)
        STG --> T1{<b>Quality Gatekeeper</b><br/>Unique? Not Null? > 0?}
        T1 -->|FAIL: Anomali Terdeteksi| HALT[🚨 <b>Abort Pipeline!</b><br/>Kirim Discord Alert]
    end

    subgraph Fase 4: Production Mart (BI & ML Ready)
        T1 -->|PASS: 100% Valid| PROD[(DuckDB: prod_mart_orders)]
        PROD --> BI[PowerBI / Metabase / Tableau]
    end

    style RAW fill:#eceff1,stroke:#455a64,stroke-width:2px
    style STG fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
    style T1 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style HALT fill:#ffebee,stroke:#c62828,stroke-width:2px
    style PROD fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
```

---

### Mengapa Prefect Lebih Baik Dibanding dbt Cloud Scheduler?

Banyak praktisi bertanya: *"dbt Cloud sudah punya scheduler sendiri, mengapa kita butuh Prefect?"*

| Kebutuhan | dbt Cloud Scheduler | Prefect Master Orchestrator |
|---|---|---|
| **Transformasi SQL dalam Warehouse** | ✅ Ya | ✅ Ya (via `prefect-dbt` / CLI task) |
| **Ingesti Data dari REST API / SFTP** | ❌ Tidak bisa | ✅ Sangat fleksibel dengan Python task |
| **Retraining Model ML (Scikit-Learn/PyTorch)** | ❌ Tidak bisa | ✅ Bisa langsung dijalankan setelah dbt selesai |
| **Reverse ETL (Push data ke CRM Salesforce)** | ❌ Terbatas | ✅ Terintegrasi mulus dalam 1 DAG |
| **Alerting Kustom ke Discord / Slack Webhook** | ❌ Format kaku | ✅ Fleksibel dengan rich embed & state hook |

> [!NOTE]
> **Kesimpulan:** dbt adalah spesialis transformasi SQL di dalam warehouse, sedangkan **Prefect adalah konduktor master** yang menghubungkan seluruh ekosistem data hulu-ke-hilir.

---

## 3. Langkah Praktik Terpandu

Pastikan virtual environment `.venv` aktif di terminal Anda.

---

### Praktik 1: Modern ELT Pipeline dengan DuckDB (`01_duckdb_elt_pipeline.py`)

Jalankan pipeline ELT in-database:

```powershell
python sesi-08/01_duckdb_elt_pipeline.py
```

**Amati Terminal & Log:**
1. **Task 1 (EL):** Memuat 120 baris data transaksi ke tabel DuckDB `raw_orders` di `./data/warehouse/analytics.duckdb`.
2. **Task 2 (Staging):** Mengeksekusi SQL untuk membersihkan status `CANCELLED` dan melakukan casting tipe ke `stg_orders`.
3. **Task 3 (Mart):** Menghitung agregasi omset harian, jumlah pembeli unik, dan Average Order Value (AOV) per kategori produk ke `mart_daily_revenue`.
4. **Task 4 (Reporting):** Mencetak matriks tabel analitik siap pakai langsung di Prefect Logger.

---

### Praktik 2: Data Quality Gatekeeping (`02_data_quality_tests.py`)

Sekarang kita menguji perlindungan data produksi dari data anomali / korup.

#### A. Uji Coba Mode Normal (Data Bersih)
Jalankan script tanpa argumen:
```powershell
python sesi-08/02_data_quality_tests.py
```
**Hasil:**
- Seluruh assertion test (Unique ID, Not Null, dan Nominal > 0) **LOLOS 100%**.
- Data dipromosikan ke tabel produksi `prod_mart_orders`. Status Flow: **Completed**.

#### B. Uji Coba Mode Anomali (Simulasi Data Rusak)
Jalankan script dengan flag `--bad-data`:
```powershell
python sesi-08/02_data_quality_tests.py --bad-data
```
**Perhatikan Reaksi Sistem:**
1. Data rusak diinjeksikan (ada ID duplikat, customer NULL, dan nominal negatif -500,000).
2. Task `run-data-quality-tests` menangkap ketiga anomali tersebut dan melempar `DataQualityBreachException`.
3. Task downstream `promote-to-production-mart` **DIBATALKAN**! Tabel produksi Anda tetap bersih dan terlindungi dari data sampah.
4. Hook alert otomatis memicu pengiriman pesan peringatan ke **Discord Webhook** Anda!

---

### Praktik 3: Master ELT & dbt Architecture Pattern (`03_dbt_architecture_pattern.py`)

Jalankan script pola arsitektur dbt:

```powershell
python sesi-08/03_dbt_architecture_pattern.py
```

**Konsep yang Teramati:**
1. Pipeline mengorkestrasikan seluruh rantai nilai data:
   - **Hulu:** Ingesti API eksternal (di luar kemampuan dbt).
   - **Tengah:** Eksekusi model dbt (`stg_payments`, `fct_monthly_revenue`) dan validasi schema assertions.
   - **Hilir:** Sinkronisasi hasil ke Reverse ETL / BI hanya jika dbt lolos 100%.

---

## 4. Review 15 Menit (Lembar Refleksi Sesi 8)

Luangkan waktu 15 menit untuk merefleksikan integrasi ini:

- [ ] **Mengapa testing kualitas data harus dilakukan SEBELUM data masuk ke tabel produksi Mart?**
  *(Jawaban: Untuk mencegah silent data corruption. Sekali data salah masuk ke tabel produksi, laporan keuangan/eksekutif akan keliru dan sangat sulit melacak sumber kesalahannya).*
- [ ] **Kapan sebaiknya transformasi dilakukan via Python pandas vs via SQL di DuckDB/Warehouse?**
  *(Jawaban: Gunakan SQL in-warehouse untuk agregasi, filtering, joining, dan pembersihan data terstruktur karena jauh lebih cepat dan hemat RAM. Gunakan Python saat membutuhkan library khusus seperti regex kompleks, parsing XML/JSON tidak terstruktur, atau pemanggilan model Machine Learning).*
- [ ] **Catat keuntungan utama menjalankan dbt di dalam Prefect flow:**
  *(Jawaban: Visibilitas terpadu di satu UI dashboard, kemampuan menghentikan proses hulu/hilir, serta alert insiden terpusat saat test dbt gagal).*
- [ ] **Pertanyaan Pengantar untuk Sesi 9:**
  *"Bagaimana kita menulis Unit Test untuk task Prefect kita menggunakan pytest, dan bagaimana memastikan pipeline berjalan secara idempoten di CI/CD GitHub Actions?"*
  *(Jawabannya: Sesi 9: Testing & CI/CD Praktik Produksi!).*
