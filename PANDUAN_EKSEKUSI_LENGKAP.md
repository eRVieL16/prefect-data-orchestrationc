# Panduan Eksekusi Lengkap: Masterclass Prefect 20 Jam (3.x)

Panduan ini adalah modul operasional pendamping untuk [rencana-belajar-prefect-20-jam.md](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/rencana-belajar-prefect-20-jam.md). Dokumen ini memandu Anda langkah demi langkah dalam membangun portfolio orkestrasi data siap-produksi dengan prinsip 80/20.

---

## 1. Setup Lingkungan Kerja (Sekali di Awal)

Sistem Anda memiliki Python 3.12 dan 3.14. Untuk orkestrasi data (Prefect, Pydantic, SQLAlchemy, DuckDB), **gunakan Python 3.12** demi kompatibilitas roda C/binary (wheels) yang stabil di Windows.

### Langkah Setup Virtual Environment:
Buka PowerShell di folder proyek ini (`Prefect tutor`):
```powershell
# 1. Buat virtual environment berbasis Python 3.12
py -3.12 -m venv .venv

# 2. Aktifkan virtual environment
.\.venv\Scripts\Activate.ps1

# Jika ada error script execution policy di Windows PowerShell, jalankan:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# 3. Upgrade pip dan install dependensi proyek
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 2. Struktur Direktori Repositori Portofolio

Agar hasil belajar langsung menjadi aset portofolio GitHub:

```text
Prefect tutor/
│
├── .gitignore
├── requirements.txt
├── rencana-belajar-prefect-20-jam.md      # Silabus 20 jam
├── PANDUAN_EKSEKUSI_LENGKAP.md            # Buku panduan ini
│
├── sesi-01/   # Mental model & First Flow
│   ├── README.md
│   ├── 01_script_biasa.py
│   └── 02_flow_dasar.py
│
├── sesi-02/   # Ketahanan: Retry, Caching, State
├── sesi-03/   # Parameter, Subflow & Konkurensi (.map / .submit)
├── sesi-04/   # Variables & Blocks (Secret Management)
├── sesi-05/   # Deployment & Scheduling (flow.serve)
├── sesi-06/   # Work Pool & Worker Architecture
├── sesi-07/   # Automations & Alerts
├── sesi-08/   # Data Stack Integration (DuckDB / dbt)
├── sesi-09/   # Testing (pytest) & CI/CD
└── sesi-10/   # Capstone Project & Komparasi (Airflow vs Prefect vs Dagster)
```

---

## 3. Rincian Teknis & Panduan Per Sesi

### Sesi 1: Mental Model & Setup
- **Konsep:** `@task` (unit kerja terkecil), `@flow` (orkestrator), dan `Flow Run` (riwayat eksekusi).
- **Latihan:** Ubah script ETL tradisional menjadi Prefect flow terkelola.
- **Perintah Penting:**
  ```powershell
  # Menjalankan server UI Prefect secara lokal (default: port 4200)
  prefect server start
  
  # Menjalankan script flow
  python sesi-01/02_flow_dasar.py
  ```
- **File Latihan:** [sesi-01/01_script_biasa.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-01/01_script_biasa.py) & [sesi-01/02_flow_dasar.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-01/02_flow_dasar.py).

---

### Sesi 2: Ketahanan Pipeline (Retries, Timeout, Caching)
- **Konsep:** Pipeline data di dunia nyata pasti akan mengalami gangguan jaringan, timeout API pihak ketiga, atau database lock.
- **Sintaks Kunci:**
  ```python
  from prefect import task
  from prefect.tasks import task_input_hash
  from datetime import timedelta

  @task(
      retries=3, 
      retry_delay_seconds=10, 
      timeout_seconds=60,
      cache_key_fn=task_input_hash, 
      cache_expiration=timedelta(hours=2)
  )
  def fetch_api_data(url: str):
      ...
  ```
- **Praktik:** Simulasikan API yang gagal 2 kali sebelum berhasil pada percobaan ke-3. Cek bagaimana UI mencatat state `Retrying` -> `Completed`.

---

### Sesi 3: Parameter, Subflow, & Konkurensi
- **Konsep:** Validasi input menggunakan Python type hints (Pydantic di balik layar), subflow untuk modularitas, dan eksekusi paralel menggunakan `.map()` atau `.submit()`.
- **Sintaks Kunci:**
  ```python
  @task
  def process_store_data(store_id: int):
      ...

  @flow
  def main_orchestrator(store_ids: list[int] = [1, 2, 3, 4]):
      # Menjalankan task secara paralel untuk setiap store
      results = process_store_data.map(store_ids)
  ```
- **Praktik:** Proses partisi data per toko/region secara konkruen tanpa blocking.

---

### Sesi 4: Konfigurasi & Secret Management (Variables & Blocks)
- **Konsep:** *Twelve-Factor App* — jangan pernah hardcode kredensial database atau token API di dalam script git.
- **Prefect CLI:**
  ```powershell
  # Membuat variabel global
  prefect variable set ENVIRONMENT "development"
  prefect variable set MAX_BATCH_SIZE 500
  ```
- **Sintaks Python:**
  ```python
  from prefect.variables import Variable
  from prefect.blocks.system import Secret

  env = Variable.get("ENVIRONMENT")
  # api_key = Secret.load("my-api-key").get()
  ```

---

### Sesi 5: Deployment & Scheduling
- **Konsep:** Menghubungkan flow ke jadwal otomatis (Cron / Interval) dan mengaktifkan background runner.
- **Pendekatan Paling Cepat di Prefect 3.x (`flow.serve`):**
  ```python
  if __name__ == "__main__":
      etl_flow.serve(
          name="daily-etl-deployment",
          cron="0 2 * * *",  # Jam 02:00 pagi setiap hari
          tags=["production", "daily"]
      )
  ```
- **Praktik:**
  1. [sesi-05/01_serve_single_deployment.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-05/01_serve_single_deployment.py): Single deployment dengan interval schedule dan parameter override via UI.
  2. [sesi-05/02_multi_serve_deployment.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-05/02_multi_serve_deployment.py): Multi-deployment runner (`prefect.serve`) memadukan interval monitoring dan cron reconciliation.
- **Panduan Detail:** Baca [sesi-05/README.md](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-05/README.md).

---

### Sesi 6: Arsitektur Work Pool & Worker
- **Konsep:** Memisahkan penjadwal (*control plane*) dari mesin eksekutor (*data plane / execution environment*).
  - Alur: `Deployment Schedule` -> `Work Queue` -> `Work Pool` -> `Worker (Polling)` -> `Flow Run execution`.
- **Perintah CLI & Praktik:**
  1. [sesi-06/01_create_work_pool.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-06/01_create_work_pool.py): Membuat work pool tipe `process` via API/CLI.
  2. [sesi-06/02_deploy_to_worker.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-06/02_deploy_to_worker.py): Mendaftarkan deployment `customer-churn-etl` ke `local-process-pool`.
  3. [sesi-06/03_trigger_and_monitor.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-06/03_trigger_and_monitor.py): Membuka antrean run dan membuktikan ketahanan antrean saat worker belum/sedang aktif.
- **Panduan Detail:** Baca [sesi-06/README.md](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-06/README.md).

---

### Sesi 7: Automations & Notifikasi
- **Konsep:** Observabilitas otomatis & zero-noise alerting. Tidak perlu memantau UI 24/7.
- **Praktik:**
  1. [sesi-07/01_state_change_hook.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-07/01_state_change_hook.py): State change hook `on_failure` dengan generasi laporan insiden JSON.
  2. [sesi-07/02_task_and_flow_notifications.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-07/02_task_and_flow_notifications.py): Task hook vs flow hook dan universal webhook card dispatcher (Slack / Discord).
  3. [sesi-07/03_production_flow_with_automation.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-07/03_production_flow_with_automation.py): Pipeline kritis dengan tag `critical` untuk pengujian Prefect UI Automations (circuit-breaker auto-pause).
- **Panduan Detail:** Baca [sesi-07/README.md](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-07/README.md).

---

### Sesi 8: Integrasi Data Stack (DuckDB & dbt)
- **Konsep:** Pola modern ELT (Extract, Load, Transform in-warehouse) dan Data Quality Gatekeeping.
- **Praktik:**
  1. [sesi-08/01_duckdb_elt_pipeline.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-08/01_duckdb_elt_pipeline.py): Pipeline ELT berlapis (Raw -> Staging -> Business Analytics Mart) menggunakan DuckDB SQL.
  2. [sesi-08/02_data_quality_tests.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-08/02_data_quality_tests.py): Data Quality Assertion tests (Unique, Not Null, Range checks) dengan circuit breaker dan notifikasi Discord saat data breach.
  3. [sesi-08/03_dbt_architecture_pattern.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-08/03_dbt_architecture_pattern.py): Master ELT orchestrator menghubungkan API Ingest -> dbt build -> Reverse ETL/BI sync.
- **Panduan Detail:** Baca [sesi-08/README.md](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-08/README.md).

---

### Sesi 9: Praktik Produksi (Testing & CI/CD)
- **Konsep:** Unit testing deterministik (`my_task.fn`), prinsip idempotency, dan otomatisasi CI/CD.
- **Praktik:**
  1. [sesi-09/pipeline_code.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-09/pipeline_code.py): Pipeline modular siap produksi dengan partition overwrite idempotency.
  2. [sesi-09/test_pipeline.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-09/test_pipeline.py): Test suite `pytest` menguji task via `.fn` dan memvalidasi pencegahan data duplikat.
  3. [.github/workflows/ci.yml](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/.github/workflows/ci.yml): GitHub Actions CI workflow untuk linting dan testing otomatis sebelum deploy.
- **Panduan Detail:** Baca [sesi-09/README.md](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-09/README.md).

---

### Sesi 10: Capstone Project & Komparasi Eksekutif
- **Deliverable:**
  - Pipeline ETL terpadu yang memuat data harian, menangani error retry, memakai variables, dan memiliki schedule aktif.
  - Dokumentasi README & diagram arsitektur.
- **Cheat Sheet Komparasi untuk Wawancara Data Engineer / Analytics Engineer:**

| Aspek | Prefect (3.x) | Apache Airflow | Dagster |
|---|---|---|---|
| **Paradigma** | Code-as-Workflows (Pythonic murni, decorator `@flow` & `@task`) | DAG-centric (Workflows terikat struktur DAG kaku) | Asset-centric (Software-Defined Assets) |
| **Eksekusi Lokal** | Sangat mudah: tinggal `python script.py` langsung jalan | Berat: butuh webserver, scheduler, postgres, redis (sering via Docker) | Sedang: butuh `dagster dev` |
| **Passing Data Antar Task** | Alami (return value fungsi Python biasa diteruskan ke task berikutnya) | Rumit (XCom dengan batasan ukuran metadata) | Terstruktur lewat Asset I/O managers |
| **Dukungan Dynamic / Parameter** | Native (looping biasa, `.map()`, validasi Pydantic) | Terbatas (Dynamic Task Mapping baru ada di Airflow 2.3+) | Native lewat dynamic partitions |
| **Kapan Memilih?** | Tim Python/Data modern yang ingin cepat, dinamis, fleksibel tanpa overhead infra berat | Standar industri legacy di korporasi besar dengan ribuan DAG terjadwal | Tim yang sangat fokus pada asset lineage dan data catalog |

---

## 4. Pola Belajar Harian 2 Jam (Prinsip 80/20)

Agar konsisten menyelesaikan 10 sesi:
1. **Menit 00–20:** Pahami konsep inti & baca referensi dokumentasi terkait.
2. **Menit 20–105:** Hands-on koding (tulis kode, simulasikan error, amati UI).
3. **Menit 105–120:** Review 15 menit:
   - Tulis 3 hal yang baru dipahami.
   - Catat 1 kendala atau hal yang masih ragu.
   - Buat 1 pertanyaan pemicu untuk sesi selanjutnya.
