# Sesi 9: Praktik Produksi (Testing & CI/CD)

Selamat datang di **Sesi 9**! Di sesi sebelumnya (Sesi 1–8), kita telah membangun pipeline data modern dengan DuckDB dan pengujian kualitas data.

Namun, di lingkungan rekayasa data profesional (*Production-Grade Data Engineering*), sebuah pipeline **tidak boleh di-deploy langsung ke server tanpa otomatisasi pengujian**. Kode yang belum teruji berisiko merusak sistem, menduplikasi data keuangan, atau menghentikan worker di produksi.

Di Sesi 9 ini, kita mempelajari 2 pilar utama rekayasa perangkat lunak modern untuk Data Engineer:
1. **Unit Testing deterministik dengan `pytest`** (menggunakan atribut `.fn` pada task).
2. **Prinsip Idempotensi (Idempotency)** pada layer penyimpanan data.
3. **Continuous Integration (CI/CD)** via **GitHub Actions**.

---

## 1. Target Pembelajaran Sesi 9
1. **Menguji Prefect Task via `.fn`:** Memanggil fungsi task secara murni (*pure Python function*) tanpa overhead Prefect Engine atau koneksi server.
2. **Pola Safe Logger:** Menangani logging agar tidak memicu `MissingContextError` saat task diuji di luar flow.
3. **Memverifikasi Idempotensi Pipeline:** Membuktikan secara matematis bahwa menjalankan pipeline 2x pada partisi tanggal yang sama menghasilkan jumlah baris yang identik (anti data duplikat).
4. **Automated CI/CD Pipeline:** Mengonfigurasi GitHub Actions untuk menguji linting, syntax, dan test suite secara otomatis sebelum pull request di-merge.

---

## 2. Konsep Inti (80/20 Mental Model)

### Mengapa Testing Task Sangat Cepat di Prefect 3.x? (`.fn` Attribute)

Di banyak tool orkestrasi lama (seperti Airflow), menguji satu fungsi transformasi membutuhkan mock context yang rumit atau menjalankan database Airflow lokal yang lambat.

Di Prefect, setiap `@task` membungkus fungsi Python biasa. Anda dapat mengakses fungsi murni tersebut melalui atribut **`.fn`**:

```python
# Task Prefect:
@task
def calculate_tax(amount: float) -> float:
    return amount * 0.11

# Di file unit test (test_pipeline.py):
def test_tax():
    # Panggil langsung via .fn tanpa perlu Prefect Server aktif!
    result = calculate_tax.fn(amount=100000.0)
    assert result == 11000.0  # Selesai dalam 0.001 detik!
```

---

### Prinsip Idempotensi (Idempotency)

> [!IMPORTANT]
> **Definisi:** Suatu operasi dikatakan **idempoten** jika menjalankannya 1 kali, 10 kali, atau 100 kali dengan input yang sama menghasilkan **kondisi akhir database yang persis sama**.

#### Masalah Tanpa Idempotensi:
Jika pipeline Anda hanya melakukan `INSERT INTO orders` dan suatu hari terjadi kegagalan jaringan di tengah jalan lalu worker melakukan *auto-retry*, data pesanan akan **terduplikasi 2 kali lipat**, menyebabkan laporan keuangan perusahaan berlipat ganda!

#### Solusi: Pola Partition Overwrite (Delete-then-Insert):
```sql
-- 1. Hapus partisi tanggal terkait terlebih dahulu (jika ada data sisa sebelumnya)
DELETE FROM tax_settlement_ledger WHERE partition_date = '2026-10-09';

-- 2. Baru masukkan data yang bersih
INSERT INTO tax_settlement_ledger VALUES (...);
```

---

### Alur CI/CD GitHub Actions

```mermaid
graph LR
    Dev[Developer: git push] --> GH[GitHub Actions CI Runner]
    GH --> Step1[1. Checkout & Setup Python 3.12]
    Step1 --> Step2[2. Install requirements.txt]
    Step2 --> Step3[3. Linting & Syntax Compile]
    Step3 --> Step4[4. pytest sesi-09/test_pipeline.py]
    Step4 -->|Test Lolos (Hijau)| Deploy[✅ Siap Deploy ke Production Work Pool]
    Step4 -->|Test Gagal (Merah)| Block[❌ PR Ditolak / Deploy Dibatalkan]

    style Dev fill:#e1f5fe,stroke:#0288d1,stroke-width:2px
    style GH fill:#ede7f6,stroke:#512da8,stroke-width:2px
    style Step4 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style Deploy fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    style Block fill:#ffebee,stroke:#c62828,stroke-width:2px
```

---

## 3. Langkah Praktik Terpandu

Pastikan terminal Anda memiliki virtual environment `.venv` aktif.

---

### Praktik 1: Eksplorasi Kode Pipeline ([sesi-09/pipeline_code.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-09/pipeline_code.py))

Jalankan pipeline pajak secara manual:

```powershell
python sesi-09/pipeline_code.py
```

**Amati Fitur di Kode:**
- `normalize_transaction`: Normalisasi ID transaksi dan casting tipe aman.
- `calculate_tax_and_discount`: Perhitungan diskon berjenjang (*Regular/Gold/Platinum*) dan PPN 11%.
- `save_partitioned_transactions`: Mempraktekkan *Delete-then-Insert* pada partisi DuckDB.
- `get_safe_logger()`: Helper cerdas agar task bisa menggunakan Prefect Logger di produksi, namun *fallback* ke standard logger saat diuji dengan pytest.

---

### Praktik 2: Menjalankan Pytest Test Suite ([sesi-09/test_pipeline.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-09/test_pipeline.py))

Jalankan test suite menggunakan pytest dengan opsi verbose:

```powershell
pytest sesi-09/test_pipeline.py -v
```

**Perhatikan Output Hijau:**
```text
collected 8 items

sesi-09/test_pipeline.py::test_normalize_transaction_valid PASSED        [ 12%]
sesi-09/test_pipeline.py::test_normalize_transaction_invalid_amount PASSED [ 25%]
sesi-09/test_pipeline.py::test_calculate_tax_and_discount_tiers[REGULAR-0.0] PASSED [ 37%]
sesi-09/test_pipeline.py::test_calculate_tax_and_discount_tiers[GOLD-0.05] PASSED [ 50%]
sesi-09/test_pipeline.py::test_calculate_tax_and_discount_tiers[PLATINUM-0.1] PASSED [ 62%]
sesi-09/test_pipeline.py::test_calculate_tax_negative_amount_rejected PASSED [ 75%]
sesi-09/test_pipeline.py::test_idempotent_partition_storage PASSED       [ 87%]
sesi-09/test_pipeline.py::test_financial_tax_pipeline_end_to_end PASSED  [100%]

============================== 8 passed in 5.31s ==============================
```

> [!TIP]
> **Sorotan Test `test_idempotent_partition_storage`:**
> Test ini menjalankan task penyimpanan **2 kali berturut-turut** dengan data partisi yang sama, lalu memeriksa database apakah jumlah baris tetap 2 (bukan 4). Karena lolos (*PASSED*), ini adalah bukti validasi matematis bahwa pipeline Anda idempoten!

---

### Praktik 3: Memeriksa Workflow CI/CD ([.github/workflows/ci.yml](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/.github/workflows/ci.yml))

Buka file [.github/workflows/ci.yml](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/.github/workflows/ci.yml) (atau duplikatnya di [sesi-09/github_ci_workflow.yml](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-09/github_ci_workflow.yml)).

**Struktur Otomatisasi:**
1. Berjalan di `ubuntu-latest` setiap ada `push` atau `pull_request` ke branch `main`/`master`.
2. Menginstall dependensi dari `requirements.txt`.
3. Memvalidasi sintaks kode Python (`py_compile`).
4. Menjalankan seluruh test suite `pytest`.
5. Memverifikasi apakah definisi Flow siap didaftarkan ke Prefect (*Deployability check*).

---

## 4. Review 15 Menit (Lembar Refleksi Sesi 9)

Luangkan waktu 15 menit untuk merefleksikan pengujian ini:

- [ ] **Mengapa memanggil task via `.fn` jauh lebih unggul untuk unit testing dibanding menjalankan flow utuh?**
  *(Jawaban: Karena mengeksekusi logika murni Python dalam hitungan milidetik tanpa butuh server Prefect aktif, tanpa I/O lambat, dan mempermudah isolasi bug spesifik).*
- [ ] **Bagaimana Anda menjelaskan konsep idempotensi kepada rekan tim data Anda?**
  *(Jawaban: Pipeline yang idempoten aman di-re-run atau di-backfill berulang kali kapan saja tanpa risiko menciptakan duplikasi data).*
- [ ] **Bagian mana dari pipeline data yang paling sulit di-test dan mengapa?**
  *(Jawaban: Pemanggilan API pihak ketiga eksternal [karena rate limit/jaringan] dan database distributed besar [karena butuh test database / Docker container terpisah]).*
- [ ] **Pertanyaan Transisi Menuju Sesi 10 (Puncak Masterclass):**
  *"Setelah menguasai seluruh konsep dari Sesi 1 hingga Sesi 9, bagaimana cara merangkum seluruh kemampuan ini ke dalam Capstone Project portfolio profesional, dan bagaimana perbandingan objektif antara Apache Airflow vs Prefect vs Dagster?"*
  *(Jawabannya: Sesi 10: Capstone Project & Komparasi Eksekutif!).*
