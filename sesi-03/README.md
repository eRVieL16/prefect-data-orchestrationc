# Sesi 3: Parameter, Subflow, & Konkurensi (.map)

## Target Pembelajaran
1. **Flow Parameters & Type Validation:** Memanfaatkan type hints Python/Pydantic untuk input yang aman dan dinamis.
2. **Subflow vs Task:** Mengetahui kapan memecah pipeline menjadi Subflow modular dan kapan cukup menggunakan Task.
3. **Konkurensi dengan `.map()`:** Mengeksekusi task secara simultan/paralel untuk memangkas waktu pemrosesan data multi-entitas (toko/partisi).
4. **`unmapped()` Argument:** Menjaga parameter konstan saat memetakan list iterable.

---

## Konsep Inti (80/20)

### 1. Kapan Pakai Subflow vs Cukup Task?
| Karakteristik | Task (`@task`) | Subflow (`@flow` di dalam `@flow`) |
|---|---|---|
| **Definisi** | Unit kerja terkecil (fungsi spesifik) | Kumpulan logika/task yang membentuk domain bisnis mandiri |
| **Visibilitas UI** | Muncul sebagai Task Run di dalam Flow Run | Memiliki **Flow Run ID tersendiri** (child run) |
| **Fitur** | Caching, retries, retry delay | Parameter terpisah, tagging terpisah, scheduler terpisah |
| **Contoh Penggunaan** | `fetch_data_from_db()`, `clean_nulls()` | `ingest_sales_pipeline()`, `dbt_transformation_pipeline()` |

### 2. Konkurensi dengan `.map()`
Di Prefect 3.x, Anda tidak perlu repot membuat thread pool manual. Cukup panggil metode `.map()` pada task:
```python
from prefect import unmapped

# Mengeksekusi process_item serentak untuk setiap id di items
futures = process_item.map(items, unmapped("STATIC_CONFIG"))
results = [f.result() for f in futures]
```

---

## Langkah Praktik

Pastikan Prefect Server Anda aktif di background (`prefect server start`) dan dashboard terbuka di `http://127.0.0.1:4200`.

### Langkah 1: Parameter & Subflow Modular
Jalankan file [01_parameter_dan_subflow.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-03/01_parameter_dan_subflow.py):
```powershell
python sesi-03/01_parameter_dan_subflow.py
```
**Apa yang perlu diamati di Prefect UI?**
1. Buka [http://127.0.0.1:4200/runs](http://127.0.0.1:4200/runs).
2. Anda akan melihat parent flow run `sesi-03-parent-orchestrator`.
3. Klik run tersebut, lalu perhatikan tab **Subflow Runs**. Anda akan melihat 3 subflow berbeda (`Jakarta-Pusat`, `Bandung-Dago`, `Surabaya-Barat`) yang masing-masing memiliki histori run-nya sendiri!

### Langkah 2: Benchmark Konkurensi `.map()`
Jalankan file [02_konkurensi_mapping.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-03/02_konkurensi_mapping.py):
```powershell
python sesi-03/02_konkurensi_mapping.py
```
**Apa yang terjadi?**
- **Mode Sekuensial (for loop biasa):** 4 cabang x 1.5 detik = **~6.0 detik**.
- **Mode Konkruen (`.map()`):** 4 cabang dikerjakan serentak = **~1.5 detik**.
- Penghematan waktu mencapai **~75%**!

---

## Review 15 Menit (Lembar Refleksi Sesi 3)
- [ ] **Kapan memakai subflow, kapan cukup task?**
  *(Gunakan subflow jika ingin pipeline modular yang bisa di-trigger sendiri dan punya Flow Run ID sendiri di UI. Gunakan task untuk fungsi komputasi atomik).*
- [ ] **Bandingkan waktu eksekusi sekuensial vs paralel:**
  *(Catat durasi aktual dari script benchmark di mesin Anda).*
- [ ] **Batasan konkurensi apa yang perlu dijaga?**
  *(Hati-hati terhadap koneksi database pool yang terbatas, misal max 10 connections, atau API rate limits seperti max 5 requests/detik. Di sesi lanjutan, ini bisa diatur via Concurrency Limits di Prefect).*
- [ ] **Satu pertanyaan untuk Sesi 4:** *"Bagaimana cara mengamankan password database atau API key agar tidak tercantum di file kode saat dipush ke git?"* (Jawabannya: Prefect Variables & Blocks di Sesi 4!).
