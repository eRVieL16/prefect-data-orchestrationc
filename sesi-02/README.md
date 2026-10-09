# Sesi 2: Ketahanan Pipeline (Retries, Timeout, Caching, & State)

## Target Pembelajaran
1. **Retries & Delays:** Menghadapi error transien (jaringan terputus sementara, rate limit, database lock).
2. **Timeouts:** Mencegah pipeline menggantung (hang) tanpa batas waktu.
3. **Caching:** Menghemat waktu & biaya komputasi saat input tidak berubah (`task_input_hash`).
4. **Prefect States:** Memahami alur state dari `Pending` ➔ `Running` ➔ `Retrying` ➔ `Completed` / `Failed` / `Cached`.

---

## Konsep Inti (80/20)

### 1. Kapan Menggunakan Retry?
- ✅ **Bagus untuk:** Masalah transien (koneksi network sesaat, API pihak ketiga timeout, deadlock database).
- ❌ **Hindari untuk:** Error logika data (contoh: format JSON korup, `KeyError`, schema mismatch). Retry tidak akan pernah berhasil jika kodenya sendiri yang salah, dan justru memperlambat pipeline serta membuang kuota.

### 2. Anatomi Decorator `@task`
```python
@task(
    retries=3,                    # Coba ulang hingga 3 kali
    retry_delay_seconds=5,        # Jeda 5 detik antar percobaan
    retry_jitter_factor=0.1,      # Variasi acak agar tidak membebani server tujuan bersamaan
    timeout_seconds=30,           # Batalkan jika melebihi 30 detik
    cache_key_fn=task_input_hash, # Cache berdasarkan hash argumen input
    cache_expiration=timedelta(minutes=15) # Masa berlaku cache
)
```

---

## Langkah Praktik

Pastikan Prefect Server Anda sedang berjalan di terminal (`prefect server start`) dan dashboard terbuka di `http://127.0.0.1:4200`.

### Langkah 1: Uji Coba Retries (Self-Healing)
Jalankan file [01_retry_simulation.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-02/01_retry_simulation.py):
```powershell
python sesi-02/01_retry_simulation.py
```
**Apa yang terjadi?**
1. Percobaan 1 gagal (`ConnectionError 503`).
2. Prefect otomatis menahan task dan masuk ke status `Retrying`.
3. Percobaan 2 gagal kembali.
4. Percobaan 3 sukses, dan flow secara keseluruhan selesai dengan status `Completed`!
5. Buka dashboard UI di browser, klik run terbaru, perhatikan bagaimana task menampilkan riwayat attempt dan log peringatannya.

### Langkah 2: Uji Coba Caching (Performa Instan)
Jalankan file [02_caching_demo.py](file:///c:/Users/Rivaldi/Documents/Papal/Project/Prefect%20tutor/sesi-02/02_caching_demo.py):
```powershell
python sesi-02/02_caching_demo.py
```
**Apa yang terjadi?**
- **Panggilan 1 (Input: '2026-10-01'):** Berjalan ~3 detik (Cache Miss).
- **Panggilan 2 (Input: '2026-10-01'):** Selesai instan ~0.001 detik (Cache Hit, status di UI: `Cached`).
- **Panggilan 3 (Input: '2026-10-02'):** Berjalan ~3 detik kembali karena nilai input berbeda.

---

## Review 15 Menit (Lembar Refleksi Sesi 2)
Jawab 3 pertanyaan ini untuk mengunci pemahaman Anda:
- [ ] **Kapan retry membantu dan kapan justru menyembunyikan masalah?**
  *(Petunjuk: Retry membantu pada error jaringan/infra sesaat. Retry menyembunyikan masalah jika ada bug kode atau data rusak yang harusnya ditolak).*
- [ ] **Caching bisa menghemat apa di pipeline Anda?**
  *(Petunjuk: Menghemat biaya query data warehouse seperti BigQuery/Snowflake, mencegah over-quota API berbayar, dan mempercepat re-run saat debugging).*
- [ ] **State apa saja yang Anda amati di Prefect UI?**
  *(Amati: `Running`, `Retrying`, `Completed`, dan `Cached`).*
- [ ] **Satu pertanyaan untuk Sesi 3:** *"Bagaimana jika kita punya 10 toko berbeda dan ingin memprosesnya secara paralel tanpa menunggu satu per satu?"* (Jawabannya: Konkurensi & `.map()` di Sesi 3!).
