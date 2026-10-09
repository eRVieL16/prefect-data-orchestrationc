# Rencana Belajar Prefect 20 Jam (Prinsip 80/20)

> Asumsi: tool yang dimaksud adalah **Prefect** (versi 3.x), tool orkestrasi workflow berbasis Python. Cek dokumentasi resmi untuk memastikan versi dan sintaks terbaru sebelum mulai.

## Prinsip 80/20

Sebagian besar nilai praktis Prefect ada di enam konsep:

1. Flow & task
2. Retry, caching, dan state
3. Deployment & schedule
4. Work pool & worker
5. Automation & notifikasi
6. Integrasi dengan stack data (dbt, warehouse)

Fitur lanjutan seperti event triggers kompleks dan custom infrastructure sengaja ditunda.

## Struktur Tiap Sesi (2 jam)

| Waktu | Aktivitas |
|---|---|
| 0:00–0:20 | Baca/tonton materi inti |
| 0:20–1:45 | Praktik langsung (kode) |
| 1:45–2:00 | **Review 15 menit**: tulis 3–5 poin ringkasan, hal yang masih bingung, dan satu pertanyaan untuk sesi berikutnya |

## Ringkasan 10 Sesi

| # | Topik | Output praktik |
|---|---|---|
| 1 | Mental model & setup | Script ETL menjadi flow |
| 2 | Ketahanan pipeline | Retry, timeout, caching |
| 3 | Parameter, subflow, konkurensi | Pemrosesan paralel |
| 4 | Konfigurasi & secrets | Kredensial lewat block/variable |
| 5 | Deployment & scheduling | Flow terjadwal harian |
| 6 | Work pool & worker | Deploy ke work pool |
| 7 | Automation & notifikasi | Alert saat gagal/telat |
| 8 | Integrasi dbt & data stack | `dbt build` dalam flow |
| 9 | Praktik produksi | Test + CI/CD |
| 10 | Capstone & perbandingan | Pipeline end-to-end |

---

## Sesi 1: Mental model & setup

- **Fokus:** apa itu flow, task, dan flow run; instalasi; menjalankan server lokal dan membuka UI
- **Praktik:** ubah satu script ETL sederhana menjadi flow dengan `@flow` dan `@task`
- **Sumber:** docs.prefect.io, bagian *Get started* dan *Quickstart*
- **Review 15 menit:**
  - [ ] Jelaskan perbedaan flow, task, dan flow run dengan kata sendiri
  - [ ] Flow berjalan dan terlihat di UI
  - [ ] Catat satu pertanyaan untuk Sesi 2

## Sesi 2: Ketahanan pipeline

- **Fokus:** retries, retry delay, timeouts, caching, logging, state
- **Praktik:** tambahkan retry ke task yang memanggil API/database, lalu simulasikan kegagalan
- **Sumber:** docs.prefect.io, bagian *Write flows*, *Write tasks*, dan *Retries*
- **Review 15 menit:**
  - [ ] Kapan retry membantu dan kapan justru menyembunyikan masalah?
  - [ ] Caching bisa menghemat apa di pipeline Anda?
  - [ ] Catat state yang muncul di UI saat task gagal

## Sesi 3: Parameter, subflow, dan konkurensi

- **Fokus:** flow parameters (validasi tipe), subflow, `.submit()` / `.map()`, task runner
- **Praktik:** jalankan pemrosesan paralel per toko atau per partisi data
- **Sumber:** docs.prefect.io, bagian *Task runners* dan *Run tasks concurrently*
- **Review 15 menit:**
  - [ ] Kapan memakai subflow, kapan cukup task?
  - [ ] Bandingkan waktu eksekusi sekuensial vs paralel
  - [ ] Catat batasan konkurensi yang perlu dijaga (rate limit, koneksi DB)

## Sesi 4: Konfigurasi & secrets

- **Fokus:** variables, blocks, secret management, pemisahan config dev/prod
- **Praktik:** pindahkan kredensial database dan path ke block/variable
- **Sumber:** docs.prefect.io, bagian *Variables* dan *Blocks*
- **Review 15 menit:**
  - [ ] Tidak ada kredensial yang tertulis di kode
  - [ ] Config dev dan prod bisa diganti tanpa mengubah kode
  - [ ] Catat apa yang masih hardcoded

## Sesi 5: Deployment & scheduling

- **Fokus:** `flow.serve()`, `prefect.yaml`, `prefect deploy`, cron/interval schedule
- **Praktik:** deploy flow dengan jadwal harian dan cek run-nya di UI
- **Sumber:** docs.prefect.io, bagian *Deployments* dan *Schedules*
- **Review 15 menit:**
  - [ ] Bedakan `flow.serve()` dan deployment via `prefect.yaml`
  - [ ] Jadwal berjalan sesuai zona waktu yang diinginkan
  - [ ] Catat pertanyaan seputar infrastruktur untuk Sesi 6

## Sesi 6: Work pool & worker

- **Fokus:** arsitektur work pool, worker, dan work queue; process pool vs Docker pool
- **Praktik:** buat work pool, jalankan worker, lalu deploy flow ke pool tersebut
- **Sumber:** docs.prefect.io, bagian *Work pools* dan *Workers*
- **Review 15 menit:**
  - [ ] Gambar alur: deployment → work pool → worker → flow run
  - [ ] Kapan memilih process pool, kapan Docker pool?
  - [ ] Catat apa yang terjadi bila worker mati

## Sesi 7: Automation & notifikasi

- **Fokus:** automations untuk event gagal/terlambat, notifikasi Slack/email
- **Praktik:** buat alert otomatis saat flow run gagal atau melebihi batas waktu
- **Sumber:** docs.prefect.io, bagian *Automations* dan *Events*
- **Review 15 menit:**
  - [ ] Alert benar-benar terkirim saat kegagalan disimulasikan
  - [ ] Pesan alert cukup informatif untuk ditindaklanjuti
  - [ ] Catat risiko alert fatigue dan cara mengurangi noise

## Sesi 8: Integrasi dbt & data stack

- **Fokus:** orkestrasi dbt via `prefect-dbt`; urutan ingest → transform → test
- **Praktik:** bungkus `dbt build` dari project Anda dalam sebuah flow, lengkap dengan penanganan kegagalan test
- **Sumber:** dokumentasi integrasi `prefect-dbt` di docs Prefect; repo contoh di GitHub PrefectHQ
- **Review 15 menit:**
  - [ ] Kegagalan dbt test tertangkap dan terlihat di UI Prefect
  - [ ] Urutan ingest → dbt → notifikasi sudah benar
  - [ ] Catat bagian yang masih manual

## Sesi 9: Praktik produksi

- **Fokus:** idempotency, backfill, struktur project, testing flow, CI/CD (GitHub Actions)
- **Praktik:** tulis unit test untuk task, lalu buat workflow CI yang men-deploy otomatis saat merge
- **Sumber:** docs.prefect.io, bagian *Deploy via CI/CD*; channel YouTube Prefect untuk walkthrough produksi
- **Review 15 menit:**
  - [ ] Flow aman dijalankan ulang tanpa menggandakan data
  - [ ] CI berjalan hijau dan deploy otomatis berhasil
  - [ ] Catat strategi backfill untuk data historis

## Sesi 10: Capstone & perbandingan

- **Fokus:** orkestrasi end-to-end pipeline Anda (ingest → dbt → notifikasi), dijadwalkan dan dimonitor
- **Praktik:** dokumentasikan arsitektur, screenshot UI, dan README untuk portofolio
- **Tambahan:** bandingkan singkat dengan Airflow dan Dagster (kapan memilih yang mana), karena pertanyaan ini sering muncul di wawancara AE/DE
- **Sumber:** docs.prefect.io, bagian *Best practices*; dokumentasi Airflow dan Dagster untuk perbandingan
- **Review 15 menit:**
  - [ ] Repo punya README, diagram arsitektur, dan screenshot
  - [ ] Bisa menjelaskan pilihan Prefect vs Airflow vs Dagster dalam 2 menit
  - [ ] Tulis rencana tindak lanjut (fitur yang ditunda)

---

## Sumber Utama

- **Dokumentasi resmi Prefect** (docs.prefect.io): sumber paling akurat dan terbaru untuk 3.x
- **Prefect Cloud tier gratis**: berguna untuk Sesi 5–7 tanpa mengelola server sendiri
- **Repo GitHub PrefectHQ**: contoh kode dan template deployment
- **Channel YouTube Prefect** dan **komunitas Prefect (Slack/Discourse)**: tutorial visual dan tanya jawab

## Tips Eksekusi

- Jangan lanjut ke sesi berikutnya jika flow dari sesi sebelumnya belum berjalan. Praktik adalah 80% dari belajar di sini.
- Simpan semua latihan dalam satu repo GitHub dengan satu folder per sesi, sehingga langsung menjadi bukti pengalaman orkestrasi.
- Jika waktu mepet, Sesi 7 dan 9 yang paling aman dipadatkan. Sesi 1–6 dan 8 adalah inti.
