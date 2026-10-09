# Sesi 1: Mental Model & Setup

## Target Pembelajaran
1. Memahami perbedaan **Flow**, **Task**, dan **Flow Run**.
2. Mengubah script ETL Python prosedural biasa menjadi Prefect Flow.
3. Menjalankan Prefect Server lokal dan memantau eksekusi di UI Dashboard (`http://127.0.0.1:4200`).

---

## Mental Model: 3 Konsep Kunci
- **Task (`@task`)**: Fungsi discrete (terisolasi) yang melakukan satu pekerjaan spesifik (misal: extract data dari API, bersihkan baris kosong, simpan ke database). Task memiliki fitur retry, caching, dan timeout.
- **Flow (`@flow`)**: Kontainer orkestrasi yang memanggil task-task tersebut secara berurutan atau paralel. Flow merepresentasikan pipeline logika Anda secara utuh.
- **Flow Run**: Instansi setiap kali flow dijalankan. Setiap run memiliki ID unik, parameter input, durasi eksekusi, log, dan status akhir (`COMPLETED`, `FAILED`, `CRASHED`).

---

## Langkah Praktik

### 1. Buat Virtual Environment & Install Dependencies
Gunakan Python 3.12 (karena kestabilan paket wheel pada Windows):
```powershell
# Di folder root project:
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Jalankan Script Biasa (Sebelum Prefect)
```powershell
python sesi-01/01_script_biasa.py
```
Perhatikan outputnya: hanya teks print biasa di console terminal. Jika gagal di tengah jalan, tidak ada riwayat terpusat.

### 3. Jalankan Prefect Server Lokal
Buka terminal terpisah untuk menjalankan UI Prefect lokal:
```powershell
.\.venv\Scripts\Activate.ps1
prefect server start
```
Buka browser di: [http://127.0.0.1:4200](http://127.0.0.1:4200)

### 4. Jalankan Prefect Flow
Di terminal pertama:
```powershell
python sesi-01/02_flow_dasar.py
```

### 5. Cek di Prefect UI
Buka halaman **Flow Runs** di browser:
- Anda akan melihat nama run (contoh: `clever-chameleon`).
- Klik run tersebut untuk melihat visualisasi DAG sederhana: `extract-transaksi` -> `transform-transaksi` -> `load-ke-sqlite`.
- Lihat tab **Logs** untuk membaca log terstruktur.

---

## Checklist Review 15 Menit
- [ ] Bisa menjelaskan perbedaan Flow, Task, dan Flow Run tanpa membuka catatan?
- [ ] Flow berhasil berjalan dan muncul di Prefect UI dashboard?
- [ ] Satu pertanyaan untuk Sesi 2: *"Bagaimana jika fungsi extract_transactions gagal karena jaringan terputus? Apakah Prefect bisa mengulang otomatis tanpa mengulang flow dari awal?"* (Jawabannya: Retries & State di Sesi 2!).
