# Job Order Data Scraper and Monitor

Proyek ini untuk mengambil data job order dari API Pelindo, menyimpannya ke CSV, dan menampilkan dashboard HTML untuk monitoring aktivitas pilot.

## Fitur
- Scraping data job order secara otomatis
- Penyimpanan incremental ke CSV
- Dashboard HTML dengan filter dan timeline
- Resume dari ID terakhir

## Cara Menjalankan
1. Install dependencies: `pip install -r requirements.txt`
2. Jalankan script: `python job.py`
3. Buka `monitor.html` di browser untuk melihat dashboard

## GitHub Actions
Workflow otomatis menjalankan script setiap jam jika di-push ke GitHub.