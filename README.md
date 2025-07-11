## Analisis Data Pelanggan Maskapai Penerbangan dengan PySpark

**Indikator:** Student mampu membuat dokumentasi yang jelas dan sistematis

Berikut adalah ringkasan langkah-langkah analisis yang dilakukan:

1.  **Extract Data**: Data dari `calendar.csv`, `customer_flight_activity.csv`, dan `customer_loyalty_history.csv` dimuat ke dalam PySpark DataFrames. Skema DataFrame diperiksa.

2.  **Cleaning Data**: Data duplikat dihapus dari semua DataFrame. Baris dengan nilai yang hilang pada kolom kunci (`loyalty_number`, `year`, `month`) dihapus dari DataFrame aktivitas penerbangan, dan baris dengan nilai yang hilang pada `LoyaltyNumber` dihapus dari DataFrame riwayat loyalitas. Kolom `year` dan `month` di DataFrame aktivitas penerbangan diubah menjadi tipe integer.

3.  **Transformasi Data**: DataFrame aktivitas penerbangan digabungkan (`JOIN`) dengan DataFrame riwayat loyalitas menggunakan `loyalty_number`. Kolom baru `NetPoints` dihitung dari selisih `points_accumulated` dan `points_redeemed`. Kolom `YearMonth` dibuat untuk analisis deret waktu. DataFrame hasil gabungan didaftarkan sebagai temporary view SQL bernama `all_data`.

4.  **Analisis Data Menggunakan PySpark SQL**: Beberapa analisis dilakukan menggunakan PySpark SQL pada temporary view `all_data`:
    *   Rata-rata penerbangan per pelanggan per tahun.
    *   Distribusi net points berdasarkan status kartu loyalitas.
    *   Hubungan antara tingkat pendidikan dan jumlah penerbangan.
    *   Tren total penerbangan dari waktu ke waktu (per bulan).

5. Visualisasi Data dan Temuan Kunci
Pada tahap ini, data hasil analisis divisualisasikan untuk mendapatkan pemahaman yang lebih baik tentang pola dan tren dalam data pelanggan maskapai penerbangan. Berikut adalah visualisasi yang dibuat beserta temuan kuncinya:
- Tren Total Penerbangan per Bulan: Grafik garis ini menunjukkan total jumlah penerbangan dari waktu ke waktu (per bulan).
  Kesimpulan: Berdasarkan visualisasi, terlihat bahwa jumlah penerbangan memiliki pola musiman, dengan puncak tertinggi kemungkinan terjadi pada bulan-bulan tertentu (misalnya, Juli 2018) dan titik terendah pada bulan lainnya (misalnya, Januari 2017), seperti yang Anda sebutkan.
- Distribusi Rata-rata Net Points berdasarkan Status Kartu Loyalitas: Grafik batang ini membandingkan rata-rata net points yang dikumpulkan pelanggan berdasarkan jenis kartu loyalitas mereka.
  Kesimpulan: Visualisasi ini menunjukkan perbedaan dalam rata-rata net points antar jenis kartu loyalitas. Seperti yang Anda indikasikan, pemegang kartu "Aurora" kemungkinan memiliki rata-rata net points tertinggi, menunjukkan tingkat akumulasi poin yang lebih tinggi atau penebusan poin yang lebih rendah dibandingkan kartu lainnya.
- Rata-rata Penerbangan berdasarkan Tingkat Pendidikan: Grafik batang ini menampilkan rata-rata jumlah penerbangan yang dilakukan pelanggan untuk setiap tingkat pendidikan.
  Kesimpulan: Visualisasi ini memperlihatkan hubungan antara tingkat pendidikan dan rata-rata frekuensi penerbangan. Sesuai dengan observasi Anda, tingkat pendidikan "Highschool or Below" kemungkinan menunjukkan rata-rata penerbangan per tahun tertinggi, menyarankan adanya korelasi antara tingkat pendidikan ini dengan frekuensi bepergian dengan maskapai.
