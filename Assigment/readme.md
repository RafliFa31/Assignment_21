# Airline Customer Data Analysis with PySpark

## 1. Pendahuluan
Deskripsi singkat tentang tujuan analisis: menggunakan PySpark untuk melakukan ETL dan analisis data pelanggan maskapai penerbangan.

### Dataset
- **calendar.csv**: data kalender (Year, Month, MonthName, DayCount)
- **customer_flight_activity.csv**: aktivitas penerbangan pelanggan (loyalty number, year, month, total flights, distance, points)
- **customer_loyalty_history.csv**: riwayat loyalitas pelanggan (lifestyle, demografi, CLV, dsb.)

## 2. Langkah-langkah Analisis

### 2.1 Extract
```python
# Download dan baca CSV
spark.read.csv(...)
```
- Memanfaatkan `spark.read.option("header", True).csv()` untuk memuat data dengan header dan inferSchema.

### 2.2 Cleaning
```python
# Hapus duplikat
.df.dropDuplicates()
# Hapus baris null pada kolom kunci
.df.dropna(subset=[...])
```
- Duplikasi dan null values ditangani agar data konsisten.

### 2.3 Transformasi
```python
# Join tabel flight dan loyalty
.join(df_loyalty, on="LoyaltyNumber")
# Hitung NetPoints
.withColumn("NetPoints", col("PointsAccumulated") - col("PointsRedeemed"))
# Tambah YearMonth
.withColumn("YearMonth", concat_ws('-',...))
```
- Menggabungkan tabel dan membuat fitur baru.

## 3. Kueri SQL
Semua DataFrame didaftarkan sebagai view `all_data`.

| No | Pertanyaan | Query SQL |
|---|------------|-----------|
|1|Rata-rata penerbangan per pelanggan per tahun|```sql
SELECT Year, ROUND(AVG(`Total Flights`),2) AS AvgFlightsPerCust
FROM all_data
GROUP BY Year
ORDER BY Year;```|
|2|Rata-rata net points per status kartu|```sql
SELECT `Loyalty Card` AS Card, ROUND(AVG(NetPoints),2) AS AvgNetPoints
FROM all_data
GROUP BY `Loyalty Card`;```|
|3|Rata-rata penerbangan berdasarkan tingkat pendidikan|```sql
SELECT Education, ROUND(AVG(`Total Flights`),2) AS AvgFlights
FROM all_data
GROUP BY Education;```|
|4|Jumlah penerbangan bulanan (trend)|```sql
SELECT YearMonth, SUM(`Total Flights`) AS TotalFlights
FROM all_data
GROUP BY YearMonth
ORDER BY YearMonth;```|

## 4. Visualisasi dan Interpretasi

1. **Trend Total Flights per Month**  
   - Grafik garis menunjukkan fluktuasi total penerbangan setiap bulan.  
   - *Insight:* periode puncak terjadi di bulan ...

2. **Average Flights per Customer per Year**  
   - Bar chart memperlihatkan rata-rata penerbangan per pelanggan setiap tahun.  
   - *Insight:* rata-rata meningkat/menurun dari tahun ke tahun.

3. **Average Net Points by Loyalty Card Status**  
   - Bar chart membandingkan rata-rata poin bersih antar kategori kartu (Star, Nova, Aurora).  
   - *Insight:* pemegang kartu ... memiliki poin rata-rata tertinggi.

4. **(Opsional) Average Flights by Education Level**  
   - Bar chart menunjukkan korelasi antara tingkat pendidikan dan jumlah penerbangan.  
   - *Insight:* pelanggan dengan pendidikan ... cenderung terbang lebih banyak.

## 5. Penyimpanan Hasil
- **CSV**: `output/results/avg_flights_per_year.csv`, `...points_by_card.csv`, dsb.
- **Parquet**: `output/results/full_customer_data.parquet`
- **JSON**: `output/results/full_customer_data.json`

## 6. Kesimpulan
Ringkasan insight utama dan rekomendasi bisnis (misal strategi promosi berdasarkan status kartu atau segmen pendidikan).

