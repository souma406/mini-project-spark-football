# Big Data Pipeline for Football Match Prediction
### *Big Data Pipeline: Spark Streaming, Kafka & MLlib*

This project implements a complete data pipeline for analyzing and predicting football match results in the Mauritanian League. It combines batch processing, real-time streaming, and Machine Learning.

---

## 🏗 System Architecture

The project relies on a modern data processing architecture:

* **Ingestion:** Kafka Producer simulating a real-time data stream.
* **Streaming:** Spark Structured Streaming with time window aggregations and late data handling (Watermarking).
* **Batch & Scraping:** Enrichment of historical archives with Web Scraping (2025/2026).
* **Machine Learning:** Random Forest classification model trained on Spark MLlib.
* **Storage:** Exclusive use of **Parquet** format for columnar efficiency.

---

## 🛠 Prerequisites and Installation

- Docker
- Git

### 1. Docker Environment

Make sure Docker is running, then deploy the infrastructure:
```bash
docker-compose up -d
```

### 2. Access Jupyter Container

**Open a terminal in the container:**
```bash
docker exec -it jupyter bash
cd /home/jovyan/work/
```

**Open Jupyter in browser:** http://localhost:8888/

### 3. Clone the Project

```bash
cd /home/jovyan/work/
git clone https://github.com/souma406/mini-project-spark-football.git
cd mini-project-spark-football
```

---

##  Execution Guide

### Step 1: Historical Data Preparation

**Option A - Notebook:**
Open `notebooks/prepare_data.ipynb` and run all cells.

**Option B - Script:**
```bash
python3 src/prepared.py
```

* **Actions:** Cleaning, team name normalization.
* **Output:** `outputs/prepared/prepared_results.parquet`

### Step 2: Scraping 2025/2026 Data

**Source:** https://www.flashscore.com/football/mauritania/super-d1/results/

```bash
python3 src/Scraper.py
```

*Note: Generates the file `data/scraped_current_season.csv`*

**Prepare scraped data:**
```bash
python3 src/prepared_scraped_data.py
```
* **Actions:** Cleaning, normalization of scraped team names.
* **Output:** `outputs/data/test_data/scraped.parquet`

**Merge datasets:**
```bash
python3 src/fusion_scraped_data.py
```
* **Actions:** Merging historical and scraped data.
* **Output:** `outputs/prepared/final_mauritania_football_dataset.parquet`

### Step 3: ML Model Training

**Option A - Notebook:**
Open `notebooks/train_model.ipynb` and run all cells.

**Option B - Script:**
```bash
python3 src/train_model.py
```

* **Model:** Random Forest (250 trees, depth 15)
* **Features:** `rolling_home_pts`, `home_win_rate`, `pts_diff`, `rest_days`
* **Performance:** Reliability reached **43.14%**

### Step 4: Streaming Pipeline (Kafka)

1. **Launch the Streaming Job:**
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/stream_job.py
   ```

2. **Launch the Kafka Producer (in another terminal):**
   ```bash
   python3 src/producer.py
   ```

---

## Project Structure

```text
mini-project-spark-football/
├── data/
│   ├── rim_championnat_results_2007-2025.csv    # Historical data
│   └── scraped_current_season.csv               # Scraped data 2025/2026
├── notebooks/
│   ├── prepare_data.ipynb                       # Cleaning & Feature Engineering
│   └── train_model.ipynb                        # MLlib: Training & Evaluation
├── src/
│   ├── Scraper.py                               # 2025/2026 season extraction
│   ├── producer.py                              # Send matches to Kafka
│   ├── stream_job.py                            # Spark Streaming processing
│   ├── train_model.py                           # ML training script
│   ├── prepared.py                              # Historical data preparation
│   ├── prepared_scraped_data.py                 # Scraped data preparation
│   └── fusion_scraped_data.py                   # Dataset merging
├── outputs/
│   ├── prepared/                                # Clean DataFrames in Parquet
│   ├── models/                                  # Saved Random Forest model
│   └── streaming/                               # Real-time aggregation results
├── checkpoints/                                 # Fault tolerance checkpoints
├── raport_simple.pdf                            # Technical report PDF
└── raport_simple.tex                            # LaTeX report source
```

---

##  ML Model Features

To optimize reliability, the project uses advanced calculated features:

* **Rolling Points:** Average points over the last 5 matches.
* **Points Difference (`pts_diff`):** Historical cumulative points gap between two teams to identify the favorite.
* **Target Label:** Multi-class classification (0: Home Win, 1: Draw, 2: Away Win).

---

##  Real-Time Streaming Aggregations

The `stream_job.py` script dynamically calculates:

* Total number of goals per team.
* Home/away win counts per 1-minute window.
* **Watermarking:** Handling late-arriving data (10 minutes).
* **Checkpointing:** All streaming data is secured to allow recovery after failure.

---

**Author:** Soumeye Ebi El Maaly  
**Academic Year:** 2026  
**Project:** Big Data Pipeline for Football Match Prediction
