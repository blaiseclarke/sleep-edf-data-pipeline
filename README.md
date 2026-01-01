# Containerized ELT Pipeline for Sleep-EDF

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-FFF000?logo=duckdb&logoColor=black)
![Pandera](https://img.shields.io/badge/Pandera-Validation-E94F37?logo=pandera&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformation-FF694B?logo=dbt&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-070E28?logo=prefect&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&logoColor=white)
![CI](https://img.shields.io/badge/GitHub_Actions-CI-2088FF?logo=github-actions&logoColor=white)

### Project Overview
This project is a production-grade ELT pipeline designed to ingest, validate, and analyze clinical sleep data. It processes the [PhysioNet Sleep-EDF Expanded](https://physionet.org/content/sleep-edfx/1.0.0/) dataset, transforming raw polysomnography (PSG) signals into queryable sleep metrics.

The architecture leverages **MNE** for advanced signal processing, **Prefect** for robust orchestration, and a hybrid warehousing strategy supporting both **DuckDB** (local) and **Snowflake** (production).

---

### Architecture
<img width="1270" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/5ecabb9a-6b37-460e-9959-8b0dbab518a9" />

| Stage | Tech Stack | Description |
| :--- | :--- | :--- |
| **Source** | **PhysioNet** | Sleep-EDF Database (Raw .edf files) |
| **Ingestion** | **Python + MNE** | Signal processing, FFT, and feature extraction |
| **Validation** | **Pandera** | Schema-level validation and error logging |
| **Orchestration** | **Prefect** | Parallel mapping and flow management |
| **Warehousing** | **DuckDB / Snowflake** | Portable storage for raw and modeled data |
| **Transformation** | **dbt** | SQL-based modeling for clinical insights |
| **Runtime** | **Docker** | Reproducible environment for ingestion and orchestration |

---

### Key Features

* **Data Validation:** Uses `pandera` for schema-level validation of signal dataframes.
* **Parallel Ingestion:** Processes subjects concurrently using Prefect's mapping execution.
* **Hybrid Warehousing:** Writes to local DuckDB (dev) or Snowflake (prod) using a unified `WarehouseClient` protocol.
* **Upfront Fetching:** Pre-fetches MNE data to prevent filesystem locking during parallel extraction.
* **Robust Observability:** Thread-safe error logging captures all extraction failures in the `INGESTION_ERRORS` table.
* **Reproducibility:** Fully containerized with Docker; local development automated via Makefile.

<img width="986" height="497" alt="Prefect dashboard" src="https://github.com/user-attachments/assets/ed9f1351-14b1-4301-a5c0-e6c18ce97ccb" />

---

### Quick Start

You can run the pipeline directly on your local machine using Python, or in a container using Docker.
Docker Compose is recommended for reproducible, containerized execution.

#### Prerequisites
- Python 3.10+ *(for host execution)*
- Docker *(Docker Desktop recommended)*
- Snowflake account *(optional, DuckDB used by default for local)*
- `make` *(for automation)*
- dbt-core (pip install dbt-snowflake or dbt-duckdb)

#### Configuration

**1. Create a `.env` file**
Create a `.env` file in the project root to store your configuration. This ensures consistency between Docker and local execution.

```env
# --- Default Configuration (DuckDB) ---
WAREHOUSE_TYPE=duckdb
DB_PATH=data/sleep_data.db

# --- Optional: Snowflake Configuration ---
# WAREHOUSE_TYPE=snowflake
# SNOWFLAKE_ACCOUNT=your_account
# SNOWFLAKE_USER=your_user
# SNOWFLAKE_PASSWORD=your_password
# SNOWFLAKE_WAREHOUSE=COMPUTE_WH
# SNOWFLAKE_DATABASE=EEG_ANALYTICS
# SNOWFLAKE_SCHEMA=RAW

# --- dbt Configuration ---
# If using DuckDB (default), these rely on DB_PATH.
# If using Snowflake, uncomment and map to above vars:
# DBT_SOURCE_DATABASE=EEG_ANALYTICS
# DBT_SOURCE_SCHEMA=RAW
```

**2. Environment Loading**

*   **Docker:** Automatically reads `.env`.
*   **Python (Local):** Automatically reads `.env` (via `python-dotenv`).
*   **dbt (Local):** dbt doesn't automatically read `.env` files. You'll need to export them to your shell environment first:
    ```bash
    # Export variables from .env
    export $(grep -v '^#' .env | xargs)
    
    # Run dbt
    dbt debug
    ```

#### Option 1: Docker Compose

Runs the pipeline locally inside a Docker container.

```bash
# 1. Clone the repository
git clone https://github.com/blaiseclarke/sleep-edf-data-pipeline.git
cd sleep-edf-data-pipeline

# 2. Create environment file
# (.env file in project root)
# Snowflake configuration (optional)
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=EEG_ANALYTICS
SNOWFLAKE_SCHEMA=RAW

# Pipeline configuration (optional overrides)
STARTING_SUBJECT=0
ENDING_SUBJECT=10
RECORDING=1
EPOCH_LENGTH=30.0
PREFECT_MAX_WORKERS=3
DB_PATH=data/sleep_data.db

# 3. Build and run pipeline
docker compose up --build

# 4. Transformations
# Point dbt to the local profiles.yml
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

# Note: dbt transformations are executed after ingestion and connect to the configured warehouse (DuckDB or Snowflake).
```
#### Option 2: Local Development (Makefile)

The recommended way for local development and testing.

```bash
# 1. Clone repo
git clone https://github.com/blaiseclarke/sleep-edf-data-pipeline.git
cd sleep-edf-data-pipeline

# 2. Setup and install
make install

# 3. Quick test (lint, format, run)
make all

# 4. Initialize local database
make setup-db

# 5. Run, lint, and test
make lint    # Check for errors
make format  # Autoformat code
make run     # Run parallel ingestion pipeline (persists to DuckDB)

# 6. Test Observability
python simulate_error.py  # Verifies the error warehouse captures failures

# 7. Transformations (local DuckDB)
dbt run --profiles-dir . --target dev_duckdb
dbt test --profiles-dir . --target dev_duckdb
```

#### Option 3: Manual Python Execution

```bash
# Install dependencies manually
pip install -r requirements.txt

# 2. Initialize local database
python scripts/setup_db.py

# 3. Run ingestion pipeline directly
python pipeline.py

# Transformations (Default to Snowflake if .env configured)
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

# Note: Use `--target dev_duckdb` to run against local storage.
```


---

### Technical Deep Dive

#### 1. Extraction (Python/MNE)
Built using `mne` for polysomnograph (PSG) ingestion and annotation alignment. This handles the heavy lifting of signal processing before data ever hits the warehouse.

* **Spectral Analysis:** Extracts Power Spectral Density (PSD) for delta, theta, alpha, sigma, and beta bands.
* **Standardization:** Maps raw annotations to standardized clinical sleep stages: `W, N1, N2, N3, REM, MOVE, NAN`.
* **Performance Optimization:** Utilizes `preload=True` to speed up FFT computations.
* **Configurable Parameters:** The pipeline range and logic are controlled via environment variables:
    * `STARTING_SUBJECT` / `ENDING_SUBJECT`: Define the participant ID range.
    * `RECORDING`: Specifies which session recording to fetch (default: 1).
    * `EPOCH_LENGTH`: Sets the window duration for EEG segmentation (default: 30s).
    * `DB_PATH`: Local path for the DuckDB database (default: `data/sleep_data.db`).
    * `PREFECT_MAX_WORKERS`: Limit on concurrent subject processing (default: 3).
    * `STUDY`: Selects the Sleep-EDF study (options: `age`, `telemetry`, default: `age`).

#### 2. Warehousing (DuckDB / Snowflake)
The pipeline is warehouse-agnostic via the `WarehouseClient` protocol.
* **DuckDB (Local):** Default for local development. Data is persisted to `data/sleep_data.db` without cloud overhead.
* **Snowflake (Cloud):** Used for production-scale storage and analytics, separating compute from storage. This allows the pipeline to scale without refactoring local memory constraints or ingestion logic.

#### 3. Transformation (dbt)
The dbt project creates a trusted data lineage, transforming raw logs into analytics-ready models:

* **Staging (`staging_sleep_data`):** Handles column standardization and explicit type casting.
* **Intermediate (`sleep_metrics`):** Calculates rolling power averages over sliding epochs to smooth out signal artifacts and deviations.
* **Marts (`sleep_summary`):** Aggregates data into clinical insights:
    * Sleep Architecture (Deep vs. Light vs. REM %)
    * Awakening counts
    * Average power across frequency bands

#### 4. Data Integrity & Observability
Reliability is enforced through automated checks and failure logging:
* **Validation (Pandera):** Sleep stages and spectral powers are validated against strict contracts.
* **Error Warehouse:** Failures are intercepted and logged sequentially to the `INGESTION_ERRORS` table, ensuring 100% thread safety and detailed stack trace persistence even during parallel runs.
* **dbt Tests:** Custom SQL tests ensure logical consistency (ex., *Rolling averages cannot be negative*).

---

### Results
The pipeline successfully processed a batch of ~24-hour recordings from the PhysioNet Sleep-EDF database.

**Generated Insights:**
* Sleep architecture breakdown
* Frequency of nocturnal awakenings
* Average spectral power distribution across EEG bands

<img width="991" alt="Results Graph" src="https://github.com/user-attachments/assets/bdfa5260-817b-4d23-a8c6-bb4967a9d31a" />

---

### References
* **Kemp B, Zwinderman AH, Tuk B, Kamphuisen HAC, Oberyé JJL.** *Analysis of a sleep-dependent neuronal feedback loop: the slow-wave microcontinuity of the EEG.* IEEE-BME 47(9):1185-1194 (2000).

* **Goldberger A, Amaral L, Glass L, Hausdorff J, Ivanov PC, Mark R, ... & Stanley HE.** *PhysioBank, PhysioToolkit, and PhysioNet: Components of a new research resource for complex physiologic signals.* Circulation [Online]. 101 (23), pp. e215–e220 (2000).
