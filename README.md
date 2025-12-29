# Cloud-Native Sleep-EDF Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformation-FF694B?logo=dbt&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-070E28?logo=prefect&logoColor=white)
![CI](https://img.shields.io/badge/GitHub_Actions-CI-2088FF?logo=github-actions&logoColor=white)

### Project Overview
This project is an end-to-end ELT pipeline that transforms raw physiological signal data (Sleep-EDF) into queryable sleep metrics. It replaces manual, script-based workflows with a modern data stack, enabling scalable query performance, automated data quality checks, and reliable warehousing.

**The Problem:** Clinical EEG data is typically locked in heavy binary formats (EDF), making analysis and SQL querying next to impossible.
**The Solution:** An automated pipeline that ingests, validates, and warehouses sleep data, allowing access to insights via Snowflake and dbt.

---

### Architecture
<img width="1270" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/5ecabb9a-6b37-460e-9959-8b0dbab518a9" />

| Stage | Tech Stack | Description |
| :--- | :--- | :--- |
| **Source** | **PhysioNet** | Sleep-EDF Database (Raw .edf files) |
| **Ingestion** | **Python + MNE** | Signal processing, FFT, and feature extraction |
| **Orchestration** | **Prefect** | Flow management, retries, and observability |
| **Warehousing** | **Snowflake** | Scalable cloud storage for raw and modeled data |
| **Transformation** | **dbt** | SQL-based modeling for clinical insights |
| **Runtime** | **Docker** | Reproducible environment for ingestion and orchestration |

---

### Engineering Highlights

* **🛡 Data Contracts (Pydantic):** Defined strict schemas to validate every epoch before ingestion. If a signal doesn't match the schema, the pipeline fails gracefully before corrupting the warehouse.
* **✨ Fast Linting (Ruff):** Integrated `ruff` for near-instant linting and code formatting, maintaining high code quality.
* **⚡ Automated CI:** GitHub Actions triggers the `pytest` suite and `ruff` checks on every push, ensuring no regressions.
* **🛠 Makefile Automation:** Simplified local development with a comprehensive `Makefile` for one-command installs, tests, and runs.
* **🧪 Data Integrity Tests:** Custom dbt tests ensure logical consistency (e.g., *Band power must be positive*, *Sleep stages must be standard clinical codes*)
* **🔄 Observability:** Prefect dashboard provides real-time logging and monitoring for all pipeline tasks.

<img width="986" alt="Prefect Dashboard" src="https://github.com/user-attachments/assets/71dabb27-486a-4b49-9dce-5d615d02172a" />

---

### Quick Start

You can run the pipeline directly on your local machine using Python, or in a container using Docker.
Docker Compose is recommended for reproducible, containerized execution.

#### Prerequisites
- Python 3.10+ *(for host execution)*
- Docker *(Docker Desktop recommended)*
- Snowflake account
- `make` *(for automation)*
- dbt-core (pip install dbt-snowflake)

#### Option 1: Docker Compose

Runs the pipeline locally inside a Docker container.

```bash
# 1. Clone the repository
git clone https://github.com/blaiseclarke/sleep-edf-data-pipeline.git
cd sleep-edf-data-pipeline

# 2. Create environment file
# (.env file in project root)
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=EEG_ANALYTICS
SNOWFLAKE_SCHEMA=RAW

# 3. Build and run pipeline
docker compose up --build

# 4. Transformations
# Point dbt to the local profiles.yml
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

# Note: dbt transformations are executed after ingestion and connect directly to Snowflake.
```
#### Option 2: Local Development (Makefile)

The recommended way for local development and testing.

```bash
# 1. Clone repo
git clone https://github.com/blaiseclarke/sleep-edf-data-pipeline.git
cd sleep-edf-data-pipeline

# 2. Setup and Install
make install

# 3. Configure environment variables (or use .env file)
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
# ... other vars as listed in Option 1

# 4. Run, Lint, and Test
make lint    # Check for errors
make format  # Autoformat code
make test    # Run unit tests
make run     # Run ingestion pipeline

# 5. Transformations
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

#### Option 3: Manual Python Execution

```bash
# Install dependencies manually
pip install -r requirements.txt

# Run ingestion pipeline directly
python pipeline.py

# Transformations
# Point dbt to the local profiles.yml
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .

# Note: dbt transformations are executed after ingestion and connect directly to Snowflake.
```


---

### Technical Deep Dive

#### 1. Extraction (Python/MNE)
Built using `mne` for polysomnograph (PSG) ingestion and annotation alignment. This handles the heavy lifting of signal processing before data ever hits the warehouse.

* **Spectral Analysis:** Extracts Power Spectral Density (PSD) for delta, theta, alpha, sigma, and beta bands.
* **Standardization:** Maps raw annotations to standardized clinical sleep stages: `W, N1, N2, N3, REM, MOVE, NAN`.
* **Performance Optimization:** Utilizes `preload=True` to speed up FFT computations, with configurable batching for larger subject sets.

#### 2. Warehousing (Snowflake)
Data is loaded into Snowflake to separate compute from storage. This allows the pipeline to scale without refactoring local memory constraints or ingestion logic.

#### 3. Transformation (dbt)
The dbt project creates a trusted data lineage, transforming raw logs into analytics-ready models:

* **Staging (`staging_sleep_data`):** Handles column standardization and explicit type casting.
* **Intermediate (`sleep_metrics`):** Calculates rolling power averages over sliding epochs to smooth out signal artifacts and deviations.
* **Marts (`sleep_summary`):** Aggregates data into clinical insights:
    * Sleep Architecture (Deep vs. Light vs. REM %)
    * Awakening counts
    * Average power across frequency bands

#### 4. Data Integrity (Testing)
Reliability is enforced through a suite of automated tests:
* **Uniqueness:** `epoch_id` checked to prevent duplication.
* **Constraints:** Sleep stages validated against accepted values defined in Pydantic.
* **Logic:** Band powers must be positive floats; `not null` checks on IDs and spectral metrics.

---

### Results
The pipeline successfully processed a batch of ~24-hour recordings from the PhysioNet Sleep-EDF database.

**Generated Insights:**
* Sleep architecture breakdown
* Frequency of nocturnal awakenings
* Average spectral power distribution across EEG bands

<img width="991" alt="Results Graph" src="https://github.com/user-attachments/assets/bdfa5260-817b-4d23-a8c6-bb4967a9d31a" />
