# Containerized Biosignal ELT Pipeline (Sleep-EDF)

![Python](https://img.shields.io/badge/Python-3.12+-blue?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-FFF000?logo=duckdb&logoColor=black)
![Pandera](https://img.shields.io/badge/Pandera-Validation-E94F37?logo=pandera&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformation-FF694B?logo=dbt&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-070E28?logo=prefect&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&logoColor=white)
![CI](https://img.shields.io/badge/GitHub_Actions-CI-2088FF?logo=github-actions&logoColor=white)

### Project Overview
This project is an ELT pipeline designed to ingest, validate, and analyze clinical sleep data. It processes the [PhysioNet Sleep-EDF Expanded](https://physionet.org/content/sleep-edfx/1.0.0/) dataset, transforming raw polysomnography (PSG) signals into queryable sleep metrics.

The architecture uses **MNE** for advanced signal processing, **Prefect** for orchestration, and a hybrid warehousing strategy supporting both **DuckDB** (local) and **Snowflake** (production).

---

### Live Demo
[Streamlit](https://sleepedf-demo.streamlit.app/)

Explore sleep architecture and power ratios from the Sleep-EDF age study dataset. Queries dbt models directly from DuckDB.

**Features:**
*   **Subject Viewer**: Inspect individual recordings (hypnogram, sleep architecture, spectral power).
*   **Clinical Metrics**: Total sleep time, sleep efficiency, awakenings, and sleep stage percentages, scoped to each subject's main sleep episode.
*   **Accessible by construction**: see below.

#### Accessibility

The dashboard is built so that nothing is gated behind colour, motion, or a mouse:

*   **Palettes are validated, not eyeballed.** Every colour is checked against colour-vision-deficiency simulation in both light and dark modes — worst adjacent CVD ΔE 9.2 (light) / 9.4 (dark) against a threshold of 8. The ordinal ramp is verified monotone in lightness and single-hue. Regression tests in `tests/test_viz.py` assert the contrast floors so a future palette edit cannot quietly break them.
*   **Colour is never the only channel.** Every chart carries direct labels, a legend where there are two or more series, and a table view. An optional **pattern fill** toggle adds hatching to the sleep architecture bar for full colour blindness, greyscale printing, and forced-colors mode.
*   **Contrast.** Body text clears WCAG AA against its surface (7.7:1 or better). In-segment labels are set at 19px bold — WCAG large text — because neither ink token clears 4.5:1 against the blue fill.
*   **Dark mode is selected, not flipped.** The dark palette is its own set of steps chosen for the dark surface, and the charts follow the viewer's light/dark setting.
*   **Motion and focus.** `prefers-reduced-motion` is respected and chart transitions are disabled outright; keyboard focus rings are restored at high contrast.

> **On `data/sleep_data.db`.** The demo is deployed straight from this repository and queries that file, and Streamlit Cloud has no build step that could regenerate it, so it is tracked (~21 MB) despite `.gitignore` excluding `data/` and `*.db` in general. `.gitignore` carries an explicit negation to record that. Run `make demo-db` to rebuild it after changing a model; it builds into a fresh file, because DuckDB does not reclaim pages and rebuilding in place grows the artifact every time.

---

### Architecture
<img width="1270" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/5ecabb9a-6b37-460e-9959-8b0dbab518a9" />

| Stage | Tech Stack | Description |
| :--- | :--- | :--- |
| **Source** | **PhysioNet** | Sleep-EDF Database (raw .edf files) |
| **Ingestion** | **Python + MNE** | Signal processing, FFT, and feature extraction |
| **Validation** | **Pandera** | Schema-level validation and error logging |
| **Orchestration** | **Prefect** | Parallel mapping and flow management |
| **Warehousing** | **DuckDB / Snowflake** | Portable storage for raw and modeled data |
| **Transformation** | **dbt** | SQL-based modeling for clinical insights |
| **Runtime** | **Docker** | Reproducible environment for ingestion and orchestration |

---

### Key Features

* **Data Validation:** Uses `pandera` for schema-level validation of biosignal dataframes.
* **Parallel Ingestion:** Processes subjects concurrently using Prefect's mapping execution.
* **Hybrid Warehousing:** Writes to local DuckDB (dev) or Snowflake (prod) using a unified `WarehouseClient` protocol.
* **Unified Orchestration:** Prefect orchestrates both Python ingestion logic and downstream `dbt` models for a seamless ELT pipeline.
* **Upfront Fetching:** Pre-fetches MNE data to prevent filesystem locking during parallel extraction.
* **Robust Observability:** Thread-safe error logging captures all extraction failures in the `INGESTION_ERRORS` table.
* **Reproducibility:** Fully containerized with Docker; local development automated via Makefile.
* **CI:** Every push runs ruff, the unit tests with coverage, a full `dbt build` against a seeded DuckDB, and a Docker image build. Ruff is pinned and its rule set declared, so a new ruff release cannot turn the build red on its own.

<img width="986" height="497" alt="Prefect dashboard" src="https://github.com/user-attachments/assets/ed9f1351-14b1-4301-a5c0-e6c18ce97ccb" />

---

### Quick Start

You can run the pipeline directly on your local machine using Python, or in a container using Docker.
Docker Compose is recommended for reproducible execution.

#### Prerequisites
- Python 3.12+ *(for host execution)*
- Docker *(Docker Desktop recommended)*
- Snowflake account *(optional, DuckDB used by default for local)*
- `make` *(for automation)*
- dbt adapters — both `dbt-duckdb` and `dbt-snowflake` are in `requirements.txt`

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
# SNOWFLAKE_ROLE=your_role
# SNOWFLAKE_WAREHOUSE=COMPUTE_WH
# SNOWFLAKE_DATABASE=EEG_ANALYTICS
# SNOWFLAKE_SCHEMA=RAW

# --- dbt Configuration ---
# If using DuckDB (default), these rely on DB_PATH.
# If using Snowflake, uncomment and map to above vars:
# DBT_SOURCE_DATABASE=EEG_ANALYTICS
# DBT_SOURCE_SCHEMA=RAW
```

**2. Prefect profile**

A fresh Prefect install runs flows in ephemeral mode, so `python pipeline.py` works with no setup. If you have previously pointed Prefect at a local server, the active profile will hold a `PREFECT_API_URL` and the run fails with *"No Prefect API URL provided"* or *"Failed to reach API"* unless that server is up. Either start one, or run against the ephemeral profile:

```bash
prefect profile ls                  # which profile is active
prefect server start                # option A: run the server (gives you the UI)
PREFECT_PROFILE=ephemeral make run  # option B: no server, no UI
```

Note that `PREFECT_API_URL=""` does **not** work — Prefect reads an empty string as "not provided" rather than as a request for ephemeral mode.

**3. Environment Loading**

*   **Docker:** Automatically reads `.env`.
*   **Python (Local):** Automatically reads `.env` (via `python-dotenv`).
*   **dbt (Local):** dbt doesn't automatically read `.env` files. You'll need to export them to your shell environment first:
    ```bash
    # Export variables from .env. `set -a` marks every assignment for export;
    # quote any value containing spaces (SNOWFLAKE_PASSWORD="my secret").
    # `export $(grep ... | xargs)` is not safe here: it word-splits values and
    # silently exports a truncated password.
    set -a; source .env; set +a

    # Run dbt. profiles.yml lives in the repo root rather than ~/.dbt,
    # so every dbt command needs --profiles-dir .
    dbt debug --profiles-dir .
    ```

#### Option 1: Docker Compose

Runs the pipeline locally inside a Docker container.

```bash
# 1. Clone the repository
git clone https://github.com/blaiseclarke/sleep-edf-data-pipeline.git
cd sleep-edf-data-pipeline

# 2. Create the env file. Compose declares `env_file: .env`, so it fails
#    outright if this is missing.
cp .env.example .env

# 3. Build and run
docker compose up --build

# Note: Prefect automatically kicks off `dbt deps` and `dbt build` against the target warehouse after successful ingestion
```
#### Option 2: Local Development (Makefile)

The recommended way for local development and testing.

```bash
# 1. Clone repo
git clone https://github.com/blaiseclarke/sleep-edf-data-pipeline.git
cd sleep-edf-data-pipeline

# 2. Setup and install
make install
cp .env.example .env        # then edit if you want anything other than the defaults

# 3. Lint, format and test
make all

# 4. Initialize the local database
make setup-db

# 5. Everyday targets
make lint       # Check for errors
make format     # Autoformat code
make test       # Unit tests
make coverage   # Unit tests with a coverage report
make run        # Run parallel ingestion and the dbt ELT pipeline
make dashboard  # Serve the Streamlit dashboard
make docker     # Build the container image

# 6. Work on the dbt models without downloading 2 GB of recordings
make seed       # Synthetic epochs, including a daytime nap
make dbt        # dbt deps + dbt build against them

# 7. Test observability
PYTHONPATH=. python scripts/simulate_error.py  # Verifies failures reach INGESTION_ERRORS
```

#### Option 3: Manual Python Execution

```bash
# 1. Install dependencies manually
pip install -r requirements.txt
cp .env.example .env

# 2. Initialize the local database. Scripts under scripts/ import from the
#    project root, so they need PYTHONPATH=. ; `make setup-db` sets it for you.
PYTHONPATH=. python scripts/setup_db.py

# 3. Run the ingestion pipeline directly. pipeline.py sits in the root, so it
#    needs no PYTHONPATH, and it creates the warehouse tables itself.
python pipeline.py  # Also executes the dbt models for the selected target
```


---

### Technical Deep Dive

#### 1. Extraction (Python/MNE)
Built using `mne` for polysomnograph (PSG) ingestion and annotation alignment. This handles the heavy lifting of signal processing before data ever hits the warehouse.

* **Spectral Analysis:** Extracts Power Spectral Density (PSD) for delta, theta, alpha, sigma, and beta bands. Channel types are corrected on load — the EDF reader labels every channel in these files as EEG, respiration and rectal temperature included — so the Welch transform runs over EEG alone rather than all seven channels.
* **Standardization:** Maps raw annotations to clinical sleep stages (`W, N1, N2, N3, REM`). Movement and unscored epochs (`MOVE`, `NAN`) are filtered out before validation.
* **Memory Efficiency:** Utilizes `preload=False` (memory mapping) to handle large EEG files with minimal RAM impact.
* **Configurable Parameters:** The pipeline range and logic are controlled via environment variables:
    * `STARTING_SUBJECT` / `ENDING_SUBJECT`: Define the participant ID range (0-82 for age study, 0-21 for telemetry study).
    *   `RECORDING`: Specifies which session recording to fetch (default: 1). Age study only — the telemetry study carries a single recording per subject and ignores this.
    * `DB_PATH`: Local path for the DuckDB database (default: `data/sleep_data.db`).
    * `PREFECT_MAX_WORKERS`: Cap on concurrently extracted subjects, applied through the flow's `ThreadPoolTaskRunner` (default: 3). Each worker holds a batch of epochs in memory, so this is the memory/throughput dial.
    * `STUDY`: Selects the Sleep-EDF study (options: `age`, `telemetry`, default: `age`).

#### 2. Warehousing (DuckDB / Snowflake)
The pipeline is warehouse-agnostic via the `WarehouseClient` protocol, which covers table creation (`ensure_tables_exist`), loading (`load_epochs`), and error logging (`log_ingestion_error`).
* **DuckDB (Local):** Default for local development. Data is persisted to `data/sleep_data.db` without cloud overhead.
* **Snowflake (Cloud):** Used for production-scale storage and analytics, separating compute from storage. Loads go through an internal stage with `PUT` followed by `COPY INTO`.

Both backends declare `SLEEP_EPOCHS` and `INGESTION_ERRORS` with identical column names, and a test asserts the two schemas cannot drift apart. Table creation is idempotent and runs automatically at the start of the flow, so no manual DDL is needed on either warehouse — `scripts/setup_db.py` remains available to do it as a separate step. The Snowflake role needs `CREATE TABLE` on the target schema.

**Running against Snowflake:**

```bash
# Set WAREHOUSE_TYPE=snowflake plus the SNOWFLAKE_* variables in .env, then
set -a; source .env; set +a                      # dbt does not read .env itself
PYTHONPATH=. python scripts/setup_db.py          # optional; the pipeline also does this
python pipeline.py
```

Set `DBT_SOURCE_DATABASE` and `DBT_SOURCE_SCHEMA` to match `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` so the dbt models read from the table the loader wrote to.

#### 3. Transformation (dbt)
The dbt project creates a trusted data lineage, transforming raw logs into analytics-ready models:

* **Staging (`staging_sleep_data`):** Handles column standardization and explicit type casting.
* **Intermediate (`sleep_metrics`):** Calculates rolling power averages over sliding epochs to smooth out signal artifacts and deviations, and detects each subject's **main sleep episode** (see below).
* **Marts (`sleep_summary`):** Aggregates data into clinical insights:
    * Sleep architecture (deep vs. light vs. REM %)
    * Sleep efficiency and wake after sleep onset (WASO)
    * Awakening counts
    * Average power across frequency bands

##### Sleep period detection
Sleep-EDF recordings are ~22 hour ambulatory recordings that span an entire day, and many subjects nap. Aggregating over the whole recording therefore describes the *day*, not the night — it reports 22 hours of "time in bed", counts every afternoon transition into wake as an awakening, and averages band power across hours of ordinary wakefulness.

`sleep_metrics` splits each recording into sleep episodes wherever a continuous wake bout runs longer than `sleep_episode_gap_minutes` (default 60), then keeps the episode containing the most sleep. Every night-level metric in `sleep_summary` is scoped to that window; only `total_recording_minutes` describes the full recording. The dataset carries no lights-off annotation, so this window is the closest available proxy for time in bed.

Across the 77 ingested subjects this puts the cohort in a physiologically plausible range — 7.6 h time in bed, 6.8 h total sleep time, 52 min WASO, and 89% mean sleep efficiency (median 92%).

#### 4. Data Integrity & Observability
Reliability is enforced through automated checks and failure logging:
* **Validation (Pandera):** Sleep stages and spectral powers are validated against strict contracts.
* **Error Warehouse:** Failures are intercepted and logged sequentially to the `INGESTION_ERRORS` table, ensuring 100% thread safety and detailed stack trace persistence even during parallel runs.
* **dbt Tests:** Generic and `dbt_utils` tests enforce both schema and logical consistency — surrogate key uniqueness, non-null spectral powers, an `accepted_values` contract on sleep stages, sleep efficiency bounded to 0–1, and cross-column invariants (*total sleep time cannot exceed the sleep period*; *stage percentages must sum to 1*).
* **Fail-Fast Transformation:** The pipeline runs `dbt build`, which walks the DAG testing each model as it is created, so a model whose tests fail never has dependents built on top of it. Running every model first and testing afterwards would let bad data reach the marts before anything noticed. dbt output is streamed into the Prefect logs line by line rather than withheld until each command ends.

---

### Results
The pipeline successfully processed the entire PhysioNet Sleep-EDF (Age Study), ingesting and analyzing over 212,000 30-second epochs across all 78 subjects.

**Generated Insights:**
* Sleep architecture breakdown
* Frequency of nocturnal awakenings
* Average spectral power distribution across EEG bands

---


### References
* **Kemp B, Zwinderman AH, Tuk B, Kamphuisen HAC, Oberyé JJL.** *Analysis of a sleep-dependent neuronal feedback loop: the slow-wave microcontinuity of the EEG.* IEEE-BME 47(9):1185-1194 (2000).

* **Goldberger A, Amaral L, Glass L, Hausdorff J, Ivanov PC, Mark R, ... & Stanley HE.** *PhysioBank, PhysioToolkit, and PhysioNet: Components of a new research resource for complex physiologic signals.* Circulation [Online]. 101 (23), pp. e215–e220 (2000).
