## Sleep-EDF Analytics Pipeline
#### Python, Prefect, MNE-Python, Pydantic, Snowflake, dbt, SQL

Converts raw Sleep-EDF EEG recordings into sleep-epoch metrics and summary statistics.

Python extracts and cleans -> Snowflake storage -> dbt models create clinical insights


### Why This Exists
Clinical data is usually trapped in raw, heavy file formats like EDF, which can be difficult to query at scale. 
This project creates a scalable ELT pipeline to turn raw EEG signals into queryable sleep metrics.

### Architecture
<img width="1270" height="269" alt="Screenshot 2025-11-22 at 8 06 29 AM" src="https://github.com/user-attachments/assets/5ecabb9a-6b37-460e-9959-8b0dbab518a9" />

**Source:** [PhysioNet Sleep-EDF Database](https://www.physionet.org/content/sleep-edfx/1.0.0/)

**Ingestion:** Python + MNE-Python (signal processing and feature extraction)

**Warehousing:** Snowflake

**Transformation:** dbt

### Engineering
This pipeline was upgraded from a script-based workflow to a stronger data system:
* **Orchestration (Prefect):** Python extraction logic wrapped in a Prefect flow, providing logging and retries for failed tasks.
* **Data Contracts (Pydantic):** Developed schema to validate every epoch before ingestion occurs.
* **Automated Testing (Pytest):** Unit tests to validate ingestion logic and constraints.
* **CI/CD (Github Actions):** Test suite is triggered on every push.

<img width="986" height="497" alt="Screenshot 2025-12-09 at 10 57 55 PM" src="https://github.com/user-attachments/assets/71dabb27-486a-4b49-9dce-5d615d02172a" />

### Quick Start
```bash
# Prerequisites 
# Python 3.10+
# Snowflake account (standard or trial)
# Prefect Cloud (optional)
# dbt (requires ~/.dbt/profiles.yml configured for Snowflake)

# Clone repo
git clone https://github.com/blaiseclarke/sleep-edf-data-pipeline
cd sleep-edf-data-pipeline

# Install dependencies (MNE, Prefect, dbt-snowflake, Pydantic)
pip install -r requirements.txt

# Run ingestion pipeline
python3 pipeline.py

# Manual step: upload to Snowflake
# 1. Create a database called EEG_ANALYTICS
# 2. Create a schema named RAW
# 3. Upload sleep_data_validated.csv into the schema
# 4. Name the table SLEEP_EPOCHS so dbt can find it

# Execute warehouse transformations
dbt deps
dbt run
dbt test
```

Outputs:
* Raw extracted epochs - `sleep_data.csv `
* Validated epochs - `sleep_data_validated.csv`
* Snowflake tables - `staging_sleep_data`, `sleep_metrics`, `sleep_summary`
* Prefect dashboard - View at `http://127.0.0.1:4200`

### Extraction (Python/MNE)
Built using `mne` for polysomnograph (PSG) ingestion and annotation alignment.

Features extracted:
- Power spectral density -> delta, theta, alpha, sigma, beta bands
- Labels mapped to standardized sleep stages: `W, N1, N2, N3, REM, MOVE, NAN`
- Configurable epochs, ~24 hours per subject
- *Current note:* `preload=True` speeds up FFT, but increases memory usage. May require batching for large datasets.

### Warehousing (Snowflake)
Loaded epoched extraction CSV data into Snowflake

Allows for the pipeline to scale without a need for refactoring or local compute

### Transformation (dbt)
Pipeline structure:

- Staging
  - Standardized column naming and explicit type casting
- Intermediate
  - Rolling power averages over sliding epochs, to smooth deviations and artifacts
  - Sleep stage transition detection
- Sleep Summary
  - Compressed epoch rows into a single summary table
  - Calculated sleep architecture: deep/light/REM %, awakenings, and average power

### Data Integrity (dbt)
- Unique and not-null tests for `epoch_id` to prevent duplication.
- Accepted values for sleep stages to ensure consistency with Pydantic constraints.
- Band powers must be positive floats
- Tests run automatially via CI

### Results
Processed batch of single ~24-hour recordings from subjects from the PhysioNet Sleep-EDF database.
Results include:
- Total sleep time and architecture breakdown
- Number of awakenings
- Average power across EEG bands

<img width="991" height="717" alt="Screenshot 2025-11-23 at 9 46 50 PM" src="https://github.com/user-attachments/assets/bdfa5260-817b-4d23-a8c6-bb4967a9d31a" />


