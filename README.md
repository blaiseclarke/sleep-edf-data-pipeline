## Sleep-EDF Analytics Pipeline
#### Python, Prefect, MNE-Python, Pydantic, Snowflake, dbt, SQL

Converts raw Sleep-EDF EEG recordings into sleep-epoch metrics and summary statistics.
Python extracts and cleans -> Snowflake storage -> dbt models create clinical insights


### Why This Exists
Clinical data is usually trapped in raw, heavy file formats like EDF, which can be difficult to query at scale. 
This project creates a scalable ELT pipeline to turn raw EEG signals into queryable sleep metrics.

### Architecture
<img width="1270" height="269" alt="Screenshot 2025-11-22 at 8 06 29 AM" src="https://github.com/user-attachments/assets/5ecabb9a-6b37-460e-9959-8b0dbab518a9" />

**Source:** [PhysioNet Sleep-EDF Database](https://www.physionet.org/content/sleep-edfx/1.0.0/) (ingestion)

**Ingestion:** Python + MNE-Python (signal processing and feature extraction)

**Warehousing:** Snowflake (storage and computing)

**Transformation:** dbt (modeling and testing)

### Engineering
This pipeline was upgraded from a script-based workflow to a production-grade data system:
* **Orchestration (Prefect):** Python extraction logic wrapped in a Prefect flow, providing structured logging and retries for failed tasks.
* **Data Contracts (Pydantic):** Developed strict schema to validate every epoch before ingestion occurs.
* **Automated Testing (Pytest):** Unit tests to validate ingestion logic and constraints.
* **CI/CD (Github Actions):** Test suite is triggered on every push.

### Quick Start
```bash
# Extract raw EDF to CSV
python3 ingest_data.py

# Run ingestion pipeline
python3 pipeline.py

# Execute warehouse transformations
dbt run && dbt test
```

Outputs:
```bash
sleep_data.csv           # raw extracted epochs
sleep_data_validated.csv # validated epochs
```

### Extraction (Python/MNE)
Built using `mne` for polysomnograph (PSG) ingestion and annotation alignment.

Features extracted:
- Power spectral density -> delta, theta, alpha, sigma, beta bands
- Labels mapped to standardized sleep stages: `W, N1, N2, N3, REM, MOVE, NAN`
- ~30-second epochs, ~24 hours per subject
- Current note: `preload=True` speeds up FFT, but increases memory usage. May require batching for large datasets.

### Warehousing (Snowflake)
Loaded epoched extraction CSV data into Snowflake
Allows for the pipeline to scale without a need for refactoring or local compute

### Transformation (dbt)
Pipeline structure:

- Staging
  - Standardized column naming and explicit type casting
- Intermediate
  - Rolling power averages over five sliding epochs, for smoothing deviations and artifacts
  - Sleep stage transition detection
- Sleep Summary
  - Compressed epoch rows into a single patient summary table
  - Calculated sleep architecture: deep/light/REM %, awakenings, and average power

### Data Integrity (dbt)
- Unique and not-null tests for `epoch_id` to prevent duplication.
- Accepted values for sleep stages to ensure consistency with Pydantic constraints.
- Band powers must be positive floats
- Tests run automatially via CI

### Results
Processed batch of single ~24-hour recordings from 10 subjects from the PhysioNet Sleep-EDF database.
Results include:
- Total sleep time and architecture breakdown
- Number of awakenings
- Average power across EEG bands

<img width="991" height="717" alt="Screenshot 2025-11-23 at 9 46 50 PM" src="https://github.com/user-attachments/assets/bdfa5260-817b-4d23-a8c6-bb4967a9d31a" />


