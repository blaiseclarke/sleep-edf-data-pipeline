## Sleep-EDF Analytics Pipeline
### Python, MNE-Python, Snowflake, dbt, SQL

Clinical data is usually trapped in raw, heavy file formats like EDF (European Data Format), which can be difficult to query at scale. 
Objective for this project was to create a scalable ELT pipeline to turn raw EEG signals into clinical insights ready for use in downstream analysis.

### Architecture
<img width="1270" height="269" alt="Screenshot 2025-11-22 at 8 06 29 AM" src="https://github.com/user-attachments/assets/5ecabb9a-6b37-460e-9959-8b0dbab518a9" />

**Source:** [PhysioNet Sleep-EDF Database](https://www.physionet.org/content/sleep-edfx/1.0.0/) (ingestion)

**Extraction:** Python (signal processing and feature extraction)

**Warehousing:** Snowflake (storage and computing)

**Transformation:** dbt (modeling and testing)

### Extraction (Python/MNE)
Used `mne` to ingest polysomnograph (PSG) files and hynogram annotations from the Sleep-EDF database. 
- Signal Processing: Transformed raw EEG signals from time to frequency domains using Power Spectral Density (PSD) calculations.
- Optimization: The `preload` parameter is currently set to `True` for EDF extraction. This saves the loaded EDF data to RAM for faster FFT and matrix math calculation,
  but there is risk of memory depletion with larger batches.
- Standardization: mapped MNE integer labels to clinical stages (`W`, `N1`, `N2`, `N3`, `REM`) before exporting.

The sleep stage annotations and band powers for each epoch are then saved to a .csv for warehousing.

### Warehousing (Snowflake)
Loaded epoched extraction data into Snowflake.
This allows for the pipeline to scale without a need for refactoring or local compute.

### Transformation (dbt)
Built a Directed Acyclic Graph (DAG):
- Staging
  - Standardized column naming and explicit type casting
- Intermediate
  - Used window functions to calculate rolling power averages over five sliding epochs, for smoothing deviations and artifacts.
  - Used `LAG()` function to detect transitions in sleep stage.
- Sleep Summary
  - Compressed epoch rows into a single patient summary table.
  - Calculated sleep architecture (deep/light/REM percentages) and sleep quality (delta and sigma power averages).

### QC and Testing
Implemented schema tests to ensure reliability and quality.
- Unique values for `epoch_id` surrogate key.
- Accepted values for sleep stages.

### Results
Processed batch of single ~24-hour recordings from 10 subjects from the PhysioNet Sleep-EDF database.
Results include number of awakenings, light/deep/REM sleep architecture, and average delta and sigma power.

<img width="991" height="717" alt="Screenshot 2025-11-23 at 9 46 50 PM" src="https://github.com/user-attachments/assets/bdfa5260-817b-4d23-a8c6-bb4967a9d31a" />


