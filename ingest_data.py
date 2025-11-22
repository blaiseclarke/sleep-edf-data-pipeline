import numpy as np
import pandas as pd
import mne
from mne.datasets.sleep_physionet.age import fetch_data

# Configuration
SUBJECT_ID = 0 # First subject
RECORDING = 1  # First recording
EPOCH_LENGTH = 30.0  # seconds


def main():
    # Fetches data
    filepaths = fetch_data(subjects=[SUBJECT_ID], recording=[RECORDING])
    psg_file = filepaths[0][0]
    hypnogram_file = filepaths[0][1]

    print(f"Processing {psg_file}...")

    # Loading raw data
    raw = mne.io.read_raw_edf(psg_file, preload=True, verbose=False)
    annotations = mne.read_annotations(hypnogram_file)
    raw.set_annotations(annotations, emit_warning=False) # why is emit_warning set to False?

    print(f"Channel names: {raw.ch_names}")

    # Renaming channel names
    mapping = {
        "EEG Fpz-Cz": "EEG",
        "EEG Pz-Oz": "EEG2",
        "EOG horizontal": "EOG",
        "EMG submental": "EMG",
    }
    map = {k: v for k, v in mapping.items() if k in raw.ch_names}
    raw.rename_channels(map)

    # Building epochs
    events, event_id = mne.events_from_annotations(
        raw,
        event_id=None,
        chunk_duration=EPOCH_LENGTH
    )

    print(f"MNE found these events: {event_id}")

    tmax = 30.0 - 1.0 / raw.info["sfreq"]
    epochs = mne.Epochs(
        raw=raw,
        events=events,
        event_id=event_id,
        tmin=0.0,
        tmax=tmax,
        baseline=None,
        verbose=False,
        preload=True,
        on_missing='ignore'
    )

    print(f"Created {len(epochs)} epochs...")

    # Calculating power across bands
    spectrum = epochs.compute_psd(picks=['eeg'])
    psd, frequencies = spectrum.get_data(return_freqs=True)

    def calculate_band_power(psd, frequencies, fmin, fmax):
        idx = np.logical_and(frequencies >= fmin, frequencies <= fmax)
        return psd[:, 0, idx].mean(axis=1)
    
    # Create dataframe
    df = pd.DataFrame()
    df['epoch_idx'] = range(len(epochs))
    df['subject_id'] = SUBJECT_ID
    df['sleep_stage_label'] = epochs.events[:, 2]

    # Map integers back to strings
    inverse_map = {v: k for k, v in event_id.items()}
    df['sleep_stage_label'] = df['sleep_stage_label'].map(inverse_map)

    # Cleaning sleep stage names 
    clean_map = {
        'Sleep stage W' : 'W',
        'Sleep stage 1' : 'N1',
        'Sleep stage 2' : 'N2',
        'Sleep stage 3' : 'N3',
        'Sleep stage 4' : 'N3',
        'Sleep stage R' : 'REM',
        'Movement time' : 'MOVE',
        'Sleep stage ?' : 'NAN'
    }

    df['stage'] = df['sleep_stage_label'].apply(lambda x: clean_map.get(x, x))

    # Add power to dataframe
    df['delta_power'] = calculate_band_power(psd, frequencies, 0.5, 4)
    df['theta_power'] = calculate_band_power(psd, frequencies, 4, 8)
    df['alpha_power'] = calculate_band_power(psd, frequencies, 8, 12)
    df['sigma_power'] = calculate_band_power(psd, frequencies, 12, 16)
    df['beta_power'] = calculate_band_power(psd, frequencies, 16, 30)

    # Export data to output file
    output = "sleep_data.csv"
    columns_to_keep = ['subject_id', 'epoch_idx', 'stage', 'delta_power', 'theta_power', 'alpha_power', 'sigma_power', 'beta_power']
    df[columns_to_keep].to_csv(output, index=False)

    print(f"Data saved to {output}.")
    print(df[columns_to_keep].head())

if __name__ == "__main__":
    main()