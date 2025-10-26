"""
Transforming NOAA's ENSO data by deriving useful metrics and adding historical context including:
- intensity classification
- phase duration
- rate of change (dONI/dt)
- percentile ranking
"""

import xarray as xr
import numpy as np
from prefect import task, flow, get_run_logger
import logging
import warnings

@task(name="classifying-ENSO-intensities", retries=2)
def calculate_intensity_array_task(oni_values):
    """
    description:
        Classifies the intensities of an array of ENSO events,
        based on their respective ONI values.
    """

    def classify_intensity(oni_value):
        """
        description:
            Categorizing ENSO events based on their intensity,
            implicated by their ONI values.
        
        Categories based on NOAA definitions:
        - None: |ONI| < 0.5
        - Weak: 0.5 ≤ |ONI| < 1.0
        - Moderate: 1.0 ≤ |ONI| < 1.5
        - Strong: 1.5 ≤ |ONI| < 2.0
        - Very Strong: |ONI| ≥ 2.0
        """
        if np.isnan(oni_value):
            return 'Unknown'
        
        abs_oni = abs(oni_value)
        
        if abs_oni < 0.5:
            return 'None'
        elif abs_oni < 1.0:
            return 'Weak'
        elif abs_oni < 1.5:
            return 'Moderate'
        elif abs_oni < 2.0:
            return 'Strong'
        else:
            return 'Very Strong'

    intensity_func = np.vectorize(classify_intensity)
    return intensity_func(oni_values)

@task(name="counting-consecutive-ENSO-phases", retries=2)
def calculate_phase_duration_task(oni_values, enso_phases):
    """
    description:
        Calculate how many consecutive months the current ENSO phase has persisted.
    input:
        xarray data array of Oceanic Niño Index values
    output:
        xarray data array of the same shape with duration values.
    """
    duration = np.zeros_like(oni_values, dtype=float)
    
    # Flatten arrays for easier iteration
    oni_flat = oni_values.flatten()
    enso_flat = enso_phases.flatten()
    duration_flat = duration.flatten()
    
    current_phase = None
    current_duration = 0
    
    for idx in range(len(oni_flat)):
        if np.isnan(oni_flat[idx]):
            duration_flat[idx] = np.nan
            continue
        
        phase = enso_flat[idx]
        
        if phase == current_phase:
            current_duration += 1
        else:
            current_phase = phase
            current_duration = 1
        
        duration_flat[idx] = current_duration
    
    return duration_flat.reshape(oni_values.shape)

@task(name="calculating-ENSO-fractional-derivative", retries=2)
def calculate_rate_of_change_task(oni_values):
    """
    description:
        Calculates the month-to-month rate of change for ONI values. 
        I.E. d(ONI)/dt. This returns differences from the previous season.
    """

    rate_of_change = np.full_like(oni_values, np.nan)
    
    # Flatten for easier sequential processing
    oni_flat = oni_values.flatten()
    roc_flat = rate_of_change.flatten()
    
    for idx in range(1, len(oni_flat)):
        if not np.isnan(oni_flat[idx]) and not np.isnan(oni_flat[idx - 1]):
            roc_flat[idx] = oni_flat[idx] - oni_flat[idx - 1]
    
    return roc_flat.reshape(oni_values.shape)

@task(name="calculating-ENSO-percentile-rankings", retries=2)
def calculate_percentile_ranking_task(oni_values):
    """
    description:
        Calculate percentile ranking for each ONI value relative to all historical values.
    output:
        Returns percentile (0-100) for each observation.
    """
    # Get all valid (non-NaN) historical values
    valid_values = oni_values[~np.isnan(oni_values)]
    
    if len(valid_values) == 0:
        return np.full_like(oni_values, np.nan)
    
    # Calculate percentile for each value
    percentiles = np.full_like(oni_values, np.nan)
    
    for year in range(oni_values.shape[0]):
        for season in range(oni_values.shape[1]):
            if not np.isnan(oni_values[year, season]):
                # Calculate what percentile this value falls into
                percentile = (valid_values < oni_values[year, season]).sum() / len(valid_values) * 100
                percentiles[year, season] = percentile
    
    return percentiles

@flow(name="transform-ONI-dataset-flow", retries=2, log_prints=True)
def process_oni_dataset(oni_xr):
    """
    description:
        this is the main transformation function that adds 
        derived metrics to the provided ONI xarray dataset.
    
    input:
        oni_xr: xarray.Dataset (Raw ONI dataset from extraction step)
    
    output:
        enhanced_oni_xr: Raw ONI dataset with additinal variables

    """
    # setting up logging
    logger = get_run_logger()
    # redirect all warnings to the logger
    logging.captureWarnings(True)
    # suppress unnecesary warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    # Extract ONI values and ENSO phases
    oni_values = oni_xr['ONI'].values
    enso_phases = oni_xr['ENSO'].values
    
    logger.info("Calculating ENSO intensity classifications...")
    intensity = calculate_intensity_array_task(oni_values)
    
    logger.info("Calculating ENSO phase durations...")
    phase_duration = calculate_phase_duration_task(oni_values, enso_phases)
    
    logger.info("Calculating d(ONI)/dt...")
    rate_of_change = calculate_rate_of_change_task(oni_values)
    
    logger.info("Calculating ENSO magnitude percentile rankings...")
    percentiles = calculate_percentile_ranking_task(oni_values)
    
    # Creating enhanced dataset
    logger.info("Building the enhanced dataset...")
    enhanced_xr = xr.Dataset(
        {
            "ONI": (["year", "season"], oni_values),
            "intensity": (["year", "season"], intensity),
            "phase_duration": (["year", "season"], phase_duration),
            "rate_of_change": (["year", "season"], rate_of_change),
            "percentile": (["year", "season"], percentiles),
        },
        coords={
            "year": oni_xr.coords["year"],
            "season": oni_xr.coords["season"],
            "ENSO": (["year", "season"], enso_phases),
        },
        attrs={
            **oni_xr.attrs,  # Preserve original attributes
            "derived_metrics": "intensity, phase_duration, rate_of_change, percentile",
            "intensity_categories": "None (<0.5), Weak (0.5-1.0), Moderate (1.0-1.5), Strong (1.5-2.0), Very Strong (≥2.0)",
        }
    )
    
    logger.info("Transformation complete!")
    return enhanced_xr

    