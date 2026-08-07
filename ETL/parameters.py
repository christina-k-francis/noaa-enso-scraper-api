"""
parameters.py contains the default values for all parameters used across ETL scripts.
"""

NOAA_ONI_URL = "https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/enso/oni/v6/"

R2_BUCKET = "noaa-enso-scraper"
R2_PREFIX = "data"
PARQUET_FILENAME = "enhanced_oni_latest.parquet"
NETCDF_FILENAME = "enhanced_oni_latest.nc"
FORMAT_PARQUET = "parquet"
FORMAT_NETCDF = "netcdf"

DIM_YEAR = "year"
DIM_SEASON = "season"
DATASET_VAR_ONI = "ONI"
DATASET_VAR_ENSO = "ENSO"
ENSO_PHASE_LA_NINA = "LaNina"
ENSO_PHASE_EL_NINO = "ElNino"
ENSO_PHASE_NEUTRAL = "Neutral"
ENSO_PHASE_UNKNOWN = "Unknown"

FIELD_INTENSITY = "intensity"
FIELD_PHASE_DURATION = "phase_duration"
FIELD_RATE_OF_CHANGE = "rate_of_change"
FIELD_PERCENTILE = "percentile"
ENSO_PHASE_LABEL_LA_NINA = "La Niña"
ENSO_PHASE_LABEL_EL_NINO = "El Niño"
ENSO_PHASE_LABEL_NEUTRAL = "Neutral"