"""
Main ETL pipeline script for execution.
"""

from prefect import flow, get_run_logger
from extract import scrape_noaa_oni
from transform import process_oni_dataset
from load import save_to_r2


@flow(name='download-enhance-upload-noaa-flow')
def noaa_enso_etl_pipeline_flow():
    """ Main ETL pipeline """
    # setting up logging
    logger = get_run_logger()
    # getting ONI data from NOAA
    raw_ds = scrape_noaa_oni(current_year=2025)
    # enhancing the ONI data with derived metrics
    processed_ds = process_oni_dataset(raw_ds)
    # uploading the enhanced ONI ds to R2
    logger.info('Uploading Enhanced ONI dataset to R2')
    results = save_to_r2(processed_ds)
    logger.info(f"Upload Complete!\n{results}")

    




