"""
Main ETL pipeline script for execution.
"""

from prefect import flow, get_run_logger
from extract import scrape_noaa_oni
from transform import process_oni_dataset
from load import save_to_r2
from datetime import datetime, timezone
from firestore_client import (
    create_pipeline_run,
    update_pipeline_stage,
    complete_pipeline_run,
    update_data_freshness

)

@flow(name='download-enhance-upload-noaa-flow')
def noaa_enso_etl_pipeline_flow():
    """ Main ETL pipeline """
    # setting up logging
    logger = get_run_logger()

    # generate a unique run ID and record the starttime
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    started_at = run_id

    # create firestore document
    create_pipeline_run(run_id, triggered_by="scheduled")
    logger.info(f"Pipeline run started: {run_id}")

    try:
        # EXTRACT: getting ONI data from NOAA
        raw_ds = scrape_noaa_oni(current_year=2026)
        update_pipeline_stage(run_id, "scrape_status", "success")
        logger.info("Extraction Complete!")
    
        # TRANFORM: enhancing the ONI data with derived metrics
        processed_ds = process_oni_dataset(raw_ds)
        update_pipeline_stage(run_id, "transform_status", "success")
        logger.info("Transformation complete!")

        # LOAD: uploading the enhanced ONI parquet to R2
        results = save_to_r2(processed_ds)
        update_pipeline_stage(run_id, "deploy_status", "success", extra={
            "records_ingested": results.get("parquet", {}).get("records"),
        })
        logger.info(f"Upload Complete!\n{results}")

        # Updating the data_freshness firestore doc
        df = processed_ds.to_dataframe().reset_index()
        valid = df[df["ONI"].notna()]
        latest_row = valid.iloc[-1]

        update_data_freshness(
            latest_year=int(latest_row["year"]),
            latest_season=str(latest_row["season"]),
            total_records=len(valid),
            source_url="https://cpc.ncep.noaa.gov/products/analysis_monitoring/"
                       "ensostuff/ONI_v5.php",
        )
        
        # Run complete
        complete_pipeline_run(run_id, started_at)
        logger.info("Pipeline run complete!")

    except Exception as e:
        # Ensure that every failure/error is recorded
        logger.error(f"Pipeline failed: {e}")
        complete_pipeline_run(run_id, started_at, error_message=str(e))
        raise
