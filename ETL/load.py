"""
Loading the useful enhanced xarray dataset of NOAA ONI magnitudes
and relevant derivative metrics to CLoudflare R2 for accessibility.
"""
# non-explicit, but required packages:
# pyarrow
# s3fs

import numpy as np
import xarray as xr
import pandas as pd
import os
import boto3
import tempfile
import logging
import warnings
from prefect import task, get_run_logger

@task(name='saving-dataset-to-R2', retries=2)
def save_to_r2(enhanced_xr,
               bucket='noaa-enso-scraper',
               prefix: str = "data",
               save_netcdf: bool=True
               ):
    """
    description:
        Saves the enhanced ONI dataset to the given
        Cloudflare R2 bucket
    input:
        enhanced_xr: xarray dataset with additional metrics
        bucket_name: string of R2 bucket name
        prefix: storage prefix for folder organization
        save_netcdf: bool, whether to save a NetCDF backup of data
    output:
        dictionary of upload statuses, file sizes, and file names
    """
    
    # setting up logging
    logger = get_run_logger()
    # redirect all warnings to the logger
    logging.captureWarnings(True)
    # suppress unnecesary warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    # Cloud Access Credentials
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY")
    secret_key = os.environ.get("R2_SECRET_KEY")
    if not account_id or not access_key or not secret_key:
        raise EnvironmentError(
            "R2 credentials (R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY) must be set."
            )

    # Configure S3 client connection
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    
    upload_results = {}
    
    # Use temporary directory for local file creation
    with tempfile.TemporaryDirectory() as tmpdir:
        
        # converting xarray to dataframe
        df = enhanced_xr.to_dataframe().reset_index()

        # replacing NaNs with None for JSON compatibility
        df = df.replace({np.nan: None})

        # save as Parquet for API
        parquet_path = os.path.join(tmpdir, "enhanced_oni_latest.parquet")
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
        
        parquet_key = f"{prefix}/enhanced_oni_latest.parquet"
        parquet_size = os.path.getsize(parquet_path)
        
        logger.info(f"uploading Parquet to R2...")
        with open(parquet_path, 'rb') as f:
            s3_client.upload_fileobj(
                f,
                bucket,
                parquet_key,
                ExtraArgs={
                    'ContentType': 'application/vnd.apache.parquet',
                    'Metadata': {
                        'format': 'parquet',
                        'records': str(len(df)),
                        'source': 'noaa-oni-etl'
                    }
                }
            )
        
        upload_results['parquet'] = {
            'key': parquet_key,
            'size_kb': parquet_size / 1024,
            'records': len(df)
        }
        logger.info(f"Parquet uploaded successfully!")
        
        # Upload enhanced df as a NetCDF file as an optional backup
        if save_netcdf:
            logger.info("Converting to NetCDF format...")
            netcdf_path = os.path.join(tmpdir, "enhanced_oni_latest.nc")
            enhanced_xr.to_netcdf(netcdf_path)
            
            netcdf_key = f"{prefix}/enhanced_oni_latest.nc"
            netcdf_size = os.path.getsize(netcdf_path)
            
            logger.info(f"Uploading NetCDF to R2...")
            with open(netcdf_path, 'rb') as f:
                s3_client.upload_fileobj(
                    f,
                    bucket,
                    netcdf_key,
                    ExtraArgs={
                        'ContentType': 'application/x-netcdf',
                        'Metadata': {
                            'format': 'netcdf',
                            'source': 'noaa-oni-etl'
                        }
                    }
                )
            
            upload_results['netcdf'] = {
                'key': netcdf_key,
                'size_kb': netcdf_size / 1024
            }
            logger.info(f"NetCDF uploaded successfully!")
    
    return upload_results

@task(name="retrieving-df-from-R2", retries=3, retry_delay_seconds=10)
def get_from_r2(
    bucket='noaa-enso-scraper',
    prefix: str = "data",
    format: str = "parquet"
    ):
    """
    description:
        downloads the lateest enhanced ONI dataset from R2 in the specified format
    input:
        bucket: string of R2 bucket name
        prefix: storage prefix for folder organization
        format: string "parquet" or "netcdf"a
    output:
        enhanced ONI file as an xarray dataset or pandas dataframe
    """

    # setting up logging
    logger = get_run_logger()
    # redirect all warnings to the logger
    logging.captureWarnings(True)
    # suppress unnecesary warnings
    warnings.filterwarnings("ignore", category=UserWarning)

    # Cloud Access Credentials
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY")
    secret_key = os.environ.get("R2_SECRET_KEY")
    if not account_id or not access_key or not secret_key:
        raise EnvironmentError(
            "R2 credentials (R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY) must be set."
            )

    # Configure S3 client connection
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    
    if format == "parquet":
        key = f"{prefix}/enhanced_oni_latest.parquet"
        logger.info('Downloading Parquet from R2...')

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            s3_client.download_fileobj(bucket, key, tmp)
            tmp_path = tmp.name
        
        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)  # Clean up temp file
        
        logger.info(f"Loaded Parquet file as pandas dataframe")
        return df
    
    elif format == "netcdf":
        key = f"{prefix}/enhanced_oni_latest.nc"
        logger.info('Downloading netCDF from R2...')

        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
            s3_client.download_fileobj(bucket, key, tmp)
            tmp_path = tmp.name
        
        ds = xr.open_dataset(tmp_path)
        os.unlink(tmp_path)  # Clean up temp file
        
        logger.info(f"Loaded NetCDF dataset as xarray dataset")
        return ds
    
    else:
        raise ValueError(f"Unsupported format: {format}. Use 'parquet' or 'netcdf'.")


