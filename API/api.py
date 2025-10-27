"""
API Service for accessing data from the Enhanced Oceanic Niño Index (ONI) dataset. 
ENSO magnitudes and definitions originated from NOAA's public data table at: 
https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php

Author: Christina Francis
Data Source: NOAA Climate Prediction Center
"""

import os
import boto3
import tempfile
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from enum import Enum


# ---- Defining ENUMS (constant values) --------------------------------#
class ENSOEvent(str, Enum):
    """Valid ENSO event types based on NOAA classifications"""
    NEUTRAL = "Neutral"
    EL_NINO = "ElNino"
    LA_NINA = "LaNina"
    UNKNOWN = "Unknown"

class Intensity(str, Enum):
    """Valid ENSO intensity classifications"""
    NONE = "None"
    WEAK = "Weak"
    MODERATE = "Moderate"
    STRONG = "Strong"
    VERY_STRONG = "Very Strong"
    UNKNOWN = "Unknown"

class Season(str, Enum):
    """Valid 3-month rolling seasons"""
    DJF = "DJF"  # December-January-February
    JFM = "JFM"  # January-February-March
    FMA = "FMA"  # February-March-April
    MAM = "MAM"  # March-April-May
    AMJ = "AMJ"  # April-May-June
    MJJ = "MJJ"  # May-June-July
    JJA = "JJA"  # June-July-August
    JAS = "JAS"  # July-August-September
    ASO = "ASO"  # August-September-October
    SON = "SON"  # September-October-November
    OND = "OND"  # October-November-December
    NDJ = "NDJ"  # November-December-January

# ---- API App Initialization --------------------------------#
app = FastAPI(
    title="Enhanced ONI API",
    description="Access historical and current Oceanic Niño Index data with derived metrics",
    version="1.0.0",
    contact={
        "name": "Your Name",
        "email": "your.email@example.com",
    },
    license_info={
        "name": "MIT",
    },
)

# ---- Primary Data Loading Function --------------------------------#
def load_oni_data() -> pd.DataFrame:
    """
    description:
        downloads the latest enhanced ONI dataset from R2 

    output:
        enhanced ONI file as a pandas dataframe
    """

    bucket = 'noaa-enso-scraper'

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
    
    key = f"data/enhanced_oni_latest.parquet"

    try:    
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            s3_client.download_fileobj(bucket, key, tmp)
            tmp_path = tmp.name
        
        df = pd.read_parquet(tmp_path)
        os.unlink(tmp_path)  # Clean up temp file
        
        return df

    except Exception as e:
        raise Exception(f"Failed to load ONI data from R2: {str(e)}")

# ---- Helper Filtering Function --------------------------------#
def filter_dataframe(
    df: pd.DataFrame,
    year: int = None,
    enso_event: ENSOEvent = None,
    season: Season = None,
    intensity: Intensity = None,
    min_oni: float = None,
    max_oni: float = None,
) -> pd.DataFrame:
    """
    description:
        applies filters to the ONI DataFrame based on query parameters.
    
    input:
        df: pd.DataFrame of Full ONI dataset
        year: int, optional filter by specific year
        enso_event: ENSOEvent, optional filter by ENSO event type (Neutral, ElNino, LaNina, Unknown)
        season: Season, optional filter by 3-month season (DJF, JFM, etc.)
        intensity: Intensity, optional filter by intensity classification
        min_oni: float, optional minimum ONI value (inclusive)
        max_oni: float, optional maximum ONI value (inclusive)

    output:
        pandas dataframe of the filtered Enhanced ONI dataset
        
    """
    # Start with full dataset
    filtered = df.copy()
    
    # Apply year filter
    if year is not None:
        filtered = filtered[filtered['year'] == year]
    
    # Apply ENSO event filter
    if enso_event is not None:
        filtered = filtered[filtered['ENSO'] == enso_event.value]
    
    # Apply season filter
    if season is not None:
        filtered = filtered[filtered['season'] == season.value]
    
    # Apply intensity filter
    if intensity is not None:
        filtered = filtered[filtered['intensity'] == intensity.value]
    
    # Apply ONI range filters
    if min_oni is not None:
        filtered = filtered[filtered['ONI'] >= min_oni]
    
    if max_oni is not None:
        filtered = filtered[filtered['ONI'] <= max_oni]
    
    return filtered

# ---- API Endpoints --------------------------------#
@app.get("/")
def root():
    """
    Root endpoint providing API information and links to documentation.
    """
    return {
        "message": "Welcome to the Enhanced ONI API",
        "version": "1.0.0",
        "documentation": "/docs",
        "endpoints": {
            "all_data": "/oni/data",
            "statistics": "/oni/stats",
            "latest": "/oni/latest",
            "by_year": "/oni/data?year=2023",
            "by_event": "/oni/data?enso_event=ElNino",
            "by_season": "/oni/data?season=DJF",
            "by_intensity": "/oni/data?intensity=Strong",
        }
    }


@app.get("/oni/data")
def get_oni_data(
    year: int = Query(None, description="Filter by specific year (e.g., 2023)"),
    enso_event: ENSOEvent = Query(None, description="Filter by ENSO event type"),
    season: Season = Query(None, description="Filter by 3-month season"),
    intensity: Intensity = Query(None, description="Filter by intensity classification"),
    min_oni: float = Query(None, description="Minimum ONI value (inclusive)"),
    max_oni: float = Query(None, description="Maximum ONI value (inclusive)"),
    limit: int = Query(None, description="Limit number of results returned"),
):
    """
    Retrieve ONI data with flexible filtering options.
    
    This endpoint allows you to query the ONI dataset using multiple filters:
    - Year: Get data for a specific year
    - ENSO Event: Filter by Neutral, ElNino, LaNina, or Unknown
    - Season: Filter by 3-month rolling season (DJF, JFM, etc.)
    - Intensity: Filter by strength (None, Weak, Moderate, Strong, Very Strong)
    - ONI Range: Filter by minimum and/or maximum ONI values
    - Limit: Restrict the number of results
    
    Examples:
    - /oni/data?year=2023
    - /oni/data?enso_event=ElNino&intensity=Strong
    - /oni/data?season=DJF&min_oni=1.0
    - /oni/data?year=2015&season=NDJ
    """
    try:
        # Load data from R2
        df = load_oni_data()
        
        # Apply filters
        filtered_df = filter_dataframe(
            df, year, enso_event, season, intensity, min_oni, max_oni
        )
        
        # Check if any results found
        if filtered_df.empty:
            raise HTTPException(
                status_code=404, 
                detail="No data found matching the specified criteria"
            )
        
        # Apply limit if specified
        if limit is not None:
            filtered_df = filtered_df.head(limit)
        
        # Convert to dictionary format for JSON response
        # 'records' orientation creates a list of dictionaries
        result = filtered_df.to_dict('records')
        
        return {
            "count": len(result),
            "filters_applied": {
                "year": year,
                "enso_event": enso_event.value if enso_event else None,
                "season": season.value if season else None,
                "intensity": intensity.value if intensity else None,
                "min_oni": min_oni,
                "max_oni": max_oni,
            },
            "data": result
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")


@app.get("/oni/stats")
def get_oni_statistics(
    year: int = Query(None, description="Calculate stats for specific year"),
    enso_event: ENSOEvent = Query(None, description="Calculate stats for specific ENSO event"),
):
    """
    Get statistical summary of ONI data.
    
    Returns aggregate statistics including:
    - Count of observations
    - Mean ONI value
    - Min/Max ONI values
    - Standard deviation
    - Distribution by ENSO event type
    - Distribution by intensity
    
    Optionally filter statistics by year or ENSO event type.
    """
    try:
        # Load data
        df = load_oni_data()
        
        # Apply filters if specified
        if year is not None:
            df = df[df['year'] == year]
        if enso_event is not None:
            df = df[df['ENSO'] == enso_event.value]
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found for specified filters")
        
        # Calculate statistics (excluding NaN values)
        valid_oni = df['ONI'].dropna()
        
        return {
            "filters": {
                "year": year,
                "enso_event": enso_event.value if enso_event else None,
            },
            "total_observations": len(df),
            "valid_oni_values": len(valid_oni),
            "oni_statistics": {
                "mean": float(valid_oni.mean()),
                "median": float(valid_oni.median()),
                "std": float(valid_oni.std()),
                "min": float(valid_oni.min()),
                "max": float(valid_oni.max()),
                "percentile_25": float(valid_oni.quantile(0.25)),
                "percentile_75": float(valid_oni.quantile(0.75)),
            },
            "enso_distribution": df['ENSO'].value_counts().to_dict(),
            "intensity_distribution": df['intensity'].value_counts().to_dict(),
            "year_range": {
                "start": int(df['year'].min()),
                "end": int(df['year'].max()),
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating statistics: {str(e)}")


@app.get("/oni/latest")
def get_latest_oni(n: int = Query(12, description="Number of most recent observations to return")):
    """
    Get the most recent ONI observations.
    
    Parameters
    ----------
    n : int
        Number of recent observations to return (default: 12 for last year)
    
    This is useful for getting current ENSO conditions without knowing the exact date.
    """
    try:
        # Load data
        df = load_oni_data()
        
        # Sort by year and season to get most recent
        df = df.sort_values(['year', 'season'])
        
        # Get last n non-null ONI values
        valid_df = df[df['ONI'].notna()]
        latest = valid_df.tail(n)
        
        if latest.empty:
            raise HTTPException(status_code=404, detail="No recent data available")
        
        return {
            "count": len(latest),
            "most_recent_year": int(latest['year'].iloc[-1]),
            "most_recent_season": latest['season'].iloc[-1],
            "data": latest.to_dict('records')
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving latest data: {str(e)}")


@app.get("/oni/events")
def get_enso_events(
    event_type: ENSOEvent = Query(..., description="Type of ENSO event to analyze"),
    min_duration: int = Query(None, description="Minimum event duration in months"),
):
    """
    Get information about ENSO events of a specific type.
    
    This endpoint identifies and returns individual ENSO episodes, including:
    - Start and end dates
    - Duration
    - Peak intensity
    - Average ONI value during the event
    
    Useful for climate impact studies and historical analysis.
    """
    try:
        # Load data
        df = load_oni_data()
        
        # Filter by event type
        event_df = df[df['ENSO'] == event_type.value].copy()
        
        if event_df.empty:
            raise HTTPException(
                status_code=404, 
                detail=f"No {event_type.value} events found in dataset"
            )
        
        # Group consecutive events by checking phase_duration resets
        # When phase_duration == 1, it's the start of a new event
        event_df['is_new_event'] = event_df['phase_duration'] == 1
        event_df['event_id'] = event_df['is_new_event'].cumsum()
        
        # Aggregate by event
        events = []
        for event_id, group in event_df.groupby('event_id'):
            duration = len(group)
            
            # Apply minimum duration filter if specified
            if min_duration is not None and duration < min_duration:
                continue
            
            events.append({
                "start_year": int(group['year'].iloc[0]),
                "start_season": group['season'].iloc[0],
                "end_year": int(group['year'].iloc[-1]),
                "end_season": group['season'].iloc[-1],
                "duration_months": duration,
                "peak_oni": float(group['ONI'].max() if event_type == ENSOEvent.EL_NINO else group['ONI'].min()),
                "average_oni": float(group['ONI'].mean()),
                "peak_intensity": group.loc[group['ONI'].abs().idxmax(), 'intensity'],
            })
        
        return {
            "event_type": event_type.value,
            "total_events": len(events),
            "events": events
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing events: {str(e)}")


@app.get("/health")
def health_check():
    """
    Health check endpoint for monitoring service status.
    
    Render and other platforms use this to verify the service is running.
    """
    return {"status": "healthy", "service": "Enhanced ONI API"}