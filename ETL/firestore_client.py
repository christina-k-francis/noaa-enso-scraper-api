"""
Script containing functions to facilitate connections to the Firestore Database for
tracking pipeline run status and data freshness
"""

import os
import json
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone

# Initializing the Firebase app
def _get_db():
    """
    Description: FX Initializes a connection to firebase via json credentials
    """
    if not firebase_admin._apps:
        cred_json = os.environ.get("FIREBASE_KEY")
        if not cred_json:
            raise EnvironmentError("FIREBASE_KEY environment variable not set!")
        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
    return firestore.client()

# function for outputting the current time
def _now():
    """
    Description: FX returns the current time in utc format
    """
    return datetime.now(timezone.utc).isoformat()

# creating the pipeline_runs collection
def create_pipeline_run(run_id: str, triggered_by: str = "scheduled") -> None:
    """
    Description: FX creates the initial pipeline_runs document, which sets all stage statuses to 'pending'.
    """
    db = _get_db()
    db.collection("pipeline_runs").document(run_id).set({
        "run_id": run_id,
        "triggered_by": triggered_by,
        "scrape_status": "pending",
        "transform_status": "pending",
        "deploy_status": "pending",
        "new_data_detected": None,
        "records_ingested": None,
        "error_message": None,
        "duration_seconds": None,
        "started_at": _now(),
        "completed_at": None,
    })

def update_pipeline_stage(run_id: str, stage: str, status: str,
                           extra: dict = None) -> None:
    """
    Description: FX updates the pipeline_runs collection with updatad stage stauses.
        - stage:  "scrape_status" | "transform_status" | "deploy_status"
        - status: "success" | "failed"
        - extra:  optional dict of additional fields to merge (e.g. records_ingested)
    """
    db = _get_db()
    update = {stage: status}
    if extra:
        update.update(extra)
    db.collection("pipeline_runs").document(run_id).update(update)

def complete_pipeline_run(run_id: str, started_at: str,
                           error_message: str = None) -> None:
    """
    Description: FX calculates pipeline run duration and marks the run as fully complete or failed.
    """
    db = _get_db()
    now = datetime.now(timezone.utc)
    started = datetime.fromisoformat(started_at)
    duration = (now - started).total_seconds()

    db.collection("pipeline_runs").document(run_id).update({
        "completed_at": now.isoformat(),
        "duration_seconds": round(duration, 2),
        "error_message": error_message,
    })

# Creating the data_freshness collection
def update_data_freshness(latest_year: int, latest_season: str,
                           total_records: int, source_url: str) -> None:
    """
    Description: The FX overwrites the current_state document after every successful deployment to R2.
    """
    db = _get_db()
    db.collection("data_freshness").document("current_state").set({
        "latest_year": latest_year,
        "latest_season": latest_season,
        "total_records": total_records,
        "source_url": source_url,
        "last_successful_update": _now(),
        "api_status": "healthy",
    })