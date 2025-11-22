# data_quality/mongodb_validator.py
import pandas as pd
from datetime import datetime, timezone
import logging
import re
from typing import Dict, List, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_listening_history_data(mongodb_collection) -> Dict[str, Any]:
    """
    Validate listening history data stored in a MongoDB collection using custom rules.

    Validation Rules:
    1. track_id must be exactly 22 characters long
    2. track_id must contain only alphanumeric characters
    3. played_at must not be a future time
    4. batch_info.collected_at must be after played_at
    5. track_id + played_at combination must be unique

    Args:
        mongodb_collection: MongoDB collection object to validate
        
    Returns:
        dict: Validation results including success status, passed/failed checks, warnings, and summary.
    """

    logger.info("Validating MongoDB listening history data...")

    # Validation results container
    validation_results = {
        "success": True,
        "total_records": 0,
        "passed_checks": [],
        "failed_checks": [],
        "warnings": [],
        "summary": {}
    }
    
    try:
        # Step 1: Retrieve data from MongoDB and convert to DataFrame
        logger.info("Reading data from MongoDB...")

        # Get data from the last 24 hours for validation
        recent_data = list(mongodb_collection.find({
            "batch_info.collected_at": {
                "$gte": datetime.now(timezone.utc).replace(hour=0, minute=0, second=0)
            }
        }).limit(1000))  # Limit to avoid memory issues
        
        if not recent_data:
            logger.warning("No recent data found for validation")
            validation_results["warnings"].append("No data available for validation")
            return validation_results
        
        validation_results["total_records"] = len(recent_data)
        logger.info(f"Found {len(recent_data)} records for validation")

        # Step 2: Flatten data - Convert nested MongoDB documents to a flat structure
        flattened_data = []
        for record in recent_data:
            flat_record = {
                'track_id': record.get('track_id'),
                'played_at': record.get('played_at'),
                'track_name': record.get('track_info', {}).get('name'),
                'artist_name': record.get('track_info', {}).get('artists', [{}])[0].get('name'),
                'duration_ms': record.get('track_info', {}).get('duration_ms'),
                'popularity': record.get('track_info', {}).get('popularity'),
                'collected_at': record.get('batch_info', {}).get('collected_at'),
                'batch_id': record.get('batch_info', {}).get('batch_id')
            }
            flattened_data.append(flat_record)

        # Step 3: Convert to DataFrame and create Great Expectations context
        df = pd.DataFrame(flattened_data)

        logger.info("Data transformation complete, starting validation rules...")

        # Step 4: Execute validation rules

        # Rule 1: track_id length check
        result1 = _validate_track_id_length(df, validation_results)

        # Rule 2: track_id format check
        result2 = _validate_track_id_format(df, validation_results)

        # Rule 3: played_at time check
        result3 = _validate_played_at_time(df, validation_results)

        # Rule 4: collected_at vs played_at logic check
        result4 = _validate_time_logic(df, validation_results)

        # Rule 5: uniqueness check
        result5 = _validate_uniqueness(df, validation_results)

        # Step 5: Summarize results
        all_passed = all([result1, result2, result3, result4, result5])
        validation_results["success"] = all_passed
        validation_results["summary"] = _generate_summary(validation_results)
        
        if all_passed:
            logger.info("Passed all validation rules")
        else:
            logger.warning(f"{len(validation_results['failed_checks'])} validation rules failed")

    except Exception as e:
        logger.error(f"Error occurred during validation: {e}")
        validation_results["success"] = False
        validation_results["failed_checks"].append(f"Execution error: {str(e)}")
    
    return validation_results

def _validate_track_id_length(df: pd.DataFrame, results: Dict) -> bool:
    """Validate track_id length is 22 characters"""
    try:
        # Use pandas to check length directly
        track_ids = df['track_id'].dropna()
        invalid_lengths = track_ids[track_ids.str.len() != 22]
        
        if len(invalid_lengths) == 0:
            results["passed_checks"].append("track_id length check passed")
            return True
        else:
            failed_count = len(invalid_lengths)
            results["failed_checks"].append(f"track_id length check failed: {failed_count} records have invalid length")
            results["warnings"].append(f"Found {failed_count} abnormal track_id lengths")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"track_id length check execution failed: {e}")
        return False

def _validate_track_id_format(df: pd.DataFrame, results: Dict) -> bool:
    """Validate track_id format (alphanumeric, 22 chars)"""
    try:
        # Use regular expression to check format
        track_ids = df['track_id'].dropna()
        pattern = r"^[A-Za-z0-9]{22}$"
        invalid_formats = track_ids[~track_ids.str.match(pattern)]
        
        if len(invalid_formats) == 0:
            results["passed_checks"].append("track_id format check passed")
            return True
        else:
            failed_count = len(invalid_formats)
            results["failed_checks"].append(f"track_id format check failed: {failed_count} records have invalid format")
            results["warnings"].append(f"Found {failed_count} abnormal track_id formats")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"track_id format check execution failed: {e}")
        return False

def _validate_played_at_time(df: pd.DataFrame, results: Dict) -> bool:
    """Validate played_at is not a future time"""
    try:
        # Ensure consistent time format
        current_time = datetime.now(timezone.utc)

        # Convert played_at to datetime objects for comparison
        future_count = 0
        for idx, row in df.iterrows():
            played_time = row['played_at']
            if isinstance(played_time, str):
                played_time = datetime.fromisoformat(played_time.replace('Z', '+00:00'))
            elif hasattr(played_time, 'to_pydatetime'):
                played_time = played_time.to_pydatetime()
            
            if played_time.replace(tzinfo=timezone.utc) > current_time:
                future_count += 1
        
        if future_count == 0:
            results["passed_checks"].append("played_at time check passed")
            return True
        else:
            results["failed_checks"].append(f"played_at time check failed: {future_count} records are in the future")
            results["warnings"].append(f"Found {future_count} playback records with future times")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"played_at time check execution failed: {e}")
        return False

def _validate_time_logic(df: pd.DataFrame, results: Dict) -> bool:
    """Validate collected_at is after played_at"""
    try:
        # Check time logic
        invalid_logic = df[df['collected_at'] <= df['played_at']]
        
        if len(invalid_logic) == 0:
            results["passed_checks"].append("collected_at is after played_at check passed")
            return True
        else:
            results["failed_checks"].append(f"collected_at is after played_at check failed: {len(invalid_logic)} records are invalid")
            results["warnings"].append(f"Found {len(invalid_logic)} records where collected_at is before played_at")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"collected_at is after played_at check execution failed: {e}")
        return False

def _validate_uniqueness(df: pd.DataFrame, results: Dict) -> bool:
    """Validate track_id + played_at combination uniqueness"""
    try:
        # Check duplicate combinations
        duplicate_combinations = df.groupby(['track_id', 'played_at']).size()
        duplicates = duplicate_combinations[duplicate_combinations > 1]
        
        if len(duplicates) == 0:
            results["passed_checks"].append("track_id + played_at combination uniqueness check passed")
            return True
        else:
            results["failed_checks"].append(f"track_id + played_at combination uniqueness check failed: {len(duplicates)} records are duplicates")
            results["warnings"].append(f"Found {len(duplicates)} duplicate track_id + played_at combinations")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"track_id + played_at combination uniqueness check execution failed: {e}")
        return False

def _generate_summary(results: Dict) -> Dict:
    """Generate validation summary"""
    return {
        "total_checks": len(results["passed_checks"]) + len(results["failed_checks"]),
        "passed_count": len(results["passed_checks"]),
        "failed_count": len(results["failed_checks"]),
        "warning_count": len(results["warnings"]),
        "success_rate": len(results["passed_checks"]) / (len(results["passed_checks"]) + len(results["failed_checks"])) if (len(results["passed_checks"]) + len(results["failed_checks"])) > 0 else 0
    }


# Usage example
if __name__ == "__main__":
    # This is an example of how to use this validator
    from pymongo import MongoClient
    import os

    # Load environment variables (requires your MongoDB connection string)
    # client = MongoClient(os.getenv('MONGODB_ATLAS_URL'))
    # db = client['music_data']
    # collection = db['daily_listening_history']
    
    # results = validate_listening_history_data(collection)
    # print("Validation results:", results)

    print("MongoDB validation module is ready!")