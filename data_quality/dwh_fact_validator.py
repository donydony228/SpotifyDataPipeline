# data_quality/dwh_fact_validator.py
import pandas as pd
from datetime import datetime, timezone
import logging
import re
from typing import Dict, List, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_dwh_fact_listening_data(dwh_fact_table: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate listening history data stored in a PostgreSQL DWH fact table.

    Validation Rules:
    1. listening_minutes should be greater than 0 minutes and less than or equal to 1440 (24 hours).
    2. hour_of_day should be between 0 and 23.
    3. played_at should not be a future time.
    4. created_at should be after played_at.
    5. All foreign keys should not be null.

    Args:
        dwh_fact_table: pd.DataFrame - DataFrame containing the DWH fact listening data.
        Expected columns: listening_key, date_key, track_key, artist_key, album_key,
                         play_count, listening_minutes, hour_of_day, is_weekend, 
                         played_at, created_at
        
    Returns:
        dict: Validation results including success status, passed/failed checks, warnings, and summary.
    """

    logger.info("Validating PostgreSQL DWH fact listening data...")

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
        # Set total records
        validation_results["total_records"] = len(dwh_fact_table)
        logger.info(f"Found {len(dwh_fact_table)} records for validation")

        if len(dwh_fact_table) == 0:
            validation_results["warnings"].append("No data available for validation")
            validation_results["summary"] = _generate_summary(validation_results)
            return validation_results

        # Execute validation rules
        results = []
        
        # Rule 1: listening_minutes range check
        try:
            result1 = _validate_listening_minutes_range(dwh_fact_table, validation_results)
            results.append(result1)
            logger.info(f"Rule 1 (listening_minutes range): {'PASSED' if result1 else 'FAILED'}")
        except Exception as e:
            logger.error(f"Rule 1 execution failed: {e}")
            validation_results["failed_checks"].append(f"listening_minutes range check execution failed: {e}")
            results.append(False)

        # Rule 2: hour_of_day range check  
        try:
            result2 = _validate_hour_of_day_range(dwh_fact_table, validation_results)
            results.append(result2)
            logger.info(f"Rule 2 (hour_of_day range): {'PASSED' if result2 else 'FAILED'}")
        except Exception as e:
            logger.error(f"Rule 2 execution failed: {e}")
            validation_results["failed_checks"].append(f"hour_of_day range check execution failed: {e}")
            results.append(False)

        # Rule 3: played_at time check
        try:
            result3 = _validate_played_at_time(dwh_fact_table, validation_results)
            results.append(result3)
            logger.info(f"Rule 3 (played_at time): {'PASSED' if result3 else 'FAILED'}")
        except Exception as e:
            logger.error(f"Rule 3 execution failed: {e}")
            validation_results["failed_checks"].append(f"played_at time check execution failed: {e}")
            results.append(False)

        # Rule 4: created_at vs played_at logic check
        try:
            result4 = _validate_dwh_time_logic(dwh_fact_table, validation_results)
            results.append(result4)
            logger.info(f"Rule 4 (time logic): {'PASSED' if result4 else 'FAILED'}")
        except Exception as e:
            logger.error(f"Rule 4 execution failed: {e}")
            validation_results["failed_checks"].append(f"time logic check execution failed: {e}")
            results.append(False)

        # Rule 5: foreign key checks
        try:
            result5 = _validate_foreign_keys(dwh_fact_table, validation_results)
            results.append(result5)
            logger.info(f"Rule 5 (foreign keys): {'PASSED' if result5 else 'FAILED'}")
        except Exception as e:
            logger.error(f"Rule 5 execution failed: {e}")
            validation_results["failed_checks"].append(f"foreign key check execution failed: {e}")
            results.append(False)

        # Summarize results
        all_passed = all(results)
        validation_results["success"] = all_passed
        validation_results["summary"] = _generate_summary(validation_results)
        
        if all_passed:
            logger.info("All validation rules passed")
        else:
            logger.warning(f"{len(validation_results['failed_checks'])} validation rules failed")

    except Exception as e:
        logger.error(f"Error occurred during validation: {e}")
        validation_results["success"] = False
        validation_results["failed_checks"].append(f"Execution error: {str(e)}")
        validation_results["summary"] = _generate_summary(validation_results)
    
    return validation_results

def _validate_listening_minutes_range(df: pd.DataFrame, results: Dict) -> bool:
    """Validate listening_minutes is between 0 and 1440 (24 hours)"""
    try:
        # Check if column exists
        if 'listening_minutes' not in df.columns:
            results["warnings"].append("listening_minutes column not found")
            return True
            
        # Check for invalid range (should be > 0 and <= 1440)
        invalid_ranges = df[(df['listening_minutes'] <= 0) | (df['listening_minutes'] > 1440)]
        
        if len(invalid_ranges) == 0:
            results["passed_checks"].append("listening_minutes range check passed")
            return True
        else:
            failed_count = len(invalid_ranges)
            results["failed_checks"].append(f"listening_minutes range check failed: {failed_count} records have invalid range")
            results["warnings"].append(f"Found {failed_count} records with listening_minutes outside 0-1440 range")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"listening_minutes range check execution failed: {e}")
        return False

def _validate_hour_of_day_range(df: pd.DataFrame, results: Dict) -> bool:
    """Validate hour_of_day is between 0 and 23"""
    try:
        # Check if column exists
        if 'hour_of_day' not in df.columns:
            results["warnings"].append("hour_of_day column not found")
            return True
            
        # Check for null values first
        null_hours = df['hour_of_day'].isnull().sum()
        if null_hours > 0:
            results["warnings"].append(f"Found {null_hours} null hour_of_day values")

        # Check valid range
        invalid_hours = df[(df['hour_of_day'].notnull()) & 
                          ((df['hour_of_day'] < 0) | (df['hour_of_day'] > 23))]

        if len(invalid_hours) == 0:
            results["passed_checks"].append("hour_of_day range check passed")
            return True
        else:
            failed_count = len(invalid_hours)
            results["failed_checks"].append(f"hour_of_day range check failed: {failed_count} records have invalid hour_of_day")
            results["warnings"].append(f"Found {failed_count} records with hour_of_day outside 0-23 range")
            return False

    except Exception as e:
        results["failed_checks"].append(f"hour_of_day range check execution failed: {e}")
        return False

def _validate_played_at_time(df: pd.DataFrame, results: Dict) -> bool:
    """Validate played_at is not a future time"""
    try:
        # Check if column exists
        if 'played_at' not in df.columns:
            results["warnings"].append("played_at column not found")
            return True
            
        current_time = datetime.now(timezone.utc)
        future_count = 0
        
        # Convert to pandas datetime and handle timezone
        df_copy = df.copy()
        df_copy['played_at'] = pd.to_datetime(df_copy['played_at'], errors='coerce')
        
        # Handle each record
        for idx, played_time in df_copy['played_at'].items():
            if pd.isna(played_time):
                continue
                
            try:
                # Convert to Python datetime for comparison
                if hasattr(played_time, 'to_pydatetime'):
                    played_time = played_time.to_pydatetime()
                
                # Ensure timezone awareness for comparison
                if played_time.tzinfo is None:
                    # If timezone-naive, assume UTC
                    played_time = played_time.replace(tzinfo=timezone.utc)
                else:
                    # Convert to UTC for consistent comparison
                    played_time = played_time.astimezone(timezone.utc)
                
                if played_time > current_time:
                    future_count += 1
                    
            except Exception as e:
                # Log individual record processing errors but continue
                logger.warning(f"Could not process played_at for record {idx}: {e}")
                continue
        
        if future_count == 0:
            results["passed_checks"].append("played_at time check passed")
            return True
        else:
            results["failed_checks"].append(f"played_at time check failed: {future_count} records are in the future")
            results["warnings"].append(f"Found {future_count} playback records with future times")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"played_at time check execution failed: {e}")
        import traceback
        logger.error(f"Played_at time validation error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def _validate_dwh_time_logic(df: pd.DataFrame, results: Dict) -> bool:
    """Validate created_at is after played_at (DWH specific)"""
    try:
        df['created_at'] = df['created_at'].dt.tz_localize(None)
        df['played_at'] = df['played_at'].dt.tz_localize(None)
        invalid_logic = df[df['created_at'] <= df['played_at']]
        if len(invalid_logic) == 0:
            results["passed_checks"].append("created_at after played_at check passed")
            return True
        else:
            failed_count = len(invalid_logic)
            results["failed_checks"].append(f"created_at after played_at check failed: {failed_count} records have invalid timestamps")
            results["warnings"].append(f"Found {failed_count} records where created_at is not after played_at")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"created_at after played_at check execution failed: {e}")
        # Add more detailed error information for debugging
        import traceback
        logger.error(f"Time logic validation error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def _validate_foreign_keys(df: pd.DataFrame, results: Dict) -> bool:
    """Validate that all foreign keys are not null"""
    try:
        foreign_key_columns = ['date_key', 'track_key', 'artist_key', 'album_key']
        failed_columns = []
        
        for column in foreign_key_columns:
            if column in df.columns:
                null_count = df[column].isnull().sum()
                if null_count > 0:
                    failed_columns.append(f"{column}: {null_count} nulls")
                    
        if len(failed_columns) == 0:
            results["passed_checks"].append("Foreign key validation passed")
            return True
        else:
            results["failed_checks"].append(f"Foreign key validation failed: {'; '.join(failed_columns)}")
            results["warnings"].append(f"Found null foreign keys in: {failed_columns}")
            return False
            
    except Exception as e:
        results["failed_checks"].append(f"Foreign key validation execution failed: {e}")
        return False

def _generate_summary(results: Dict) -> Dict:
    """Generate validation summary"""
    total_checks = len(results["passed_checks"]) + len(results["failed_checks"])
    
    return {
        "total_checks": total_checks,
        "passed_count": len(results["passed_checks"]),
        "failed_count": len(results["failed_checks"]),
        "warning_count": len(results["warnings"]),
        "success_rate": len(results["passed_checks"]) / total_checks if total_checks > 0 else 0
    }


# Quick test function for debugging
# def debug_validation(df: pd.DataFrame):
#     """Debug function to help identify issues"""
#     print("🔍 DEBUG: DataFrame Info")
#     print(f"Shape: {df.shape}")
#     print(f"Columns: {list(df.columns)}")
#     print(f"Data types:\n{df.dtypes}")
    
#     if len(df) > 0:
#         print(f"\nFirst few rows:\n{df.head()}")
        
#         # Check for specific issues
#         if 'listening_minutes' in df.columns:
#             print(f"\nlistening_minutes stats:")
#             print(f"  Min: {df['listening_minutes'].min()}")
#             print(f"  Max: {df['listening_minutes'].max()}")
#             print(f"  Nulls: {df['listening_minutes'].isnull().sum()}")
        
#         if 'hour_of_day' in df.columns:
#             print(f"\nhour_of_day stats:")
#             print(f"  Min: {df['hour_of_day'].min()}")
#             print(f"  Max: {df['hour_of_day'].max()}")
#             print(f"  Nulls: {df['hour_of_day'].isnull().sum()}")
            
#         # Enhanced datetime debugging
#         for datetime_col in ['played_at', 'created_at']:
#             if datetime_col in df.columns:
#                 print(f"\n{datetime_col} info:")
#                 print(f"  Type: {df[datetime_col].dtype}")
#                 print(f"  Nulls: {df[datetime_col].isnull().sum()}")
                
#                 if hasattr(df[datetime_col].dtype, 'tz'):
#                     print(f"  Timezone: {df[datetime_col].dt.tz}")
#                 else:
#                     print("  No timezone info available")
                    
#                 # Check for timezone-aware vs timezone-naive
#                 sample_value = df[datetime_col].dropna().iloc[0] if len(df[datetime_col].dropna()) > 0 else None
#                 if sample_value is not None:
#                     print(f"  Sample value: {sample_value}")
#                     if hasattr(sample_value, 'tzinfo'):
#                         print(f"  Sample timezone: {sample_value.tzinfo}")
    
#     print("\n" + "="*50)


# Usage example and testing function
# def test_validator_with_sample_data():
#     """Test the validator with sample data"""
#     import numpy as np
    
#     # Create sample test data
#     sample_data = pd.DataFrame({
#         'listening_key': [1, 2, 3, 4, 5],
#         'date_key': [1, 2, 3, 4, 5],
#         'track_key': [101, 102, 103, 104, None],  # One null foreign key
#         'artist_key': [201, 202, 203, 204, 205],
#         'album_key': [301, 302, 303, 304, 305],
#         'play_count': [1, 1, 1, 1, 1],
#         'listening_minutes': [3.5, 4.2, 2800.0, 0.5, 180.0],  # One invalid (2800 > 1440)
#         'hour_of_day': [14, 9, 25, 16, 20],  # One invalid (25 > 23)
#         'is_weekend': [False, True, False, False, True],
#         'played_at': [
#             datetime(2024, 1, 1, 14, 30, tzinfo=timezone.utc),
#             datetime(2024, 1, 2, 9, 15, tzinfo=timezone.utc),
#             datetime(2024, 1, 3, 18, 45, tzinfo=timezone.utc),
#             datetime(2090, 1, 4, 16, 20, tzinfo=timezone.utc),  # Future time
#             datetime(2024, 1, 5, 20, 10, tzinfo=timezone.utc)
#         ],
#         'created_at': [
#             datetime(2024, 1, 1, 14, 35, tzinfo=timezone.utc),
#             datetime(2024, 1, 2, 9, 20, tzinfo=timezone.utc),
#             datetime(2024, 1, 3, 18, 50, tzinfo=timezone.utc),
#             datetime(2090, 1, 4, 16, 25, tzinfo=timezone.utc),
#             datetime(2024, 1, 5, 20, 15, tzinfo=timezone.utc)
#         ]
#     })
    
#     print("Testing DWH Fact Table Validator with sample data...")
    
#     # Debug first
#     debug_validation(sample_data)
    
#     # Run validation
#     results = validate_dwh_fact_listening_data(sample_data)
    
#     print("\n" + "="*50)
#     print("VALIDATION RESULTS")
#     print("="*50)
#     print(f"Total Records: {results['total_records']}")
#     print(f"Success: {results['success']}")
#     print(f"\nPassed Checks ({len(results['passed_checks'])}):")
#     for check in results['passed_checks']:
#         print(f"  ✅ {check}")
    
#     print(f"\nFailed Checks ({len(results['failed_checks'])}):")
#     for check in results['failed_checks']:
#         print(f"  ❌ {check}")
        
#     print(f"\nWarnings ({len(results['warnings'])}):")
#     for warning in results['warnings']:
#         print(f"  ⚠️  {warning}")
        
#     print(f"\nSummary:")
#     for key, value in results['summary'].items():
#         print(f"  {key}: {value}")
    
#     return results


# Main execution
if __name__ == "__main__":
    print("DWH Fact Table Validator Module")
    print("="*40)
    
    # Run test
    # test_results = test_validator_with_sample_data()
    
    print("\nValidator module is ready for use!")
    print("Import this module and use validate_dwh_fact_listening_data(df) function.")