# data_quality/dim_track_validator.py
import pandas as pd
from datetime import datetime, timezone
import logging
import re
from typing import Dict, List, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_dim_track_data(dwh_dim_table: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate track data stored in a PostgreSQL DWH dimension table.

    Validation Rules:
    1. created_at should be after first_heard.
    2. duration_minutes should be positive.

    Args:
        dwh_dim_table: pd.DataFrame - DataFrame containing the DWH dimension track data.

    Returns:
        dict: Validation results including success status, passed/failed checks, warnings, and summary.
    """

    logger.info("Validating PostgreSQL DWH dimension track data...")

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
        validation_results["total_records"] = len(dwh_dim_table)
        logger.info(f"Found {len(dwh_dim_table)} records for validation")

        if len(dwh_dim_table) == 0:
            validation_results["warnings"].append("No data available for validation")
            validation_results["summary"] = _generate_summary(validation_results)
            return validation_results

        # Execute validation rules
        results = []
        
        # Rule 1: created_at vs first_heard logic check
        try:
            result1 = _validate_created_at_vs_first_heard(dwh_dim_table, validation_results)
            results.append(result1)
            # logger.info(f"Rule 1 (created_at vs first_heard): {'PASSED' if result1 else 'FAILED'}")
        except Exception as e:
            logger.error(f"Rule 1 execution failed: {e}")
            validation_results["failed_checks"].append(f"created_at vs first_heard check execution failed: {e}")
            results.append(False)

        # Rule 2: duration_minutes positive check
        try:
            result2 = _validate_duration_minutes_positive(dwh_dim_table, validation_results)
            results.append(result2)
            # logger.info(f"Rule 2 (duration_minutes positive): {'PASSED' if result2 else 'FAILED'}")
        except Exception as e:
            logger.error(f"Rule 2 execution failed: {e}")
            validation_results["failed_checks"].append(f"duration_minutes positive check execution failed: {e}")
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

def _validate_created_at_vs_first_heard(df: pd.DataFrame, results: Dict) -> bool:
    """Validate created_at is after first_heard"""
    try:
        # Check if column exists
        if 'created_at' not in df.columns or 'first_heard' not in df.columns:
            results["warnings"].append("created_at or first_heard column not found")
            return True

        # Check for invalid logic (created_at should be after first_heard)
        invalid_logic = df[df['created_at'] <= df['first_heard']]
        if len(invalid_logic) == 0:
            results["passed_checks"].append("created_at vs first_heard check passed")
            return True
        else:
            failed_count = len(invalid_logic)
            results["failed_checks"].append(f"created_at vs first_heard check failed: {failed_count} records have invalid logic")
            return False

    except Exception as e:
        results["failed_checks"].append(f"created_at vs first_heard check execution failed: {e}")
        return False

def _validate_duration_minutes_positive(df: pd.DataFrame, results: Dict) -> bool:
    """Validate duration_minutes is positive"""
    try:
        # Check if column exists
        if 'duration_minutes' not in df.columns:
            results["warnings"].append("duration_minutes column not found")
            return True

        # Check for non-positive duration_minutes
        non_positive_duration = df[df['duration_minutes'] <= 0]
        if len(non_positive_duration) == 0:
            results["passed_checks"].append("duration_minutes positive check passed")
            return True
        else:
            failed_count = len(non_positive_duration)
            results["failed_checks"].append(f"duration_minutes positive check failed: {failed_count} records have non-positive duration")
            return False

    except Exception as e:
        results["failed_checks"].append(f"duration_minutes positive check execution failed: {e}")
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

# Main execution
if __name__ == "__main__":
    print("Dimension Album Validator Module")
    print("="*40)
    
    # Run test
    # test_results = test_validator_with_sample_data()
    
    print("\nValidator module is ready for use!")
    print("Import this module and use validate_dwh_dim_album_data(df) function.")