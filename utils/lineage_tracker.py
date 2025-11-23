# utils/lineage_tracker.py
"""
Data Lineage Tracking Utilities for Spotify Music Analytics Platform
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dataclasses import dataclass
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LineageRecord:
    """Data Lineage Record"""
    source_system: str
    source_table: str
    target_system: str
    target_table: str
    transformation_type: str
    
    # Optional fields
    source_column: Optional[str] = None
    target_column: Optional[str] = None
    transformation_logic: Optional[str] = None
    transformation_function: Optional[str] = None
    etl_batch_id: Optional[str] = None
    dag_id: Optional[str] = None
    task_id: Optional[str] = None
    records_processed: Optional[int] = None
    records_successful: Optional[int] = None
    execution_start: Optional[datetime] = None
    execution_end: Optional[datetime] = None

class LineageTracker:
    """Data Lineage Tracker"""
    
    def __init__(self, db_connection_string: str = None):
        """
        Initialize Lineage Tracker
        
        Args:
            db_connection_string: PostgreSQL connection string, if None, read from environment variables
        """
        self.db_url = db_connection_string or os.getenv('SUPABASE_DB_URL')
        if not self.db_url:
            raise ValueError("Database connection string is required")
        
        self.connection = None
        logger.info("LineageTracker initialized")
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                self.db_url,
                cursor_factory=RealDictCursor
            )
            self.connection.autocommit = False
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        if not self.connect():
            raise Exception("Failed to connect to database")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def record_lineage(self, lineage: LineageRecord) -> bool:
        """
        Record data lineage
        
        Args:
            lineage: LineageRecord object

        Returns:
            bool: Record success status
        """
        if not self.connection:
            logger.error("No database connection")
            return False
        
        try:
            with self.connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO dwh.data_lineage (
                    source_system, source_table, source_column,
                    target_system, target_table, target_column,
                    transformation_type, transformation_logic, transformation_function,
                    etl_batch_id, dag_id, task_id,
                    records_processed, records_successful,
                    execution_start, execution_end
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
                
                params = (
                    lineage.source_system,
                    lineage.source_table,
                    lineage.source_column,
                    lineage.target_system, 
                    lineage.target_table,
                    lineage.target_column,
                    lineage.transformation_type,
                    lineage.transformation_logic,
                    lineage.transformation_function,
                    lineage.etl_batch_id,
                    lineage.dag_id,
                    lineage.task_id,
                    lineage.records_processed,
                    lineage.records_successful,
                    lineage.execution_start,
                    lineage.execution_end
                )
                
                cursor.execute(insert_sql, params)
                self.connection.commit()
                
                logger.info(f"Lineage recorded: {lineage.source_table} -> {lineage.target_table}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to record lineage: {e}")
            self.connection.rollback()
            return False
    
    def batch_record_lineage(self, lineages: List[LineageRecord]) -> int:
        """
        Batch record multiple data lineage

        Args:
            lineages: List of LineageRecord objects

        Returns:
            int: Number of successfully recorded lineages
        """
        successful_count = 0
        
        for lineage in lineages:
            if self.record_lineage(lineage):
                successful_count += 1
        
        logger.info(f"Batch lineage recording completed: {successful_count}/{len(lineages)} successful")
        return successful_count
    
    def get_upstream_lineage(self, table_name: str, system_name: str = None, depth: int = 5) -> List[Dict]:
        """
        Query upstream lineage (data source)

        Args:
            table_name: Target table name
            system_name: Target system name (optional)
            depth: Tracking depth
            
        Returns:
            List[Dict]: Upstream lineage records
        """
        if not self.connection:
            logger.error("No database connection")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                if system_name:
                    where_clause = "target_table = %s AND target_system = %s"
                    params = (table_name, system_name)
                else:
                    where_clause = "target_table = %s"
                    params = (table_name,)
                
                query = f"""
                WITH RECURSIVE lineage_tree AS (
                    -- Base case: direct upstream
                    SELECT 
                        lineage_id, source_system, source_table, source_column,
                        target_system, target_table, target_column,
                        transformation_type, transformation_logic,
                        created_at, 1 as level
                    FROM dwh.data_lineage
                    WHERE {where_clause}
                    
                    UNION ALL
                    
                    -- Recursive case: indirect upstream (limited by depth)
                    SELECT 
                        dl.lineage_id, dl.source_system, dl.source_table, dl.source_column,
                        dl.target_system, dl.target_table, dl.target_column,
                        dl.transformation_type, dl.transformation_logic,
                        dl.created_at, lt.level + 1
                    FROM dwh.data_lineage dl
                    JOIN lineage_tree lt ON dl.target_table = lt.source_table
                    WHERE lt.level < %s
                )
                SELECT DISTINCT * FROM lineage_tree ORDER BY level, created_at DESC
                """
                
                cursor.execute(query, params + (depth,))
                results = cursor.fetchall()
                
                logger.info(f"Found {len(results)} upstream lineage records for {table_name}")
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to query upstream lineage: {e}")
            return []
    
    def get_downstream_lineage(self, table_name: str, system_name: str = None, depth: int = 5) -> List[Dict]:
        """
        Query downstream lineage (data impact)

        Args:
            table_name: Source table name
            system_name: Source system name (optional)
            depth: Tracking depth
            
        Returns:
            List[Dict]: Downstream lineage records
        """
        if not self.connection:
            logger.error("No database connection")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                if system_name:
                    where_clause = "source_table = %s AND source_system = %s"
                    params = (table_name, system_name)
                else:
                    where_clause = "source_table = %s"
                    params = (table_name,)
                
                query = f"""
                WITH RECURSIVE lineage_tree AS (
                    -- Base case: direct downstream
                    SELECT 
                        lineage_id, source_system, source_table, source_column,
                        target_system, target_table, target_column,
                        transformation_type, transformation_logic,
                        created_at, 1 as level
                    FROM dwh.data_lineage
                    WHERE {where_clause}
                    
                    UNION ALL
                    
                    -- Recursive case: indirect downstream (limited by depth)
                    SELECT 
                        dl.lineage_id, dl.source_system, dl.source_table, dl.source_column,
                        dl.target_system, dl.target_table, dl.target_column,
                        dl.transformation_type, dl.transformation_logic,
                        dl.created_at, lt.level + 1
                    FROM dwh.data_lineage dl
                    JOIN lineage_tree lt ON dl.source_table = lt.target_table
                    WHERE lt.level < %s
                )
                SELECT DISTINCT * FROM lineage_tree ORDER BY level, created_at DESC
                """
                
                cursor.execute(query, params + (depth,))
                results = cursor.fetchall()
                
                logger.info(f"Found {len(results)} downstream lineage records for {table_name}")
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to query downstream lineage: {e}")
            return []
    
    def get_table_impact_analysis(self, table_name: str, system_name: str = None) -> Dict:
        """
        Get complete impact analysis of a table

        Args:
            table_name: Table name
            system_name: System name (optional)

        Returns:
            Dict: Impact analysis report
        """
        upstream = self.get_upstream_lineage(table_name, system_name)
        downstream = self.get_downstream_lineage(table_name, system_name)
        
        # 統計分析
        upstream_systems = set(record['source_system'] for record in upstream)
        downstream_systems = set(record['target_system'] for record in downstream)
        
        transformation_types = {}
        for record in upstream + downstream:
            trans_type = record['transformation_type']
            transformation_types[trans_type] = transformation_types.get(trans_type, 0) + 1
        
        return {
            'table_name': table_name,
            'system_name': system_name,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'upstream': {
                'records': upstream,
                'count': len(upstream),
                'affected_systems': list(upstream_systems),
                'max_depth': max([r['level'] for r in upstream], default=0)
            },
            'downstream': {
                'records': downstream,
                'count': len(downstream),
                'affected_systems': list(downstream_systems),
                'max_depth': max([r['level'] for r in downstream], default=0)
            },
            'transformation_summary': transformation_types,
            'total_dependencies': len(upstream) + len(downstream)
        }
    
    def get_lineage_by_batch(self, batch_id: str) -> List[Dict]:
        """
        Based on ETL batch ID, get all lineage records
        
        Args:
            batch_id: ETL batch ID

        Returns:
            List[Dict]: All lineage records for the batch
        """
        if not self.connection:
            logger.error("No database connection")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                query = """
                SELECT * FROM dwh.v_data_lineage_full
                WHERE etl_batch_id = %s
                ORDER BY created_at
                """
                
                cursor.execute(query, (batch_id,))
                results = cursor.fetchall()
                
                logger.info(f"Found {len(results)} lineage records for batch {batch_id}")
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to query lineage by batch: {e}")
            return []
    
    def get_recent_lineage(self, hours: int = 24, limit: int = 100) -> List[Dict]:
        """
        Get recent lineage records

        Args:
            hours: Records from the past few hours
            limit: Maximum number of records

        Returns:
            List[Dict]: Recent lineage records
        """
        if not self.connection:
            logger.error("No database connection")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                query = """
                SELECT * FROM dwh.v_data_lineage_full
                WHERE created_at >= NOW() - INTERVAL '%s hours'
                ORDER BY created_at DESC
                LIMIT %s
                """
                
                cursor.execute(query, (hours, limit))
                results = cursor.fetchall()
                
                logger.info(f"Found {len(results)} recent lineage records")
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to query recent lineage: {e}")
            return []
    
    def generate_lineage_graph_data(self, table_name: str = None) -> Dict:
        """
        Generate data lineage graph data
        
        Args:
            table_name: Table name to filter (optional)
            
        Returns:
            Dict: Graph data containing nodes and edges
        """
        if not self.connection:
            logger.error("No database connection")
            return {}
        
        try:
            with self.connection.cursor() as cursor:
                if table_name:
                    # Query specific table lineage graph
                    query = """
                    SELECT DISTINCT
                        source_node, target_node, edge_type,
                        relationship_count, last_execution
                    FROM dwh.v_lineage_graph
                    WHERE source_node LIKE %s OR target_node LIKE %s
                    ORDER BY last_execution DESC
                    """
                    cursor.execute(query, (f'%{table_name}%', f'%{table_name}%'))
                else:
                    # Query global lineage graph
                    query = """
                    SELECT * FROM dwh.v_lineage_graph
                    ORDER BY last_execution DESC
                    LIMIT 100
                    """
                    cursor.execute(query)
                
                edges = cursor.fetchall()

                # Extract all unique nodes
                nodes = set()
                edge_data = []
                
                for edge in edges:
                    source = edge['source_node']
                    target = edge['target_node']
                    
                    nodes.add(source)
                    nodes.add(target)
                    
                    edge_data.append({
                        'source': source,
                        'target': target,
                        'type': edge['edge_type'],
                        'weight': edge['relationship_count'],
                        'last_execution': edge['last_execution'].isoformat() if edge['last_execution'] else None
                    })
                
                node_data = [{'id': node, 'label': node.split(':')[-1]} for node in nodes]
                
                logger.info(f"Generated graph data: {len(node_data)} nodes, {len(edge_data)} edges")
                
                return {
                    'nodes': node_data,
                    'edges': edge_data,
                    'metadata': {
                        'table_filter': table_name,
                        'generated_at': datetime.now(timezone.utc).isoformat(),
                        'total_nodes': len(node_data),
                        'total_edges': len(edge_data)
                    }
                }
                
        except Exception as e:
            logger.error(f"Failed to generate graph data: {e}")
            return {}

# ============================================================================
# Helper Functions
# ============================================================================

def create_lineage_record(
    source_system: str,
    source_table: str,
    target_system: str,
    target_table: str,
    transformation_type: str,
    **kwargs
) -> LineageRecord:
    """
    Create a LineageRecord object

    Args:
        source_system: Source system
        source_table: Source table
        target_system: Target system
        target_table: Target table
        transformation_type: Transformation type
        **kwargs: Other optional parameters

    Returns:
        LineageRecord: Lineage record object
    """
    return LineageRecord(
        source_system=source_system,
        source_table=source_table,
        target_system=target_system,
        target_table=target_table,
        transformation_type=transformation_type,
        **kwargs
    )

def log_etl_lineage(
    batch_id: str,
    dag_id: str,
    task_id: str,
    lineages: List[LineageRecord]
) -> int:
    """
    Log ETL lineage records in batch
    
    Args:
        batch_id: ETL batch ID
        dag_id: Airflow DAG ID
        task_id: Airflow Task ID
        lineages: Lineage record list

    Returns:
        int: Number of successfully recorded lineage
    """
    # Add ETL information to all records
    for lineage in lineages:
        lineage.etl_batch_id = batch_id
        lineage.dag_id = dag_id
        lineage.task_id = task_id
    
    with LineageTracker() as tracker:
        return tracker.batch_record_lineage(lineages)

def get_table_dependencies(table_name: str, system_name: str = None) -> Dict:
    """
    Quickly query table dependencies

    Args:
        table_name: Table name
        system_name: System name (optional)
        
    Returns:
        Dict: Dependency information
    """
    with LineageTracker() as tracker:
        return tracker.get_table_impact_analysis(table_name, system_name)

# ============================================================================
# Testing and Examples
# ============================================================================

def test_lineage_tracker():
    """Test lineage tracking functionality"""
    print("Testing LineageTracker...")
    
    try:
        # Create test lineage record
        test_lineage = create_lineage_record(
            source_system='mongodb_atlas',
            source_table='daily_listening_history',
            target_system='supabase_pg',
            target_table='raw_staging.spotify_listening_raw',
            transformation_type='direct_copy',
            transformation_function='sync_listening_to_raw_staging',
            records_processed=100,
            records_successful=95,
            execution_start=datetime.now(timezone.utc),
            execution_end=datetime.now(timezone.utc)
        )

        # Log lineage
        with LineageTracker() as tracker:
            success = tracker.record_lineage(test_lineage)
            if success:
                print("Lineage recording test passed")

                # Test query upstream lineage
                downstream = tracker.get_downstream_lineage('daily_listening_history', 'mongodb_atlas')
                print(f"Found {len(downstream)} downstream dependencies")

                # Test impact analysis
                impact = tracker.get_table_impact_analysis('raw_staging.spotify_listening_raw', 'supabase_pg')
                print(f"Impact analysis: {impact['total_dependencies']} total dependencies")
                
            else:
                print("Lineage recording test failed")
                
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lineage_tracker()