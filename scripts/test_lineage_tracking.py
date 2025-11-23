# scripts/test_lineage_tracking.py
"""
測試血緣追蹤系統
Test Data Lineage Tracking System
"""

import sys
import os
sys.path.append('/Users/desmond/airflow')

from utils.lineage_tracker import LineageTracker

def test_lineage_queries():
    """測試血緣查詢功能"""
    print("🔍 測試血緣追蹤查詢功能")
    print("=" * 60)
    
    try:
        with LineageTracker() as tracker:
            
            # 1. 查詢最近的血緣記錄
            print("\n📊 最近的血緣記錄:")
            recent = tracker.get_recent_lineage(hours=24, limit=5)
            for record in recent:
                print(f"  {record['source_table']} -> {record['target_table']} "
                      f"({record['transformation_type']}) "
                      f"[{record['records_successful']} records]")
            
            # 2. 查詢 fact_listening 的上游血緣
            print("\n🔍 dwh.fact_listening 的上游血緣:")
            upstream = tracker.get_upstream_lineage('dwh.fact_listening')
            for record in upstream:
                print(f"  Level {record['level']}: {record['source_table']} -> {record['target_table']} "
                      f"({record['transformation_type']})")
            
            # 3. 查詢 clean_staging.listening_cleaned 的下游影響
            print("\n📤 clean_staging.listening_cleaned 的下游影響:")
            downstream = tracker.get_downstream_lineage('clean_staging.listening_cleaned')
            for record in downstream:
                print(f"  Level {record['level']}: {record['source_table']} -> {record['target_table']} "
                      f"({record['transformation_type']})")
            
            # 4. 完整影響分析
            print("\n🎯 dwh.fact_listening 完整影響分析:")
            impact = tracker.get_table_impact_analysis('dwh.fact_listening')
            print(f"  總依賴關係: {impact['total_dependencies']}")
            print(f"  上游記錄數: {impact['upstream']['count']}")
            print(f"  下游記錄數: {impact['downstream']['count']}")
            print(f"  涉及系統: {impact['upstream']['affected_systems']} -> {impact['downstream']['affected_systems']}")
            print(f"  轉換類型分佈: {impact['transformation_summary']}")
            
            # 5. 產生血緣關係圖資料
            print("\n🕸️ 生成血緣關係圖資料:")
            graph_data = tracker.generate_lineage_graph_data('fact_listening')
            print(f"  節點數量: {len(graph_data.get('nodes', []))}")
            print(f"  邊數量: {len(graph_data.get('edges', []))}")
            
            if graph_data.get('edges'):
                print("  關係範例:")
                for edge in graph_data['edges'][:3]:
                    print(f"    {edge['source']} -> {edge['target']} ({edge['type']})")
                    
    except Exception as e:
        print(f"❌ 測試失敗: {e}")
        import traceback
        traceback.print_exc()

def show_lineage_statistics():
    """顯示血緣追蹤統計資訊"""
    print("\n📈 血緣追蹤統計資訊")
    print("=" * 60)
    
    try:
        with LineageTracker() as tracker:
            with tracker.connection.cursor() as cursor:
                
                # 統計各種轉換類型的數量
                cursor.execute("""
                    SELECT 
                        transformation_type,
                        COUNT(*) as count,
                        SUM(records_processed) as total_records
                    FROM dwh.data_lineage 
                    GROUP BY transformation_type 
                    ORDER BY count DESC
                """)
                
                print("\n轉換類型統計:")
                for row in cursor.fetchall():
                    print(f"  {row['transformation_type']}: {row['count']} 次, "
                          f"處理 {row['total_records']} 筆記錄")
                
                # 統計各系統間的資料流
                cursor.execute("""
                    SELECT 
                        source_system,
                        target_system,
                        COUNT(*) as flow_count
                    FROM dwh.data_lineage 
                    GROUP BY source_system, target_system 
                    ORDER BY flow_count DESC
                """)
                
                print("\n系統間資料流統計:")
                for row in cursor.fetchall():
                    print(f"  {row['source_system']} -> {row['target_system']}: {row['flow_count']} 次")
                
                # 統計表格依賴關係
                cursor.execute("""
                    SELECT 
                        source,
                        target,
                        dependency_strength,
                        dependency_level
                    FROM dwh.v_table_dependencies
                    ORDER BY dependency_strength DESC
                    LIMIT 10
                """)
                
                print("\n表格依賴關係 (Top 10):")
                for row in cursor.fetchall():
                    print(f"  {row['source']} -> {row['target']}: "
                          f"強度 {row['dependency_strength']} ({row['dependency_level']})")
                    
    except Exception as e:
        print(f"❌ 統計查詢失敗: {e}")

def show_recent_execution_summary():
    """顯示最近執行摘要"""
    print("\n⚡ 最近 ETL 執行摘要")
    print("=" * 60)
    
    try:
        with LineageTracker() as tracker:
            with tracker.connection.cursor() as cursor:
                
                cursor.execute("""
                    SELECT 
                        dag_id,
                        task_id,
                        COUNT(*) as execution_count,
                        SUM(records_processed) as total_processed,
                        SUM(records_successful) as total_successful,
                        AVG(EXTRACT(EPOCH FROM (execution_end - execution_start))) as avg_duration,
                        MAX(created_at) as last_execution
                    FROM dwh.data_lineage
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY dag_id, task_id
                    ORDER BY last_execution DESC
                """)
                
                print("\n24小時內 ETL 任務執行情況:")
                for row in cursor.fetchall():
                    avg_duration = row['avg_duration'] or 0
                    success_rate = (row['total_successful'] / row['total_processed'] * 100) if row['total_processed'] else 0
                    
                    print(f"  {row['dag_id']}.{row['task_id']}:")
                    print(f"    執行次數: {row['execution_count']}")
                    print(f"    處理記錄: {row['total_processed']} (成功: {row['total_successful']})")
                    print(f"    成功率: {success_rate:.1f}%")
                    print(f"    平均耗時: {avg_duration:.2f} 秒")
                    print(f"    最後執行: {row['last_execution']}")
                    print()
                    
    except Exception as e:
        print(f"❌ 摘要查詢失敗: {e}")

if __name__ == "__main__":
    print("🎵 Spotify 血緣追蹤系統測試")
    print("=" * 60)
    
    # 執行各種測試
    test_lineage_queries()
    show_lineage_statistics() 
    show_recent_execution_summary()
    
    print("\n✅ 測試完成!")