# p3_complete_test.py
import sys
sys.path.insert(0, '.')

print("=== P3阶段完整测试 ===")
print("1. 初始化所有模块...")

try:
    # 1. 初始化采集器
    from src.data.akshare_collector import AKShareCollector
    collector = AKShareCollector()
    print(" AKShare采集器初始化成功")
    
    # 2. 测试数据采集
    print("\n2. 测试数据采集...")
    test_symbol = "000001.SZ"
    df = collector.fetch_daily_data(test_symbol, "20240101", "20240105")
    
    if df is not None and not df.empty:
        print(f" 数据采集成功，获取 {len(df)} 条记录")
        print(f"   数据列: {list(df.columns)}")
        
        # 清理数据列
        if '股票代码' in df.columns:
            df = df.drop(columns=['股票代码'])
            print(" 已清理'股票代码'列")
        
        # 3. 测试数据存储
        print("\n3. 测试数据存储...")
        from src.data.data_storage import DataStorage
        storage = DataStorage()
        
        rows_affected = storage.store_daily_data(df)
        print(f" 数据存储成功，影响 {rows_affected} 行")
        
        # 4. 验证数据库
        print("\n4. 验证数据库...")
        from src.database.db_connector import DatabaseConnector
        db = DatabaseConnector()
        
        # 检查数据
        result = db.execute_query('''
            SELECT 
                COUNT(*) as total_records,
                MIN(trade_date) as earliest,
                MAX(trade_date) as latest
            FROM stock_daily_data
            WHERE symbol = %s
        ''', (test_symbol,))[0]
        
        print(f"   总记录数: {result['total_records']}")
        print(f"   最早日期: {result['earliest']}")
        print(f"   最晚日期: {result['latest']}")
        
        # 5. 测试调度器
        print("\n5. 测试采集调度器...")
        from src.data.data_scheduler import DataScheduler
        scheduler = DataScheduler()
        print(f" 调度器初始化成功，找到 {len(scheduler.csi_a50_symbols)} 只股票")
        
        print("\n P3阶段所有模块测试通过！")
        print("\n下一步操作:")
        print("  python main.py --action p3_demo     # 运行P3演示")
        print("  python main.py --action collect_all # 采集所有股票")
        print("  python main.py --action validate    # 验证数据状态")
        
    else:
        print(" 数据采集失败")
        
except Exception as e:
    print(f" 测试失败: {e}")
    import traceback
    traceback.print_exc()
