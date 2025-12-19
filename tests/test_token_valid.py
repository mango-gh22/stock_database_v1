# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/tests\test_token_valid.py
# File Name: test_token_valid
# @ Author: mango-gh22
# @ Date：2025/12/13 16:44
"""
desc 
"""
import tushare as ts

TOKEN = "4ce587fd8aefdef2d2073c28b2f34b80a7a62c668e420f51abc07ed9"  # 临时硬编码测试

# 方法1：查看积分和权限（最可靠）
pro = ts.pro_api(TOKEN)

# ✅ 查询用户积分（通常有权限）
try:
    df_user = pro.query('user', token=TOKEN)  # 注意接口名是'user'
    print("✅ Token认证成功！")
    print(f"用户名: {df_user.loc[0, 'user']}")
    print(f"积分余额: {df_user.loc[0, 'points']}")
    print(f"注册状态: {df_user.loc[0, 'status']}")
except Exception as e:
    print(f"❌ 认证失败: {e}")

# 方法2：测试有积分就能调用的接口
try:
    # stock_basic是常用基础接口
    df_stock = pro.stock_basic(exchange='', list_status='L',
                               fields='ts_code,name',
                               limit=5)  # 只查5条
    print(f"\n✅ 接口调用成功！获取到 {len(df_stock)} 只股票")
    print(df_stock)
except Exception as e:
    print(f"\n❌ 接口调用失败: {e}")
    print("可能原因：积分不足或Token无效")

# 方法3：查询指数列表（验证指数数据权限）
try:
    df_index = pro.index_basic(market='CSI', fields='ts_code,name')
    print(f"\n✅ 指数接口成功！共 {len(df_index)} 只指数")
    # 查找中证A50
    a50 = df_index[df_index['ts_code'] == '930050.CSI']
    if not a50.empty:
        print("找到中证A50指数:", a50.iloc[0]['name'])
except Exception as e:
    print(f"\n❌ 指数接口失败: {e}")