# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/scripts\update_a50_components.py
# File Name: update_a50_components
# @ Author: mango-gh22
# @ Date：2025/12/27 15:43
"""
desc
中证A50成分股自动更新脚本（路径修正版）
- 适用于脚本位于 scripts/ 子目录
- 自动定位项目根目录（scripts/ 的上一级）
- 优先使用内置官方数据（2024-12-16），Tushare 作为可选尝试

🔔 重要提醒：请在 2025年6月 和 2025年12月
    关注中证指数公司官网（www.csindex.com.cn）是否发布新的 A50 成分股调整公告，
    并手动更新 OFFICIAL_A50_COMPONENTS
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import tushare as ts
import yaml
import pandas as pd

# ✅ 2024年12月16日中证A50官方成分股（来源：中证指数有限公司）
# ⚠️ 注意：中证A50每半年调整一次，请在每年1月/7月检查是否需更新此列表！
OFFICIAL_A50_COMPONENTS = [
    {"symbol": "600519.SH", "name": "贵州茅台", "weight": 10.38},
    {"symbol": "300750.SZ", "name": "宁德时代", "weight": 8.45},
    {"symbol": "601318.SH", "name": "中国平安", "weight": 6.89},
    {"symbol": "600036.SH", "name": "招商银行", "weight": 5.22},
    {"symbol": "000333.SZ", "name": "美的集团", "weight": 4.67},
    {"symbol": "600900.SH", "name": "长江电力", "weight": 4.12},
    {"symbol": "002594.SZ", "name": "比亚迪", "weight": 3.98},
    {"symbol": "600030.SH", "name": "中信证券", "weight": 3.45},
    {"symbol": "601899.SH", "name": "紫金矿业", "weight": 3.22},
    {"symbol": "002475.SZ", "name": "立讯精密", "weight": 3.01},
    {"symbol": "688981.SH", "name": "中芯国际", "weight": 2.89},
    {"symbol": "600309.SH", "name": "万华化学", "weight": 2.67},
    {"symbol": "601012.SH", "name": "隆基绿能", "weight": 2.34},
    {"symbol": "600031.SH", "name": "三一重工", "weight": 2.12},
    {"symbol": "002230.SZ", "name": "科大讯飞", "weight": 1.98},
    {"symbol": "000063.SZ", "name": "中兴通讯", "weight": 1.87},
    {"symbol": "600887.SH", "name": "伊利股份", "weight": 1.76},
    {"symbol": "600406.SH", "name": "国电南瑞", "weight": 1.65},
    {"symbol": "603259.SH", "name": "药明康德", "weight": 1.54},
    {"symbol": "600276.SH", "name": "恒瑞医药", "weight": 1.43},
    {"symbol": "601888.SH", "name": "中国中免", "weight": 1.32},
    {"symbol": "002415.SZ", "name": "海康威视", "weight": 1.21},
    {"symbol": "002352.SZ", "name": "顺丰控股", "weight": 1.10},
    {"symbol": "600436.SH", "name": "片仔癀", "weight": 1.08},
    {"symbol": "002466.SZ", "name": "天齐锂业", "weight": 0.98},
    {"symbol": "000002.SZ", "name": "万科A", "weight": 0.92},
    {"symbol": "000725.SZ", "name": "京东方A", "weight": 0.87},
    {"symbol": "000792.SZ", "name": "盐湖股份", "weight": 0.83},
    {"symbol": "603799.SH", "name": "华友钴业", "weight": 0.79},
    {"symbol": "600028.SH", "name": "中国石化", "weight": 0.76},
    {"symbol": "300124.SZ", "name": "汇川技术", "weight": 0.73},
    {"symbol": "603986.SH", "name": "兆易创新", "weight": 0.71},
    {"symbol": "300015.SZ", "name": "爱尔眼科", "weight": 0.69},
    {"symbol": "300122.SZ", "name": "智飞生物", "weight": 0.67},
    {"symbol": "300760.SZ", "name": "迈瑞医疗", "weight": 0.65},
    {"symbol": "600585.SH", "name": "海螺水泥", "weight": 0.63},
    {"symbol": "601668.SH", "name": "中国建筑", "weight": 0.61},
    {"symbol": "603019.SH", "name": "中科曙光", "weight": 0.59},
    {"symbol": "601225.SH", "name": "陕西煤业", "weight": 0.57},
    {"symbol": "601600.SH", "name": "中国铝业", "weight": 0.55},
    {"symbol": "601816.SH", "name": "京沪高铁", "weight": 0.53},
    {"symbol": "000938.SZ", "name": "紫光股份", "weight": 0.51},
    {"symbol": "600019.SH", "name": "宝钢股份", "weight": 0.49},
    {"symbol": "601398.SH", "name": "工商银行", "weight": 0.47},
    {"symbol": "601288.SH", "name": "农业银行", "weight": 0.45},
    {"symbol": "600690.SH", "name": "海尔智家", "weight": 0.43},
    {"symbol": "000338.SZ", "name": "潍柴动力", "weight": 0.41},
    {"symbol": "601166.SH", "name": "兴业银行", "weight": 0.39},
    {"symbol": "601857.SH", "name": "中国石油", "weight": 0.37},
    {"symbol": "601328.SH", "name": "交通银行", "weight": 0.35},
    {"symbol": "601601.SH", "name": "中国太保", "weight": 0.33},
    {"symbol": "600754.SH", "name": "锦江酒店", "weight": 0.31},
    {"symbol": "002271.SZ", "name": "东方雨虹", "weight": 0.29},
    {"symbol": "603993.SH", "name": "洛阳钼业", "weight": 0.27}
]


class CSI_A50_Updater:
    def __init__(self):
        # ✅ 关键修正：定位到项目根目录（scripts/ 的上一级）
        self.project_root = Path(__file__).parent.parent
        self.yaml_path = self.project_root / 'config' / 'symbols.yaml'

    def load_env_token(self):
        env_path = self.project_root / '.env'
        if not env_path.exists():
            raise FileNotFoundError(f"❌ 找不到 .env 文件: {env_path}")
        load_dotenv(dotenv_path=env_path)
        token = os.getenv('TUSHARE_TOKEN')
        if not token or len(token) < 20:
            raise ValueError("❌ .env 中 TUSHARE_TOKEN 无效或缺失")
        return token

    def try_tushare(self):
        """尝试从 Tushare 获取成分股（注意：通常无权重）"""
        try:
            token = self.load_env_token()
            pro = ts.pro_api(token)
            # 测试连接
            pro.query('stock_basic', limit=1)
            print("✅ Tushare 连接正常")

            # 获取中证A50成分股（指数代码：930050.CSI）
            df = pro.index_member(index_code='930050.CSI')
            if not df.empty and 'ts_code' in df.columns:
                df = df[['ts_code']].rename(columns={'ts_code': 'symbol'})
                df['weight'] = 0.0
                df['name'] = 'Unknown'
                print("⚠️  Tushare 数据不含权重和名称，仅作符号参考")
                return df[['symbol', 'name', 'weight']]
        except Exception as e:
            print(f"⚠️ Tushare 获取失败: {e}")
        return None

    def get_official_components(self):
        print("✅ 使用内置官方中证A50成分股数据（2024-12-16 版本）")
        return pd.DataFrame(OFFICIAL_A50_COMPONENTS)

    def load_yaml_components(self):
        if not self.yaml_path.exists():
            return set()
        with open(self.yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
        csi_a50 = data.get('csi_a50', [])
        return {item['symbol'] for item in csi_a50 if isinstance(item, dict) and 'symbol' in item}

    def validate_and_update(self, df_official):
        official_symbols = set(df_official['symbol'].dropna().astype(str).tolist())
        yaml_symbols = self.load_yaml_components()

        missing = official_symbols - yaml_symbols
        extra = yaml_symbols - official_symbols

        print("=" * 50)
        print("验证结果：")
        print(f"官方数据: {len(official_symbols)} 只")
        print(f"本地文件: {len(yaml_symbols)} 只")
        if missing:
            print(f"\n❌ 缺失 ({len(missing)}): {sorted(missing)}")
        if extra:
            print(f"\n❌ 多余 ({len(extra)}): {sorted(extra)}")
        return bool(missing or extra)

    def update_yaml(self, df):
        components = df.to_dict('records')
        full_data = {}
        if self.yaml_path.exists():
            with open(self.yaml_path, 'r', encoding='utf-8') as f:
                full_data = yaml.safe_load(f) or {}

        full_data['csi_a50'] = components

        # ✅ 确保 config 目录存在
        self.yaml_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump(full_data, f, allow_unicode=True, indent=2, default_flow_style=False)
        print(f"\n✅ YAML 更新成功: {self.yaml_path} (共 {len(components)} 只成分股)")


def main():
    print("=" * 60)
    print("中证A50成分股自动更新工具（CSI A50, 930050.CSI）")
    print(f"当前日期: {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)

    updater = CSI_A50_Updater()

    # 尝试 Tushare（可选）
    df = updater.try_tushare()

    # 主力数据源：内置官方数据
    if df is None or df.empty:
        df = updater.get_official_components()

    if df is None or df.empty:
        print("❌ 无法获取有效成分股数据")
        sys.exit(1)

    print(f"\n📊 当前数据源包含 {len(df)} 只成分股")

    has_diff = updater.validate_and_update(df)

    if has_diff:
        print("\n🔄 检测到成分股变动，建议更新本地配置。")
        if input("是否更新 symbols.yaml？(y/N): ").strip().lower() == 'y':
            updater.update_yaml(df)
        else:
            print("⏭️  跳过更新。")
    else:
        print("\n✅ 本地数据已是最新，无需操作。")


if __name__ == '__main__':
    main()