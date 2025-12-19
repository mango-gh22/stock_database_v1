# _*_ coding: utf-8 _*_
# File Path: E:/MyFile/stock_database_v1/config\generate_a50_symbols_yaml.py
# File Name: generate_a50_symbols_yaml
# @ Author: mango-gh22
# @ Date：2025/12/13 17:40
"""
desc
定期更新流程---每月10日，运行
python -m config.generate_a50_symbols_yaml

每月从中证指数官网下载最新 930050cons.xls 和 930050closeweight.xls
覆盖旧文件
运行此脚本 → 自动同步成分股变动和最终权重

"""
import pandas as pd
import yaml
import os

# 配置路径
config_dir = r"E:/MyFile/stock_database_v1/config"
cons_file = os.path.join(config_dir, "930050cons.xls")
weight_file = os.path.join(config_dir, "930050closeweight.xls")
output_file = os.path.join(config_dir, "symbols.yaml")

# --- 第一步：读取现有 symbols.yaml（如果存在）---
existing_data = {}
header_comment = """# config/symbols.yaml
# 中证A50指数成分股（官方50只，2024年12月调整）
"""

if os.path.exists(output_file):
    with open(output_file, 'r', encoding='utf-8') as f:
        content = f.read()
        lines = content.splitlines()
        comment_lines = []
        for line in lines:
            if line.strip().startswith('#'):
                comment_lines.append(line)
            else:
                break
        if comment_lines:
            header_comment = '\n'.join(comment_lines) + '\n\n'
        try:
            existing_data = yaml.safe_load(content)
        except Exception:
            existing_data = {}

existing_a50 = existing_data.get('csi_a50', [])
existing_dict = {item['symbol']: item for item in existing_a50 if 'symbol' in item}

# --- 第二步：读取成分股基础信息 ---
df_cons = pd.read_excel(cons_file, dtype=str)
df_cons.columns = df_cons.columns.str.strip()

code_col = '成份券代码Constituent Code'
name_col = '成份券名称Constituent Name'
exchange_col = '交易所Exchange'

for col in [code_col, name_col, exchange_col]:
    if col not in df_cons.columns:
        raise ValueError(f"❌ 930050cons.xls 缺少列: {col}. 可用列: {list(df_cons.columns)}")

df_cons = df_cons[[code_col, name_col, exchange_col]].dropna(subset=[code_col]).drop_duplicates(subset=[code_col])
df_cons[code_col] = df_cons[code_col].str.strip()

# --- 第三步：读取权重文件，并智能识别权重列 ---
df_weight = pd.read_excel(weight_file, dtype=str)
df_weight.columns = df_weight.columns.str.strip()

# 智能查找权重列
weight_col = None
for col in df_weight.columns:
    if '权重' in col or 'weight' in col.lower():
        weight_col = col
        break

if weight_col is None:
    raise ValueError(f"❌ 在 {weight_file} 中未找到包含 '权重' 或 'weight' 的列！可用列: {list(df_weight.columns)}")

print(f"✅ 使用权重列: '{weight_col}'")

# 提取代码和权重
df_weight = df_weight[[code_col, weight_col]].dropna(subset=[code_col]).drop_duplicates(subset=[code_col])
df_weight[code_col] = df_weight[code_col].str.strip()
df_weight[weight_col] = pd.to_numeric(df_weight[weight_col], errors='coerce').fillna(0.0)

# --- 第四步：合并数据 ---
df_merged = pd.merge(df_cons, df_weight, on=code_col, how='left')
df_merged[weight_col] = df_merged[weight_col].fillna(0.0)

# --- 第五步：构建新列表 ---
new_a50_list = []
for _, row in df_merged.iterrows():
    code = row[code_col].strip()
    name = row[name_col].strip() if pd.notna(row[name_col]) else ""
    exchange = row[exchange_col].strip() if pd.notna(row[exchange_col]) else ""
    weight = float(row[weight_col])

    # 构造 symbol 和 market
    if code.startswith('6'):
        symbol = f"{code}.SH"
        market = "科创板" if code.startswith('688') else "主板"
    elif code.startswith(('0', '3')):
        symbol = f"{code}.SZ"
        market = "创业板" if code.startswith('3') else "主板"
    else:
        symbol = f"{code}.UNKNOWN"
        market = "未知"

    if symbol in existing_dict:
        updated = existing_dict[symbol].copy()
        updated['weight'] = weight
        # 可选：也清理 existing 中可能存在的空字符串（如果需要）
        for key in ['name', 'industry', 'list_date', 'area', 'fullname']:
            if key in updated and updated[key] == "":
                updated[key] = None
        new_a50_list.append(updated)
    else:
        new_a50_list.append({
            "symbol": symbol,
            "name": empty_to_none(name),
            "industry": None,
            "list_date": None,
            "weight": weight,
            "area": None,
            "market": market,  # market 有实际值，不设为 None
            "fullname": None
        })

new_a50_list.sort(key=lambda x: x['symbol'])

# --- 第六步：写回文件 ---
final_data = {
    'csi_a50': new_a50_list,
    'csi_300': existing_data.get('csi_300', []),
    'csi_500': existing_data.get('csi_500', [])
}

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(header_comment)
    yaml.dump(
        final_data,
        f,
        allow_unicode=True,
        indent=2,
        sort_keys=False,
        default_flow_style=False,
        width=1000
    )

print(f"✅ 增量更新完成！共 {len(new_a50_list)} 只股票。权重列来源: '{weight_col}'")