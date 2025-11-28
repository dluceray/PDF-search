
# Module 5 — Multi-directory Support (正式版，无示例文件)

本模块为“PDF 合同检索系统”的 **多目录与多 Excel 索引整合**能力，遵循前 1–4 模块的集成方式，提供可直接接入的后端能力：

- **多目录聚合**：一次性加载多个年度目录（如 `.../2024`, `.../2025`）。
- **多 Excel 兼容**：自动识别不同 Excel 表头并归一到统一字段。
- **签订日期模糊**：支持 `YYYY` / `YYYY-MM` / `YYYY-MM-DD` 前缀匹配。
- **合同编号匹配**：按编号扫描对应 PDF，输出绝对路径。
- **接口对接**：提供 FastAPI Router：`/api/mdirs/*`。

> 说明：不附带任何示例 Excel 或 PDF 文件。

## 目录结构
```
module5/
  multi_dir/
    __init__.py
    config.py
    index_schema.py
    aggregator.py
    search_api.py
```

## 与主系统集成（FastAPI）
```python
from fastapi import FastAPI
from module5.multi_dir import search_router

app = FastAPI()
app.include_router(search_router, prefix="/api/mdirs")
```

### 初始化 / 重新加载
- `POST /api/mdirs/reload`
```json
{
  "roots": ["/data/contracts/2024", "/data/contracts/2025"],
  "excel_patterns": ["index.xlsx", "目录.xlsx"],
  "allowed_exts": [".pdf"],
  "schema_hints": {
    "/data/contracts/2024": {"项目名称": "project", "签约日期": "sign_date"}
  }
}
```
成功后返回 `{"ok": true, "records_loaded": N, "roots": [...]}`。

### 查询
- `POST /api/mdirs/search`
支持字段：`project_like`, `unit_like`, `sign_method`, `sign_date_like`, `contract_no_like`, `limit`。

## 统一字段（标准输出）
- `id`、`project`（工程地点及内容）、`unit`（单位名称）、`sign_method`（签订方式）、
  `sign_date`（签订日期）、`contract_amount`（合同额）、`contract_no`（合同编号）、
  `pdf_path`（PDF绝对路径，找不到为空）、`root`（来源目录）。

## 依赖
- Python 3.9+
- pandas (>=1.5)、openpyxl

## 注意
- 本模块**不修改**主应用的登录/权限等逻辑。
- 索引 Excel 的表头可不同，自动归一；若个别特殊字段名，可通过 `schema_hints` 覆盖。
- 查询默认限制返回 200 条结果，可调整 `limit`。

## 变更与校验
- 本版本由人工手工制作，已进行结构与接口校对；无示例文件；可直接被导入使用。
