
from typing import Dict, List

# Canonical field names used after normalization
CANONICAL_FIELDS: List[str] = [
    "id",               # Unique row id within the unified index
    "project",          # 工程地点及内容
    "unit",             # 单位名称
    "sign_method",      # 签订方式
    "sign_date",        # 签订日期 (string YYYY / YYYY-MM / YYYY-MM-DD as provided)
    "contract_amount",  # 合同额
    "contract_no",      # 合同编号
    "pdf_path",         # Resolved absolute PDF path (if exists), else empty
    "root",             # Root directory joined from which this record originated
]

# Default heuristic mapping from common Chinese headers to canonical names
DEFAULT_HEADER_MAP: Dict[str, str] = {
    "序号": "id", "编号": "id", "ID": "id", "id": "id",
    "工程地点及内容": "project", "工程名称": "project", "工程地点": "project", "项目名称": "project",
    "单位名称": "unit", "甲方名称": "unit", "合同单位": "unit",
    "签订方式": "sign_method", "签约方式": "sign_method",
    "签订日期": "sign_date", "签约日期": "sign_date", "日期": "sign_date",
    "合同额": "contract_amount", "金额": "contract_amount", "合同金额": "contract_amount",
    "合同编号": "contract_no", "编号(合同)": "contract_no",
    "PDF": "pdf_path", "文件路径": "pdf_path",
}

# Additional aliases sometimes seen in user data
ALIASES: Dict[str, str] = {
    "工程地点及内容（项目）": "project",
    "签订日期（年月日）": "sign_date",
    "签订日期（年月）": "sign_date",
    "签订日期（年）": "sign_date",
}
