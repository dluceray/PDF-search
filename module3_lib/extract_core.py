
import re
from typing import Dict
DATE_PATTERNS = [
    re.compile(r"(?P<y>20\d{2})[.\-/年](?P<m>\d{1,2})(?:[.\-/月](?P<d>\d{1,2})日?)?"),
    re.compile(r"(?P<y>20\d{2})年"),
]
def normalize_date(s: str) -> str:
    if not s: return ""
    s = s.strip()
    m = DATE_PATTERNS[0].search(s)
    if m:
        y, mth, d = int(m.group("y")), int(m.group("m")), m.group("d")
        if d:
            return f"{y:04d}-{int(mth):02d}-{int(d):02d}"
        return f"{y:04d}-{int(mth):02d}"
    m2 = DATE_PATTERNS[1].search(s)
    if m2:
        return f"{int(m2.group('y')):04d}"
    return ""
def _field(text: str, keys):
    for k in keys:
        m = re.search(rf"{k}\s*[:：]\s*(.+)", text)
        if m:
            return m.group(1).strip()
    return ""
def extract_fields(text: str) -> Dict[str, str]:
    text = (text or "").strip()
    if not text:
        return {}
    project = _field(text, ["工程名称", "工程地点及内容", "项目名称"])
    unit = _field(text, ["单位名称", "甲方名称", "合同单位"])
    sign_method = _field(text, ["签订方式", "签约方式"])
    raw_date = _field(text, ["签订日期", "签约日期", "日期"])
    sign_date = normalize_date(raw_date)
    return {"project": project,"unit": unit,"sign_method": sign_method,"sign_date": sign_date}
