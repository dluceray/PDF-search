
import re
from typing import Optional, Tuple

DATE_RE_Y   = re.compile(r"^\s*(\d{4})\s*$")
DATE_RE_YM  = re.compile(r"^\s*(\d{4})[-/.](\d{1,2})\s*$")
DATE_RE_YMD = re.compile(r"^\s*(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})\s*$")

def normalize_date(s: Optional[str]) -> str:
    """
    Normalise date strings to one of:
      - YYYY
      - YYYY-MM  (month zero-padded)
      - YYYY-MM-DD (day zero-padded)
    Non-conforming values return "".
    """
    if not s:
        return ""
    s = str(s).strip()
    m = DATE_RE_YMD.match(s)
    if m:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mth <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mth:02d}-{d:02d}"
        return ""
    m = DATE_RE_YM.match(s)
    if m:
        y, mth = int(m.group(1)), int(m.group(2))
        if 1 <= mth <= 12:
            return f"{y:04d}-{mth:02d}"
        return ""
    m = DATE_RE_Y.match(s)
    if m:
        return f"{int(m.group(1)):04d}"
    # Chinese formats like "2025年6月" / "2025年6月3日"
    s2 = s.replace("年","-").replace("月","-").replace("日","").replace("．",".").replace("。",".")
    return normalize_date(s2)

def normalize_amount(s: Optional[str]) -> str:
    """
    Normalise currency style amounts by removing commas and spaces.
    Leaves exact text if non-numeric, because source may contain units (万元等).
    """
    if not s:
        return ""
    s = str(s).strip()
    s = s.replace(",", "").replace(" ", "")
    return s

def contains_ci(hay: Optional[str], needle: str) -> bool:
    return bool(hay) and (needle.lower() in str(hay).lower())

def prefix_match(val: Optional[str], prefix: str) -> bool:
    return bool(val) and str(val).strip().startswith(prefix.strip())
