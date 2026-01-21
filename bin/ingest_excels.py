#!/root/pdfsearch/venv/bin/python

# === probe: log interpreter & xlrd presence ===
try:
    import sys, json, time
    _probe = {"ts": time.strftime("%F %T"), "exe": sys.executable}
    try:
        import xlrd
        _probe["xlrd"] = getattr(xlrd, "__version__", "?")
    except Exception as e:
        _probe["xlrd_error"] = repr(e)
    with open("/tmp/ingest_probe.json","a",encoding="utf-8") as f:
        f.write(json.dumps(_probe, ensure_ascii=False)+"\n")
except Exception:
    pass
# === probe end ===
# -*- coding: utf-8 -*-
import os, sys, glob, time, json, shutil, argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional

YEAR_MIN, YEAR_MAX = 2000, 2050
STD_HEADER = ["序号","工程地点及内容","单位名称","签订途径","启动时间","结果确定时间","签订日期","控制价","合同额","结算值","已付款","欠付款","备注"]
REQUIRED_HEADER = ["序号", "工程地点及内容", "单位名称"]
HEADER_ALIASES = {
    "序号": ["序号", "编号"],
    "工程地点及内容": ["工程地点及内容", "工程地点", "工程内容"],
    "单位名称": ["单位名称", "单位"],
    "签订途径": ["签订途径", "签订方式"],
    "启动时间": ["启动时间", "启动日期"],
    "结果确定时间": ["结果确定时间", "结果确定日期"],
    "签订日期": ["签订日期", "合同签订日期"],
    "控制价": ["控制价", "控制金额"],
    "合同额": ["合同额", "合同金额"],
    "结算值": ["结算值", "结算金额"],
    "已付款": ["已付款", "已支付", "已付金额"],
    "欠付款": ["欠付款", "欠付", "欠付金额"],
    "备注": ["备注", "备注说明"],
}

def ts(): return datetime.now().strftime("%Y%m%d%H%M%S")
def header_equal(a,b): return len(a)==len(b) and all((a[i]==b[i]) for i in range(len(a)))
def normalize_header_cells(row):
    vals=[ ("" if v is None else str(v).strip()) for v in row ]
    while vals and vals[-1]=="": vals.pop()
    return vals
def canonicalize_header(cell: str):
    val = (cell or "").strip()
    if not val:
        return ""
    for std, aliases in HEADER_ALIASES.items():
        if val in aliases:
            return std
    return val
def header_map_from_cells(cells: List[str]):
    header_map = {}
    headers = []
    for i, cell in enumerate(cells):
        name = canonicalize_header(cell)
        headers.append(name)
        if name and name not in header_map:
            header_map[name] = i
    return header_map, headers
def match_header(row):
    cells = normalize_header_cells(row)
    header_map, headers = header_map_from_cells(cells)
    missing_required = [k for k in REQUIRED_HEADER if k not in header_map]
    if missing_required:
        return False, {}, headers
    return True, header_map, headers
def parse_year_from_seq(s: str):
    s=(s or "").strip()
    if len(s)<2 or not s[:2].isdigit(): return False,-1,"序号前两位非数字"
    yy=int(s[:2]); year=2000+yy
    if not (YEAR_MIN<=year<=YEAR_MAX): return False,-1,f"年份超出范围:{year}"
    return True,year,""

def find_index_file(year_dir: Path) -> Optional[Path]:
    """在年份目录中大小写不敏感地寻找 index.xlsx；返回实际存在的文件路径（保留原始大小写）。"""
    if not year_dir.is_dir(): return None
    for p in year_dir.iterdir():
        if p.is_file() and p.name.lower()=="index.xlsx":
            return p
    return None

def find_template_index(root: Path, target_year: int) -> Optional[Path]:
    years = sorted(
        [p for p in root.iterdir() if p.is_dir() and p.name.isdigit()],
        key=lambda p: int(p.name),
        reverse=True
    )
    for y in years:
        if int(y.name) == target_year:
            continue
        idx = find_index_file(y)
        if idx:
            return idx
    return None

def ensure_year_index(root: Path, year: int) -> Optional[Path]:
    year_dir = root / str(year)
    idx_path = find_index_file(year_dir)
    if idx_path:
        return idx_path
    tmpl = find_template_index(root, year)
    if not tmpl:
        return None
    year_dir.mkdir(parents=True, exist_ok=True)
    idx_path = year_dir / "index.xlsx"
    shutil.copy2(tmpl, idx_path)
    return idx_path

def load_year_sheet(idx_path: Path, headers_needed: List[str]):
    if not idx_path or not idx_path.is_file():
        return False, None, None, {}, {}, "缺少index.xlsx"
    try:
        from openpyxl import load_workbook
        wb=load_workbook(idx_path); ws=wb.active
        hdr=[ ("" if c.value is None else str(c.value).strip()) for c in ws[1] ]
        header_map, _headers = header_map_from_cells(hdr)
        for key in REQUIRED_HEADER:
            if key not in header_map:
                col = ws.max_column + 1
                ws.cell(row=1, column=col, value=key)
                header_map[key] = col - 1
        for key in headers_needed:
            if key and key not in header_map:
                col = ws.max_column + 1
                ws.cell(row=1, column=col, value=key)
                header_map[key] = col - 1
        seq_pos = header_map.get("序号")
        if seq_pos is None:
            return False, None, None, {}, {}, "索引表缺少必填列: 序号"
        seq_map = {}
        for r in ws.iter_rows(min_row=2):
            if not r:
                continue
            val = r[seq_pos].value if seq_pos < len(r) else None
            if val is None:
                continue
            seq = str(val).strip()
            if not seq:
                continue
            seq_map[seq] = r[0].row
        return True, wb, ws, header_map, seq_map, ""
    except Exception as e:
        return False, None, None, {}, {}, f"无法读取index.xlsx:{e}"


def backup_and_merge(idx_path: Path, rows, headers_needed: List[str]):
    """
    写入策略（覆盖+追加）：
    - 若序号已存在，则按导入值覆盖该行对应列；
    - 若序号不存在，则追加新行；
    - 若导入包含服务端缺失的列，则自动新增列。
    """
    date_dir = idx_path.parent / datetime.now().strftime("%Y%m%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    bak = date_dir / idx_path.name
    if bak.exists():
        bak = date_dir / f"{idx_path.stem}.{ts()}{idx_path.suffix}"
    tmp = idx_path.with_suffix(".tmp.xlsx")
    shutil.copy2(idx_path, bak)

    ok, wb, ws, header_map, seq_map, why = load_year_sheet(idx_path, headers_needed)
    if not ok:
        raise ValueError(why)

    added = 0
    updated = 0
    for rec in rows:
        seq = str(rec.get("序号", "") or "").strip()
        if not seq:
            continue
        row_idx = seq_map.get(seq)
        if row_idx is None:
            row_idx = ws.max_row + 1
            seq_map[seq] = row_idx
            added += 1
        else:
            updated += 1
        for key, val in rec.items():
            if not key:
                continue
            if key not in header_map:
                col = ws.max_column + 1
                ws.cell(row=1, column=col, value=key)
                header_map[key] = col - 1
            ws.cell(row=row_idx, column=header_map[key] + 1, value=val)

    wb.save(tmp)
    os.replace(tmp, idx_path)
    return bak.name, added, updated
def detect_header_row(values_rows, mode:str):
    rows=[normalize_header_cells(r) for r in values_rows[:2]]
    if mode=="2":
        hdr = rows[1] if len(rows)>1 else []
        ok, header_map, cells = match_header(hdr) if len(rows)>1 else (False, {}, [])
        return (len(rows)>1 and ok, 1, "表头不匹配" if not (len(rows)>1 and ok) else "", cells, header_map)
    if mode=="1":
        hdr = rows[0] if rows else []
        ok, header_map, cells = match_header(hdr) if len(rows)>0 else (False, {}, [])
        return (len(rows)>0 and ok, 0, "表头不匹配" if not (len(rows)>0 and ok) else "", cells, header_map)
    if len(rows)>1:
        ok, header_map, cells = match_header(rows[1])
        if ok:
            return True,1,"",cells, header_map
    if len(rows)>0:
        ok, header_map, cells = match_header(rows[0])
        if ok:
            return True,0,"",cells, header_map
    return False,-1,"前两行均非标准表头", rows[0] if rows else [], {}

def load_pending_rows(xlsx_path: Path, header_mode: str):
    ext = xlsx_path.suffix.lower()
    errors = []
    rows_out = []
    headers_seen = []
    try:
        if ext == ".xlsx":
            from openpyxl import load_workbook
            wb = load_workbook(xlsx_path, data_only=True, read_only=True)
            for ws in wb.worksheets:
                values_rows = [ (list(r) if r else []) for r in ws.iter_rows(min_row=1, values_only=True) ]
                if not any(any((v is not None and str(v).strip() != "") for v in row) for row in values_rows):
                    continue
                ok, hdr_idx, why, hdr_cells, header_map = detect_header_row(values_rows, header_mode)
                if not ok:
                    errors.append(f"{ws.title}: {why}")
                    continue
                for h in hdr_cells:
                    if h and h not in headers_seen:
                        headers_seen.append(h)
                for r in values_rows[hdr_idx+1:]:
                    r = list(r) if isinstance(r, (list, tuple)) else [r]
                    row = {}
                    for key, ci in header_map.items():
                        row[key] = r[ci] if ci < len(r) else ""
                    rows_out.append(row)
        elif ext == ".xls":
            try:
                import xlrd  # 1.2.0
            except Exception:
                return False, "处理 .xls 需要安装 xlrd==1.2.0", [], []
            book = xlrd.open_workbook(xlsx_path, formatting_info=False)
            for si in range(book.nsheets):
                sh = book.sheet_by_index(si)
                values_rows = [[sh.cell_value(i,j) for j in range(sh.ncols)] for i in range(sh.nrows)]
                if not any(any((v is not None and str(v).strip() != "") for v in row) for row in values_rows):
                    continue
                ok, hdr_idx, why, hdr_cells, header_map = detect_header_row(values_rows, header_mode)
                if not ok:
                    errors.append(f"{sh.name}: {why}")
                    continue
                for h in hdr_cells:
                    if h and h not in headers_seen:
                        headers_seen.append(h)
                for r in values_rows[hdr_idx+1:]:
                    r = list(r) if isinstance(r, (list, tuple)) else [r]
                    row = {}
                    for key, ci in header_map.items():
                        row[key] = r[ci] if ci < len(r) else ""
                    rows_out.append(row)
        else:
            return False, "不支持的扩展名（仅 .xlsx/.xls）", [], []
    except Exception as e:
        return False, f"无法读取Excel: {e}", [], []

    if errors:
        return False, "以下工作表不符合要求（全部中止处理）：" + "; ".join(errors), [], []
    return True, "", rows_out, headers_seen


def process_file(xlsx: Path, root: Path, header_mode: str):
    res={"file": xlsx.name, "added":0, "updated":0, "skipped":0, "invalid":0, "details":[]}
    ok, why, rows, headers_seen = load_pending_rows(xlsx, header_mode)
    if not ok: return {"file": xlsx.name, "__file_failed__": True, "reason": why}
    buckets: Dict[int, List[Dict]] = {}
    for r in rows:
        seq = "" if r.get("序号") is None else str(r.get("序号")).strip()
        if seq=="":
            res["invalid"]+=1; res["details"].append({"seq":None,"status":"invalid","reason":"序号为空"}); continue
        ok_year,year,rr = parse_year_from_seq(seq)
        if not ok_year:
            res["invalid"]+=1; res["details"].append({"seq":seq,"status":"invalid","reason":rr}); continue
        buckets.setdefault(year, []).append(r)
    for year, rows2 in buckets.items():
        if not rows2: continue
        idx_path = ensure_year_index(root, year)
        if not idx_path:
            res["invalid"] += len(rows2)
            res["details"].append({"year": year, "status":"invalid", "reason":"缺少index.xlsx模板"})
            continue
        lock = idx_path.with_name(idx_path.name + ".lock")
        waited=0.0
        while lock.exists():
            import time as _t; _t.sleep(0.1); waited+=0.1
            if waited>5.0: return {"file": xlsx.name, "__file_failed__": True, "reason": f"年索引被锁:{idx_path.name}"}
        try:
            lock.touch()
            bak, added, updated = backup_and_merge(idx_path, rows2, headers_seen)
            res["details"].append({"year":year,"status":"wrote","rows":len(rows2),"backup":bak})
            res["added"] += added
            res["updated"] += updated
        finally:
            try: lock.unlink(missing_ok=True)
            except: pass
    return res

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--contracts-root", default="/data/contracts")
    ap.add_argument("--pending-dir", default="/data/add_pending")
    ap.add_argument("--done-dir", default="/data/add_done")
    ap.add_argument("--error-dir", default="/data/add_error")
    ap.add_argument("--summary-out", default="")
    ap.add_argument("--lock-file", default="/var/run/excel-ingest.lock")
    ap.add_argument("--header-row", default="2", choices=["1","2","auto"], help="待处理 Excel 的表头所在行（1-based），默认 2")
    args=ap.parse_args()
    root=Path(args.contracts_root); pending=Path(args.pending_dir); done=Path(args.done_dir); error=Path(args.error_dir)
    try:
        fd=os.open(args.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644); os.close(fd)
    except FileExistsError:
        print(json.dumps({"ok": False, "error": "another ingest running"}, ensure_ascii=False)); sys.exit(1)
    try:
        files=[p for p in pending.iterdir() if p.is_file() and not p.name.startswith("~$") and p.suffix.lower() in {".xls",".xlsx",".xlsm"}]
        files.sort(key=lambda p: p.stat().st_mtime)
        from pathlib import Path as _P
        _P("/tmp/ingest_scan.json").write_text("\n".join(f.name for f in files)+"\n", encoding="utf-8")
        total={"files_total":len(files),"files_processed":0,"files_failed":0,"rows_added":0,"rows_updated":0,"rows_skipped":0,"rows_invalid":0,"per_file":[]}
        for f in files:
            res=process_file(f, root, args.header_row)
            if res.get("__file_failed__"):
                error.mkdir(parents=True, exist_ok=True); shutil.move(str(f), str(error/f.name))
                if res.get("reason"): (error/(f.name + ".reason.txt")).write_text(res["reason"], encoding="utf-8")
                total["files_failed"]+=1; total["per_file"].append(res); continue
            done.mkdir(parents=True, exist_ok=True); shutil.move(str(f), str(done/f.name))
            total["files_processed"]+=1
            total["rows_added"]+=res["added"]; total["rows_updated"]+=res["updated"]; total["rows_skipped"]+=res["skipped"]; total["rows_invalid"]+=res["invalid"]
            total["per_file"].append(res)
        if args.summary_out:
            Path(args.summary_out).write_text(json.dumps({"ok":True, **total}, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"ok":True, **total}, ensure_ascii=False, indent=2))
    finally:
        try: os.unlink(args.lock_file)
        except: pass

if __name__=="__main__": main()
