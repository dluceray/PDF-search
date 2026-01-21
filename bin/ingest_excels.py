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
def match_header(row):
    cells = normalize_header_cells(row)
    positions = []
    for std in STD_HEADER:
        aliases = HEADER_ALIASES.get(std, [std])
        idx = next((i for i, c in enumerate(cells) if c in aliases), None)
        if idx is None:
            return False, [], cells
        positions.append(idx)
    return True, positions, cells
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

def load_year_ids_and_check_header(idx_path: Path):
    ids=set()
    if not idx_path or not idx_path.is_file(): return False, ids, "缺少index.xlsx", []
    try:
        from openpyxl import load_workbook
        wb=load_workbook(idx_path, data_only=True, read_only=True); ws=wb.active
        hdr=[ ("" if c.value is None else str(c.value).strip()) for c in ws[1] ]
        while hdr and hdr[-1]=="": hdr.pop()
        ok_hdr, _pos, _cells = match_header(hdr)
        if not ok_hdr: return False, ids, "目标索引表头不一致", []
        seq_col = _pos[0] + 1
        for r in ws.iter_rows(min_row=2, values_only=True):
            if not r: continue
            v=r[seq_col-1] if seq_col-1 < len(r) else None
            if v is None: continue
            ids.add(str(v).strip())
        return True, ids, "", _pos
    except Exception as e:
        return False, ids, f"无法读取index.xlsx:{e}", []


def backup_and_append(idx_path: Path, rows, header_positions):
    """
    写入策略（占空+尾追加）：
    - 优先把记录写入 “序号” 列为空的既有行（即便该行有边框/样式，也视为空槽）；
    - 空槽用尽后，再使用 ws.append() 追加到表尾。
    """
    from openpyxl import load_workbook
    bak = idx_path.with_name(idx_path.name + f".bak.{ts()}")
    tmp = idx_path.with_suffix(".tmp.xlsx")
    shutil.copy2(idx_path, bak)

    wb = load_workbook(idx_path)
    ws = wb.active

    seq_col = header_positions[0] + 1
    def a_col_empty(r):
        return (ws.cell(row=r, column=seq_col).value in (None, ""))
    from openpyxl.cell.cell import MergedCell
    def _row_has_merged(r):
        for _ci in header_positions:
            if isinstance(ws.cell(row=r, column=_ci + 1), MergedCell):
                return True
        return False


    r_ptr = 2
    maxr = ws.max_row  # 含已画表格的空行
    for rec in rows:
        # 先占用 A 列为空的空槽
        while r_ptr <= maxr and (not a_col_empty(r_ptr) or _row_has_merged(r_ptr)):
            r_ptr += 1
        if r_ptr <= maxr:
            for std_idx, val in enumerate(rec):
                col = header_positions[std_idx] + 1
                ws.cell(row=r_ptr, column=col, value=val)
            r_ptr += 1
        else:
            # 空槽用尽才追加
            for std_idx, val in enumerate(rec):
                col = header_positions[std_idx] + 1
                ws.cell(row=ws.max_row + 1, column=col, value=val)

    wb.save(tmp)
    os.replace(tmp, idx_path)
    return bak.name
def detect_header_row(values_rows, mode:str):
    rows=[normalize_header_cells(r) for r in values_rows[:2]]
    if mode=="2":
        hdr = rows[1] if len(rows)>1 else []
        ok, pos, _cells = match_header(hdr) if len(rows)>1 else (False, [], [])
        return (len(rows)>1 and ok, 1, "表头不匹配" if not (len(rows)>1 and ok) else "", hdr, pos)
    if mode=="1":
        hdr = rows[0] if rows else []
        ok, pos, _cells = match_header(hdr) if len(rows)>0 else (False, [], [])
        return (len(rows)>0 and ok, 0, "表头不匹配" if not (len(rows)>0 and ok) else "", hdr, pos)
    if len(rows)>1:
        ok, pos, _cells = match_header(rows[1])
        if ok:
            return True,1,"",rows[1], pos
    if len(rows)>0:
        ok, pos, _cells = match_header(rows[0])
        if ok:
            return True,0,"",rows[0], pos
    return False,-1,"前两行均非标准表头", rows[0] if rows else [], []

def load_pending_rows(xlsx_path: Path, header_mode: str):
    ext = xlsx_path.suffix.lower()
    errors = []
    rows_out = []
    N = len(STD_HEADER)
    try:
        if ext == ".xlsx":
            from openpyxl import load_workbook
            wb = load_workbook(xlsx_path, data_only=True, read_only=True)
            for ws in wb.worksheets:
                values_rows = [ (list(r) if r else []) for r in ws.iter_rows(min_row=1, values_only=True) ]
                ok, hdr_idx, why, _hdr, pos = detect_header_row(values_rows, header_mode)
                if not ok:
                    errors.append(f"{ws.title}: {why}")
                    continue
                for r in values_rows[hdr_idx+1:]:
                    r = list(r) if isinstance(r, (list, tuple)) else [r]
                    row = []
                    for ci in pos:
                        row.append(r[ci] if ci < len(r) else "")
                    row += [""] * (N - len(row))
                    rows_out.append(row[:N])
        elif ext == ".xls":
            try:
                import xlrd  # 1.2.0
            except Exception:
                return False, "处理 .xls 需要安装 xlrd==1.2.0", []
            book = xlrd.open_workbook(xlsx_path, formatting_info=False)
            for si in range(book.nsheets):
                sh = book.sheet_by_index(si)
                values_rows = [[sh.cell_value(i,j) for j in range(sh.ncols)] for i in range(sh.nrows)]
                ok, hdr_idx, why, _hdr, pos = detect_header_row(values_rows, header_mode)
                if not ok:
                    errors.append(f"{sh.name}: {why}")
                    continue
                for r in values_rows[hdr_idx+1:]:
                    r = list(r) if isinstance(r, (list, tuple)) else [r]
                    row = []
                    for ci in pos:
                        row.append(r[ci] if ci < len(r) else "")
                    row += [""] * (N - len(row))
                    rows_out.append(row[:N])
        else:
            return False, "不支持的扩展名（仅 .xlsx/.xls）", []
    except Exception as e:
        return False, f"无法读取Excel: {e}", []

    if errors:
        return False, "以下工作表不符合要求（全部中止处理）：" + "; ".join(errors), []
    return True, "", rows_out


def process_file(xlsx: Path, root: Path, header_mode: str, year_cache: Dict[int, Tuple[Path,set,list]]):
    res={"file": xlsx.name, "added":0, "skipped":0, "invalid":0, "details":[]}
    ok, why, rows = load_pending_rows(xlsx, header_mode)
    if not ok: return {"file": xlsx.name, "__file_failed__": True, "reason": why}
    buckets: Dict[int, List[List]] = {}
    for r in rows:
        seq = "" if r[0] is None else str(r[0]).strip()
        if seq=="":
            res["invalid"]+=1; res["details"].append({"seq":None,"status":"invalid","reason":"序号为空"}); continue
        ok_year,year,rr = parse_year_from_seq(seq)
        if not ok_year:
            res["invalid"]+=1; res["details"].append({"seq":seq,"status":"invalid","reason":rr}); continue
        year_dir = root/str(year)
        idx_path = None
        cache = year_cache.get(year)
        if cache is None:
            idx_path = find_index_file(year_dir)
            ok_idx, existing, why2, header_positions = load_year_ids_and_check_header(idx_path)
            if not ok_idx:
                res["invalid"] += 1; res["details"].append({"seq":seq,"year":year,"status":"invalid","reason":why2}); continue
            year_cache[year]=(idx_path, existing, header_positions)
        else:
            idx_path, existing, header_positions = cache
        if seq in year_cache[year][1]:
            res["skipped"]+=1; res["details"].append({"seq":seq,"year":year,"status":"skipped"}); continue
        buckets.setdefault(year, []).append(r)
    for year, rows2 in buckets.items():
        if not rows2: continue
        idx_path, existing, header_positions = year_cache[year]
        lock = idx_path.with_name(idx_path.name + ".lock")
        waited=0.0
        while lock.exists():
            import time as _t; _t.sleep(0.1); waited+=0.1
            if waited>5.0: return {"file": xlsx.name, "__file_failed__": True, "reason": f"年索引被锁:{idx_path.name}"}
        try:
            lock.touch()
            bak = backup_and_append(idx_path, rows2, header_positions)
            res["details"].append({"year":year,"status":"wrote","rows":len(rows2),"backup":bak})
            res["added"] += len(rows2)
            for rr in rows2: existing.add(str(rr[0]).strip())
            year_cache[year]=(idx_path, existing, header_positions)
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
        total={"files_total":len(files),"files_processed":0,"files_failed":0,"rows_added":0,"rows_skipped":0,"rows_invalid":0,"per_file":[]}
        year_cache: Dict[int, Tuple[Path,set,list]]={}
        for f in files:
            res=process_file(f, root, args.header_row, year_cache)
            if res.get("__file_failed__"):
                error.mkdir(parents=True, exist_ok=True); shutil.move(str(f), str(error/f.name))
                if res.get("reason"): (error/(f.name + ".reason.txt")).write_text(res["reason"], encoding="utf-8")
                total["files_failed"]+=1; total["per_file"].append(res); continue
            done.mkdir(parents=True, exist_ok=True); shutil.move(str(f), str(done/f.name))
            total["files_processed"]+=1
            total["rows_added"]+=res["added"]; total["rows_skipped"]+=res["skipped"]; total["rows_invalid"]+=res["invalid"]
            total["per_file"].append(res)
        if args.summary_out:
            Path(args.summary_out).write_text(json.dumps({"ok":True, **total}, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"ok":True, **total}, ensure_ascii=False, indent=2))
    finally:
        try: os.unlink(args.lock_file)
        except: pass

if __name__=="__main__": main()
