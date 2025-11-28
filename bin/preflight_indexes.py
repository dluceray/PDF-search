#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, sys
from pathlib import Path
from openpyxl import load_workbook

ROOT = Path("/data/contracts")
STD_HEADER = ["序号","工程地点及内容","单位名称","签订途径","启动时间","结果确定时间","签订日期","控制价","合同额","结算值","已付款","欠付款","备注"]

def header_equal(a,b): return len(a)==len(b) and all((a[i]==b[i]) for i in range(len(a)))

def trim(row):
    vals=[ ("" if v is None else str(v).strip()) for v in row ]
    while vals and vals[-1]=="": vals.pop()
    return vals

report=[]
for y in sorted([p for p in ROOT.iterdir() if p.is_dir() and p.name.isdigit()]):
    idx=None
    for f in y.iterdir():
        if f.is_file() and f.name.lower()=="index.xlsx":
            idx=f; break
    if not idx:
        report.append({"year": y.name, "ok": False, "reason": "缺少index.xlsx"}); continue
    try:
        wb=load_workbook(idx, data_only=True, read_only=True); ws=wb.active
        hdr=trim([c.value for c in ws[1]])
        ok=header_equal(hdr, STD_HEADER)
        report.append({"year": y.name, "ok": ok, "file": str(idx), "header": hdr if not ok else "OK"})
    except Exception as e:
        report.append({"year": y.name, "ok": False, "file": str(idx), "reason": f"读取失败:{e}"})

print(json.dumps({"ok": True, "report": report}, ensure_ascii=False, indent=2))
