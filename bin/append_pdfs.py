#!/root/pdfsearch/venv/bin/python3
import os, sys, time, shutil, glob, logging
from pathlib import Path
from contextlib import contextmanager

ROOT_DIR    = "/data/contracts"     # 你的PDF库根
PENDING_DIR = "/data/add_pending"   # 待处理 to*.pdf
DONE_DIR    = "/data/add_done"      # 成功归档
ERROR_DIR   = "/data/add_error"     # 失败归档
LOG_FILE    = "/var/log/pdf-append.log"
LOCK_FILE   = "/var/run/pdf-append.lock"

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

@contextmanager
def single_lock(path):
    import fcntl
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.write(str(os.getpid())); f.flush()
            yield
        except BlockingIOError:
            print("another instance running; exit")
            sys.exit(0)

def merge_pdf(main_pdf, tail_pdf, out_pdf):
    from PyPDF2 import PdfReader, PdfWriter
    w = PdfWriter()
    r1 = PdfReader(main_pdf, strict=False)
    for p in r1.pages: w.add_page(p)
    r2 = PdfReader(tail_pdf, strict=False)
    for p in r2.pages: w.add_page(p)
    with open(out_pdf, "wb") as f: w.write(f)
    return len(r1.pages), len(r2.pages), len(r1.pages)+len(r2.pages)

def safe_replace(tmp_new, dst_old):
    ts  = time.strftime("%Y%m%d%H%M%S")
    bak = f"{dst_old}.bak.{ts}"
    shutil.copy2(dst_old, bak)           # 旁路备份
    os.replace(tmp_new, dst_old)         # 原子替换
    return bak

def process_one(p: Path):
    name = p.name
    if not name.lower().endswith(".pdf") or not name.lower().startswith("to"):
        return "skip"
    target_name = name[2:]  # 去掉 to 前缀
    # 仅允许纯“文件名.pdf”（防止路径注入）
    if "/" in target_name or "\\" in target_name:
        shutil.move(str(p), str(Path(ERROR_DIR)/name))
        logging.error(f"illegal name: {name}")
        return "illegal"

    matches = glob.glob(f"{ROOT_DIR}/**/{target_name}", recursive=True)
    if len(matches)==0:
        shutil.move(str(p), str(Path(ERROR_DIR)/name))
        logging.error(f"not found target for {name}")
        return "not_found"
    if len(matches)>1:
        shutil.move(str(p), str(Path(ERROR_DIR)/name))
        logging.error(f"ambiguous target for {name}: {matches}")
        return "ambiguous"

    target = Path(matches[0])
    tmp_out = target.with_suffix(".merged.tmp.pdf")
    try:
        n_main, n_tail, n_total = merge_pdf(str(target), str(p), str(tmp_out))
        bak = safe_replace(str(tmp_out), str(target))
        ts = time.strftime("%Y%m%d%H%M%S")
        done_name = f"{p.stem}.{ts}.pdf"
        shutil.move(str(p), str(Path(DONE_DIR)/done_name))
        logging.info(f"OK {target} + {p} => {n_main}+{n_tail}={n_total}, bak={bak}")
        print(f"{name}: ok ({n_main}+{n_tail}={n_total})")
        return "ok"
    except Exception as e:
        logging.exception(f"merge fail {p} -> {target}: {e}")
        try:
            if tmp_out.exists(): tmp_out.unlink()
        except: pass
        shutil.move(str(p), str(Path(ERROR_DIR)/name))
        return "error"

def main():
    Path(PENDING_DIR).mkdir(parents=True, exist_ok=True)
    Path(DONE_DIR).mkdir(parents=True, exist_ok=True)
    Path(ERROR_DIR).mkdir(parents=True, exist_ok=True)
    with single_lock(LOCK_FILE):
        files = sorted(Path(PENDING_DIR).glob("[tT][oO]*.pdf"))
        if not files:
            print("nothing to do")
            return
        for f in files:
            try:
                process_one(f)
            except Exception as e:
                logging.exception(f"unexpected on {f}: {e}")
                try: shutil.move(str(f), str(Path(ERROR_DIR)/f.name))
                except: pass

if __name__ == "__main__":
    main()
