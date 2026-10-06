# -*- coding: utf-8 -*-
# GitHub Actions 云端引擎 · 123盘 WebDAV(源:main 136) 读指定库 PDF → fitz dpi=120 渲染 webp(quality=80)
#   → open-api 上传 guji(139) 古籍/GufangP/古方webp/<TOP_NAME>/<book> 共N册/<ce>/page_XXXX.webp
# 幂等: 1) 册级云端查重(ce 目录已存在且文件数=PDF页数则跳过)  2) 页级 upload duplicate=2(同名跳过)
# 用法: python gh_webdav_webp.py --lib-wd /古籍PDF/某库 --top-name 某库 --limit 0 --offset 0 --book-conc 2 --upl-conc 4
# 环境变量: WDAV_USER / WDAV_PASS / GUJI_CID / GUJI_SEC
import os, sys, re, io, json, time, queue, threading, datetime, urllib.parse, hashlib, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import requests
import fitz
from PIL import Image

WD = "https://webdav.123pan.cn/webdav"
WD_USER = os.environ.get("WDAV_USER", "")
WD_PASS = os.environ.get("WDAV_PASS", "")
B123 = "https://open-api.123pan.com"

ap = argparse.ArgumentParser()
ap.add_argument('--lib-wd', required=True, help='WebDAV 源库路径(不含/webdav前缀), 如 /古籍PDF/某库')
ap.add_argument('--top-name', required=True, help='目标顶层目录名(古方webp/<top-name>)')
ap.add_argument('--limit', type=int, default=0)
ap.add_argument('--offset', type=int, default=0)
ap.add_argument('--book-conc', type=int, default=2)
ap.add_argument('--upl-conc', type=int, default=4)
ap.add_argument('--ledger', default='gh_webdav_webp_ledger.jsonl')
ap.add_argument('--list-file', default='', help='增量清单文件路径(仓库内相对路径)；每行一个PDF绝对路径或书级目录路径，空=整库')
ap.add_argument('--cat-name', default='', help='目标顶层下的二级分类目录名(可选)')
ap.add_argument('--fanhao-map', default='fanhao_map.json', help='官方番号映射JSON(番号->{title,n_ce})，空=关闭')
args = ap.parse_args()
LIB_WD = args.lib_wd.rstrip('/')
TOP_NAME = args.top_name
LIST_FILE = args.list_file
CAT_NAME = args.cat_name
FANHAO_MAP = args.fanhao_map
LEDGER = args.ledger
TEMP = 'pdf_tmp'

os.makedirs(TEMP, exist_ok=True)
LG_LOCK = threading.Lock()
stat = {'ok': 0, 'fail': 0, 'skip': 0}
def log_done(rec):
    with LG_LOCK:
        with open(LEDGER, 'a', encoding='utf-8') as f:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')

# ---- Pan 客户端(guji 139) ----
class Pan:
    def __init__(self):
        self.cfg = {"client_id": os.environ["GUJI_CID"], "client_secret": os.environ["GUJI_SEC"]}
        self.s = requests.Session(); self.s.trust_env = False
        ad = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.s.mount("https://", ad)
        self.tok = None; self.lock = threading.Lock()
    def _login(self, _retry=0):
        try:
            r = self.s.post(B123 + "/api/v1/access_token", headers={"Platform": "open_platform"},
                            json={"clientID": self.cfg["client_id"], "clientSecret": self.cfg["client_secret"]}, timeout=90)
            r = r.json()
        except Exception:
            if _retry < 6: time.sleep(5*(_retry+1)); print(f"[login] 重试 {_retry+1}", flush=True); return self._login(_retry+1)
            raise
        self.tok = r.get("data", {}).get("accessToken")
        if not self.tok: raise RuntimeError("123 token 失败: " + str(r)[:200])
    def call(self, method, path, body=None, params=None, _retry=0):
        with self.lock:
            if not self.tok: self._login()
            tok = self.tok
        h = {"Platform": "open_platform", "Authorization": "Bearer " + tok}
        if body is not None: h["Content-Type"] = "application/json"
        try:
            r = self.s.request(method, B123 + path, headers=h, params=params,
                               data=json.dumps(body) if body is not None else None, timeout=90)
            j = r.json()
        except Exception:
            if _retry < 6: time.sleep(5*(_retry+1)); return self.call(method, path, body, params, _retry+1)
            raise
        code = j.get("code")
        if code == 0: return j
        msg = str(j.get("message", ""))
        if code == 401 or "token" in msg.lower():
            with self.lock: self.tok = None
            if _retry < 4: return self.call(method, path, body, params, _retry+1)
        if "频繁" in msg or "exceed" in msg.lower() or "limit" in msg.lower() or code in (429, 5066):
            if _retry < 12: time.sleep(min(90, 5*(_retry+1))); return self.call(method, path, body, params, _retry+1)
        raise RuntimeError(f"123 {path} code={code} {msg[:120]}")
    def list_dir(self, fid):
        out, last = [], 0
        while True:
            j = self.call("GET", "/api/v2/file/list", params={"parentFileId": fid, "limit": 100, "lastFileId": last})
            d = j.get("data", {}); out += d.get("fileList", [])
            last = d.get("lastFileId", -1)
            if last == -1: break
        return [x for x in out if x.get("trashed", 0) == 0]
    def mkdir(self, parent, name):
        j = self.call("POST", "/upload/v1/file/mkdir", body={"parentID": str(parent), "name": name})
        return int(j["data"]["dirID"])
    def upload_domain(self):
        with self.lock:
            if getattr(self, "_updom", None): return self._updom
        j = self.call("GET", "/upload/v2/file/domain")
        dom = j["data"][0]
        with self.lock: self._updom = dom
        return dom
    def upload(self, parent, name, data, _retry=0):
        etag = hashlib.md5(data).hexdigest()
        try:
            dom = self.upload_domain()
            with self.lock:
                if not self.tok: self._login()
                tok = self.tok
            r = self.s.post(dom + "/upload/v2/file/single/create",
                headers={"Platform": "open_platform", "Authorization": "Bearer " + tok},
                files={"file": (name, data, "application/octet-stream")},
                data={"parentFileID": str(parent), "filename": name, "etag": etag,
                      "size": str(len(data)), "duplicate": "2"}, timeout=180)
            j = r.json()
            if j.get("code") == 0 and (j["data"].get("completed") or j["data"].get("fileID")): return "ok"
            msg = str(j.get("message", ""))
            if "频繁" in msg or "exceed" in msg.lower() or j.get("code") in (429, 401):
                if _retry < 8: time.sleep(min(60, 5*(_retry+1))); return self.upload(parent, name, data, _retry+1)
            if _retry < 3: time.sleep(3*(_retry+1)); return self.upload(parent, name, data, _retry+1)
            raise RuntimeError(f"upload {name} {msg[:120]}")
        except Exception as e:
            if _retry < 3: time.sleep(3*(_retry+1)); return self.upload(parent, name, data, _retry+1)
            raise

# ---- WebDAV 递归列目录 ----
_s = requests.Session(); _s.trust_env = False
_s.auth = (WD_USER, WD_PASS)
def wd_list(path):
    r = _s.request("PROPFIND", WD + urllib.parse.quote(path, safe="/"),
                   headers={"Depth": "1"}, timeout=60)
    if r.status_code != 207:
        raise RuntimeError(f"PROPFIND {path} -> {r.status_code} {r.text[:100]}")
    hrefs = re.findall(r"<D:href>(.*?)</D:href>", r.text)
    if not hrefs: hrefs = re.findall(r"<href>(.*?)</href>", r.text)
    return [urllib.parse.unquote(h) for h in hrefs]

def _strip_wdav(h):
    if h.startswith("/webdav"):
        return h[len("/webdav"):]
    if "/webdav" in h:
        return "/" + h.split("/webdav", 1)[-1].lstrip("/")
    return h

def wd_tree(path, out=None, _depth=0):
    out = out or []
    if _depth > 8: return out
    for h in wd_list(path):
        rel = _strip_wdav(h)
        if rel.rstrip("/") == path.rstrip("/"):
            continue
        out.append(rel)
        if rel.endswith("/"):
            wd_tree(rel.rstrip("/"), out, _depth+1)
    return out

def wd_download(wd_path, local):
    r = _s.get(WD + urllib.parse.quote(wd_path, safe="/"), timeout=180, stream=True)
    if r.status_code != 200:
        raise RuntimeError(f"GET {wd_path} -> {r.status_code}")
    with open(local, "wb") as f:
        for chunk in r.iter_content(1024 * 256):
            f.write(chunk)

# ---- 番号映射与命名规范化 ----
def _load_fanhao_map():
    if not os.path.exists(FANHAO_MAP):
        return {}
    with open(FANHAO_MAP, encoding='utf-8') as f:
        return json.load(f)

_FHM = _load_fanhao_map()

def _norm_fh(raw):
    s = str(raw or '').strip()
    s = s.translate(str.maketrans('０１２３４５６７８９－', '0123456789-'))
    m = re.search(r'([子別史經集])?\s*(\d+\s*-\s*\d+)', s)
    if not m:
        return None
    pref = m.group(1) or '子'
    num = m.group(2).replace(' ', '')
    return f"{pref}{num}"

def _parse_pdf(fn):
    # fn: 去扩展名的文件名或目录名；返回(番号, 书名, 册数)
    fh = None
    mfh = re.search(r'\[番号\]\s*([^\s\.\]]+)', fn)
    if mfh:
        fh = _norm_fh(mfh.group(1))
    mtitle = re.match(r'^([^\.\[]+)', fn)
    title_file = mtitle.group(1).strip() if mtitle else fn.strip()
    mce = re.search(r'\.(\d+)\s*[冊册]', fn)
    n_file = int(mce.group(1)) if mce else 1
    info = _FHM.get(fh) if fh else None
    title = (info or {}).get('title') or title_file
    n_ce = (info or {}).get('n_ce') or n_file
    return fh, title, n_ce

# ---- 收集清单 ----
def build_todo():
    items = wd_tree(LIB_WD)
    todo = []
    for it in items:
        if not it.lower().endswith(".pdf"):
            continue
        rel = it[len(LIB_WD):].lstrip("/")
        if "/" in rel:
            book_dir, ce_fn = rel.rsplit("/", 1)
            fh, title, n_ce = _parse_pdf(book_dir)
            book_name = f"{fh} {title} 共{n_ce}册" if fh else f"{title} 共{n_ce}册"
            ce = ce_fn[:-4]
            todo.append({"wdav": it, "book": book_name, "ce": ce, "pdf_name": ce_fn})
        else:
            fn = rel[:-4]
            fh, title, n_ce = _parse_pdf(fn)
            book_name = f"{fh} {title} 共{n_ce}册" if fh else f"{title} 共{n_ce}册"
            ce = title
            todo.append({"wdav": it, "book": book_name, "ce": ce, "pdf_name": rel})
    def ce_no(x):
        m = re.search(r"(\d+)", x["ce"])
        return int(m.group(1)) if m else 10 ** 9
    todo.sort(key=lambda x: (x["book"], ce_no(x)))
    return todo

todo = build_todo()
print(f"[scan] {LIB_WD} -> PDF 共 {len(todo)} 册, book 数={len(set(t['book'] for t in todo))}", flush=True)

done = set()
if os.path.exists(LEDGER):
    for line in open(LEDGER, encoding='utf-8'):
        try:
            j = json.loads(line)
            if j.get('status') == 'ok': done.add(j.get('wdav') or j.get('ce'))
        except: pass
todo = [t for t in todo if (t['wdav'] not in done and t['ce'] not in done)]
if LIST_FILE:
    _prefixes, _exacts = set(), set()
    with open(LIST_FILE, encoding='utf-8') as _f:
        for _line in _f:
            _line = _line.rstrip('\r\n').strip()
            if not _line:
                continue
            if _line.lower().endswith('.pdf'):
                _exacts.add(_line.rstrip('/'))
            else:
                _prefixes.add(_line.rstrip('/'))
    _kept = []
    for _t in todo:
        _w = _t['wdav'].rstrip('/')
        if _w in _exacts or any(_w == p or _w.startswith(p + '/') for p in _prefixes):
            _kept.append(_t)
    todo = _kept
    print(f"[todo] 清单过滤后 {len(todo)} 册", flush=True)
if args.offset > 0: todo = todo[args.offset:]
if args.limit > 0: todo = todo[:args.limit]
print(f"[todo] 实跑 {len(todo)} 册 (已跳过 ledger ok {len(done)})", flush=True)
for t in todo[:10]:
    print("  ", t["wdav"], flush=True)

pan = Pan()
def get_or_mkdir(parent, name):
    for it in pan.list_dir(parent):
        if it['type'] == 1 and it['filename'] == name:
            return int(it['fileId'])
    return pan.mkdir(parent, name)

def build_target():
    d1 = get_or_mkdir(0, '古籍')
    d2 = get_or_mkdir(d1, 'GufangP')
    d3 = get_or_mkdir(d2, '古方webp')
    top = get_or_mkdir(d3, TOP_NAME)
    if CAT_NAME:
        top = get_or_mkdir(top, CAT_NAME)
    return top

TOP_FID = build_target()
print(f"[target] 古方webp/{TOP_NAME} fid={TOP_FID}", flush=True)

def cloud_ce_files(ce_fid):
    return [x['filename'] for x in pan.list_dir(ce_fid) if x['type'] == 0]

def process_one(t):
    ce_key = t['ce']
    local_pdf = os.path.join(TEMP, "pdf_" + hashlib.md5(ce_key.encode()).hexdigest() + ".pdf")
    for attempt in range(1, 4):
        try:
            t0 = time.time()
            wd_download(t['wdav'], local_pdf)
            doc = fitz.open(local_pdf)
            N = doc.page_count
            book_name = t['book']
            book_fid = get_or_mkdir(TOP_FID, book_name)
            ce_fid = get_or_mkdir(book_fid, ce_key)
            # 云端查重: ce 目录已有 N 个 page_*.webp 则跳过
            ex = set(cloud_ce_files(ce_fid))
            if len(ex) >= N and all(f.startswith('page_') and f.endswith('.webp') for f in ex):
                doc.close()
                rec = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': ce_key,
                       'book': t['book'], 'pdf_name': t['pdf_name'], 'pages_pdf': N, 'status': 'ok_cloud_skip', 'wdav': t['wdav']}
                log_done(rec)
                with LG_LOCK: stat['skip'] += 1
                print(f"  [SKIP-cloud] {ce_key} 已在云端(N={N})", flush=True)
                return True
            # 并发上传页(duplicate=2 幂等)
            q = queue.Queue(maxsize=max(8, args.upl_conc * 3))
            ok_cnt = [0]
            ulock = threading.Lock()
            def uploader():
                while True:
                    item = q.get()
                    if item is None: q.task_done(); break
                    name, data = item
                    try:
                        pan.upload(ce_fid, name, data)
                        with ulock: ok_cnt[0] += 1
                    except Exception as e:
                        print(f"    [upl-err] {ce_key}/{name} {str(e)[:80]}", flush=True)
                    q.task_done()
            threads = [threading.Thread(target=uploader, daemon=True) for _ in range(args.upl_conc)]
            for th in threads: th.start()
            for i in range(N):
                page = doc[i]
                pix = page.get_pixmap(dpi=120)
                img = Image.frombytes('RGB', (pix.width, pix.height), pix.samples)
                if pix.width > 16383 or pix.height > 16383:
                    r = 16383 / max(pix.width, pix.height)
                    img = img.resize((int(pix.width * r), int(pix.height * r)), Image.LANCZOS)
                buf = io.BytesIO(); img.save(buf, 'webp', quality=80, method=0)
                q.put((f'page_{i+1:04d}.webp', buf.getvalue()))
                del img, pix, buf
            doc.close()
            for _ in threads: q.put(None)
            for th in threads: th.join()
            elapsed = time.time() - t0
            if ok_cnt[0] == N:
                rec = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji',
                       'ce': ce_key, 'book': t['book'], 'pdf_name': t['pdf_name'],
                       'pages_pdf': N, 'pages_uploaded': ok_cnt[0], 'status': 'ok',
                       'elapsed_s': round(elapsed, 1), 'wdav': t['wdav']}
                log_done(rec)
                with LG_LOCK: stat['ok'] += 1
                print(f"  [OK] {ce_key} · {N}p · {elapsed:.0f}s (累计OK={stat['ok']})", flush=True)
                return True
            print(f"  [PARTIAL] {ce_key} {ok_cnt[0]}/{N}", flush=True)
            raise RuntimeError(f"upload partial {ok_cnt[0]}/{N}")
        except Exception as e:
            print(f"  [FAIL-{attempt}] {ce_key} {str(e)[:150]}", flush=True)
            time.sleep(3 * attempt)
        finally:
            if os.path.exists(local_pdf):
                try: os.remove(local_pdf)
                except: pass
    rec = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji',
           'ce': ce_key, 'book': t['book'], 'pdf_name': t['pdf_name'], 'status': 'fail',
           'wdav': t['wdav']}
    log_done(rec)
    with LG_LOCK: stat['fail'] += 1
    print(f"  [FAIL-FINAL] {ce_key}", flush=True)
    return False

print(f"[start] book并发={args.book_conc} 页并发={args.upl_conc}", flush=True)
with ThreadPoolExecutor(max_workers=args.book_conc) as ex:
    futs = {ex.submit(process_one, t): t for t in todo}
    for fu in as_completed(futs):
        try: fu.result()
        except Exception as e: print("[exec-err]", str(e)[:100], flush=True)
print(f"[done] OK={stat['ok']} FAIL={stat['fail']} SKIP={stat['skip']}", flush=True)
