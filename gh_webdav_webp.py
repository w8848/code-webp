# -*- coding: utf-8 -*-
import os, sys, re, io, json, time, queue, threading, datetime, urllib.parse, hashlib, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import requests
import fitz
from PIL import Image
WD = 'https://webdav.123pan.cn/webdav'
WD_USER = os.environ.get('WDAV_USER', '')
WD_PASS = os.environ.get('WDAV_PASS', '')
B123 = 'https://open-api.123pan.com'
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

def _jboheicw(rec):
    with LG_LOCK:
        with open(LEDGER, 'a', encoding='utf-8') as f:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')

class _gjzvwgnd:

    def __init__(self):
        self.cfg = {'client_id': os.environ['GUJI_CID'], 'client_secret': os.environ['GUJI_SEC']}
        self.s = requests.Session()
        self.s.trust_env = False
        ad = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.s.mount('https://', ad)
        self.tok = None
        self.lock = threading.Lock()

    def _login(self, _retry=0):
        try:
            r = self.s.post(B123 + '/api/v1/access_token', headers={'Platform': 'open_platform'}, json={'clientID': self.cfg['client_id'], 'clientSecret': self.cfg['client_secret']}, timeout=90)
            r = r.json()
        except Exception:
            if _retry < 6:
                time.sleep(5 * (_retry + 1))
                print(f'[login] 重试 {_retry + 1}', flush=True)
                return self._login(_retry + 1)
            raise
        self.tok = r.get('data', {}).get('accessToken')
        if not self.tok:
            raise RuntimeError('123 token 失败: ' + str(r)[:200])

    def call(self, method, path, body=None, params=None, _retry=0):
        with self.lock:
            if not self.tok:
                self._login()
            tok = self.tok
        h = {'Platform': 'open_platform', 'Authorization': 'Bearer ' + tok}
        if body is not None:
            h['Content-Type'] = 'application/json'
        try:
            r = self.s.request(method, B123 + path, headers=h, params=params, data=json.dumps(body) if body is not None else None, timeout=90)
            j = r.json()
        except Exception:
            if _retry < 6:
                time.sleep(5 * (_retry + 1))
                return self.call(method, path, body, params, _retry + 1)
            raise
        code = j.get('code')
        if code == 0:
            return j
        msg = str(j.get('message', ''))
        if code == 401 or 'token' in msg.lower():
            with self.lock:
                self.tok = None
            if _retry < 4:
                return self.call(method, path, body, params, _retry + 1)
        if '频繁' in msg or 'exceed' in msg.lower() or 'limit' in msg.lower() or (code in (429, 5066)):
            if _retry < 12:
                time.sleep(min(90, 5 * (_retry + 1)))
                return self.call(method, path, body, params, _retry + 1)
        raise RuntimeError(f'123 {path} code={code} {msg[:120]}')

    def list_dir(self, fid):
        out, last = ([], 0)
        while True:
            j = self.call('GET', '/api/v2/file/list', params={'parentFileId': fid, 'limit': 100, 'lastFileId': last})
            d = j.get('data', {})
            out += d.get('fileList', [])
            last = d.get('lastFileId', -1)
            if last == -1:
                break
        return [x for x in out if x.get('trashed', 0) == 0]

    def mkdir(self, parent, name):
        j = self.call('POST', '/upload/v1/file/mkdir', body={'parentID': str(parent), 'name': name})
        return int(j['data']['dirID'])

    def upload_domain(self):
        with self.lock:
            if getattr(self, '_updom', None):
                return self._updom
        j = self.call('GET', '/upload/v2/file/domain')
        dom = j['data'][0]
        with self.lock:
            self._updom = dom
        return dom

    def upload(self, parent, name, data, _retry=0):
        etag = hashlib.md5(data).hexdigest()
        try:
            dom = self.upload_domain()
            with self.lock:
                if not self.tok:
                    self._login()
                tok = self.tok
            r = self.s.post(dom + '/upload/v2/file/single/create', headers={'Platform': 'open_platform', 'Authorization': 'Bearer ' + tok}, files={'file': (name, data, 'application/octet-stream')}, data={'parentFileID': str(parent), 'filename': name, 'etag': etag, 'size': str(len(data)), 'duplicate': '2'}, timeout=180)
            j = r.json()
            if j.get('code') == 0 and (j['data'].get('completed') or j['data'].get('fileID')):
                return 'ok'
            msg = str(j.get('message', ''))
            if '频繁' in msg or 'exceed' in msg.lower() or j.get('code') in (429, 401):
                if _retry < 8:
                    time.sleep(min(60, 5 * (_retry + 1)))
                    return self.upload(parent, name, data, _retry + 1)
            if _retry < 3:
                time.sleep(3 * (_retry + 1))
                return self.upload(parent, name, data, _retry + 1)
            raise RuntimeError(f'upload {name} {msg[:120]}')
        except Exception as e:
            if _retry < 3:
                time.sleep(3 * (_retry + 1))
                return self.upload(parent, name, data, _retry + 1)
            raise
_s = requests.Session()
_s.trust_env = False
_s.auth = (WD_USER, WD_PASS)

def _sxoamlhh(path):
    r = _s.request('PROPFIND', WD + urllib.parse.quote(path, safe='/'), headers={'Depth': '1'}, timeout=60)
    if r.status_code != 207:
        raise RuntimeError(f'PROPFIND {path} -> {r.status_code} {r.text[:100]}')
    hrefs = re.findall('<D:href>(.*?)</D:href>', r.text)
    if not hrefs:
        hrefs = re.findall('<href>(.*?)</href>', r.text)
    return [urllib.parse.unquote(h) for h in hrefs]

def _jobgcrjv(h):
    if h.startswith('/webdav'):
        return h[len('/webdav'):]
    if '/webdav' in h:
        return '/' + h.split('/webdav', 1)[-1].lstrip('/')
    return h

def _mpoobazl(path, out=None, _depth=0):
    out = out or []
    if _depth > 8:
        return out
    for h in _sxoamlhh(path):
        rel = _jobgcrjv(h)
        if rel.rstrip('/') == path.rstrip('/'):
            continue
        out.append(rel)
        if rel.endswith('/'):
            _mpoobazl(rel.rstrip('/'), out, _depth + 1)
    return out

def _cypckzsq(wd_path, local):
    r = _s.get(WD + urllib.parse.quote(wd_path, safe='/'), timeout=180, stream=True)
    if r.status_code != 200:
        raise RuntimeError(f'GET {wd_path} -> {r.status_code}')
    with open(local, 'wb') as f:
        for chunk in r.iter_content(1024 * 256):
            f.write(chunk)

def _tckelbcx():
    if not os.path.exists(FANHAO_MAP):
        return {}
    with open(FANHAO_MAP, encoding='utf-8-sig') as f:
        return json.load(f)
_FHM = _tckelbcx()

def _dvzjgizw(raw):
    s = str(raw or '').strip()
    s = s.translate(str.maketrans('０１２３４５６７８９－', '0123456789-'))
    m = re.search('([子別史經集])?\\s*(\\d+\\s*-\\s*\\d+)', s)
    if not m:
        return None
    pref = m.group(1) or '子'
    num = m.group(2).replace(' ', '')
    return f'{pref}{num}'

def _fcizagyd(fn):
    fh = None
    mfh = re.search('\\[番号\\]\\s*([^\\s\\.\\]]+)', fn)
    if mfh:
        fh = _dvzjgizw(mfh.group(1))
    mtitle = re.match('^([^\\.\\[]+)', fn)
    title_file = mtitle.group(1).strip() if mtitle else fn.strip()
    mce = re.search('\\.(\\d+)\\s*[冊册]', fn)
    n_file = int(mce.group(1)) if mce else 1
    info = _FHM.get(fh) if fh else None
    title = (info or {}).get('title') or title_file
    n_ce = (info or {}).get('n_ce') or n_file
    return (fh, title, n_ce)

def _aqnzacqy():
    items = _mpoobazl(LIB_WD)
    todo = []
    for it in items:
        if not it.lower().endswith('.pdf'):
            continue
        rel = it[len(LIB_WD):].lstrip('/')
        if '/' in rel:
            book_dir, ce_fn = rel.rsplit('/', 1)
            fh, title, n_ce = _fcizagyd(book_dir)
            book_name = f'{fh} {title} 共{n_ce}册' if fh else f'{title} 共{n_ce}册'
            ce = ce_fn[:-4]
            todo.append({'wdav': it, 'book': book_name, 'ce': ce, 'pdf_name': ce_fn})
        else:
            fn = rel[:-4]
            fh, title, n_ce = _fcizagyd(fn)
            book_name = f'{fh} {title} 共{n_ce}册' if fh else f'{title} 共{n_ce}册'
            ce = title
            todo.append({'wdav': it, 'book': book_name, 'ce': ce, 'pdf_name': rel})

    def _wcnzdbne(x):
        m = re.search('(\\d+)', x['ce'])
        return int(m.group(1)) if m else 10 ** 9
    todo.sort(key=lambda x: (x['book'], _wcnzdbne(x)))
    return todo
todo = _aqnzacqy()
print(f"[scan] {LIB_WD} -> PDF 共 {len(todo)} 册, book 数={len(set((t['book'] for t in todo)))}", flush=True)
done = set()
if os.path.exists(LEDGER):
    for line in open(LEDGER, encoding='utf-8'):
        try:
            j = json.loads(line)
            if j.get('status') == 'ok':
                done.add(j.get('wdav') or j.get('ce'))
        except:
            pass
todo = [t for t in todo if t['wdav'] not in done and t['ce'] not in done]
if LIST_FILE:
    _prefixes, _exacts = (set(), set())
    with open(LIST_FILE, encoding='utf-8-sig') as _f:
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
        if _w in _exacts or any((_w == p or _w.startswith(p + '/') for p in _prefixes)):
            _kept.append(_t)
    todo = _kept
    print(f'[todo] 清单过滤后 {len(todo)} 册', flush=True)
if args.offset > 0:
    todo = todo[args.offset:]
if args.limit > 0:
    todo = todo[:args.limit]
print(f'[todo] 实跑 {len(todo)} 册 (已跳过 ledger ok {len(done)})', flush=True)
for t in todo[:10]:
    print('  ', t['wdav'], flush=True)
pan = _gjzvwgnd()

def _xfwknvyc(parent, name):
    for it in pan.list_dir(parent):
        if it['type'] == 1 and it['filename'] == name:
            return int(it['fileId'])
    return pan.mkdir(parent, name)

def _jjldsylb():
    d1 = _xfwknvyc(0, '古籍')
    d2 = _xfwknvyc(d1, 'GufangP')
    d3 = _xfwknvyc(d2, '古方webp')
    top = _xfwknvyc(d3, TOP_NAME)
    if CAT_NAME:
        top = _xfwknvyc(top, CAT_NAME)
    return top
TOP_FID = _jjldsylb()
print(f'[target] 古方webp/{TOP_NAME} fid={TOP_FID}', flush=True)

def _hxyyugoj(ce_fid):
    return [x['filename'] for x in pan.list_dir(ce_fid) if x['type'] == 0]

def _ixeahvrd(t):
    ce_key = t['ce']
    local_pdf = os.path.join(TEMP, 'pdf_' + hashlib.md5(ce_key.encode()).hexdigest() + '.pdf')
    for attempt in range(1, 4):
        try:
            t0 = time.time()
            _cypckzsq(t['wdav'], local_pdf)
            doc = fitz.open(local_pdf)
            N = doc.page_count
            book_name = t['book']
            book_fid = _xfwknvyc(TOP_FID, book_name)
            ce_fid = _xfwknvyc(book_fid, ce_key)
            ex = set(_hxyyugoj(ce_fid))
            if len(ex) >= N and all((f.startswith('page_') and f.endswith('.webp') for f in ex)):
                doc.close()
                rec = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': ce_key, 'book': t['book'], 'pdf_name': t['pdf_name'], 'pages_pdf': N, 'status': 'ok_cloud_skip', 'wdav': t['wdav']}
                _jboheicw(rec)
                with LG_LOCK:
                    stat['skip'] += 1
                print(f'  [SKIP-cloud] {ce_key} 已在云端(N={N})', flush=True)
                return True
            q = queue.Queue(maxsize=max(8, args.upl_conc * 3))
            ok_cnt = [0]
            ulock = threading.Lock()

            def _iaoixqht():
                while True:
                    item = q.get()
                    if item is None:
                        q.task_done()
                        break
                    name, data = item
                    try:
                        pan.upload(ce_fid, name, data)
                        with ulock:
                            ok_cnt[0] += 1
                    except Exception as e:
                        print(f'    [upl-err] {ce_key}/{name} {str(e)[:80]}', flush=True)
                    q.task_done()
            threads = [threading.Thread(target=_iaoixqht, daemon=True) for _ in range(args.upl_conc)]
            for th in threads:
                th.start()
            for i in range(N):
                page = doc[i]
                pix = page.get_pixmap(dpi=120)
                img = Image.frombytes('RGB', (pix.width, pix.height), pix.samples)
                if pix.width > 16383 or pix.height > 16383:
                    r = 16383 / max(pix.width, pix.height)
                    img = img.resize((int(pix.width * r), int(pix.height * r)), Image.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, 'webp', quality=80, method=0)
                q.put((f'page_{i + 1:04d}.webp', buf.getvalue()))
                del img, pix, buf
            doc.close()
            for _ in threads:
                q.put(None)
            for th in threads:
                th.join()
            elapsed = time.time() - t0
            if ok_cnt[0] == N:
                rec = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': ce_key, 'book': t['book'], 'pdf_name': t['pdf_name'], 'pages_pdf': N, 'pages_uploaded': ok_cnt[0], 'status': 'ok', 'elapsed_s': round(elapsed, 1), 'wdav': t['wdav']}
                _jboheicw(rec)
                with LG_LOCK:
                    stat['ok'] += 1
                print(f"  [OK] {ce_key} · {N}p · {elapsed:.0f}s (累计OK={stat['ok']})", flush=True)
                return True
            print(f'  [PARTIAL] {ce_key} {ok_cnt[0]}/{N}', flush=True)
            raise RuntimeError(f'upload partial {ok_cnt[0]}/{N}')
        except Exception as e:
            print(f'  [FAIL-{attempt}] {ce_key} {str(e)[:150]}', flush=True)
            time.sleep(3 * attempt)
        finally:
            if os.path.exists(local_pdf):
                try:
                    os.remove(local_pdf)
                except:
                    pass
    rec = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': ce_key, 'book': t['book'], 'pdf_name': t['pdf_name'], 'status': 'fail', 'wdav': t['wdav']}
    _jboheicw(rec)
    with LG_LOCK:
        stat['fail'] += 1
    print(f'  [FAIL-FINAL] {ce_key}', flush=True)
    return False
print(f'[start] book并发={args.book_conc} 页并发={args.upl_conc}', flush=True)
with ThreadPoolExecutor(max_workers=args.book_conc) as ex:
    futs = {ex.submit(_ixeahvrd, t): t for t in todo}
    for fu in as_completed(futs):
        try:
            fu.result()
        except Exception as e:
            print('[exec-err]', str(e)[:100], flush=True)
print(f"[done] OK={stat['ok']} FAIL={stat['fail']} SKIP={stat['skip']}", flush=True)
