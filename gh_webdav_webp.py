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
        with open(LEDGER, 'a', encoding='utf-8') as _gjzvwgnd:
            _gjzvwgnd.write(json.dumps(rec, ensure_ascii=False) + '\n')

class _sxoamlhh:

    def __init__(self):
        self.cfg = {'client_id': os.environ['GUJI_CID'], 'client_secret': os.environ['GUJI_SEC']}
        self.s = requests.Session()
        self.s.trust_env = False
        _jobgcrjv = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.s.mount('https://', _jobgcrjv)
        self.tok = None
        self.lock = threading.Lock()

    def _mpoobazl(self, _retry=0):
        try:
            _cypckzsq = self.s.post(B123 + '/api/v1/access_token', headers={'Platform': 'open_platform'}, json={'clientID': self.cfg['client_id'], 'clientSecret': self.cfg['client_secret']}, timeout=90)
            _cypckzsq = _cypckzsq.json()
        except Exception:
            if _retry < 6:
                time.sleep(5 * (_retry + 1))
                print(f'[login] 重试 {_retry + 1}', flush=True)
                return self._login(_retry + 1)
            raise
        self.tok = _cypckzsq.get('data', {}).get('accessToken')
        if not self.tok:
            raise RuntimeError('123 token 失败: ' + str(_cypckzsq)[:200])

    def _tckelbcx(self, method, path, body=None, params=None, _retry=0):
        with self.lock:
            if not self.tok:
                self._login()
            _xfwknvyc = self.tok
        _fcizagyd = {'Platform': 'open_platform', 'Authorization': 'Bearer ' + _xfwknvyc}
        if body is not None:
            _fcizagyd['Content-Type'] = 'application/json'
        try:
            _cypckzsq = self.s.request(method, B123 + path, headers=_fcizagyd, params=params, data=json.dumps(body) if body is not None else None, timeout=90)
            _dvzjgizw = _cypckzsq.json()
        except Exception:
            if _retry < 6:
                time.sleep(5 * (_retry + 1))
                return self.call(method, path, body, params, _retry + 1)
            raise
        _aqnzacqy = _dvzjgizw.get('code')
        if _aqnzacqy == 0:
            return _dvzjgizw
        _wcnzdbne = str(_dvzjgizw.get('message', ''))
        if _aqnzacqy == 401 or 'token' in _wcnzdbne.lower():
            with self.lock:
                self.tok = None
            if _retry < 4:
                return self.call(method, path, body, params, _retry + 1)
        if '频繁' in _wcnzdbne or 'exceed' in _wcnzdbne.lower() or 'limit' in _wcnzdbne.lower() or (_aqnzacqy in (429, 5066)):
            if _retry < 12:
                time.sleep(min(90, 5 * (_retry + 1)))
                return self.call(method, path, body, params, _retry + 1)
        raise RuntimeError(f'123 {path} code={_aqnzacqy} {_wcnzdbne[:120]}')

    def _jjldsylb(self, fid):
        _kdrjjtbx, _ixeahvrd = ([], 0)
        while True:
            _dvzjgizw = self.call('GET', '/api/v2/file/list', params={'parentFileId': fid, 'limit': 100, 'lastFileId': _ixeahvrd})
            _iaoixqht = _dvzjgizw.get('data', {})
            _kdrjjtbx += _iaoixqht.get('fileList', [])
            _ixeahvrd = _iaoixqht.get('lastFileId', -1)
            if _ixeahvrd == -1:
                break
        return [_hxyyugoj for _hxyyugoj in _kdrjjtbx if _hxyyugoj.get('trashed', 0) == 0]

    def _jbqcblda(self, parent, name):
        _dvzjgizw = self.call('POST', '/upload/v1/file/mkdir', body={'parentID': str(parent), 'name': name})
        return int(_dvzjgizw['data']['dirID'])

    def _ocnpgylw(self):
        with self.lock:
            if getattr(self, '_updom', None):
                return self._updom
        _dvzjgizw = self.call('GET', '/upload/v2/file/domain')
        _eigezznn = _dvzjgizw['data'][0]
        with self.lock:
            self._updom = _eigezznn
        return _eigezznn

    def _egjvxqxb(self, parent, name, data, _retry=0):
        _wevguxeu = hashlib.md5(data).hexdigest()
        try:
            _eigezznn = self.upload_domain()
            with self.lock:
                if not self.tok:
                    self._login()
                _xfwknvyc = self.tok
            _cypckzsq = self.s.post(_eigezznn + '/upload/v2/file/single/create', headers={'Platform': 'open_platform', 'Authorization': 'Bearer ' + _xfwknvyc}, files={'file': (name, data, 'application/octet-stream')}, data={'parentFileID': str(parent), 'filename': name, 'etag': _wevguxeu, 'size': str(len(data)), 'duplicate': '2'}, timeout=180)
            _dvzjgizw = _cypckzsq.json()
            if _dvzjgizw.get('code') == 0 and (_dvzjgizw['data'].get('completed') or _dvzjgizw['data'].get('fileID')):
                return 'ok'
            _wcnzdbne = str(_dvzjgizw.get('message', ''))
            if '频繁' in _wcnzdbne or 'exceed' in _wcnzdbne.lower() or _dvzjgizw.get('code') in (429, 401):
                if _retry < 8:
                    time.sleep(min(60, 5 * (_retry + 1)))
                    return self.upload(parent, name, data, _retry + 1)
            if _retry < 3:
                time.sleep(3 * (_retry + 1))
                return self.upload(parent, name, data, _retry + 1)
            raise RuntimeError(f'upload {name} {_wcnzdbne[:120]}')
        except Exception as e:
            if _retry < 3:
                time.sleep(3 * (_retry + 1))
                return self.upload(parent, name, data, _retry + 1)
            raise
_s = requests.Session()
_s.trust_env = False
_s.auth = (WD_USER, WD_PASS)

def _olhjvveb(path):
    _cypckzsq = _s.request('PROPFIND', WD + urllib.parse.quote(path, safe='/'), headers={'Depth': '1'}, timeout=60)
    if _cypckzsq.status_code != 207:
        raise RuntimeError(f'PROPFIND {path} -> {_cypckzsq.status_code} {_cypckzsq.text[:100]}')
    _iujfbcro = re.findall('<D:href>(.*?)</D:href>', _cypckzsq.text)
    if not _iujfbcro:
        _iujfbcro = re.findall('<href>(.*?)</href>', _cypckzsq.text)
    return [urllib.parse.unquote(_fcizagyd) for _fcizagyd in _iujfbcro]

def _ahtzsxfw(h):
    if _fcizagyd.startswith('/webdav'):
        return _fcizagyd[len('/webdav'):]
    if '/webdav' in _fcizagyd:
        return '/' + _fcizagyd.split('/webdav', 1)[-1].lstrip('/')
    return _fcizagyd

def _ioocghfh(path, out=None, _depth=0):
    _kdrjjtbx = _kdrjjtbx or []
    if _depth > 8:
        return _kdrjjtbx
    for _fcizagyd in _olhjvveb(path):
        _otkumxia = _ahtzsxfw(_fcizagyd)
        if _otkumxia.rstrip('/') == path.rstrip('/'):
            continue
        _kdrjjtbx.append(_otkumxia)
        if _otkumxia.endswith('/'):
            _ioocghfh(_otkumxia.rstrip('/'), _kdrjjtbx, _depth + 1)
    return _kdrjjtbx

def _pxyobtnw(wd_path, local):
    _cypckzsq = _s.get(WD + urllib.parse.quote(wd_path, safe='/'), timeout=180, stream=True)
    if _cypckzsq.status_code != 200:
        raise RuntimeError(f'GET {wd_path} -> {_cypckzsq.status_code}')
    with open(local, 'wb') as _gjzvwgnd:
        for _xsxvxwpt in _cypckzsq.iter_content(1024 * 256):
            _gjzvwgnd.write(_xsxvxwpt)

def _gpyougkz():
    if not os.path.exists(FANHAO_MAP):
        return {}
    with open(FANHAO_MAP, encoding='utf-8-sig') as _gjzvwgnd:
        return json.load(_gjzvwgnd)
_FHM = _gpyougkz()

def _ivweuthe(raw):
    _xfmkzjez = str(raw or '').strip()
    _xfmkzjez = _xfmkzjez.translate(str.maketrans('０１２３４５６７８９－', '0123456789-'))
    _ignfimad = re.search('([子別史經集])?\\s*(\\d+\\s*-\\s*\\d+)', _xfmkzjez)
    if not _ignfimad:
        return None
    _rdsfvooi = _ignfimad.group(1) or '子'
    _cjoczgig = _ignfimad.group(2).replace(' ', '')
    return f'{_rdsfvooi}{_cjoczgig}'

def _fmnoswwo(fn):
    _zvlcxijm = None
    _wruxkkpx = re.search('\\[番号\\]\\s*([^\\s\\.\\]]+)', fn)
    if _wruxkkpx:
        _zvlcxijm = _ivweuthe(_wruxkkpx.group(1))
    _nknqbrbg = re.match('^([^\\.\\[]+)', fn)
    _tusnmdkn = _nknqbrbg.group(1).strip() if _nknqbrbg else fn.strip()
    _qtagfzym = re.search('\\.(\\d+)\\s*[冊册]', fn)
    _zuktvrri = int(_qtagfzym.group(1)) if _qtagfzym else 1
    _toxkdzbd = _FHM.get(_zvlcxijm) if _zvlcxijm else None
    _xiljiugx = (_toxkdzbd or {}).get('title') or _tusnmdkn
    _fapegfdm = (_toxkdzbd or {}).get('n_ce') or _zuktvrri
    return (_zvlcxijm, _xiljiugx, _fapegfdm)

def _ttmnpack():
    _afzecino = _ioocghfh(LIB_WD)
    _pvbvxish = []
    for _suidhumc in _afzecino:
        if not _suidhumc.lower().endswith('.pdf'):
            continue
        _otkumxia = _suidhumc[len(LIB_WD):].lstrip('/')
        if '/' in _otkumxia:
            _zrunbsto, _gbazxfyk = _otkumxia.rsplit('/', 1)
            _zvlcxijm, _xiljiugx, _fapegfdm = _fmnoswwo(_zrunbsto)
            _fjzlcjbx = f'{_zvlcxijm} {_xiljiugx} 共{_fapegfdm}册' if _zvlcxijm else f'{_xiljiugx} 共{_fapegfdm}册'
            _bmtebxmo = _gbazxfyk[:-4]
            _pvbvxish.append({'wdav': _suidhumc, 'book': _fjzlcjbx, 'ce': _bmtebxmo, 'pdf_name': _gbazxfyk})
        else:
            _qbygqqsm = _otkumxia[:-4]
            _zvlcxijm, _xiljiugx, _fapegfdm = _fmnoswwo(_qbygqqsm)
            _fjzlcjbx = f'{_zvlcxijm} {_xiljiugx} 共{_fapegfdm}册' if _zvlcxijm else f'{_xiljiugx} 共{_fapegfdm}册'
            _bmtebxmo = _xiljiugx
            _pvbvxish.append({'wdav': _suidhumc, 'book': _fjzlcjbx, 'ce': _bmtebxmo, 'pdf_name': _otkumxia})

    def _ywotcfkl(x):
        _ignfimad = re.search('(\\d+)', _hxyyugoj['ce'])
        return int(_ignfimad.group(1)) if _ignfimad else 10 ** 9
    _pvbvxish.sort(key=lambda x: (_hxyyugoj['book'], _ywotcfkl(_hxyyugoj)))
    return _pvbvxish
_pvbvxish = _ttmnpack()
print(f"[scan] {LIB_WD} -> PDF 共 {len(_pvbvxish)} 册, book 数={len(set((t['book'] for t in _pvbvxish)))}", flush=True)
done = set()
if os.path.exists(LEDGER):
    for line in open(LEDGER, encoding='utf-8'):
        try:
            _dvzjgizw = json.loads(line)
            if _dvzjgizw.get('status') == 'ok':
                done.add(_dvzjgizw.get('wdav') or _dvzjgizw.get('ce'))
        except:
            pass
_pvbvxish = [t for t in _pvbvxish if t['wdav'] not in done and t['ce'] not in done]
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
    for _t in _pvbvxish:
        _w = _t['wdav'].rstrip('/')
        if _w in _exacts or any((_w == p or _w.startswith(p + '/') for p in _prefixes)):
            _kept.append(_t)
    _pvbvxish = _kept
    print(f'[todo] 清单过滤后 {len(_pvbvxish)} 册', flush=True)
if args.offset > 0:
    _pvbvxish = _pvbvxish[args.offset:]
if args.limit > 0:
    _pvbvxish = _pvbvxish[:args.limit]
print(f'[todo] 实跑 {len(_pvbvxish)} 册 (已跳过 ledger ok {len(done)})', flush=True)
for t in _pvbvxish[:10]:
    print('  ', t['wdav'], flush=True)
pan = _sxoamlhh()

def _ulsprhrt(parent, name):
    for _suidhumc in pan.list_dir(parent):
        if _suidhumc['type'] == 1 and _suidhumc['filename'] == name:
            return int(_suidhumc['fileId'])
    return pan.mkdir(parent, name)

def _ydqvwwsf():
    _aomxndod = _ulsprhrt(0, '古籍')
    _egkpkbdc = _ulsprhrt(_aomxndod, 'GufangP')
    _lijdjurv = _ulsprhrt(_egkpkbdc, '古方webp')
    _vxepgfde = _ulsprhrt(_lijdjurv, TOP_NAME)
    if CAT_NAME:
        _vxepgfde = _ulsprhrt(_vxepgfde, CAT_NAME)
    return _vxepgfde
TOP_FID = _ydqvwwsf()
print(f'[target] 古方webp/{TOP_NAME} fid={TOP_FID}', flush=True)

def _wkkgwuen(ce_fid):
    return [_hxyyugoj['filename'] for _hxyyugoj in pan.list_dir(ce_fid) if _hxyyugoj['type'] == 0]

def _bjrldhah(t):
    _quiicaer = t['ce']
    _tnwimcdv = os.path.join(TEMP, 'pdf_' + hashlib.md5(_quiicaer.encode()).hexdigest() + '.pdf')
    for _grktuoez in range(1, 4):
        try:
            _fpegggyn = time.time()
            _pxyobtnw(t['wdav'], _tnwimcdv)
            _ckeaueht = fitz.open(_tnwimcdv)
            _uglezegq = _ckeaueht.page_count
            _fjzlcjbx = t['book']
            _roaetaiu = _ulsprhrt(TOP_FID, _fjzlcjbx)
            _lskyowmt = _ulsprhrt(_roaetaiu, _quiicaer)
            _yxcreyhy = set(_wkkgwuen(_lskyowmt))
            if len(_yxcreyhy) >= _uglezegq and all((_gjzvwgnd.startswith('page_') and _gjzvwgnd.endswith('.webp') for _gjzvwgnd in _yxcreyhy)):
                _ckeaueht.close()
                _gpzxbhsh = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': _quiicaer, 'book': t['book'], 'pdf_name': t['pdf_name'], 'pages_pdf': _uglezegq, 'status': 'ok_cloud_skip', 'wdav': t['wdav']}
                _jboheicw(_gpzxbhsh)
                with LG_LOCK:
                    stat['skip'] += 1
                print(f'  [SKIP-cloud] {_quiicaer} 已在云端(N={_uglezegq})', flush=True)
                return True
            _wcelhbwi = queue.Queue(maxsize=max(8, args.upl_conc * 3))
            _dqvcwyap = [0]
            _dkehuhxq = threading.Lock()

            def _entdnurk():
                while True:
                    _swcrhpuq = _wcelhbwi.get()
                    if _swcrhpuq is None:
                        _wcelhbwi.task_done()
                        break
                    _vdysrmev, _oioueemp = _swcrhpuq
                    try:
                        pan.upload(_lskyowmt, _vdysrmev, _oioueemp)
                        with _dkehuhxq:
                            _dqvcwyap[0] += 1
                    except Exception as e:
                        print(f'    [upl-err] {_quiicaer}/{_vdysrmev} {str(e)[:80]}', flush=True)
                    _wcelhbwi.task_done()
            _erunqslo = [threading.Thread(target=_entdnurk, daemon=True) for _ibdraqbm in range(args.upl_conc)]
            for _shpeclsi in _erunqslo:
                _shpeclsi.start()
            for _ehvyywvh in range(_uglezegq):
                _azlfqlgk = _ckeaueht[_ehvyywvh]
                _iscnkqxv = _azlfqlgk.get_pixmap(dpi=120)
                _zufczind = Image.frombytes('RGB', (_iscnkqxv.width, _iscnkqxv.height), _iscnkqxv.samples)
                if _iscnkqxv.width > 16383 or _iscnkqxv.height > 16383:
                    _cypckzsq = 16383 / max(_iscnkqxv.width, _iscnkqxv.height)
                    _zufczind = _zufczind.resize((int(_iscnkqxv.width * _cypckzsq), int(_iscnkqxv.height * _cypckzsq)), Image.LANCZOS)
                _tylfvcsv = io.BytesIO()
                _zufczind.save(_tylfvcsv, 'webp', quality=80, method=0)
                _wcelhbwi.put((f'page_{_ehvyywvh + 1:04d}.webp', _tylfvcsv.getvalue()))
                del img, pix, buf
            _ckeaueht.close()
            for _ibdraqbm in _erunqslo:
                _wcelhbwi.put(None)
            for _shpeclsi in _erunqslo:
                _shpeclsi.join()
            _umtwrzaa = time.time() - _fpegggyn
            if _dqvcwyap[0] == _uglezegq:
                _gpzxbhsh = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': _quiicaer, 'book': t['book'], 'pdf_name': t['pdf_name'], 'pages_pdf': _uglezegq, 'pages_uploaded': _dqvcwyap[0], 'status': 'ok', 'elapsed_s': round(_umtwrzaa, 1), 'wdav': t['wdav']}
                _jboheicw(_gpzxbhsh)
                with LG_LOCK:
                    stat['ok'] += 1
                print(f"  [OK] {_quiicaer} · {_uglezegq}p · {_umtwrzaa:.0f}s (累计OK={stat['ok']})", flush=True)
                return True
            print(f'  [PARTIAL] {_quiicaer} {_dqvcwyap[0]}/{_uglezegq}', flush=True)
            raise RuntimeError(f'upload partial {_dqvcwyap[0]}/{_uglezegq}')
        except Exception as e:
            print(f'  [FAIL-{_grktuoez}] {_quiicaer} {str(e)[:150]}', flush=True)
            time.sleep(3 * _grktuoez)
        finally:
            if os.path.exists(_tnwimcdv):
                try:
                    os.remove(_tnwimcdv)
                except:
                    pass
    _gpzxbhsh = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': _quiicaer, 'book': t['book'], 'pdf_name': t['pdf_name'], 'status': 'fail', 'wdav': t['wdav']}
    _jboheicw(_gpzxbhsh)
    with LG_LOCK:
        stat['fail'] += 1
    print(f'  [FAIL-FINAL] {_quiicaer}', flush=True)
    return False
print(f'[start] book并发={args.book_conc} 页并发={args.upl_conc}', flush=True)
with ThreadPoolExecutor(max_workers=args.book_conc) as _yxcreyhy:
    futs = {_yxcreyhy.submit(_bjrldhah, t): t for t in _pvbvxish}
    for fu in as_completed(futs):
        try:
            fu.result()
        except Exception as e:
            print('[exec-err]', str(e)[:100], flush=True)
print(f"[done] OK={stat['ok']} FAIL={stat['fail']} SKIP={stat['skip']}", flush=True)
