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
            _dvzjgizw = self.tok
        _aqnzacqy = {'Platform': 'open_platform', 'Authorization': 'Bearer ' + _dvzjgizw}
        if body is not None:
            _aqnzacqy['Content-Type'] = 'application/json'
        try:
            _cypckzsq = self.s.request(method, B123 + path, headers=_aqnzacqy, params=params, data=json.dumps(body) if body is not None else None, timeout=90)
            _wcnzdbne = _cypckzsq.json()
        except Exception:
            if _retry < 6:
                time.sleep(5 * (_retry + 1))
                return self.call(method, path, body, params, _retry + 1)
            raise
        _fcizagyd = _wcnzdbne.get('code')
        if _fcizagyd == 0:
            return _wcnzdbne
        _xfwknvyc = str(_wcnzdbne.get('message', ''))
        if _fcizagyd == 401 or 'token' in _xfwknvyc.lower():
            with self.lock:
                self.tok = None
            if _retry < 4:
                return self.call(method, path, body, params, _retry + 1)
        if '频繁' in _xfwknvyc or 'exceed' in _xfwknvyc.lower() or 'limit' in _xfwknvyc.lower() or (_fcizagyd in (429, 5066)):
            if _retry < 12:
                time.sleep(min(90, 5 * (_retry + 1)))
                return self.call(method, path, body, params, _retry + 1)
        raise RuntimeError(f'123 {path} code={_fcizagyd} {_xfwknvyc[:120]}')

    def _jjldsylb(self, fid):
        _ixeahvrd, _hxyyugoj = ([], 0)
        while True:
            _wcnzdbne = self.call('GET', '/api/v2/file/list', params={'parentFileId': fid, 'limit': 100, 'lastFileId': _hxyyugoj})
            _kdrjjtbx = _wcnzdbne.get('data', {})
            _ixeahvrd += _kdrjjtbx.get('fileList', [])
            _hxyyugoj = _kdrjjtbx.get('lastFileId', -1)
            if _hxyyugoj == -1:
                break
        return [_iaoixqht for _iaoixqht in _ixeahvrd if _iaoixqht.get('trashed', 0) == 0]

    def _jbqcblda(self, parent, name):
        _wcnzdbne = self.call('POST', '/upload/v1/file/mkdir', body={'parentID': str(parent), 'name': name})
        return int(_wcnzdbne['data']['dirID'])

    def _ocnpgylw(self):
        with self.lock:
            if getattr(self, '_updom', None):
                return self._updom
        _wcnzdbne = self.call('GET', '/upload/v2/file/domain')
        _eigezznn = _wcnzdbne['data'][0]
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
                _dvzjgizw = self.tok
            _cypckzsq = self.s.post(_eigezznn + '/upload/v2/file/single/create', headers={'Platform': 'open_platform', 'Authorization': 'Bearer ' + _dvzjgizw}, files={'file': (name, data, 'application/octet-stream')}, data={'parentFileID': str(parent), 'filename': name, 'etag': _wevguxeu, 'size': str(len(data)), 'duplicate': '2'}, timeout=180)
            _wcnzdbne = _cypckzsq.json()
            if _wcnzdbne.get('code') == 0 and (_wcnzdbne['data'].get('completed') or _wcnzdbne['data'].get('fileID')):
                return 'ok'
            _xfwknvyc = str(_wcnzdbne.get('message', ''))
            if '频繁' in _xfwknvyc or 'exceed' in _xfwknvyc.lower() or _wcnzdbne.get('code') in (429, 401):
                if _retry < 8:
                    time.sleep(min(60, 5 * (_retry + 1)))
                    return self.upload(parent, name, data, _retry + 1)
            if _retry < 3:
                time.sleep(3 * (_retry + 1))
                return self.upload(parent, name, data, _retry + 1)
            raise RuntimeError(f'upload {name} {_xfwknvyc[:120]}')
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
    return [urllib.parse.unquote(_aqnzacqy) for _aqnzacqy in _iujfbcro]

def _ahtzsxfw(h):
    if _aqnzacqy.startswith('/webdav'):
        return _aqnzacqy[len('/webdav'):]
    if '/webdav' in _aqnzacqy:
        return '/' + _aqnzacqy.split('/webdav', 1)[-1].lstrip('/')
    return _aqnzacqy

def _ioocghfh(path, out=None, _depth=0):
    _ixeahvrd = _ixeahvrd or []
    if _depth > 8:
        return _ixeahvrd
    for _aqnzacqy in _olhjvveb(path):
        _otkumxia = _ahtzsxfw(_aqnzacqy)
        if _otkumxia.rstrip('/') == path.rstrip('/'):
            continue
        _ixeahvrd.append(_otkumxia)
        if _otkumxia.endswith('/'):
            _ioocghfh(_otkumxia.rstrip('/'), _ixeahvrd, _depth + 1)
    return _ixeahvrd

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
    with open(FANHAO_MAP, encoding='utf-8') as _gjzvwgnd:
        return json.load(_gjzvwgnd)
_FHM = _gpyougkz()

def _ivweuthe(raw):
    _cjoczgig = str(raw or '').strip()
    _cjoczgig = _cjoczgig.translate(str.maketrans('０１２３４５６７８９－', '0123456789-'))
    _ignfimad = re.search('([子別史經集])?\\s*(\\d+\\s*-\\s*\\d+)', _cjoczgig)
    if not _ignfimad:
        return None
    _rdsfvooi = _ignfimad.group(1) or '子'
    _xfmkzjez = _ignfimad.group(2).replace(' ', '')
    return f'{_rdsfvooi}{_xfmkzjez}'

def _fmnoswwo(fn):
    _nknqbrbg = None
    _tusnmdkn = re.search('\\[番号\\]\\s*([^\\s\\.\\]]+)', fn)
    if _tusnmdkn:
        _nknqbrbg = _ivweuthe(_tusnmdkn.group(1))
    _wruxkkpx = re.match('^([^\\.\\[]+)', fn)
    _xiljiugx = _wruxkkpx.group(1).strip() if _wruxkkpx else fn.strip()
    _fapegfdm = re.search('\\.(\\d+)\\s*[冊册]', fn)
    _qtagfzym = int(_fapegfdm.group(1)) if _fapegfdm else 1
    _toxkdzbd = _FHM.get(_nknqbrbg) if _nknqbrbg else None
    _zuktvrri = (_toxkdzbd or {}).get('title') or _xiljiugx
    _zvlcxijm = (_toxkdzbd or {}).get('n_ce') or _qtagfzym
    return (_nknqbrbg, _zuktvrri, _zvlcxijm)

def _ttmnpack():
    _zrunbsto = _ioocghfh(LIB_WD)
    _pvbvxish = []
    for _fjzlcjbx in _zrunbsto:
        if not _fjzlcjbx.lower().endswith('.pdf'):
            continue
        _otkumxia = _fjzlcjbx[len(LIB_WD):].lstrip('/')
        if '/' in _otkumxia:
            _gbazxfyk, _afzecino = _otkumxia.rsplit('/', 1)
            _nknqbrbg, _zuktvrri, _zvlcxijm = _fmnoswwo(_gbazxfyk)
            _qbygqqsm = f'{_nknqbrbg} {_zuktvrri} 共{_zvlcxijm}册' if _nknqbrbg else f'{_zuktvrri} 共{_zvlcxijm}册'
            _suidhumc = _afzecino[:-4]
            _pvbvxish.append({'wdav': _fjzlcjbx, 'book': _qbygqqsm, 'ce': _suidhumc, 'pdf_name': _afzecino})
        else:
            _bmtebxmo = _otkumxia[:-4]
            _nknqbrbg, _zuktvrri, _zvlcxijm = _fmnoswwo(_bmtebxmo)
            _qbygqqsm = f'{_nknqbrbg} {_zuktvrri} 共{_zvlcxijm}册' if _nknqbrbg else f'{_zuktvrri} 共{_zvlcxijm}册'
            _suidhumc = _zuktvrri
            _pvbvxish.append({'wdav': _fjzlcjbx, 'book': _qbygqqsm, 'ce': _suidhumc, 'pdf_name': _otkumxia})

    def _ywotcfkl(x):
        _ignfimad = re.search('(\\d+)', _iaoixqht['ce'])
        return int(_ignfimad.group(1)) if _ignfimad else 10 ** 9
    _pvbvxish.sort(key=lambda x: (_iaoixqht['book'], _ywotcfkl(_iaoixqht)))
    return _pvbvxish
_pvbvxish = _ttmnpack()
print(f"[scan] {LIB_WD} -> PDF 共 {len(_pvbvxish)} 册, book 数={len(set((t['book'] for t in _pvbvxish)))}", flush=True)
done = set()
if os.path.exists(LEDGER):
    for line in open(LEDGER, encoding='utf-8'):
        try:
            _wcnzdbne = json.loads(line)
            if _wcnzdbne.get('status') == 'ok':
                done.add(_wcnzdbne.get('wdav') or _wcnzdbne.get('ce'))
        except:
            pass
_pvbvxish = [t for t in _pvbvxish if t['wdav'] not in done and t['ce'] not in done]
if LIST_FILE:
    _prefixes, _exacts = (set(), set())
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
    for _fjzlcjbx in pan.list_dir(parent):
        if _fjzlcjbx['type'] == 1 and _fjzlcjbx['filename'] == name:
            return int(_fjzlcjbx['fileId'])
    return pan.mkdir(parent, name)

def _ydqvwwsf():
    _vxepgfde = _ulsprhrt(0, '古籍')
    _lijdjurv = _ulsprhrt(_vxepgfde, 'GufangP')
    _egkpkbdc = _ulsprhrt(_lijdjurv, '古方webp')
    _aomxndod = _ulsprhrt(_egkpkbdc, TOP_NAME)
    if CAT_NAME:
        _aomxndod = _ulsprhrt(_aomxndod, CAT_NAME)
    return _aomxndod
TOP_FID = _ydqvwwsf()
print(f'[target] 古方webp/{TOP_NAME} fid={TOP_FID}', flush=True)

def _wkkgwuen(ce_fid):
    return [_iaoixqht['filename'] for _iaoixqht in pan.list_dir(ce_fid) if _iaoixqht['type'] == 0]

def _bjrldhah(t):
    _roaetaiu = t['ce']
    _shpeclsi = os.path.join(TEMP, 'pdf_' + hashlib.md5(_roaetaiu.encode()).hexdigest() + '.pdf')
    for _vdysrmev in range(1, 4):
        try:
            _ckeaueht = time.time()
            _pxyobtnw(t['wdav'], _shpeclsi)
            _grktuoez = fitz.open(_shpeclsi)
            _dqvcwyap = _grktuoez.page_count
            _qbygqqsm = t['book']
            _oioueemp = _ulsprhrt(TOP_FID, _qbygqqsm)
            _wcelhbwi = _ulsprhrt(_oioueemp, _roaetaiu)
            _ehvyywvh = set(_wkkgwuen(_wcelhbwi))
            if len(_ehvyywvh) >= _dqvcwyap and all((_gjzvwgnd.startswith('page_') and _gjzvwgnd.endswith('.webp') for _gjzvwgnd in _ehvyywvh)):
                _grktuoez.close()
                _gpzxbhsh = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': _roaetaiu, 'book': t['book'], 'pdf_name': t['pdf_name'], 'pages_pdf': _dqvcwyap, 'status': 'ok_cloud_skip', 'wdav': t['wdav']}
                _jboheicw(_gpzxbhsh)
                with LG_LOCK:
                    stat['skip'] += 1
                print(f'  [SKIP-cloud] {_roaetaiu} 已在云端(N={_dqvcwyap})', flush=True)
                return True
            _umtwrzaa = queue.Queue(maxsize=max(8, args.upl_conc * 3))
            _azlfqlgk = [0]
            _dkehuhxq = threading.Lock()

            def _entdnurk():
                while True:
                    _erunqslo = _umtwrzaa.get()
                    if _erunqslo is None:
                        _umtwrzaa.task_done()
                        break
                    _quiicaer, _fpegggyn = _erunqslo
                    try:
                        pan.upload(_wcelhbwi, _quiicaer, _fpegggyn)
                        with _dkehuhxq:
                            _azlfqlgk[0] += 1
                    except Exception as e:
                        print(f'    [upl-err] {_roaetaiu}/{_quiicaer} {str(e)[:80]}', flush=True)
                    _umtwrzaa.task_done()
            _yxcreyhy = [threading.Thread(target=_entdnurk, daemon=True) for _iscnkqxv in range(args.upl_conc)]
            for _ibdraqbm in _yxcreyhy:
                _ibdraqbm.start()
            for _tylfvcsv in range(_dqvcwyap):
                _lskyowmt = _grktuoez[_tylfvcsv]
                _tnwimcdv = _lskyowmt.get_pixmap(dpi=120)
                _zufczind = Image.frombytes('RGB', (_tnwimcdv.width, _tnwimcdv.height), _tnwimcdv.samples)
                if _tnwimcdv.width > 16383 or _tnwimcdv.height > 16383:
                    _cypckzsq = 16383 / max(_tnwimcdv.width, _tnwimcdv.height)
                    _zufczind = _zufczind.resize((int(_tnwimcdv.width * _cypckzsq), int(_tnwimcdv.height * _cypckzsq)), Image.LANCZOS)
                _uglezegq = io.BytesIO()
                _zufczind.save(_uglezegq, 'webp', quality=80, method=0)
                _umtwrzaa.put((f'page_{_tylfvcsv + 1:04d}.webp', _uglezegq.getvalue()))
                del img, pix, buf
            _grktuoez.close()
            for _iscnkqxv in _yxcreyhy:
                _umtwrzaa.put(None)
            for _ibdraqbm in _yxcreyhy:
                _ibdraqbm.join()
            _swcrhpuq = time.time() - _ckeaueht
            if _azlfqlgk[0] == _dqvcwyap:
                _gpzxbhsh = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': _roaetaiu, 'book': t['book'], 'pdf_name': t['pdf_name'], 'pages_pdf': _dqvcwyap, 'pages_uploaded': _azlfqlgk[0], 'status': 'ok', 'elapsed_s': round(_swcrhpuq, 1), 'wdav': t['wdav']}
                _jboheicw(_gpzxbhsh)
                with LG_LOCK:
                    stat['ok'] += 1
                print(f"  [OK] {_roaetaiu} · {_dqvcwyap}p · {_swcrhpuq:.0f}s (累计OK={stat['ok']})", flush=True)
                return True
            print(f'  [PARTIAL] {_roaetaiu} {_azlfqlgk[0]}/{_dqvcwyap}', flush=True)
            raise RuntimeError(f'upload partial {_azlfqlgk[0]}/{_dqvcwyap}')
        except Exception as e:
            print(f'  [FAIL-{_vdysrmev}] {_roaetaiu} {str(e)[:150]}', flush=True)
            time.sleep(3 * _vdysrmev)
        finally:
            if os.path.exists(_shpeclsi):
                try:
                    os.remove(_shpeclsi)
                except:
                    pass
    _gpzxbhsh = {'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'acct': 'guji', 'ce': _roaetaiu, 'book': t['book'], 'pdf_name': t['pdf_name'], 'status': 'fail', 'wdav': t['wdav']}
    _jboheicw(_gpzxbhsh)
    with LG_LOCK:
        stat['fail'] += 1
    print(f'  [FAIL-FINAL] {_roaetaiu}', flush=True)
    return False
print(f'[start] book并发={args.book_conc} 页并发={args.upl_conc}', flush=True)
with ThreadPoolExecutor(max_workers=args.book_conc) as _ehvyywvh:
    futs = {_ehvyywvh.submit(_bjrldhah, t): t for t in _pvbvxish}
    for fu in as_completed(futs):
        try:
            fu.result()
        except Exception as e:
            print('[exec-err]', str(e)[:100], flush=True)
print(f"[done] OK={stat['ok']} FAIL={stat['fail']} SKIP={stat['skip']}", flush=True)
