"""Ingestão Bronze RF -> MinIO (efêmero, parity bronze_raw_core.py).

Uso: python script.py --dataset cnaes|empresas|estabelecimentos|socios [--periodo YYYY-MM]
Discovery via PROPFIND + probe HEAD/GET com fallback 6 meses; resume local;
is_zipfile; retry upload + size verify; _SUCCESS em _control/.
"""

import argparse
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from calendar import monthrange
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestao_rf")

BASE_DAV = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9"
PREFIX = {
    "estabelecimentos": "Estabelecimentos",
    "empresas": "Empresas",
    "socios": "Socios",
    "cnaes": "Cnaes",
}


def last_day(ym: str) -> str:
    y, m = map(int, ym.split("-"))
    return f"{y:04d}{m:02d}{monthrange(y, m)[1]:02d}"


def minio_client():
    from minio import Minio

    return Minio(
        os.environ.get("MINIO_LOCAL_DOMAIN", "minio:9000"),
        access_key=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
        secure=False,
    )


def discover(base_ym: str, prefix: str) -> list[int]:
    import httpx

    y, m = map(int, base_ym.split("-"))
    for back in range(6):
        yy, mm = y, m - back
        while mm <= 0:
            yy -= 1
            mm += 12
        url = f"{BASE_DAV}/{yy}-{mm:02d}/"
        body = (
            '<?xml version="1.0"?><d:propfind xmlns:d="DAV:">'
            "<d:prop><d:getcontentlength/></d:prop></d:propfind>"
        )
        try:
            with httpx.Client(timeout=30, follow_redirects=True) as c:
                r = c.request("PROPFIND", url, headers={"Depth": "1"}, content=body)
            if r.status_code != 207:
                continue
            root = ET.fromstring(r.text)  # noqa: S314 - resposta DAV da RF (fonte oficial, sem entidades)
            ns = {"d": "DAV:"}
            pat = re.compile(rf"{re.escape(prefix)}(\d*)\.zip$")
            idxs = []
            for resp in root.findall(".//d:response", ns):
                href = resp.find("d:href", ns)
                if href is None or not href.text:
                    continue
                name = href.text.rstrip("/").split("/")[-1]
                mt = pat.match(name)
                if mt:
                    n = mt.group(1)
                    idxs.append(int(n) if n else -1)
            if idxs or _exists(yy, mm, f"{prefix}.zip"):
                return sorted(idxs) if idxs else [-1], f"{yy}-{mm:02d}"
        except Exception as e:
            log.warning("discovery retry %s: %s", url, e)
    return [], base_ym


def _exists(y: int, m: int, filename: str) -> bool:
    import httpx

    try:
        with httpx.Client(timeout=20, follow_redirects=True) as c:
            r = c.head(f"{BASE_DAV}/{y}-{m:02d}/{filename}")
            if r.status_code == 200:
                return True
            r = c.get(f"{BASE_DAV}/{y}-{m:02d}/{filename}", headers={"Range": "bytes=0-0"})
            return r.status_code in (200, 206)
    except Exception:
        return False


def download(base_ym: str, filename: str, dest: Path) -> bool:
    import httpx

    y, m = map(int, base_ym.split("-"))
    url = f"{BASE_DAV}/{y}-{m:02d}/{filename}"
    try:
        with httpx.Client(timeout=600, follow_redirects=True) as c, c.stream("GET", url) as r:
            if r.status_code != 200:
                return False
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(1024 * 1024):
                    f.write(chunk)
        return zipfile.is_zipfile(dest)
    except Exception as e:
        log.error("download %s: %s", filename, e)
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=list(PREFIX))
    ap.add_argument("--periodo", default="")
    ap.add_argument("--estado", default="MA")
    a = ap.parse_args()
    ds, prefix = a.dataset, PREFIX[a.dataset]
    periodo_req = a.periodo.strip() or os.environ.get("PROCESSING_PERIODO", "").strip()
    base = periodo_req or __import__("datetime").datetime.now().strftime("%Y-%m")

    cli = minio_client()
    bucket = os.environ.get("MINIO_BUCKET_NAME", "organizacao")
    if not cli.bucket_exists(bucket):
        cli.make_bucket(bucket)

    idxs, resolved = discover(base, prefix)
    if not idxs:
        log.error("nenhum arquivo RF prefix=%s periodo=%s", prefix, base)
        sys.exit(1)
    suffix = last_day(resolved)
    marker = f"bronze/{ds}/_control/_SUCCESS_{suffix}"
    try:
        cli.stat_object(bucket, marker)
        log.info("idempotencia=skip %s", marker)
        return
    except Exception:
        log.debug("sem marker, segue download | marker=%s", marker)

    tmp = Path(os.environ.get("MAGE_TEMP_DIR", tempfile.gettempdir())) / f"{ds}_zip_{resolved}"
    tmp.mkdir(parents=True, exist_ok=True)
    files = [f"{prefix}{i}.zip" if i >= 0 else f"{prefix}.zip" for i in idxs]
    ok = 0
    for fn in files:
        zp = tmp / fn
        if zp.exists() and zipfile.is_zipfile(zp):
            log.info("pulado %s", fn)
            ok += 1
            continue
        t0 = time.time()
        if download(resolved, fn, zp):
            log.info("ok %s %.1fMB %.1fs", fn, zp.stat().st_size / 2**20, time.time() - t0)
            ok += 1
        else:
            log.error("falha %s", fn)
            zp.unlink(missing_ok=True)

    uploaded = 0
    for zp in sorted(tmp.glob("*.zip")):
        key = f"bronze/{ds}/{zp.name}"
        for att in range(3):
            try:
                cli.fput_object(bucket, key, str(zp))
                if cli.stat_object(bucket, key).size == zp.stat().st_size:
                    uploaded += 1
                    break
            except Exception as e:
                log.warning("upload retry %s: %s", zp.name, e)
                time.sleep(att + 1)
    if uploaded >= len(files):
        fd, tpath = tempfile.mkstemp(suffix=".txt")
        with open(fd, "w") as f:
            f.write(f"completed_at={time.time()}\n")
        cli.fput_object(bucket, marker, tpath)
        Path(tpath).unlink(missing_ok=True)
        log.info("marker=ok %s", marker)
    else:
        log.warning("parcial %d/%d marker adiado", uploaded, len(files))
    shutil.rmtree(tmp, ignore_errors=True)
    log.info("done dataset=%s periodo=%s up=%d", ds, resolved, uploaded)


if __name__ == "__main__":
    main()
