import os
import sys

# Enforce zero shard/chunk disk cache on HuggingFace Xet to prevent 10GB+ cache accumulation
os.environ.setdefault("HF_XET_SHARD_CACHE_SIZE_LIMIT", "0")
os.environ.setdefault("HF_XET_CHUNK_CACHE_SIZE_BYTES", "0")
os.environ.setdefault("HF_XET_LOG_DIR_MAX_SIZE", "10000000")
os.environ.setdefault("HF_HUB_DISABLE_DISK_INTEGRITY_CHECK", "1")

# Resolve the directory containing the local modules (db_service, qobuz_service, etc.)
# In dev: app.py and db_service.py are co-located in lossless/
# In production container: app.py is at /app/app.py, modules are at /app/lossless/
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
_LOSSLESS_SUBDIR = os.path.join(_CURR_DIR, 'lossless')

# Pick whichever directory actually contains db_service.py
if os.path.exists(os.path.join(_CURR_DIR, 'db_service.py')):
    _MODULES_DIR = _CURR_DIR
elif os.path.exists(os.path.join(_LOSSLESS_SUBDIR, 'db_service.py')):
    _MODULES_DIR = _LOSSLESS_SUBDIR
else:
    _MODULES_DIR = _CURR_DIR  # fallback; will surface a clear error later

# Always ensure _MODULES_DIR is first on sys.path
if sys.path[:1] != [_MODULES_DIR]:
    if _MODULES_DIR in sys.path:
        sys.path.remove(_MODULES_DIR)
    sys.path.insert(0, _MODULES_DIR)

import logging
import urllib.parse
import httpx
import time
import collections
import threading
import re
import json
import queue
import gc
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse

# Load local modules by absolute path — immune to sys.path/CWD issues
import importlib.util as _ilu

def _load_local_module(mod_name: str):
    path = os.path.join(_MODULES_DIR, f"{mod_name}.py")
    spec = _ilu.spec_from_file_location(mod_name, path)
    mod = _ilu.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod

try:
    import db_service
except ModuleNotFoundError:
    db_service = _load_local_module('db_service')

try:
    import qobuz_service
except ModuleNotFoundError:
    qobuz_service = _load_local_module('qobuz_service')

# Store last 100 log lines in a thread-safe list/deque
RECENT_LOGS = collections.deque(maxlen=100)
FATAL_ERRORS = collections.deque(maxlen=10)

class InMemoryLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = self.format(record)
            RECENT_LOGS.append(log_entry)
        except Exception:
            self.handleError(record)

# Setup logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

stdout_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler("stream_service.log", encoding="utf-8")
log_handler = InMemoryLogHandler()

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
stdout_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
log_handler.setFormatter(formatter)

root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
root_logger.addHandler(stdout_handler)
root_logger.addHandler(file_handler)
root_logger.addHandler(log_handler)

log = logging.getLogger("stream-service")

# Config
HF_TOKEN = os.environ.get("HFTOKEN")
BUCKET_ID = "shashwatIDR/stream"

import ipaddress

def is_safe_domain_url(url: str) -> bool:
    """Strict security check: ensures 0 direct IP access. Returns False if target is an IPv4 or IPv6."""
    try:
        hostname = urllib.parse.urlparse(url).hostname or ""
        if not hostname:
            return False
        ipaddress.ip_address(hostname)
        return False  # Target is a raw IP address! Block it.
    except ValueError:
        return True   # Target is a valid domain name.

# Zero Direct IP Access: Invidious queries & streams are routed strictly through Antideploy proxy layer
INVIDIOUS_PROXY_URL = os.environ.get("INVIDIOUS_PROXY_URL", "https://noipaccess.antideploy.app").rstrip("/")
raw_instances = [inst.strip() for inst in os.environ.get("INVIDIOUS_INSTANCES", "").split(",") if inst.strip()]
if not raw_instances:
    raw_instances = [INVIDIOUS_PROXY_URL]

# Filter out any raw IP addresses with zero tolerance
INVIDIOUS_INSTANCES = [inst for inst in raw_instances if is_safe_domain_url(inst)]
if not INVIDIOUS_INSTANCES:
    INVIDIOUS_INSTANCES = [INVIDIOUS_PROXY_URL]

# Track stats in session
STATS = {
    "total": 0,
    "done": 0,
    "progressing": 0,
    "failed": 0
}
STREAM_JOBS = collections.OrderedDict()  # video_id -> {"status": "queued"|"progressing"|"done"|"failed", "timestamp": float, "ext": str}
UPLOAD_TIMESTAMPS = collections.deque(maxlen=1000)

NETWORK_STATS = {
    "last_download_speed_kbps": 0.0,
    "last_upload_speed_kbps": 0.0,
    "total_downloaded_mb": 0.0,
    "total_uploaded_mb": 0.0
}

# Base URL & DB Caches (Unified with db_service)
DB_CACHE = db_service.DB_CACHE
DB_CACHE_LOCK = db_service.DB_CACHE_LOCK
BASE_URL_CACHE = db_service.BASE_URL_CACHE
BASE_URL_CACHE_LOCK = db_service.BASE_URL_CACHE_LOCK

# Thread-safe Task Queue & Workers (deque wrapped with Lock for LIFO/FIFO priority put)
class PriorityTaskQueue:
    def __init__(self):
        self.deque = collections.deque()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)

    def put(self, item, high_priority=False):
        with self.cond:
            if high_priority:
                try:
                    self.deque.remove(item)
                except ValueError:
                    pass
                self.deque.appendleft(item)
            else:
                if item not in self.deque:
                    self.deque.append(item)
            self.cond.notify()

    def get(self):
        with self.cond:
            while not self.deque:
                self.cond.wait()
            return self.deque.popleft()

    def qsize(self):
        with self.lock:
            return len(self.deque)

    def task_done(self):
        pass

TASK_QUEUE = PriorityTaskQueue()
QUEUED_SET = set()
QUEUED_SET_LOCK = threading.Lock()
NUM_WORKERS = 50
WORKER_THREADS = []

URL_PATTERN = re.compile(r'https?://[^\s]+')

def mask_urls(text: str) -> str:
    return URL_PATTERN.sub('[URL]', text)

def get_avg_uploads_per_min() -> float:
    now = time.time()
    while UPLOAD_TIMESTAMPS and UPLOAD_TIMESTAMPS[0] < now - 120:
        UPLOAD_TIMESTAMPS.popleft()
    return len(UPLOAD_TIMESTAMPS) / 2.0

def load_db_cache():
    global DB_CACHE, BASE_URL_CACHE
    hf_loaded = False
    try:
        url = f"https://huggingface.co/buckets/{BUCKET_ID}/resolve/db_backup.json"
        headers = {}
        if HF_TOKEN:
            headers["Authorization"] = f"Bearer {HF_TOKEN}"
        log.info(f"Loading full database from Hugging Face db_backup.json ({url})...")
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            resp = client.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            songs_list = data.get("songs", [])
            if songs_list:
                with DB_CACHE_LOCK:
                    DB_CACHE.clear()
                    DB_CACHE.update({s["id"]: s for s in songs_list if s.get("id")})
                    if hasattr(db_service, "DB_CACHE") and db_service.DB_CACHE is not DB_CACHE:
                        db_service.DB_CACHE.clear()
                        db_service.DB_CACHE.update(DB_CACHE)
                base_urls_list = data.get("base_urls", [])
                with BASE_URL_CACHE_LOCK:
                    BASE_URL_CACHE.clear()
                    for item in base_urls_list:
                        if isinstance(item, dict):
                            BASE_URL_CACHE[int(item["id"])] = item["url"]
                        elif isinstance(item, (list, tuple)) and len(item) >= 2:
                            BASE_URL_CACHE[int(item[0])] = item[1]
                    BASE_URL_CACHE[2] = "https://velamhere-img.hf.space"
                    if hasattr(db_service, "BASE_URL_CACHE") and db_service.BASE_URL_CACHE is not BASE_URL_CACHE:
                        db_service.BASE_URL_CACHE.clear()
                        db_service.BASE_URL_CACHE.update(BASE_URL_CACHE)
                log.info(f"Successfully loaded ALL {len(DB_CACHE)} songs from HF db_backup.json!")
                hf_loaded = True
    except Exception as hf_ex:
        log.error(f"Failed to load complete dataset from HF backup: {hf_ex}")

def load_db_stats():
    try:
        with DB_CACHE_LOCK:
            total_count = len(DB_CACHE)
            done_count = sum(1 for song in DB_CACHE.values() if song.get("stream_sources"))
        STATS["done"] = done_count
        STATS["total"] = total_count
        log.info(f"Loaded DB stats: Total Songs = {total_count}, Cached Streams = {done_count}")
    except Exception as e:
        log.error(f"Failed to load DB stats: {e}")

def init_db():
    load_db_cache()
    load_db_stats()

DB_BACKUP_INTERVAL = int(os.environ.get("DB_BACKUP_INTERVAL", "180"))

def db_backup_loop():
    log.info(f"Starting database backup daemon thread (interval: {DB_BACKUP_INTERVAL}s, dirty-checked)...")
    while True:
        try:
            time.sleep(DB_BACKUP_INTERVAL)
            if not getattr(db_service, "DB_CACHE_DIRTY", True):
                continue

            with DB_CACHE_LOCK:
                songs = list(DB_CACHE.values())
            
            # SAFEGUARD: Never overwrite backup with empty data (prevents clearing backup if DB is down)
            if not songs:
                log.warning("DB backup skipped: DB_CACHE is empty. Refusing to overwrite db_backup.json.")
                continue
            
            with BASE_URL_CACHE_LOCK:
                base_urls = [{"id": k, "url": v} for k, v in BASE_URL_CACHE.items()]
            if not base_urls:
                base_urls = [
                    {"id": 1, "url": "https://velamhere-img.hf.space"},
                    {"id": 2, "url": "https://velamhere-img.hf.space"}
                ]
            
            backup_data = {"songs": songs, "base_urls": base_urls}
            json_bytes = json.dumps(backup_data, default=str, separators=(',', ':')).encode('utf-8')
            from db_service import safe_batch_bucket_files
            safe_batch_bucket_files(BUCKET_ID, add=[(json_bytes, "db_backup.json")], token=HF_TOKEN)
            del json_bytes
            db_service.cleanup_upload_cache()
            db_service.DB_CACHE_DIRTY = False
            log.info(f"Database backup ({len(songs)} songs) successfully uploaded to Hugging Face (db_backup.json).")
        except Exception as e:
            log.error(f"Failed to backup database to Hugging Face: {e}")

def fetch_correct_metadata(video_id: str) -> tuple[str, str]:
    """Fetch track metadata (title, artist) using upstream metadata service API."""
    worker_api = os.environ.get("METADATA_API_BASE", os.environ.get("WORKER_API_BASE", "https://beautiful-junie-scoder-53c5187c.koyeb.app"))
    url = f"{worker_api.rstrip('/')}/?id={video_id}"
    try:
        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                matched = data.get("matched_track", {})
                yt = data.get("ytmusic", {})
                title = matched.get("title") or yt.get("title") or ""
                artist = matched.get("artist") or yt.get("artist") or ""
                if title:
                    return title.strip(), artist.strip()
    except Exception as e:
        log.warning(f"Metadata API fetch failed for {video_id}: {e}")
    
    headers = {
        "User-Agent": "MuzoAudioCatalog/2.0 (+https://kouzu.in)"
    }
    # Priority 3: Direct Invidious video info metadata fallback
    for instance in INVIDIOUS_INSTANCES[:3]:
        try:
            inv_url = f"{instance.rstrip('/')}/api/v1/videos/{video_id}"
            if not is_safe_domain_url(inv_url):
                continue
            with httpx.Client(headers=headers, timeout=8.0) as client:
                resp = client.get(inv_url)
                if resp.status_code == 200:
                    data = resp.json()
                    title = (data.get("title") or "").strip()
                    author = (data.get("author") or "").strip()
                    if title:
                        return title, author
        except Exception:
            pass

    return "", ""

def db_save_stream_source(video_id: str, value: str = "m4a"):
    name = ""
    artists = ""
    with DB_CACHE_LOCK:
        existing = DB_CACHE.get(video_id)
        if existing:
            name = existing.get("name") or ""
            artists = existing.get("artists") or ""
    
    if not name or name == "Unknown Song" or not artists:
        name, artists = fetch_correct_metadata(video_id)
        
    with DB_CACHE_LOCK:
        if video_id in DB_CACHE:
            DB_CACHE[video_id]["stream_sources"] = value
            if name and name != "Unknown Song":
                DB_CACHE[video_id]["name"] = name
            if artists and artists != "Unknown Artist":
                DB_CACHE[video_id]["artists"] = artists
        else:
            DB_CACHE[video_id] = {
                "id": video_id,
                "name": name,
                "artists": artists,
                "stream_sources": value
            }
        if hasattr(db_service, "DB_CACHE") and db_service.DB_CACHE is not DB_CACHE:
            db_service.DB_CACHE[video_id] = dict(DB_CACHE[video_id])
    log.info(f"Updated stream_sources to {value} for {video_id}")

def db_save_lossless_source(video_id: str, value: str = "4:flac"):
    name = ""
    artists = ""
    with DB_CACHE_LOCK:
        existing = DB_CACHE.get(video_id)
        if existing:
            name = existing.get("name") or ""
            artists = existing.get("artists") or ""
            existing["lossless"] = value
        else:
            existing = None
    
    if not name or name == "Unknown Song" or not artists:
        name, artists = fetch_correct_metadata(video_id)
        
    with DB_CACHE_LOCK:
        if video_id in DB_CACHE:
            DB_CACHE[video_id]["lossless"] = value
            if name and name != "Unknown Song":
                DB_CACHE[video_id]["name"] = name
            if artists and artists != "Unknown Artist":
                DB_CACHE[video_id]["artists"] = artists
        else:
            DB_CACHE[video_id] = {
                "id": video_id,
                "name": name,
                "artists": artists,
                "lossless": value
            }
        if hasattr(db_service, "DB_CACHE") and db_service.DB_CACHE is not DB_CACHE:
            db_service.DB_CACHE[video_id] = dict(DB_CACHE[video_id])
    log.info(f"Updated lossless to '{value}' for {video_id}")

def process_stream_job(video_id: str) -> bool:
    STREAM_JOBS[video_id] = {"status": "progressing", "timestamp": time.time()}
    STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
    
    for instance in INVIDIOUS_INSTANCES:
        try:
            log.info(f"Attempting download/upload for {video_id} using {instance}")
            with httpx.Client(timeout=30.0) as client:
                url = f"{instance.rstrip('/')}/api/v1/videos/{video_id}"
                if not is_safe_domain_url(url):
                    log.error(f"SECURITY: Blocked attempt to contact direct IP: {url}")
                    continue
                resp = client.get(url)
                if resp.status_code != 200:
                    continue
                try:
                    data = resp.json()
                except ValueError:
                    continue
                formats = data.get("adaptiveFormats", [])
                m4a_formats = [f for f in formats if f.get("type", "").startswith("audio/") and ("mp4" in f.get("type", "") or f.get("container") == "m4a")]
                if m4a_formats:
                    m4a_formats.sort(key=lambda x: int(x.get("bitrate") or 0), reverse=True)
                    selected_format = m4a_formats[0]
                else:
                    other_formats = [f for f in formats if f.get("type", "").startswith("audio/")]
                    if other_formats:
                        other_formats.sort(key=lambda x: int(x.get("bitrate") or 0), reverse=True)
                        selected_format = other_formats[0]
                    else:
                        selected_format = None
                if not selected_format:
                    continue
                original_url = selected_format.get("url")
                container = selected_format.get("container")
                if not container:
                    mime_type = selected_format.get("type", "")
                    container = "webm" if "webm" in mime_type else "m4a"
                ext = container or "m4a"
                parsed = urllib.parse.urlparse(original_url)
                original_host = parsed.netloc
                proxied_url = f"{instance.rstrip('/')}/videoplayback?{parsed.query}&host={original_host}"
                if not is_safe_domain_url(proxied_url):
                    log.error(f"SECURITY: Blocked attempt to stream from direct IP: {proxied_url}")
                    continue
                t0 = time.time()
                stream_resp = client.get(proxied_url)
                if stream_resp.status_code != 200:
                    stream_resp = client.get(f"{instance.rstrip('/')}/stream/{video_id}")
                    if stream_resp.status_code != 200:
                        continue
                    if stream_resp.headers.get("x-audio-extension"):
                        ext = stream_resp.headers.get("x-audio-extension")
                audio_bytes = stream_resp.content
                t1 = time.time()
                dl_duration = t1 - t0
                if dl_duration > 0:
                    NETWORK_STATS["last_download_speed_kbps"] = len(audio_bytes) / dl_duration / 1024.0
                NETWORK_STATS["total_downloaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
                
            t0 = time.time()
            from db_service import safe_batch_bucket_files
            safe_batch_bucket_files(BUCKET_ID, add=[(audio_bytes, f"{video_id}.{ext}")], token=HF_TOKEN)
            t1 = time.time()
            ul_duration = t1 - t0
            if ul_duration > 0:
                NETWORK_STATS["last_upload_speed_kbps"] = len(audio_bytes) / ul_duration / 1024.0
            NETWORK_STATS["total_uploaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
            
            # Explicit memory and disk cache cleanup
            del audio_bytes
            db_service.cleanup_upload_cache()

            db_save_stream_source(video_id, ext)
            with DB_CACHE_LOCK:
                STREAM_JOBS[video_id] = {"status": "done", "timestamp": time.time(), "ext": ext}
                if len(STREAM_JOBS) > 500:
                    STREAM_JOBS.popitem(last=False)
            STATS["done"] += 1
            STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
            UPLOAD_TIMESTAMPS.append(time.time())
            log.info(f"Stream upload successfully completed for {video_id}.{ext}")
            return True
        except Exception as e:
            if 'audio_bytes' in locals():
                del audio_bytes
            db_service.cleanup_upload_cache()
            if not isinstance(e, (httpx.HTTPError, httpx.NetworkError, httpx.TimeoutException, ValueError, KeyError)):
                err_msg = f"{type(e).__name__} for {video_id}: {str(e)}"
                if err_msg not in FATAL_ERRORS:
                    FATAL_ERRORS.append(err_msg)
            continue

    log.info(f"Attempting ultimate fallback for {video_id}...")
    ext = "m4a"
    try:
        fallback_url = f"https://effective-disco-3nfb.onrender.com/api/stream/{video_id}"
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            t0 = time.time()
            resp = client.get(fallback_url)
            if resp.status_code == 200:
                audio_bytes = resp.content
                t1 = time.time()
                dl_duration = t1 - t0
                if dl_duration > 0:
                    NETWORK_STATS["last_download_speed_kbps"] = len(audio_bytes) / dl_duration / 1024.0
                NETWORK_STATS["total_downloaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
                ext = "webm" if "webm" in resp.headers.get("content-type", "") else "m4a"
                t0_up = time.time()
                from db_service import safe_batch_bucket_files
                safe_batch_bucket_files(BUCKET_ID, add=[(audio_bytes, f"{video_id}.{ext}")], token=HF_TOKEN)
                t1_up = time.time()
                ul_duration = t1_up - t0_up
                if ul_duration > 0:
                    NETWORK_STATS["last_upload_speed_kbps"] = len(audio_bytes) / ul_duration / 1024.0
                NETWORK_STATS["total_uploaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
                
                del audio_bytes
                db_service.cleanup_upload_cache()

                db_save_stream_source(video_id, ext)
                with DB_CACHE_LOCK:
                    STREAM_JOBS[video_id] = {"status": "done", "timestamp": time.time(), "ext": ext}
                    if len(STREAM_JOBS) > 500:
                        STREAM_JOBS.popitem(last=False)
                STATS["done"] += 1
                STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
                UPLOAD_TIMESTAMPS.append(time.time())

                # Enqueue lossless job to worker queue instead of spawning thread per request
                with LOSSLESS_QUEUED_LOCK:
                    if video_id not in LOSSLESS_QUEUED_SET:
                        LOSSLESS_QUEUED_SET.add(video_id)
                        with LOSSLESS_JOBS_LOCK:
                            LOSSLESS_JOBS[video_id] = {"status": "queued", "timestamp": time.time()}
                        LOSSLESS_TASK_QUEUE.put(video_id, high_priority=False)
                        LOSSLESS_STATS["queued"] += 1
                        start_lossless_workers_if_needed()

                return True
    except Exception as fallback_ex:
        if 'audio_bytes' in locals():
            del audio_bytes
        db_service.cleanup_upload_cache()
        log.error(f"Ultimate fallback failed for {video_id}: {fallback_ex}")

    with DB_CACHE_LOCK:
        STREAM_JOBS[video_id] = {"status": "failed", "error": "All instances failed", "timestamp": time.time()}
        if len(STREAM_JOBS) > 500:
            STREAM_JOBS.popitem(last=False)
    STATS["failed"] += 1
    STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
    return False

# Dedicated Lossless Queue for Backup sequence in app.py
LOSSLESS_TASK_QUEUE = PriorityTaskQueue()
LOSSLESS_QUEUED_SET = set()
LOSSLESS_QUEUED_LOCK = threading.Lock()
LOSSLESS_JOBS_LOCK = threading.Lock()
LOSSLESS_STATS = {"queued": 0, "progressing": 0, "done": 0, "failed": 0}
LOSSLESS_JOBS = {}  # video_id -> {"status": "queued"|"progressing"|"done"|"failed", "timestamp": float}
FAILED_LOSSLESS_IDS = set()
FAILED_LOSSLESS_LOCK = threading.Lock()

def lossless_worker_loop():
    while True:
        try:
            video_id = LOSSLESS_TASK_QUEUE.get()
            if video_id is None:
                continue
            with LOSSLESS_JOBS_LOCK:
                LOSSLESS_JOBS[video_id] = {"status": "progressing", "timestamp": time.time()}
                if len(LOSSLESS_JOBS) > 1000:
                    # Prune oldest job entry to preserve RAM
                    oldest = next(iter(LOSSLESS_JOBS))
                    LOSSLESS_JOBS.pop(oldest, None)
            LOSSLESS_STATS["progressing"] += 1
            if LOSSLESS_STATS["queued"] > 0:
                LOSSLESS_STATS["queued"] -= 1
            try:
                name, artists = "", ""
                with db_service.DB_CACHE_LOCK:
                    row = db_service.DB_CACHE.get(video_id) or {}
                    name = row.get("name") or ""
                    artists = row.get("artists") or ""
                if not name or name == "Unknown Song":
                    name, artists = db_service.fetch_correct_metadata(video_id)
                success = qobuz_service.process_qobuz_lossless(video_id, name, artists)
                if isinstance(success, dict) and success.get("success"):
                    ext = success.get("ext", "flac")
                    db_service.db_save_lossless_source(video_id, f"4:{ext}")
                    with LOSSLESS_JOBS_LOCK:
                        LOSSLESS_JOBS[video_id] = {"status": "done", "timestamp": time.time(), "ext": ext}
                    LOSSLESS_STATS["done"] += 1
                else:
                    with FAILED_LOSSLESS_LOCK:
                        FAILED_LOSSLESS_IDS.add(video_id)
                    with LOSSLESS_JOBS_LOCK:
                        LOSSLESS_JOBS[video_id] = {"status": "failed", "timestamp": time.time()}
                    LOSSLESS_STATS["failed"] += 1
            except Exception as ex:
                with FAILED_LOSSLESS_LOCK:
                    FAILED_LOSSLESS_IDS.add(video_id)
                log.error(f"Lossless worker error for {video_id}: {ex}")
                with LOSSLESS_JOBS_LOCK:
                    LOSSLESS_JOBS[video_id] = {"status": "failed", "timestamp": time.time()}
                LOSSLESS_STATS["failed"] += 1
            finally:
                if LOSSLESS_STATS["progressing"] > 0:
                    LOSSLESS_STATS["progressing"] -= 1
                with LOSSLESS_QUEUED_LOCK:
                    LOSSLESS_QUEUED_SET.discard(video_id)
                LOSSLESS_TASK_QUEUE.task_done()
        except Exception as e:
            log.error(f"Error in lossless worker thread loop: {e}")
            time.sleep(1)

LOSSLESS_WORKER_THREADS = []
NUM_LOSSLESS_WORKERS = 50

def start_lossless_workers_if_needed():
    global LOSSLESS_WORKER_THREADS
    alive_threads = [t for t in LOSSLESS_WORKER_THREADS if t.is_alive()]
    if len(alive_threads) < NUM_LOSSLESS_WORKERS:
        needed = NUM_LOSSLESS_WORKERS - len(alive_threads)
        for i in range(needed):
            t = threading.Thread(target=lossless_worker_loop, daemon=True, name=f"LosslessWorker-{len(alive_threads)+i+1}")
            t.start()
            LOSSLESS_WORKER_THREADS.append(t)
        log.info(f"Started {needed} parallel lossless worker threads (total active: {NUM_LOSSLESS_WORKERS}).")

# --- Lossless Cleanup & Audit System State ---
CLEANUP_STATE = {
    "running": False,
    "total": 0,
    "checked": 0,
    "valid": 0,
    "invalid": 0,
    "errors": 0,
    "started_at": None,
    "last_updated": None,
    "recent_invalid": [],
}
CLEANUP_LOCK = threading.Lock()

def is_cleanup_active() -> bool:
    with CLEANUP_LOCK:
        return CLEANUP_STATE["running"]

def cleanup_worker_task(flac_server_url: str = "https://beautiful-junie-scoder-53c5187c.koyeb.app"):
    """Background worker that audits all 108,000+ files directly from HF bucket & JSON cache in continuous batches of 50."""
    global CLEANUP_STATE
    log.info(f"Starting Complete Lossless Cleanup against {flac_server_url}...")

    # 1. Fetch file list from HF lossless bucket (108,000+ files)
    bucket_ids = set()
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_TOKEN)
        log.info("Listing all files in HF bucket 'shashwatIDR/lossless' (108,000+ files)...")
        for item in api.list_bucket_tree("shashwatIDR/lossless"):
            fname = getattr(item, "path", "") or str(item)
            if fname.endswith(".flac") or fname.endswith(".mp3"):
                vid = fname.rsplit(".", 1)[0]
                if len(vid) == 11 or len(vid) > 0:
                    bucket_ids.add(vid)
        log.info(f"Successfully retrieved {len(bucket_ids)} tracks directly from HF lossless bucket!")
    except Exception as b_ex:
        log.error(f"Error reading HF lossless bucket tree: {b_ex}")

    # 2. Merge with DB_CACHE IDs
    with DB_CACHE_LOCK:
        for s in DB_CACHE.values():
            if s.get("id"):
                bucket_ids.add(s["id"])

    # Build tracks_to_check
    with DB_CACHE_LOCK:
        tracks_to_check = []
        for vid in bucket_ids:
            s = DB_CACHE.get(vid, {})
            tracks_to_check.append({
                "id": vid,
                "name": s.get("name", ""),
                "artists": s.get("artists", ""),
                "lossless": s.get("lossless")
            })

    total_len = len(tracks_to_check)
    log.info(f"[CLEANUP] Total dataset loaded for audit: {total_len} tracks.")

    with CLEANUP_LOCK:
        CLEANUP_STATE["running"] = True
        CLEANUP_STATE["total"] = total_len
        CLEANUP_STATE["checked"] = 0
        CLEANUP_STATE["valid"] = 0
        CLEANUP_STATE["invalid"] = 0
        CLEANUP_STATE["errors"] = 0
        CLEANUP_STATE["started_at"] = time.time()
        CLEANUP_STATE["recent_invalid"] = []
        CLEANUP_STATE["recent_valid"] = []

    def check_single(item):
        vid = item["id"]
        url = f"{flac_server_url.rstrip('/')}/?id={vid}"
        try:
            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                resp = client.get(url)
                
                # Check if FLAC server returned streams for this video ID
                if resp.status_code == 200:
                    data = resp.json().catch(lambda: {}) if resp.headers.get("content-type", "").startswith("application/json") else {}
                    streams = data.get("streams", {})
                    matched = data.get("matched_track", {})
                    yt = data.get("ytmusic", {})

                    # Valid if at least one stream is returned
                    if streams and any(v.get("url") for v in streams.values()):
                        correct_title = matched.get("title") or yt.get("title") or item.get("name") or "Verified FLAC"
                        correct_artist = matched.get("artist") or yt.get("artist") or item.get("artists") or "Verified Artist"
                        
                        # Update DB / JSON cache with genuine metadata from Qobuz / YouTube
                        db_service.db_save_lossless_source(vid, "4:flac")
                        with DB_CACHE_LOCK:
                            if vid in DB_CACHE:
                                if correct_title: DB_CACHE[vid]["name"] = correct_title
                                if correct_artist: DB_CACHE[vid]["artists"] = correct_artist

                        with CLEANUP_LOCK:
                            CLEANUP_STATE["checked"] += 1
                            CLEANUP_STATE["valid"] += 1
                            CLEANUP_STATE["last_updated"] = time.time()
                            if len(CLEANUP_STATE.get("recent_valid", [])) < 50:
                                CLEANUP_STATE.setdefault("recent_valid", []).append({
                                    "id": vid,
                                    "name": correct_title,
                                    "artists": correct_artist,
                                    "matched": correct_title,
                                    "timestamp": time.time()
                                })
                        return
                
                # If HTTP 404 or no streams available for this ID -> remove and mark as "0"
                db_service.db_remove_lossless_source(vid, mark_unavailable=True)
                with CLEANUP_LOCK:
                    CLEANUP_STATE["checked"] += 1
                    CLEANUP_STATE["invalid"] += 1
                    CLEANUP_STATE["last_updated"] = time.time()
                    CLEANUP_STATE.setdefault("recent_invalid", [])
                    if len(CLEANUP_STATE["recent_invalid"]) > 100:
                        CLEANUP_STATE["recent_invalid"].pop(0)
                    CLEANUP_STATE["recent_invalid"].append({
                        "id": vid,
                        "name": item.get("name") or "Track " + vid,
                        "artists": item.get("artists") or "Unknown",
                        "timestamp": time.time()
                    })

        except Exception as e:
            with CLEANUP_LOCK:
                CLEANUP_STATE["checked"] += 1
                CLEANUP_STATE["errors"] += 1
                CLEANUP_STATE["last_updated"] = time.time()

    # Process in continuous batches of 50 workers
    from concurrent.futures import ThreadPoolExecutor
    batch_size = 50
    invalid_files_to_delete = []

    for i in range(0, total_len, batch_size):
        batch = tracks_to_check[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=50) as executor:
            list(executor.map(check_single, batch))

        # Collect invalid files to purge from bucket
        with CLEANUP_LOCK:
            current_invalid = [it["id"] for it in CLEANUP_STATE.get("recent_invalid", [])]
        for vid in current_invalid:
            flac_name = f"{vid}.flac"
            mp3_name = f"{vid}.mp3"
            if flac_name not in invalid_files_to_delete: invalid_files_to_delete.append(flac_name)
            if mp3_name not in invalid_files_to_delete: invalid_files_to_delete.append(mp3_name)

        # Batch delete every 500 tracks from Hugging Face lossless bucket
        if len(invalid_files_to_delete) >= 100:
            to_del = invalid_files_to_delete[:]
            invalid_files_to_delete.clear()
            db_service.delete_files_from_lossless_bucket(to_del)

        # Periodic incremental sync to Hugging Face json every 500 tracks
        if (i + batch_size) % 500 == 0:
            db_service.trigger_immediate_backup()

    # Final purge of remaining invalid files from HF bucket
    if invalid_files_to_delete:
        db_service.delete_files_from_lossless_bucket(invalid_files_to_delete)

    # Final sync complete JSON cache to db_backup.json on Hugging Face
    db_service.trigger_immediate_backup()

    with CLEANUP_LOCK:
        CLEANUP_STATE["running"] = False
        CLEANUP_STATE["last_updated"] = time.time()
    log.info(f"Lossless Cleanup Completed! Total: {total_len}, Valid: {CLEANUP_STATE['valid']}, Invalid: {CLEANUP_STATE['invalid']}, Errors: {CLEANUP_STATE['errors']}")

def start_lossless_cleanup(flac_server_url: str = "https://beautiful-junie-scoder-53c5187c.koyeb.app"):
    with CLEANUP_LOCK:
        if CLEANUP_STATE["running"]:
            return False, "Cleanup is already in progress"
        CLEANUP_STATE["running"] = True

    t = threading.Thread(target=cleanup_worker_task, args=(flac_server_url,), daemon=True, name="LosslessCleanupWorker")
    t.start()
    return True, "Lossless audit and cleanup process initiated"

def auto_lossless_sync_loop():
    """Background daemon loop that continuously ensures all DB songs missing lossless audio remain enqueued."""
    log.info("Starting automatic lossless sync daemon thread...")
    time.sleep(10)  # initial delay for cache loading
    while True:
        try:
            if not is_cleanup_active() and LOSSLESS_TASK_QUEUE.qsize() < 25:
                trigger_lossless_backup()
        except Exception as e:
            log.error(f"Error in auto_lossless_sync_loop: {e}")
        time.sleep(5)  # Re-check every 5 seconds continuously

def trigger_lossless_backup() -> int:
    if is_cleanup_active():
        log.info("Skipping lossless backup enqueueing: Cleanup process is currently active.")
        return 0

    if LOSSLESS_TASK_QUEUE.qsize() >= 50:
        return 0

    with DB_CACHE_LOCK:
        missing_ids = [vid for vid, song in DB_CACHE.items() if not song.get("lossless") or str(song.get("lossless")).strip() in ("", "0")]

    count = 0
    with LOSSLESS_QUEUED_LOCK:
        for vid in missing_ids:
            if vid not in LOSSLESS_QUEUED_SET and vid not in FAILED_LOSSLESS_IDS:
                LOSSLESS_QUEUED_SET.add(vid)
                with LOSSLESS_JOBS_LOCK:
                    LOSSLESS_JOBS[vid] = {"status": "queued", "timestamp": time.time()}
                LOSSLESS_TASK_QUEUE.put(vid, high_priority=False)
                count += 1
                if LOSSLESS_TASK_QUEUE.qsize() >= 50:
                    break

    LOSSLESS_STATS["queued"] = LOSSLESS_TASK_QUEUE.qsize()
    if count > 0:
        log.info(f"Lossless Backup: Enqueued {count} songs missing lossless audio.")

    start_lossless_workers_if_needed()
    return count


def worker_loop():
    while True:
        try:
            video_id = TASK_QUEUE.get()
            if video_id is None:
                continue
            try:
                process_stream_job(video_id)
            finally:
                with QUEUED_SET_LOCK:
                    QUEUED_SET.discard(video_id)
                TASK_QUEUE.task_done()
        except Exception as e:
            log.error(f"Error in worker thread: {e}")
            time.sleep(1)

def enqueue_song(video_id: str, high_priority: bool = True) -> bool:
    enqueued = False

    # Check if standard stream and lossless are already available in DB_CACHE
    with DB_CACHE_LOCK:
        row = DB_CACHE.get(video_id) or {}
        has_stream = bool(row.get("stream_sources"))
        lossless_val = str(row.get("lossless") or "").strip()
        has_lossless = bool(lossless_val and lossless_val != "0")

    # Only enqueue standard stream if NOT already cached/done
    if not has_stream:
        with QUEUED_SET_LOCK:
            is_progressing = (video_id in STREAM_JOBS and STREAM_JOBS[video_id].get("status") == "progressing")
            if high_priority or video_id not in QUEUED_SET:
                if not is_progressing:
                    QUEUED_SET.add(video_id)
                    STREAM_JOBS[video_id] = {"status": "queued", "timestamp": time.time()}
                    STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
                    TASK_QUEUE.put(video_id, high_priority=high_priority)
                    enqueued = True

    # Only enqueue into Lossless queue if NOT already cached/done
    if not has_lossless:
        with LOSSLESS_QUEUED_LOCK:
            is_lossless_progressing = False
            with LOSSLESS_JOBS_LOCK:
                is_lossless_progressing = (video_id in LOSSLESS_JOBS and LOSSLESS_JOBS[video_id].get("status") == "progressing")
            if high_priority or video_id not in LOSSLESS_QUEUED_SET:
                if not is_lossless_progressing:
                    LOSSLESS_QUEUED_SET.add(video_id)
                    with LOSSLESS_JOBS_LOCK:
                        LOSSLESS_JOBS[video_id] = {"status": "queued", "timestamp": time.time()}
                    LOSSLESS_TASK_QUEUE.put(video_id, high_priority=high_priority)
                    LOSSLESS_STATS["queued"] = LOSSLESS_TASK_QUEUE.qsize()
                    enqueued = True
                    start_lossless_workers_if_needed()

    return enqueued

def enqueue_songs_batch(video_ids: list[str]) -> list[str]:
    added = []
    for vid in video_ids:
        if enqueue_song(vid, high_priority=False):
            added.append(vid)
    return added

def start_queue_workers():
    global WORKER_THREADS
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=worker_loop, daemon=True, name=f"StreamWorker-{i+1}")
        t.start()
        WORKER_THREADS.append(t)
    log.info(f"Started {NUM_WORKERS} background stream processing worker threads.")

def db_get_song_stream(video_id: str) -> Optional[dict]:
    with DB_CACHE_LOCK:
        return DB_CACHE.get(video_id)

app = FastAPI(title="Muzo Stream Service", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def memory_cleanup_daemon():
    """Periodic daemon thread to collect garbage and keep RAM footprint minimal."""
    while True:
        try:
            time.sleep(120)
            gc.collect()
        except Exception:
            pass

def auto_stream_sync_loop():
    """Background daemon loop that continuously feeds DB songs missing stream sources into TASK_QUEUE."""
    log.info("Starting automatic stream queue feeder daemon thread...")
    time.sleep(15)
    while True:
        try:
            if TASK_QUEUE.qsize() < 100:
                with DB_CACHE_LOCK:
                    missing = [vid for vid, song in DB_CACHE.items() if not song.get("stream_sources")]
                with QUEUED_SET_LOCK:
                    for vid in missing:
                        if vid not in QUEUED_SET:
                            enqueue_song(vid, high_priority=False)
                            if TASK_QUEUE.qsize() >= 100:
                                break
        except Exception as e:
            log.error(f"Error in auto_stream_sync_loop: {e}")
        time.sleep(10)

@app.on_event("startup")
def startup_event():
    init_db()
    start_queue_workers()
    backup_thread = threading.Thread(target=db_backup_loop, daemon=True)
    backup_thread.start()
    stream_feeder_thread = threading.Thread(target=auto_stream_sync_loop, daemon=True)
    stream_feeder_thread.start()
    lossless_sync_thread = threading.Thread(target=auto_lossless_sync_loop, daemon=True)
    lossless_sync_thread.start()
    gc_thread = threading.Thread(target=memory_cleanup_daemon, daemon=True)
    gc_thread.start()

def trigger_startup_sync():
    log.info("Automatic startup sync disabled by configuration.")

def get_admin_html() -> str:
    try:
        import admin
        return admin.get_admin_html()
    except ImportError:
        pass
    except Exception:
        pass
    try:
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        admin_path = os.path.join(curr_dir, "admin.py")
        if os.path.exists(admin_path):
            import importlib.util
            spec = importlib.util.spec_from_file_location("admin_module", admin_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod.get_admin_html()
    except Exception as ex:
        log.error(f"Failed dynamic import of admin.py: {ex}")
    return ADMIN_HTML_EMBEDDED

ADMIN_HTML_EMBEDDED = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Muzo Stream Admin Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Bungee&family=Space+Mono:wght@400;700&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #14100c;
      --bg-raised: #1c1610;
      --metal: #2b241d;
      --metal-light: #3c3227;
      --glass: #1c1712;
      --glass-deep: #110d0a;
      --cream: #f4e9d2;
      --cream-dim: #c9b78f;
      --ink: #241c14;
      --amber: #ffb733;
      --amber-soft: rgba(255, 183, 51, 0.14);
      --amber-glow: rgba(255, 183, 51, 0.55);
      --cyan: #63e6ea;
      --cyan-soft: rgba(99, 230, 234, 0.14);
      --cyan-glow: rgba(99, 230, 234, 0.55);
      --red: #c1483c;
      --red-soft: rgba(193, 72, 60, 0.16);
      --rule: rgba(244, 233, 210, 0.10);
      --rule-strong: rgba(244, 233, 210, 0.20);
      --font-display: "Bungee", system-ui, sans-serif;
      --font-body: "Manrope", -apple-system, sans-serif;
      --font-mono: "Space Mono", monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      color: var(--cream);
      font-family: var(--font-body);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      padding: 1.5rem;
    }
    header {
      background: var(--metal);
      border: 2px solid var(--rule-strong);
      border-radius: 12px;
      padding: 1.2rem 1.8rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 1.5rem;
    }
    header h1 {
      font-family: var(--font-display);
      color: var(--amber);
      font-size: 1.5rem;
    }
    .badge {
      background: var(--glass-deep);
      border: 1px solid var(--rule-strong);
      padding: 0.4rem 0.8rem;
      border-radius: 6px;
      font-family: var(--font-mono);
      font-size: 0.8rem;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 1.2rem;
      margin-bottom: 1.5rem;
    }
    .card {
      background: var(--bg-raised);
      border: 1px solid var(--rule-strong);
      border-radius: 10px;
      padding: 1.2rem;
    }
    .card h3 {
      font-family: var(--font-mono);
      font-size: 0.85rem;
      color: var(--cream-dim);
      text-transform: uppercase;
      margin-bottom: 0.8rem;
    }
    .stat-val {
      font-size: 1.8rem;
      font-weight: 800;
      color: var(--cyan);
    }
    .progress-bar-bg {
      background: var(--glass-deep);
      border-radius: 6px;
      height: 12px;
      width: 100%;
      overflow: hidden;
      margin: 0.8rem 0;
      border: 1px solid var(--rule);
    }
    .progress-bar-fill {
      background: linear-gradient(90deg, var(--amber), var(--cyan));
      height: 100%;
      width: 0%;
      transition: width 0.4s ease;
    }
    .section-title {
      font-family: var(--font-display);
      font-size: 1.1rem;
      color: var(--amber);
      margin-bottom: 0.8rem;
    }
    .tables-container {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1.5rem;
      margin-bottom: 1.5rem;
    }
    @media(max-width: 900px) { .tables-container { grid-template-columns: 1fr; } }
    .queue-table {
      width: 100%;
      border-collapse: collapse;
      font-family: var(--font-mono);
      font-size: 0.8rem;
    }
    .queue-table th, .queue-table td {
      border: 1px solid var(--rule);
      padding: 0.6rem;
      text-align: left;
    }
    .queue-table th {
      background: var(--metal);
      color: var(--amber);
    }
    .status-queued { color: var(--amber); font-weight: bold; }
    .status-progressing { color: var(--cyan); font-weight: bold; }
    .status-done { color: #50fa7b; font-weight: bold; }
    .status-failed { color: var(--red); font-weight: bold; }
    .log-box {
      background: var(--glass-deep);
      border: 1px solid var(--rule-strong);
      border-radius: 8px;
      padding: 1rem;
      font-family: var(--font-mono);
      font-size: 0.75rem;
      height: 250px;
      overflow-y: auto;
      white-space: pre-wrap;
    }
    .btn {
      background: var(--amber);
      color: var(--ink);
      border: none;
      padding: 0.5rem 1rem;
      border-radius: 6px;
      font-weight: 700;
      font-family: var(--font-mono);
      cursor: pointer;
    }
    .btn:hover { background: #ffa000; }
    .btn-cyan {
      background: var(--cyan);
      color: var(--glass-deep);
    }
    .btn-cyan:hover { background: #40c4c8; }
  </style>
</head>
<body>
  <header>
    <h1>Muzo Admin Dashboard</h1>
    <div class="badge">Workers: <strong id="worker-count">10 Active Threads</strong></div>
  </header>

  <div class="grid">
    <div class="card">
      <h3>Overall Queue Progress</h3>
      <div class="stat-val" id="progress-percent">0%</div>
      <div class="progress-bar-bg">
        <div class="progress-bar-fill" id="progress-fill"></div>
      </div>
      <div style="font-size: 0.8rem; color: var(--cream-dim);">
        Processed <span id="done-count">0</span> / <span id="total-count">0</span> songs
      </div>
    </div>
    <div class="card">
      <h3>Queue State & Controls</h3>
      <div style="display: flex; justify-content: space-between; margin-top: 0.5rem;">
        <div>In Queue: <strong id="val-queued" style="color: var(--amber);">0</strong></div>
        <div>Progressing: <strong id="val-progressing" style="color: var(--cyan);">0</strong></div>
        <div>Failed: <strong id="val-failed" style="color: var(--red);">0</strong></div>
      </div>
      <div style="margin-top: 1rem; display: flex; gap: 0.5rem; flex-wrap: wrap;">
        <button class="btn" onclick="triggerSync()">Trigger Bulk Sync</button>
        <button class="btn btn-cyan" onclick="triggerLosslessBackup()">Start Lossless Backup</button>
        <button class="btn" id="reconstruct-btn" style="background: rgba(255, 183, 51, 0.15); border-color: var(--amber); color: var(--amber);" onclick="triggerDbReconstruct()">🔄 Reconstruct DB from Buckets</button>
      </div>
    </div>
    <div class="card">
      <h3>Lossless Audio Stats</h3>
      <div class="stat-val" id="lossless-count" style="color: var(--amber);">0</div>
      <div style="font-size: 0.8rem; color: var(--cream-dim); margin-top: 0.2rem;">
        Total Lossless Songs Cached
      </div>
      <div style="display: flex; justify-content: space-between; margin-top: 0.8rem; font-size: 0.8rem;">
        <div>Lossless Queue: <strong id="lossless-queued" style="color: var(--amber);">0</strong></div>
        <div>Active: <strong id="lossless-progressing" style="color: var(--cyan);">0</strong></div>
      </div>
    </div>
    <div class="card">
      <h3>Transfer Rates</h3>
      <div style="font-size: 0.85rem; margin-bottom: 0.4rem;">Upload Rate: <strong id="upload-rate">0.0 / min</strong></div>
      <div style="font-size: 0.85rem; margin-bottom: 0.4rem;">DL Speed: <strong id="dl-speed">0 KB/s</strong></div>
      <div style="font-size: 0.85rem;">UL Speed: <strong id="ul-speed">0 KB/s</strong></div>
    </div>
  </div>

  <div class="tables-container">
    <div class="card">
      <div class="section-title">Pending Queue IDs (<span id="pending-count">0</span>)</div>
      <div class="log-box" id="queued-ids-box" style="height: 250px;">No pending IDs in queue</div>
    </div>

    <div class="card">
      <div class="section-title">Active & Recent Stream Jobs</div>
      <div style="max-height: 250px; overflow-y: auto;">
        <table class="queue-table">
          <thead>
            <tr>
              <th>Video ID</th>
              <th>Status</th>
              <th>Format</th>
            </tr>
          </thead>
          <tbody id="jobs-tbody">
            <tr><td colspan="3" style="text-align: center; color: var(--cream-dim);">No active jobs</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card" style="margin-bottom: 1.5rem;">
    <div class="section-title" style="color: var(--cyan);">Active & Recent Lossless Jobs</div>
    <div style="max-height: 250px; overflow-y: auto;">
      <table class="queue-table">
        <thead>
          <tr>
            <th>Video ID</th>
            <th>Status</th>
            <th>Format</th>
          </tr>
        </thead>
        <tbody id="lossless-jobs-tbody">
          <tr><td colspan="3" style="text-align: center; color: var(--cream-dim);">No active lossless jobs</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div class="card" style="margin-bottom: 1.5rem;">
    <div class="section-title">Live Server Logs</div>
    <div class="log-box" id="log-box" style="height: 220px;">Loading logs...</div>
  </div>

  <script>
    async function triggerSync() {
      try {
        const res = await fetch('/api/stream/sync', { method: 'POST' });
        const data = await res.json();
        alert(data.message);
      } catch (e) {
        alert("Failed to start sync");
      }
    }

    async function triggerLosslessBackup() {
      try {
        const res = await fetch('/api/lossless/sync', { method: 'POST' });
        const data = await res.json();
        alert(data.message);
      } catch (e) {
        alert("Failed to start lossless backup");
      }
    }

    async function triggerDbReconstruct() {
      if (!confirm("Reconstruct database from all audio files in stream & lossless buckets?")) return;
      const btn = document.getElementById('reconstruct-btn');
      if (btn) {
        btn.disabled = true;
        btn.innerText = "⏳ Reconstructing...";
      }
      try {
        const res = await fetch('/api/db/reconstruct', { method: 'POST' });
        const data = await res.json();
        const d = data.result || data.details || {};
        alert(`Reconstruction Complete!\nStream Tracks: ${d.stream_tracks_indexed || 0}\nLossless Tracks: ${d.lossless_tracks_indexed || 0}\nTotal in Cache: ${d.total_songs_in_cache || 0}`);
      } catch (e) {
        alert("Reconstruction failed: " + e.message);
      } finally {
        if (btn) {
          btn.disabled = false;
          btn.innerText = "🔄 Reconstruct DB from Buckets";
        }
      }
    }

    async function updateAdminWithData(data) {
      const total = data.total || 0;
      const done = data.stream || 0;
      const pct = total > 0 ? ((done / total) * 100).toFixed(1) : 0;
      document.getElementById('progress-percent').textContent = pct + '%';
      document.getElementById('progress-fill').style.width = pct + '%';
      document.getElementById('done-count').textContent = done;
      document.getElementById('total-count').textContent = total;

      if (data.lossless !== undefined) {
        document.getElementById('lossless-count').textContent = data.lossless;
      }
      if (data.lossless_stats) {
        document.getElementById('lossless-queued').textContent = data.lossless_stats.queued || 0;
        document.getElementById('lossless-progressing').textContent = data.lossless_stats.progressing || 0;
      }

      if (data.stats) {
        document.getElementById('val-progressing').textContent = data.stats.progressing || 0;
        document.getElementById('val-failed').textContent = data.stats.failed || 0;
        const inQueue = data.queue_length || 0;
        document.getElementById('val-queued').textContent = inQueue;
      }

      if (data.queued_ids) {
        document.getElementById('pending-count').textContent = data.queued_ids.length;
        const queuedBox = document.getElementById('queued-ids-box');
        if (data.queued_ids.length === 0) {
          queuedBox.textContent = 'No pending IDs in queue';
        } else {
          queuedBox.textContent = data.queued_ids.join('\n');
        }
      }

      if (data.avg_uploads_per_min !== undefined) {
        document.getElementById('upload-rate').textContent = data.avg_uploads_per_min.toFixed(1) + ' / min';
      }

      if (data.network_stats) {
        const dl = data.network_stats.last_download_speed_kbps;
        document.getElementById('dl-speed').textContent = dl > 1024 ? (dl/1024).toFixed(1) + ' MB/s' : dl.toFixed(1) + ' KB/s';
        const ul = data.network_stats.last_upload_speed_kbps;
        document.getElementById('ul-speed').textContent = ul > 1024 ? (ul/1024).toFixed(1) + ' MB/s' : ul.toFixed(1) + ' KB/s';
      }

      if (data.jobs) {
        const entries = Object.entries(data.jobs).sort((a, b) => b[1].timestamp - a[1].timestamp).slice(0, 30);
        const tbody = document.getElementById('jobs-tbody');
        if (entries.length === 0) {
          tbody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: var(--cream-dim);">No active jobs</td></tr>';
        } else {
          tbody.innerHTML = entries.map(([id, job]) => {
            return `<tr>
              <td>${id}</td>
              <td class="status-${job.status}">${job.status}</td>
              <td>${job.ext || 'm4a'}</td>
            </tr>`;
          }).join('');
        }
      }

      if (data.lossless_jobs) {
        const lEntries = Object.entries(data.lossless_jobs).sort((a, b) => b[1].timestamp - a[1].timestamp).slice(0, 30);
        const ltbody = document.getElementById('lossless-jobs-tbody');
        if (ltbody) {
          if (lEntries.length === 0) {
            ltbody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: var(--cream-dim);">No active lossless jobs</td></tr>';
          } else {
            ltbody.innerHTML = lEntries.map(([id, job]) => {
              return `<tr>
                <td>${id}</td>
                <td class="status-${job.status}">${job.status}</td>
                <td>${job.ext || 'flac'}</td>
              </tr>`;
            }).join('');
          }
        }
      }

      if (data.logs) {
        const logBox = document.getElementById('log-box');
        logBox.textContent = data.logs.join('\n');
        logBox.scrollTop = logBox.scrollHeight;
      }
    }

    async function updateAdmin() {
      try {
        const res = await fetch('/api/stats');
        if (!res.ok) return;
        const data = await res.json();
        updateAdminWithData(data);
      } catch (e) {
        console.error("Error updating admin dashboard:", e);
      }
    }

    function initWebSocket() {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws/admin`;
      let ws = new WebSocket(wsUrl);

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          updateAdminWithData(data);
        } catch(e) {}
      };

      ws.onclose = () => {
        setTimeout(initWebSocket, 2000);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    initWebSocket();
    setInterval(updateAdmin, 5000);
    updateAdmin();
  </script>
</body>
</html>
"""

@app.get("/admin", response_class=HTMLResponse)
def get_admin_page():
    return HTMLResponse(content=get_admin_html())

@app.get("/api/stream")
def get_stream(id: str = Query(..., description="YouTube videoId")):
    # Only enqueue what is missing with high priority in background
    enqueue_song(id, high_priority=True)

    with DB_CACHE_LOCK:
        row = DB_CACHE.get(id)

    with LOSSLESS_JOBS_LOCK:
        lossless_done = (id in LOSSLESS_JOBS and LOSSLESS_JOBS[id].get("status") == "done")

    if row:
        name = row.get("name") or ""
        artists = row.get("artists") or ""
        stream_src = row.get("stream_sources") or ""
        lossless_src = str(row.get("lossless") or "").strip()

        if lossless_src == "0":
            lossless_val = 0
        elif lossless_src or lossless_done:
            lossless_val = f"https://velamhere-img.hf.space/lossless/{id}"
        else:
            lossless_val = "processing in lossless"

        return {
            "id": id,
            "name": name,
            "artist": artists,
            "url": f"https://velamhere-img.hf.space/song/{id}",
            "lossless": lossless_val
        }

    # Instant response on cache miss for new songs - workers will fetch stream and lossless in background immediately
    lossless_val = f"https://velamhere-img.hf.space/lossless/{id}" if lossless_done else "processing in lossless"
    return {
        "id": id,
        "name": "",
        "artist": "",
        "url": f"https://velamhere-img.hf.space/song/{id}",
        "lossless": lossless_val
    }

@app.api_route("/api/db/reconstruct", methods=["GET", "POST"])
def reconstruct_db():
    """Reconstruct and reconcile database cache directly against both physical storage buckets."""
    try:
        res = db_service.sync_cache_with_buckets(save_backup=True)
        load_db_stats()
        return {
            "message": "Reconstruction complete",
            "result": res
        }
    except Exception as e:
        log.error(f"Reconstruction endpoint failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/api/buckets/sync", methods=["GET", "POST"])
def sync_buckets_endpoint():
    """On-demand endpoint to reconcile in-memory cache with all physical bucket files and prune non-existing entries."""
    try:
        res = db_service.sync_cache_with_buckets(save_backup=True)
        load_db_stats()
        return {"status": "success", "result": res}
    except Exception as e:
        log.error(f"Bucket sync endpoint failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/stream/sync")
def sync_streams():
    try:
        with DB_CACHE_LOCK:
            song_ids = [song_id for song_id, song in DB_CACHE.items() if not song.get("stream_sources")]
        added = enqueue_songs_batch(song_ids)
        return {"message": f"Sync started: queued {len(added)} songs for processing."}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.api_route("/api/lossless/sync", methods=["GET", "POST"])
def sync_lossless_backup():
    try:
        count = trigger_lossless_backup()
        return {"message": f"Lossless bulk backup started: queued {count} songs missing lossless audio."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/api/stream/add", methods=["GET", "POST"])
def add_stream_to_queue(id: str = Query(..., description="YouTube videoId or comma-separated videoIds")):
    try:
        raw_ids = [v.strip() for v in id.split(",") if v.strip()]
        if not raw_ids:
            raise HTTPException(status_code=400, detail="No valid IDs provided")
        cached_ids, queued_ids = [], []
        for vid in raw_ids:
            row = db_get_song_stream(vid)
            if row and row.get("stream_sources"):
                cached_ids.append(vid)
            else:
                if enqueue_song(vid):
                    queued_ids.append(vid)
                else:
                    cached_ids.append(vid)
        return {
            "message": f"Processed {len(raw_ids)} ID(s). {len(queued_ids)} queued, {len(cached_ids)} already cached/queued.",
            "queued": queued_ids, "cached": cached_ids
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/api/lossless/cleanup", methods=["GET", "POST"])
def trigger_lossless_cleanup_endpoint(server: Optional[str] = None):
    flac_url = server or "https://beautiful-junie-scoder-53c5187c.koyeb.app"
    started, msg = start_lossless_cleanup(flac_url)
    if not started:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg, "status": "started", "server": flac_url}

@app.get("/api/stats")
def get_stats():
    total, lyrics, canvas, stream, lossless_count = 0, 0, 0, 0, 0
    try:
        with DB_CACHE_LOCK:
            total = len(DB_CACHE)
            for song in DB_CACHE.values():
                if song.get("lyrics_sources"): lyrics += 1
                if song.get("canvas_sources"): canvas += 1
                if song.get("stream_sources"): stream += 1
                lossless_val = str(song.get("lossless") or "").strip()
                if lossless_val and lossless_val != "0":
                    lossless_count += 1
    except Exception as e:
        log.error(f"Failed to calculate stats: {e}")

    with QUEUED_SET_LOCK:
        queued_ids_list = list(QUEUED_SET)

    with LOSSLESS_JOBS_LOCK:
        lossless_jobs_snapshot = {jid: {"status": job["status"], "timestamp": job["timestamp"], "ext": job.get("ext")} for jid, job in LOSSLESS_JOBS.items()}

    with CLEANUP_LOCK:
        cleanup_snapshot = dict(CLEANUP_STATE)
        cleanup_snapshot["recent_invalid"] = list(CLEANUP_STATE.get("recent_invalid", []))
        cleanup_snapshot["recent_valid"] = list(CLEANUP_STATE.get("recent_valid", []))

    return {
        "total": total, "lyrics": lyrics, "canvas": canvas, "stream": stream,
        "lossless": lossless_count, "lossless_stats": LOSSLESS_STATS,
        "stats": STATS, "queue_length": TASK_QUEUE.qsize(),
        "queued_ids": queued_ids_list,
        "avg_uploads_per_min": get_avg_uploads_per_min(), "network_stats": NETWORK_STATS,
        "fatal_errors": list(FATAL_ERRORS),
        "jobs": {jid: {"status": job["status"], "timestamp": job["timestamp"], "ext": job.get("ext")} for jid, job in STREAM_JOBS.items()},
        "lossless_jobs": lossless_jobs_snapshot,
        "cleanup": cleanup_snapshot,
        "logs": [mask_urls(line) for line in RECENT_LOGS]
    }

@app.websocket("/ws/admin")
async def websocket_admin(websocket: WebSocket):
    await websocket.accept()
    import asyncio
    try:
        while True:
            stats_data = get_stats()
            await websocket.send_json(stats_data)
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

@app.get("/api/search")
def search_songs(q: Optional[str] = None, limit: int = 12):
    try:
        results = []
        q_lower = q.lower() if q else None
        with DB_CACHE_LOCK:
            songs = list(DB_CACHE.values())
        if q_lower:
            for s in songs:
                if (q_lower in (s.get("id") or "").lower() or 
                    q_lower in (s.get("name") or "").lower() or 
                    q_lower in (s.get("artists") or "").lower()):
                    results.append(s)
                    if len(results) >= limit: break
        else:
            results = songs[-limit:]
            results.reverse()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lyrics/{video_id}")
async def get_lyrics(video_id: str):
    try:
        with DB_CACHE_LOCK:
            row = DB_CACHE.get(video_id)
        if not row or not row.get("lyrics_sources"):
            raise HTTPException(status_code=404, detail="No cached lyrics")
        lyrics_src = row["lyrics_sources"]
        url_id, ext = 1, "json"
        if ":" in lyrics_src:
            parts = lyrics_src.split(":")
            try: url_id = int(parts[0])
            except ValueError: url_id = 1
            if len(parts) > 1: ext = parts[1]
        with BASE_URL_CACHE_LOCK:
            url_base = BASE_URL_CACHE.get(url_id)
        target_url = f"{url_base}/{video_id}.{ext}" if url_base else f"https://huggingface.co/buckets/allnewuser/lyrics/resolve/{video_id}.{ext}"
        headers = {"Authorization": f"Bearer {HF_TOKEN}"} if HF_TOKEN else {}
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(target_url, headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail="Failed to fetch lyrics")
            try: return resp.json()
            except Exception: return HTMLResponse(content=resp.text, media_type="application/json")
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/canvas")
def get_canvas(id: str):
    try:
        with DB_CACHE_LOCK:
            row = DB_CACHE.get(id)
        if not row or not row.get("canvas_sources"):
            raise HTTPException(status_code=404, detail="No cached canvas")
        with BASE_URL_CACHE_LOCK:
            url_base = BASE_URL_CACHE.get(2)
        if not url_base or "huggingface.co/buckets" in str(url_base):
            url_base = "https://velamhere-img.hf.space"
        stream_url = f"{url_base}/{id}"
        return {"id": id, "name": row["name"], "artist": row["artists"], "url": stream_url}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/canvas/stream/{video_id}")
def stream_canvas(video_id: str):
    try:
        with BASE_URL_CACHE_LOCK:
            url_base = BASE_URL_CACHE.get(2)
        if not url_base or "huggingface.co/buckets" in str(url_base):
            url_base = "https://velamhere-img.hf.space"
        redirect_url = f"{url_base}/{video_id}"
        return RedirectResponse(url=redirect_url, status_code=307)
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stream/listen/{video_id}")
def listen_stream(video_id: str):
    try:
        with DB_CACHE_LOCK:
            row = DB_CACHE.get(video_id)
        url_id, ext = 3, None
        if row and row.get("stream_sources"):
            stream_src = row["stream_sources"]
            if ":" in stream_src:
                parts = stream_src.split(":")
                try: url_id = int(parts[0])
                except ValueError: url_id = 3
                if len(parts) > 1: ext = parts[1]
            else: ext = stream_src
        if not ext:
            enqueue_song(video_id)
            raise HTTPException(status_code=404, detail="Stream processing or not available")
        with BASE_URL_CACHE_LOCK: url_base = BASE_URL_CACHE.get(url_id)
        redirect_url = f"{url_base}/{video_id}" if url_base else f"https://velamhere-img.hf.space/song/{video_id}"
        return RedirectResponse(url=redirect_url, status_code=307)
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/status")
@app.get("/api/status")
def get_service_status():
    lossless_count = 0
    m4a_count = 0
    try:
        with DB_CACHE_LOCK:
            for song in DB_CACHE.values():
                lossless_val = str(song.get("lossless") or "").strip()
                if lossless_val and lossless_val != "0":
                    lossless_count += 1
                stream_src = str(song.get("stream_sources") or "").strip().lower()
                if stream_src:
                    ext = stream_src.split(":")[-1] if ":" in stream_src else stream_src
                    if ext == "m4a":
                        m4a_count += 1
    except Exception as e:
        log.error(f"Failed to calculate status stats: {e}")

    with QUEUED_SET_LOCK:
        stream_queued_ids = set(QUEUED_SET)
    with LOSSLESS_QUEUED_LOCK:
        lossless_queued_ids = set(LOSSLESS_QUEUED_SET)

    combined_queued = len(stream_queued_ids | lossless_queued_ids)

    return {
        "status": "online",
        "lossless": lossless_count,
        "m4a": m4a_count,
        "in_queue": combined_queued,
        "queue_details": {
            "stream_queue": TASK_QUEUE.qsize(),
            "lossless_queue": LOSSLESS_TASK_QUEUE.qsize(),
            "total_combined_queued": combined_queued
        }
    }
