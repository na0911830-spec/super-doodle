import os
import sys

# Resolve the directory containing the local modules
# In dev: co-located in lossless/. In container: app.py at /app/, modules at /app/lossless/
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
_LOSSLESS_SUBDIR = os.path.join(_CURR_DIR, 'lossless')

if os.path.exists(os.path.join(_CURR_DIR, 'db_service.py')):
    _MODULES_DIR = _CURR_DIR
elif os.path.exists(os.path.join(_LOSSLESS_SUBDIR, 'db_service.py')):
    _MODULES_DIR = _LOSSLESS_SUBDIR
else:
    _MODULES_DIR = _CURR_DIR

if sys.path[:1] != [_MODULES_DIR]:
    if _MODULES_DIR in sys.path:
        sys.path.remove(_MODULES_DIR)
    sys.path.insert(0, _MODULES_DIR)

import time
import queue
import threading
import urllib.parse
import collections
import httpx
from logger_config import log, FATAL_ERRORS

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

from db_service import db_save_stream_source, STATS, BUCKET_ID, HF_TOKEN, cleanup_upload_cache


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

STREAM_JOBS = {}  # video_id -> {"status": "queued"|"progressing"|"done"|"failed", "error": str, "timestamp": float}
UPLOAD_TIMESTAMPS = collections.deque()

NETWORK_STATS = {
    "last_download_speed_kbps": 0.0,
    "last_upload_speed_kbps": 0.0,
    "total_downloaded_mb": 0.0,
    "total_uploaded_mb": 0.0
}

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

# Thread-safe Task Queue
TASK_QUEUE = PriorityTaskQueue()
QUEUED_SET = set()
QUEUED_SET_LOCK = threading.Lock()

# Worker threads pool
WORKER_THREADS = []
NUM_WORKERS = 100

def get_avg_uploads_per_min() -> float:
    now = time.time()
    while UPLOAD_TIMESTAMPS and UPLOAD_TIMESTAMPS[0] < now - 120:
        UPLOAD_TIMESTAMPS.popleft()
    return len(UPLOAD_TIMESTAMPS) / 2.0

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
                    log.warning(f"Invidious API call to {instance} failed: HTTP {resp.status_code}")
                    continue
                
                try:
                    data = resp.json()
                except ValueError:
                    log.warning(f"Failed to decode Invidious API JSON from {instance} for {video_id}")
                    continue
                formats = data.get("adaptiveFormats", [])
                
                m4a_formats = [
                    f for f in formats 
                    if f.get("type", "").startswith("audio/") and 
                    ("mp4" in f.get("type", "") or f.get("container") == "m4a")
                ]
                
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
                    log.warning(f"No audio formats found on Invidious instance {instance}")
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
                log.info(f"Downloading stream via proxy {instance}...")
                stream_resp = client.get(proxied_url)
                if stream_resp.status_code != 200:
                    log.warning(f"Stream download failed from proxy {instance}: HTTP {stream_resp.status_code}. Trying direct /stream endpoint...")
                    stream_resp = client.get(f"{instance.rstrip('/')}/stream/{video_id}")
                    if stream_resp.status_code != 200:
                        continue
                    if stream_resp.headers.get("x-audio-extension"):
                        ext = stream_resp.headers.get("x-audio-extension")
                audio_bytes = stream_resp.content
                t1 = time.time()
                dl_duration = t1 - t0
                if dl_duration > 0:
                    dl_speed = len(audio_bytes) / dl_duration / 1024.0
                    NETWORK_STATS["last_download_speed_kbps"] = dl_speed
                NETWORK_STATS["total_downloaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
                
            log.info(f"Uploading {len(audio_bytes)} bytes to HF bucket for {video_id}.{ext}")
            t0 = time.time()
            from db_service import safe_batch_bucket_files
            safe_batch_bucket_files(
                BUCKET_ID,
                add=[(audio_bytes, f"{video_id}.{ext}")],
                token=HF_TOKEN
            )
            t1 = time.time()
            ul_duration = t1 - t0
            if ul_duration > 0:
                ul_speed = len(audio_bytes) / ul_duration / 1024.0
                NETWORK_STATS["last_upload_speed_kbps"] = ul_speed
            NETWORK_STATS["total_uploaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
            
            del audio_bytes
            cleanup_upload_cache()

            db_save_stream_source(video_id, ext)
            STREAM_JOBS[video_id] = {"status": "done", "timestamp": time.time(), "ext": ext}
            if len(STREAM_JOBS) > 500:
                STREAM_JOBS.pop(next(iter(STREAM_JOBS)), None)
            STATS["done"] += 1
            STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
            UPLOAD_TIMESTAMPS.append(time.time())
            log.info(f"Stream upload successfully completed for {video_id}.{ext} using {instance}")

            # Also attempt lossless resolution using upstream audio service
            try:
                lossless_res = qobuz_service.process_qobuz_lossless(video_id)
                if isinstance(lossless_res, dict) and lossless_res.get("success"):
                    db_service.db_save_lossless_source(video_id, "4:flac")
                    # Update song title and artist if fetched from worker API
                    c_title = lossless_res.get("title")
                    c_artist = lossless_res.get("artist")
                    if c_title and c_artist:
                        with db_service.DB_CACHE_LOCK:
                            if video_id in db_service.DB_CACHE:
                                db_service.DB_CACHE[video_id]["name"] = c_title
                                db_service.DB_CACHE[video_id]["artists"] = c_artist
            except Exception as q_ex:
                log.warning(f"Qobuz lossless integration step exception for {video_id}: {q_ex}")

            return True
            
        except Exception as e:
            if 'audio_bytes' in locals():
                del audio_bytes
            cleanup_upload_cache()
            log.warning(f"Exception using instance {instance} for {video_id}: {e}")
            if not isinstance(e, (httpx.HTTPError, httpx.NetworkError, httpx.TimeoutException, ValueError, KeyError)):
                err_msg = f"{type(e).__name__} during sync for {video_id}: {str(e)}"
                if err_msg not in FATAL_ERRORS:
                    FATAL_ERRORS.append(err_msg)
            continue
            
    log.info(f"All Invidious instances failed. Attempting ultimate fallback for {video_id}...")
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
                    dl_speed = len(audio_bytes) / dl_duration / 1024.0
                    NETWORK_STATS["last_download_speed_kbps"] = dl_speed
                NETWORK_STATS["total_downloaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
                
                ext = "m4a"
                content_type = resp.headers.get("content-type", "")
                if "webm" in content_type:
                    ext = "webm"
                elif "mp3" in content_type:
                    ext = "mp3"

                log.info(f"Uploading fallback {len(audio_bytes)} bytes to HF bucket for {video_id}.{ext}")
                t0_up = time.time()
                from db_service import safe_batch_bucket_files
                safe_batch_bucket_files(
                    BUCKET_ID,
                    add=[(audio_bytes, f"{video_id}.{ext}")],
                    token=HF_TOKEN
                )
                t1_up = time.time()
                ul_duration = t1_up - t0_up
                if ul_duration > 0:
                    ul_speed = len(audio_bytes) / ul_duration / 1024.0
                    NETWORK_STATS["last_upload_speed_kbps"] = ul_speed
                NETWORK_STATS["total_uploaded_mb"] += len(audio_bytes) / (1024.0 * 1024.0)
                
                db_save_stream_source(video_id, ext)
                STREAM_JOBS[video_id] = {"status": "done", "timestamp": time.time(), "ext": ext}
                if len(STREAM_JOBS) > 500:
                    STREAM_JOBS.pop(next(iter(STREAM_JOBS)), None)
                STATS["done"] += 1
                STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
                UPLOAD_TIMESTAMPS.append(time.time())
                log.info(f"Stream upload successfully completed for {video_id}.{ext} via ultimate fallback")

                del audio_bytes
                cleanup_upload_cache()

                # Also attempt Qobuz Lossless process
                try:
                    name, artists = "", ""
                    with db_service.DB_CACHE_LOCK:
                        row = db_service.DB_CACHE.get(video_id) or {}
                        name = row.get("name") or ""
                        artists = row.get("artists") or ""
                    if not name or name == "Unknown Song":
                        name, artists = db_service.fetch_correct_metadata(video_id)
                    lossless_ok = qobuz_service.process_qobuz_lossless(video_id, name, artists)
                    if isinstance(lossless_ok, dict) and lossless_ok.get("success"):
                        ext = lossless_ok.get("ext", "flac")
                        db_service.db_save_lossless_source(video_id, f"4:{ext}")
                except Exception as q_ex:
                    log.warning(f"Qobuz lossless integration step exception for {video_id}: {q_ex}")

                return True
    except Exception as fallback_ex:
        if 'audio_bytes' in locals():
            del audio_bytes
        cleanup_upload_cache()
        log.error(f"Ultimate fallback failed for {video_id}: {fallback_ex}")

    STREAM_JOBS[video_id] = {"status": "failed", "error": "All Invidious instances and ultimate fallback failed", "timestamp": time.time()}
    if len(STREAM_JOBS) > 500:
        STREAM_JOBS.pop(next(iter(STREAM_JOBS)), None)
    STATS["failed"] += 1
    STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["queued", "progressing"])
    return False

# Dedicated Lossless Queue for Backup sequence
LOSSLESS_TASK_QUEUE = PriorityTaskQueue()
LOSSLESS_QUEUED_SET = set()
LOSSLESS_QUEUED_LOCK = threading.Lock()
LOSSLESS_STATS = {"queued": 0, "progressing": 0, "done": 0, "failed": 0}
FAILED_LOSSLESS_IDS = set()
FAILED_LOSSLESS_LOCK = threading.Lock()

def lossless_worker_loop():
    while True:
        try:
            video_id = LOSSLESS_TASK_QUEUE.get()
            if video_id is None:
                break
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
                    LOSSLESS_STATS["done"] += 1
                else:
                    with FAILED_LOSSLESS_LOCK:
                        FAILED_LOSSLESS_IDS.add(video_id)
                    LOSSLESS_STATS["failed"] += 1
            except Exception as ex:
                with FAILED_LOSSLESS_LOCK:
                    FAILED_LOSSLESS_IDS.add(video_id)
                log.error(f"Lossless worker error for {video_id}: {ex}")
                LOSSLESS_STATS["failed"] += 1
            finally:
                if LOSSLESS_STATS["progressing"] > 0:
                    LOSSLESS_STATS["progressing"] -= 1
                with LOSSLESS_QUEUED_LOCK:
                    LOSSLESS_QUEUED_SET.discard(video_id)
                LOSSLESS_TASK_QUEUE.task_done()
        except Exception as e:
            log.error(f"Error in lossless worker thread loop: {e}")

LOSSLESS_WORKER_THREADS = []
NUM_LOSSLESS_WORKERS = 100

def trigger_lossless_backup() -> int:
    """Enqueues songs in DB where lossless source is missing (throttled queue to protect RAM & CPU)."""
    global LOSSLESS_WORKER_THREADS
    if LOSSLESS_TASK_QUEUE.qsize() >= 100:
        return 0

    with db_service.DB_CACHE_LOCK:
        missing_ids = [vid for vid, song in db_service.DB_CACHE.items() if not song.get("lossless") or str(song.get("lossless")).strip() in ("", "0")]

    count = 0
    with LOSSLESS_QUEUED_LOCK:
        with FAILED_LOSSLESS_LOCK:
            for vid in missing_ids:
                if vid not in LOSSLESS_QUEUED_SET and vid not in FAILED_LOSSLESS_IDS:
                    LOSSLESS_QUEUED_SET.add(vid)
                    LOSSLESS_TASK_QUEUE.put(vid)
                    count += 1
                    if LOSSLESS_TASK_QUEUE.qsize() >= 100:
                        break

    LOSSLESS_STATS["queued"] = LOSSLESS_TASK_QUEUE.qsize()
    if count > 0:
        log.info(f"Triggered Lossless Bulk Backup: Enqueued {count} songs missing lossless audio.")

    alive_threads = [t for t in LOSSLESS_WORKER_THREADS if t.is_alive()]
    if len(alive_threads) < NUM_LOSSLESS_WORKERS:
        needed = NUM_LOSSLESS_WORKERS - len(alive_threads)
        for i in range(needed):
            t = threading.Thread(target=lossless_worker_loop, daemon=True, name=f"LosslessWorker-{len(alive_threads)+i+1}")
            t.start()
            LOSSLESS_WORKER_THREADS.append(t)
        log.info(f"Started {needed} parallel lossless worker threads (total active: {NUM_LOSSLESS_WORKERS}).")

    return count

def worker_loop():
    while True:
        try:
            video_id = TASK_QUEUE.get()
            if video_id is None:
                break
            try:
                process_stream_job(video_id)
            finally:
                with QUEUED_SET_LOCK:
                    QUEUED_SET.discard(video_id)
                TASK_QUEUE.task_done()
        except Exception as e:
            log.error(f"Error in worker thread: {e}")

def enqueue_song(video_id: str, high_priority: bool = True) -> bool:
    """Enqueue a song for standard stream and lossless processing with priority."""
    enqueued = False

    with db_service.DB_CACHE_LOCK:
        row = db_service.DB_CACHE.get(video_id) or {}
        has_stream = bool(row.get("stream_sources"))
        lossless_val = str(row.get("lossless") or "").strip()
        has_lossless = bool(lossless_val and lossless_val != "0")

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

    if not has_lossless:
        with LOSSLESS_QUEUED_LOCK:
            if high_priority or video_id not in LOSSLESS_QUEUED_SET:
                LOSSLESS_QUEUED_SET.add(video_id)
                LOSSLESS_TASK_QUEUE.put(video_id, high_priority=high_priority)
                LOSSLESS_STATS["queued"] = LOSSLESS_TASK_QUEUE.qsize()
                enqueued = True

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

