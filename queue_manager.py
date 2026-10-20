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

    def get_nowait(self):
        with self.lock:
            if not self.deque:
                raise queue.Empty
            return self.deque.popleft()

    def get_batch(self, max_items=50):
        with self.lock:
            items = []
            while self.deque and len(items) < max_items:
                items.append(self.deque.popleft())
            return items

    def empty(self):
        with self.lock:
            return len(self.deque) == 0

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
BATCH_SIZE = 50
NUM_STREAM_WORKERS = 50
NUM_LOSSLESS_WORKERS = 50
NUM_WORKERS = 50
STREAM_PROCESSOR_THREAD = None
LOSSLESS_PROCESSOR_THREAD = None
WORKER_THREADS = []

def get_avg_uploads_per_min() -> float:
    now = time.time()
    while UPLOAD_TIMESTAMPS and UPLOAD_TIMESTAMPS[0] < now - 120:
        UPLOAD_TIMESTAMPS.popleft()
    return len(UPLOAD_TIMESTAMPS) / 2.0

def fetch_stream_audio(video_id: str) -> Optional[tuple[bytes, str]]:
    """
    Downloads audio bytes for video_id via Invidious / antideploy proxy or fallback.
    Returns (audio_bytes, ext) or None on failure.
    Does NOT upload to Hugging Face directly (batch worker uploads all at once).
    """
    for instance in INVIDIOUS_INSTANCES:
        try:
            with httpx.Client(timeout=30.0) as client:
                url = f"{instance.rstrip('/')}/api/v1/videos/{video_id}"
                if not is_safe_domain_url(url):
                    continue
                resp = client.get(url)
                if resp.status_code != 200:
                    continue
                try:
                    data = resp.json()
                except ValueError:
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
                    continue

                stream_resp = client.get(proxied_url)
                if stream_resp.status_code != 200:
                    stream_resp = client.get(f"{instance.rstrip('/')}/stream/{video_id}")
                    if stream_resp.status_code != 200:
                        continue
                    if stream_resp.headers.get("x-audio-extension"):
                        ext = stream_resp.headers.get("x-audio-extension")
                audio_bytes = stream_resp.content
                if audio_bytes and len(audio_bytes) > 1024:
                    return audio_bytes, ext
        except Exception:
            continue

    # Fallback to Render stream endpoint
    try:
        fallback_url = f"https://effective-disco-3nfb.onrender.com/api/stream/{video_id}"
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            resp = client.get(fallback_url)
            if resp.status_code == 200:
                audio_bytes = resp.content
                if audio_bytes and len(audio_bytes) > 1024:
                    ext = "webm" if "webm" in resp.headers.get("content-type", "") else "m4a"
                    return audio_bytes, ext
    except Exception:
        pass

    return None

def process_stream_job(video_id: str) -> bool:
    """Standalone fallback to process a single video stream if invoked directly."""
    res = fetch_stream_audio(video_id)
    if res:
        audio_bytes, ext = res
        try:
            from db_service import safe_batch_bucket_files
            safe_batch_bucket_files(BUCKET_ID, add=[(audio_bytes, f"{video_id}.{ext}")], token=HF_TOKEN)
            cleanup_upload_cache()
            db_save_stream_source(video_id, ext)
            with db_service.DB_CACHE_LOCK:
                STREAM_JOBS[video_id] = {"status": "done", "timestamp": time.time(), "ext": ext}
            STATS["done"] += 1
            UPLOAD_TIMESTAMPS.append(time.time())
            return True
        except Exception as e:
            log.error(f"[STREAM] Upload error for {video_id}: {e}")
    with db_service.DB_CACHE_LOCK:
        STREAM_JOBS[video_id] = {"status": "failed", "timestamp": time.time()}
    STATS["failed"] += 1
    return False

def stream_batch_worker_loop():
    """
    Dedicated stream batch processor:
    Pulls up to 50 stream tasks from TASK_QUEUE.
    Spawns 50 parallel workers to download audio bytes.
    When all 50 finish (success or fail), uploads all successfully downloaded files at once!
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    batch_num = 0
    executor = ThreadPoolExecutor(max_workers=NUM_STREAM_WORKERS, thread_name_prefix="StreamWorker")
    
    log.info(f"[STREAM] ⚡ Stream batch processor online with {NUM_STREAM_WORKERS} parallel workers.")
    
    while True:
        try:
            batch_ids = TASK_QUEUE.get_batch(BATCH_SIZE)
            
            if not batch_ids:
                time.sleep(1)
                continue
                
            batch_num += 1
            t_start = time.time()
            with db_service.DB_CACHE_LOCK:
                for vid in batch_ids:
                    STREAM_JOBS[vid] = {"status": "progressing", "timestamp": time.time()}
            STATS["progressing"] = len(batch_ids)
            
            log.info(f"[STREAM] 🚀 Batch #{batch_num}: Processing {len(batch_ids)} tracks in parallel (50 workers | Queue: {TASK_QUEUE.qsize()} remaining)")
            
            futures = {executor.submit(fetch_stream_audio, vid): vid for vid in batch_ids}
            results = {}
            for fut in as_completed(futures):
                vid = futures[fut]
                try:
                    res = fut.result()
                    results[vid] = res
                except Exception:
                    results[vid] = None
                    
            elapsed_dl = max(0.001, time.time() - t_start)
            
            success_files = []
            failed_vids = []
            total_bytes = 0
            
            for vid in batch_ids:
                res = results.get(vid)
                if res and res[0]:
                    audio_bytes, ext = res
                    success_files.append((vid, audio_bytes, ext))
                    total_bytes += len(audio_bytes)
                else:
                    failed_vids.append(vid)
                    
            mb = total_bytes / (1024.0 * 1024.0)
            dl_speed = mb / elapsed_dl
            log.info(f"[STREAM] ⚡ Batch #{batch_num}: {len(success_files)}/{len(batch_ids)} downloaded in {elapsed_dl:.1f}s ({dl_speed:.1f} MB/s) | {len(failed_vids)} failed")
            
            NETWORK_STATS["last_download_speed_kbps"] = (total_bytes / elapsed_dl) / 1024.0 if elapsed_dl > 0 else 0
            NETWORK_STATS["total_downloaded_mb"] += mb
            
            if success_files:
                log.info(f"[STREAM] 📦 Batch #{batch_num}: Uploading {len(success_files)} files ({mb:.1f} MB) at once to HF bucket '{BUCKET_ID}'...")
                t_up = time.time()
                add_list = [(audio_bytes, f"{vid}.{ext}") for vid, audio_bytes, ext in success_files]
                try:
                    from db_service import safe_batch_bucket_files
                    safe_batch_bucket_files(BUCKET_ID, add=add_list, token=HF_TOKEN)
                    up_elapsed = max(0.001, time.time() - t_up)
                    up_speed = mb / up_elapsed
                    log.info(f"[STREAM] ✅ Batch #{batch_num}: Upload completed in {up_elapsed:.1f}s ({up_speed:.1f} MB/s)!")
                    
                    NETWORK_STATS["last_upload_speed_kbps"] = (total_bytes / up_elapsed) / 1024.0
                    NETWORK_STATS["total_uploaded_mb"] += mb
                    
                    for _ in range(len(success_files)):
                        UPLOAD_TIMESTAMPS.append(time.time())
                        
                    for vid, _, ext in success_files:
                        db_save_stream_source(vid, ext)
                        with db_service.DB_CACHE_LOCK:
                            STREAM_JOBS[vid] = {"status": "done", "timestamp": time.time(), "ext": ext}
                        
                        # Also enqueue to lossless queue if missing lossless
                        with LOSSLESS_QUEUED_LOCK:
                            if vid not in LOSSLESS_QUEUED_SET:
                                with db_service.DB_CACHE_LOCK:
                                    lossless_val = str((db_service.DB_CACHE.get(vid) or {}).get("lossless") or "").strip()
                                if not lossless_val or lossless_val == "0":
                                    LOSSLESS_QUEUED_SET.add(vid)
                                    LOSSLESS_TASK_QUEUE.put(vid, high_priority=False)
                                    LOSSLESS_STATS["queued"] = LOSSLESS_TASK_QUEUE.qsize()

                    STATS["done"] += len(success_files)
                except Exception as up_err:
                    log.error(f"[STREAM] ❌ Batch #{batch_num}: Upload failed: {up_err}")
                    for vid, _, _ in success_files:
                        failed_vids.append(vid)
                finally:
                    del add_list
                    del success_files
                    cleanup_upload_cache()
                    
            if failed_vids:
                with db_service.DB_CACHE_LOCK:
                    for vid in failed_vids:
                        STREAM_JOBS[vid] = {"status": "failed", "timestamp": time.time()}
                STATS["failed"] += len(failed_vids)
                
            with QUEUED_SET_LOCK:
                for vid in batch_ids:
                    QUEUED_SET.discard(vid)
            for _ in range(len(batch_ids)):
                TASK_QUEUE.task_done()
                
            STATS["progressing"] = 0
            log.info(f"[STREAM] 📊 Batch #{batch_num} complete | Total Stream Cached: {STATS['done']} | Failed: {STATS['failed']}")
            
        except Exception as e:
            log.error(f"[STREAM] Error in stream batch loop: {e}")
            time.sleep(1)

# Dedicated Lossless Queue for Backup sequence
LOSSLESS_TASK_QUEUE = PriorityTaskQueue()
LOSSLESS_QUEUED_SET = set()
LOSSLESS_QUEUED_LOCK = threading.Lock()
LOSSLESS_STATS = {"queued": 0, "progressing": 0, "done": 0, "failed": 0}
FAILED_LOSSLESS_IDS = set()
FAILED_LOSSLESS_LOCK = threading.Lock()

def lossless_batch_worker_loop():
    """
    Dedicated lossless batch processor:
    Pulls up to 50 lossless tasks from LOSSLESS_TASK_QUEUE.
    Spawns 50 parallel workers to download/convert FLAC bytes.
    When all 50 finish (success or fail), uploads all successfully downloaded FLAC files at once!
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    batch_num = 0
    executor = ThreadPoolExecutor(max_workers=NUM_LOSSLESS_WORKERS, thread_name_prefix="LosslessWorker")
    
    log.info(f"[LOSSLESS] ⚡ Lossless batch processor online with {NUM_LOSSLESS_WORKERS} parallel workers.")
    
    while True:
        try:
            batch_ids = LOSSLESS_TASK_QUEUE.get_batch(BATCH_SIZE)
                     
            if not batch_ids:
                time.sleep(1)
                continue
                
            batch_num += 1
            t_start = time.time()
            LOSSLESS_STATS["progressing"] = len(batch_ids)
            LOSSLESS_STATS["queued"] = LOSSLESS_TASK_QUEUE.qsize()
            
            log.info(f"[LOSSLESS] 🚀 Batch #{batch_num}: Processing {len(batch_ids)} tracks in parallel (50 workers | Queue: {LOSSLESS_TASK_QUEUE.qsize()} remaining)")
            
            def process_single_lossless(vid: str):
                name, artists = "", ""
                with db_service.DB_CACHE_LOCK:
                    row = db_service.DB_CACHE.get(vid) or {}
                    name = row.get("name") or ""
                    artists = row.get("artists") or ""
                if not name or name == "Unknown Song":
                    name, artists = db_service.fetch_correct_metadata(vid)
                return qobuz_service.fetch_lossless_audio(vid, name, artists)

            futures = {executor.submit(process_single_lossless, vid): vid for vid in batch_ids}
            results = {}
            for fut in as_completed(futures):
                vid = futures[fut]
                try:
                    res = fut.result()
                    results[vid] = res
                except Exception:
                    results[vid] = None
                    
            elapsed_dl = max(0.001, time.time() - t_start)
            
            success_files = []
            failed_vids = []
            total_bytes = 0
            
            for vid in batch_ids:
                res = results.get(vid)
                if res and res.get("audio_bytes"):
                    success_files.append(res)
                    total_bytes += len(res["audio_bytes"])
                else:
                    failed_vids.append(vid)
                    
            mb = total_bytes / (1024.0 * 1024.0)
            dl_speed = mb / elapsed_dl
            log.info(f"[LOSSLESS] ⚡ Batch #{batch_num}: {len(success_files)}/{len(batch_ids)} downloaded in {elapsed_dl:.1f}s ({dl_speed:.1f} MB/s) | {len(failed_vids)} failed")
            
            if success_files:
                lossless_bucket = getattr(qobuz_service, "LOSSLESS_BUCKET_ID", "shashwatIDR/lossless")
                log.info(f"[LOSSLESS] 📦 Batch #{batch_num}: Uploading {len(success_files)} FLAC files ({mb:.1f} MB) at once to HF bucket '{lossless_bucket}'...")
                t_up = time.time()
                add_list = [(item["audio_bytes"], f"{item['video_id']}.{item.get('ext', 'flac')}") for item in success_files]
                try:
                    from db_service import safe_batch_bucket_files
                    safe_batch_bucket_files(lossless_bucket, add=add_list, token=HF_TOKEN)
                    up_elapsed = max(0.001, time.time() - t_up)
                    up_speed = mb / up_elapsed
                    log.info(f"[LOSSLESS] ✅ Batch #{batch_num}: Upload completed in {up_elapsed:.1f}s ({up_speed:.1f} MB/s)!")
                    
                    for item in success_files:
                        vid = item["video_id"]
                        ext = item.get("ext", "flac")
                        db_service.db_save_lossless_source(vid, f"4:{ext}")
                            
                    LOSSLESS_STATS["done"] += len(success_files)
                except Exception as up_err:
                    log.error(f"[LOSSLESS] ❌ Batch #{batch_num}: Upload failed: {up_err}")
                    for item in success_files:
                        failed_vids.append(item["video_id"])
                finally:
                    del add_list
                    del success_files
                    cleanup_upload_cache()
                    
            if failed_vids:
                with FAILED_LOSSLESS_LOCK:
                    for vid in failed_vids:
                        FAILED_LOSSLESS_IDS.add(vid)
                LOSSLESS_STATS["failed"] += len(failed_vids)
                
            with LOSSLESS_QUEUED_LOCK:
                for vid in batch_ids:
                    LOSSLESS_QUEUED_SET.discard(vid)
            for _ in range(len(batch_ids)):
                LOSSLESS_TASK_QUEUE.task_done()
                
            LOSSLESS_STATS["progressing"] = 0
            log.info(f"[LOSSLESS] 📊 Batch #{batch_num} complete | Total Lossless Cached: {LOSSLESS_STATS['done']} | Failed: {LOSSLESS_STATS['failed']}")
            
        except Exception as e:
            log.error(f"[LOSSLESS] Error in lossless batch loop: {e}")
            time.sleep(1)

def trigger_lossless_backup() -> int:
    """Enqueues songs in DB where lossless source is missing (throttled queue to protect RAM & CPU)."""
    global LOSSLESS_PROCESSOR_THREAD
    if LOSSLESS_TASK_QUEUE.qsize() >= 300:
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
                    if LOSSLESS_TASK_QUEUE.qsize() >= 300:
                        break

    LOSSLESS_STATS["queued"] = LOSSLESS_TASK_QUEUE.qsize()
    if count > 0:
        log.info(f"[LOSSLESS] Bulk Backup feeder: Enqueued {count} songs missing lossless audio (Total in queue: {LOSSLESS_STATS['queued']}).")

    if LOSSLESS_PROCESSOR_THREAD is None or not LOSSLESS_PROCESSOR_THREAD.is_alive():
        LOSSLESS_PROCESSOR_THREAD = threading.Thread(target=lossless_batch_worker_loop, daemon=True, name="LosslessBatchProcessor")
        LOSSLESS_PROCESSOR_THREAD.start()

    return count

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
                if LOSSLESS_PROCESSOR_THREAD is None or not LOSSLESS_PROCESSOR_THREAD.is_alive():
                    start_queue_workers()

    return enqueued

def enqueue_songs_batch(video_ids: list[str]) -> list[str]:
    added = []
    for vid in video_ids:
        if enqueue_song(vid, high_priority=False):
            added.append(vid)
    return added

def start_queue_workers():
    global STREAM_PROCESSOR_THREAD, LOSSLESS_PROCESSOR_THREAD
    if STREAM_PROCESSOR_THREAD is None or not STREAM_PROCESSOR_THREAD.is_alive():
        STREAM_PROCESSOR_THREAD = threading.Thread(target=stream_batch_worker_loop, daemon=True, name="StreamBatchProcessor")
        STREAM_PROCESSOR_THREAD.start()
    if LOSSLESS_PROCESSOR_THREAD is None or not LOSSLESS_PROCESSOR_THREAD.is_alive():
        LOSSLESS_PROCESSOR_THREAD = threading.Thread(target=lossless_batch_worker_loop, daemon=True, name="LosslessBatchProcessor")
        LOSSLESS_PROCESSOR_THREAD.start()
    log.info(f"Started 100 parallel workers in total (50 Stream workers + 50 Lossless workers, batch-upload).")

