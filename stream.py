import os
import sys

# Enforce zero shard/chunk disk cache on HuggingFace Xet to prevent 10GB+ cache accumulation
os.environ.setdefault("HF_XET_SHARD_CACHE_SIZE_LIMIT", "0")
os.environ.setdefault("HF_XET_CHUNK_CACHE_SIZE_BYTES", "0")
os.environ.setdefault("HF_XET_LOG_DIR_MAX_SIZE", "10000000")
os.environ.setdefault("HF_HUB_DISABLE_DISK_INTEGRITY_CHECK", "1")

import logging
import urllib.parse
import httpx
import time
import collections
import threading
import re
import zipfile
import shutil
import json
from typing import Optional, Any
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse
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

# Create handlers
stdout_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler("stream_service.log", encoding="utf-8")
log_handler = InMemoryLogHandler()

# Set formatters
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
stdout_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
log_handler.setFormatter(formatter)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
root_logger.addHandler(stdout_handler)
root_logger.addHandler(file_handler)
root_logger.addHandler(log_handler)

log = logging.getLogger("stream-service")


# Config
DB_URL = os.environ.get("DB_URL", "")
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
STREAM_JOBS = {}  # video_id -> {"status": "fetching"|"progressing"|"done"|"failed", "error": str, "timestamp": float}
UPLOAD_TIMESTAMPS = collections.deque()

NETWORK_STATS = {
    "last_download_speed_kbps": 0.0,
    "last_upload_speed_kbps": 0.0,
    "total_downloaded_mb": 0.0,
    "total_uploaded_mb": 0.0
}

# Base URL cache to avoid redundant database reads
BASE_URL_CACHE = {}
BASE_URL_CACHE_LOCK = threading.Lock()

# In-memory database cache
DB_CACHE = {}
DB_CACHE_LOCK = threading.Lock()

def load_db_cache():
    """Load all records from SQL database or fallback HF db_backup.json."""
    global DB_CACHE, BASE_URL_CACHE
    sql_loaded = False
    try:
        log.info("Syncing cache from MySQL database on startup...")
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, url FROM base_urls")
            url_rows = cursor.fetchall()
            cursor.execute("SELECT id, name, artists, lyrics_sources, canvas_sources, stream_sources FROM songs")
            song_rows = cursor.fetchall()
        conn.close()
        
        with DB_CACHE_LOCK:
            DB_CACHE = {row["id"]: row for row in song_rows}
        with BASE_URL_CACHE_LOCK:
            BASE_URL_CACHE = {row["id"]: row["url"] for row in url_rows}
            BASE_URL_CACHE[2] = "https://velamhere-img.hf.space"
            
        log.info(f"Successfully synced {len(song_rows)} songs and {len(url_rows)} base URLs from MySQL.")
        sql_loaded = True
    except Exception as sql_ex:
        log.warning(f"Failed to sync from MySQL database: {sql_ex}")

    if not sql_loaded:
        try:
            url = f"https://huggingface.co/buckets/{BUCKET_ID}/resolve/db_backup.json"
            headers = {}
            if HF_TOKEN:
                headers["Authorization"] = f"Bearer {HF_TOKEN}"
                
            log.info(f"Fallback: loading database cache from HF backup ({url})...")
            with httpx.Client(follow_redirects=True) as client:
                resp = client.get(url, headers=headers)
                
            if resp.status_code == 200:
                data = resp.json()
                songs_list = data.get("songs", [])
                
                if not songs_list:
                    raise ValueError("HF backup JSON contains 0 songs")
                    
                has_lyrics_metadata = any("lyrics_sources" in s for s in songs_list)
                if not has_lyrics_metadata:
                    raise ValueError("HF backup JSON is missing lyrics_sources metadata (old schema)")
                    
                with DB_CACHE_LOCK:
                    DB_CACHE = {s["id"]: s for s in songs_list}
                
                base_urls_list = data.get("base_urls", [])
                with BASE_URL_CACHE_LOCK:
                    BASE_URL_CACHE = {}
                    for item in base_urls_list:
                        if isinstance(item, dict):
                            BASE_URL_CACHE[int(item["id"])] = item["url"]
                        elif isinstance(item, (list, tuple)) and len(item) >= 2:
                            BASE_URL_CACHE[int(item[0])] = item[1]
                    BASE_URL_CACHE[2] = "https://velamhere-img.hf.space"
                            
                log.info(f"Successfully loaded {len(DB_CACHE)} songs and {len(BASE_URL_CACHE)} base URLs from HF backup JSON.")
            else:
                log.warning(f"HF backup JSON not found (HTTP {resp.status_code}).")
        except Exception as e:
            log.error(f"Failed to load cache from HF backup fallback: {e}")

# Regular expression to mask URLs
URL_PATTERN = re.compile(r'https?://[^\s]+')

def mask_urls(text: str) -> str:
    return URL_PATTERN.sub('[URL]', text)

def get_avg_uploads_per_min() -> float:
    now = time.time()
    while UPLOAD_TIMESTAMPS and UPLOAD_TIMESTAMPS[0] < now - 120:
        UPLOAD_TIMESTAMPS.popleft()
    return len(UPLOAD_TIMESTAMPS) / 2.0

def get_ssl_config():
    ca_paths = [
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/ssl/cert.pem",
        "/usr/local/etc/openssl/cert.pem",
        "/etc/pki/tls/certs/ca-bundle.crt",
    ]
    for path in ca_paths:
        if os.path.exists(path):
            return {"ca": path, "check_hostname": True}
    return {"check_hostname": True}

def get_db_connection():
    parsed = urllib.parse.urlparse(DB_URL)
    host = parsed.hostname
    port = parsed.port or 4000
    user = parsed.username
    password = parsed.password
    db = parsed.path.lstrip('/')
    
    ssl_config = get_ssl_config()
    
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        ssl=ssl_config,
        cursorclass=pymysql.cursors.DictCursor
    )

def get_base_url(url_id: int) -> str:
    """Fetch base URL from cached storage or database lookup."""
    with BASE_URL_CACHE_LOCK:
        if url_id in BASE_URL_CACHE:
            return BASE_URL_CACHE[url_id]
            
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT url FROM base_urls WHERE id = %s", (url_id,))
            row = cursor.fetchone()
        conn.close()
        if row:
            url = row["url"]
            with BASE_URL_CACHE_LOCK:
                BASE_URL_CACHE[url_id] = url
            return url
    except Exception as e:
        log.error(f"Failed to fetch base url for ID {url_id}: {e}")
    return ""

def load_db_stats():
    """Load stats from DB cache to know how many streams are already done and total songs."""
    try:
        with DB_CACHE_LOCK:
            total_count = len(DB_CACHE)
            done_count = sum(1 for song in DB_CACHE.values() if song.get("stream_sources"))
            
        STATS["done"] = done_count
        STATS["total"] = total_count
        log.info(f"Loaded DB stats from cache: Total Songs = {total_count}, Cached Streams = {done_count}")
    except Exception as e:
        log.error(f"Failed to load DB stats: {e}")

def init_db():
    """Ensure the songs and base_urls tables exist, run migrations, and seed default URLs."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 1. Create base_urls table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS base_urls (
                id INT AUTO_INCREMENT PRIMARY KEY,
                url VARCHAR(500) UNIQUE NOT NULL
            )
            """)
            conn.commit()
            
            # 2. Seed default base_urls
            cursor.execute("""
            INSERT INTO base_urls (id, url) VALUES
            (1, 'https://huggingface.co/buckets/allnewuser/lyrics/resolve'),
            (2, 'https://velamhere-img.hf.space'),
            (3, 'https://velamhere-img.hf.space/song'),
            (4, 'https://huggingface.co/buckets/shashwatIDR/lossless/resolve')
            ON DUPLICATE KEY UPDATE url = VALUES(url)
            """)
            conn.commit()

            # 3. Create songs table if not exists
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS songs (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                artists VARCHAR(500),
                lyrics_sources VARCHAR(50),
                canvas_sources VARCHAR(50),
                stream_sources VARCHAR(50),
                lossless VARCHAR(50)
            )
            """)
            conn.commit()
            
            # Add stream_sources column if it does not exist
            cursor.execute("SHOW COLUMNS FROM songs LIKE 'stream_sources'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE songs ADD COLUMN stream_sources VARCHAR(50)")
                conn.commit()

            cursor.execute("SHOW COLUMNS FROM songs LIKE 'lossless'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE songs ADD COLUMN lossless VARCHAR(50)")
                conn.commit()
                log.info("Added lossless column to songs table.")
            else:
                log.info("Database schema verified.")
        conn.close()
        # Load database cache and stats
        load_db_cache()
        load_db_stats()
    except Exception as e:
        log.error(f"Database initialization failed: {e}")

DB_BACKUP_INTERVAL = int(os.environ.get("DB_BACKUP_INTERVAL", "180"))

def db_backup_loop():
    log.info(f"Starting database backup daemon thread (interval: {DB_BACKUP_INTERVAL}s, dirty-checked)...")
    while True:
        try:
            time.sleep(DB_BACKUP_INTERVAL)
            try:
                import db_service
                if not getattr(db_service, "DB_CACHE_DIRTY", True):
                    continue
            except Exception:
                pass

            # Read from memory cache instead of DB
            with DB_CACHE_LOCK:
                songs = list(DB_CACHE.values())

            # SAFEGUARD: Never overwrite backup with empty data (prevents clearing backup if DB is down)
            if not songs:
                log.warning("DB backup skipped: DB_CACHE is empty. Refusing to overwrite db_backup.json.")
                continue

            with BASE_URL_CACHE_LOCK:
                base_urls = [{"id": k, "url": v} for k, v in BASE_URL_CACHE.items()]

            try:
                conn = get_db_connection()
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM base_urls")
                    db_base_urls = cursor.fetchall()
                    if db_base_urls:
                        base_urls = db_base_urls
                conn.close()
            except Exception as conn_err:
                log.warning(f"Database connection unavailable during backup, using in-memory base_urls: {conn_err}")
            
            backup_data = {
                "songs": songs,
                "base_urls": base_urls
            }
            json_bytes = json.dumps(backup_data, default=str, separators=(',', ':')).encode('utf-8')
            
            from db_service import safe_batch_bucket_files
            safe_batch_bucket_files(
                BUCKET_ID,
                add=[(json_bytes, "db_backup.json")],
                token=HF_TOKEN
            )
            del json_bytes
            try:
                import db_service
                db_service.cleanup_upload_cache()
                db_service.DB_CACHE_DIRTY = False
            except Exception:
                pass
            log.info(f"Database backup ({len(songs)} songs) successfully uploaded to Hugging Face (db_backup.json).")
        except Exception as e:
            log.error(f"Failed to backup database to Hugging Face: {e}")

# Initialize FastAPI App
app = FastAPI(title="Muzo Stream Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    init_db()
    # Start database backup loop in a separate daemon thread
    backup_thread = threading.Thread(target=db_backup_loop, daemon=True)
    backup_thread.start()

def trigger_startup_sync():
    log.info("Automatic startup sync disabled by configuration.")

def db_get_song_stream(video_id: str) -> Optional[dict]:
    with DB_CACHE_LOCK:
        return DB_CACHE.get(video_id)

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
    return "Unknown Song", "Unknown Artist"

def db_save_stream_source(video_id: str, value: str = "m4a"):
    # Determine metadata from memory cache
    name = ""
    artists = ""
    with DB_CACHE_LOCK:
        existing = DB_CACHE.get(video_id)
        if existing:
            name = existing.get("name") or ""
            artists = existing.get("artists") or ""
    
    # Only fetch metadata if it is missing or invalid
    if not name or name == "Unknown Song" or not artists:
        name, artists = fetch_correct_metadata(video_id)
        
    # Update in-memory cache first
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
        
    try:
        # Update DB
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Query for existing metadata
            cursor.execute("SELECT id FROM songs WHERE id = %s", (video_id,))
            row = cursor.fetchone()
            
            if row:
                cursor.execute("""
                UPDATE songs SET name = %s, artists = %s, stream_sources = %s WHERE id = %s
                """, (name, artists, value, video_id))
            else:
                cursor.execute("""
                INSERT INTO songs (id, name, artists, stream_sources)
                VALUES (%s, %s, %s, %s)
                """, (video_id, name, artists, value))
            conn.commit()
        conn.close()
        log.info(f"Updated stream_sources to {value} for {video_id}")
    except Exception as e:
        log.error(f"Failed to save stream source to DB for {video_id} (preserved in memory cache): {e}")

# --------------------------------------------------------------------------
# Automatic Cache Clearance Logic
# --------------------------------------------------------------------------

def check_and_clear_cache():
    """Check if Hugging Face bucket streams size exceeds 20 GB, and if so, purge all streams and clear DB references."""
    return

def download_and_upload_loop(video_id: str) -> Optional[str]:
    """Loop through Invidious instances, fetch stream metadata, download (30s timeout), and upload to HF."""
        
    STREAM_JOBS[video_id] = {"status": "progressing", "timestamp": time.time()}
    STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["fetching", "progressing"])
    
    for instance in INVIDIOUS_INSTANCES:
        try:
            log.info(f"Attempting download/upload for {video_id} using {instance}")
            # Fetch metadata and download audio in one HTTP client context with 60s timeout
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
                
                # Filter and prioritize m4a formats
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
                
                # Reconstruct proxied URL using the same instance
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
                
            # Upload to HF
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
            
            # Update DB with ext
            db_save_stream_source(video_id, ext)
            STREAM_JOBS[video_id] = {"status": "done", "timestamp": time.time()}
            STATS["done"] += 1
            UPLOAD_TIMESTAMPS.append(time.time())
            log.info(f"Stream upload successfully completed for {video_id}.{ext} using {instance}")
            
            # Post-upload check for cache limits
            check_and_clear_cache()
            return ext
            
        except Exception as e:
            log.warning(f"Exception using instance {instance} for {video_id}: {e}")
            if not isinstance(e, (httpx.HTTPError, httpx.NetworkError, httpx.TimeoutException, ValueError, KeyError)):
                err_msg = f"{type(e).__name__} during sync for {video_id}: {str(e)}"
                if err_msg not in FATAL_ERRORS:
                    FATAL_ERRORS.append(err_msg)
            continue
            
    # All instances failed - Try ultimate fallback
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
                
                ext = "m4a"  # default to m4a
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
                STREAM_JOBS[video_id] = {"status": "done", "timestamp": time.time()}
                STATS["done"] += 1
                UPLOAD_TIMESTAMPS.append(time.time())
                log.info(f"Stream upload successfully completed for {video_id}.{ext} via ultimate fallback")
                return ext
    except Exception as fallback_ex:
        log.error(f"Ultimate fallback failed for {video_id}: {fallback_ex}")

    STREAM_JOBS[video_id] = {"status": "failed", "error": "All Invidious instances and ultimate fallback failed", "timestamp": time.time()}
    STATS["failed"] += 1
    STATS["progressing"] = sum(1 for j in STREAM_JOBS.values() if j["status"] in ["fetching", "progressing"])
    return None

@app.get("/api/stream")
async def get_stream(id: str = Query(..., description="YouTube videoId"), background_tasks: BackgroundTasks = None):
    # 1. Check if stream is already cached
    row = db_get_song_stream(id)
    name = ""
    artists = ""
    lossless_val = None
    if row:
        name = row.get("name") or ""
        artists = row.get("artists") or ""
        stream_src = row.get("stream_sources") or ""
        lossless_src = row.get("lossless") or ""
        
        if lossless_src:
            lossless_val = f"https://huggingface.co/buckets/shashwatIDR/lossless/resolve/{id}.flac"
        else:
            # Enqueue background lossless processing if not already present
            lossless_val = "processing in lossless"
            try:
                from queue_manager import trigger_lossless_backup
                if background_tasks:
                    background_tasks.add_task(trigger_lossless_backup)
            except Exception:
                pass

        if stream_src:
            cached_url = f"https://velamhere-img.hf.space/song/{id}"
            log.info(f"Cache hit: returning stream URL for {id}")
            return {"id": id, "name": name, "artist": artists, "url": cached_url, "lossless": lossless_val}
    else:
        # Increment total count as this song ID is new to the DB
        STATS["total"] += 1
    
    # 2. Cache miss: download and upload using the retry loop
    ext = download_and_upload_loop(id)
    if not ext:
        raise HTTPException(status_code=502, detail="Failed to fetch and cache stream from all Invidious instances")
        
    cached_url = f"https://velamhere-img.hf.space/song/{id}"
    row = db_get_song_stream(id) or {}
    lossless_src = row.get("lossless") or ""
    if lossless_src:
        lossless_val = f"https://huggingface.co/buckets/shashwatIDR/lossless/resolve/{id}.flac"
    else:
        lossless_val = "processing in lossless"

    log.info(f"Cache miss resolved: returning stream URL for {id}")
    return {"id": id, "name": name, "artist": artists, "url": cached_url, "lossless": lossless_val}

def bulk_sync_worker(song_ids):
    from concurrent.futures import ThreadPoolExecutor
    log.info(f"Bulk sync worker started for {len(song_ids)} songs with 100 parallel threads")
    
    def sync_single_song(song_id):
        try:
            # Check if already processed while waiting
            row = db_get_song_stream(song_id)
            if row:
                stream_src = row.get("stream_sources") or ""
                if stream_src:
                    return
                
            log.info(f"Syncing stream for {song_id}")
            download_and_upload_loop(song_id)
        except Exception as e:
            log.error(f"Error syncing {song_id}: {e}")

    with ThreadPoolExecutor(max_workers=50) as executor:
        executor.map(sync_single_song, song_ids)
    log.info("Bulk sync worker completed.")

@app.post("/api/stream/sync")
def sync_streams(background_tasks: BackgroundTasks):
    """Scan all songs in DB and cache streams for those that don't have it yet."""
    try:
        with DB_CACHE_LOCK:
            song_ids = [song_id for song_id, song in DB_CACHE.items() if not song.get("stream_sources")]
        log.info(f"Sync triggered: found {len(song_ids)} songs to cache.")
        
        if song_ids:
            background_tasks.add_task(bulk_sync_worker, song_ids)
        return {"message": f"Sync started in background for {len(song_ids)} songs."}
    except Exception as e:
        log.error(f"Sync failed to start: {e}")
        raise HTTPException(status_code=502, detail=str(e))

@app.post("/api/lossless/sync")
def sync_lossless_backup(background_tasks: BackgroundTasks):
    """Trigger background bulk sync for all songs missing lossless audio."""
    try:
        from queue_manager import trigger_lossless_backup
        count = trigger_lossless_backup()
        return {"message": f"Lossless bulk backup started: queued {count} songs missing lossless audio."}
    except Exception as e:
        log.error(f"Lossless sync failed to start: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Internet Archive endpoints removed

@app.get("/api/stats")
def get_stats():
    total, lyrics, canvas, stream, lossless_count = 0, 0, 0, 0, 0
    try:
        with DB_CACHE_LOCK:
            total = len(DB_CACHE)
            for song in DB_CACHE.values():
                if song.get("lyrics_sources"):
                    lyrics += 1
                if song.get("canvas_sources"):
                    canvas += 1
                if song.get("stream_sources"):
                    stream += 1
                lossless_val = str(song.get("lossless") or "").strip()
                if lossless_val and lossless_val != "0":
                    lossless_count += 1
    except Exception as e:
        log.error(f"Failed to calculate stats from cache: {e}")

    try:
        from queue_manager import LOSSLESS_STATS
    except ImportError:
        LOSSLESS_STATS = {"queued": 0, "progressing": 0, "done": 0, "failed": 0}

    return {
        "total": total,
        "lyrics": lyrics,
        "canvas": canvas,
        "stream": stream,
        "lossless": lossless_count,
        "lossless_stats": LOSSLESS_STATS,
        "stats": STATS,
        "avg_uploads_per_min": get_avg_uploads_per_min(),
        "network_stats": NETWORK_STATS,
        "fatal_errors": list(FATAL_ERRORS),
        "jobs": {jid: {"status": job["status"], "timestamp": job["timestamp"]} for jid, job in STREAM_JOBS.items()},
        "logs": [mask_urls(line) for line in RECENT_LOGS]
    }

@app.get("/api/search")
def search_songs(q: Optional[str] = None, limit: int = 12):
    try:
        results = []
        q_lower = q.lower() if q else None
        
        with DB_CACHE_LOCK:
            songs = list(DB_CACHE.values())
            
        if q_lower:
            for s in songs:
                s_id = s.get("id") or ""
                s_name = s.get("name") or ""
                s_artists = s.get("artists") or ""
                if (q_lower in s_id.lower() or 
                    q_lower in s_name.lower() or 
                    q_lower in s_artists.lower()):
                    results.append(s)
                    if len(results) >= limit:
                        break
        else:
            # Return last N songs
            results = songs[-limit:]
            results.reverse()
            
        return results
    except Exception as e:
        log.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lyrics/{video_id}")
async def get_lyrics(video_id: str):
    try:
        with DB_CACHE_LOCK:
            row = DB_CACHE.get(video_id)
        
        if not row or not row.get("lyrics_sources"):
            raise HTTPException(status_code=404, detail="No cached lyrics for this videoId in fallback DB")
            
        lyrics_src = row["lyrics_sources"]
        url_id = 1
        ext = "json"
        if ":" in lyrics_src:
            parts = lyrics_src.split(":")
            try:
                url_id = int(parts[0])
            except ValueError:
                url_id = 1
            if len(parts) > 1:
                ext = parts[1]
                
        with BASE_URL_CACHE_LOCK:
            url_base = BASE_URL_CACHE.get(url_id)
        
        if url_base:
            target_url = f"{url_base}/{video_id}.{ext}"
        else:
            target_url = f"https://huggingface.co/buckets/allnewuser/lyrics/resolve/{video_id}.{ext}"
            
        headers = {}
        if HF_TOKEN:
            headers["Authorization"] = f"Bearer {HF_TOKEN}"
            
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(target_url, headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail="Failed to fetch lyrics from Hugging Face bucket/IA")
            try:
                return resp.json()
            except Exception:
                return HTMLResponse(content=resp.text, media_type="application/json")
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Lyrics fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/canvas")
def get_canvas(id: str):
    try:
        with DB_CACHE_LOCK:
            row = DB_CACHE.get(id)
        
        if not row or not row.get("canvas_sources"):
            raise HTTPException(status_code=404, detail="No cached canvas for this id in fallback DB")
            
        with BASE_URL_CACHE_LOCK:
            url_base = BASE_URL_CACHE.get(2)
        
        if not url_base or "huggingface.co/buckets" in str(url_base):
            url_base = "https://velamhere-img.hf.space"
        
        stream_url = f"{url_base}/{id}"
            
        return {
            "id": id,
            "name": row["name"],
            "artist": row["artists"],
            "url": stream_url
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Canvas fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/canvas/stream/{video_id}")
def stream_canvas(video_id: str):
    try:
        with BASE_URL_CACHE_LOCK:
            url_base = BASE_URL_CACHE.get(2)
        
        if not url_base or "huggingface.co/buckets" in str(url_base):
            url_base = "https://velamhere-img.hf.space"
        
        redirect_url = f"{url_base}/{video_id}"
            
        return RedirectResponse(url=redirect_url, status_code=307)
    except Exception as e:
        log.error(f"Canvas stream redirect failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stream/listen/{video_id}")
def listen_stream(video_id: str):
    try:
        row = db_get_song_stream(video_id)
        
        url_id = 3
        ext = None
        if row and row.get("stream_sources"):
            stream_src = row["stream_sources"]
            if ":" in stream_src:
                parts = stream_src.split(":")
                try:
                    url_id = int(parts[0])
                except ValueError:
                    url_id = 3
                if len(parts) > 1:
                    ext = parts[1]
            else:
                ext = stream_src

        # If not cached yet, download and upload it first
        if not ext:
            log.info(f"Stream miss in listen_stream for {video_id}, downloading and uploading...")
            ext = download_and_upload_loop(video_id)
            if not ext:
                raise HTTPException(status_code=502, detail="Failed to fetch and cache stream")
                    
        with BASE_URL_CACHE_LOCK:
            url_base = BASE_URL_CACHE.get(url_id)
        
        if url_base:
            redirect_url = f"{url_base}/{video_id}"
        else:
            redirect_url = f"https://velamhere-img.hf.space/song/{video_id}"
            
        return RedirectResponse(url=redirect_url, status_code=307)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Stream listen redirect failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.api_route("/api/stream/add", methods=["GET", "POST"])
def add_stream_to_queue(id: str = Query(..., description="YouTube videoId or comma-separated videoIds"), background_tasks: BackgroundTasks = None):
    """Add video ID(s) to the stream processing queue (in-memory execution). Accepts comma-separated IDs."""
    try:
        raw_ids = [v.strip() for v in id.split(",") if v.strip()]
        if not raw_ids:
            raise HTTPException(status_code=400, detail="No valid IDs provided")
            
        cached_ids = []
        queued_ids = []
        
        for vid in raw_ids:
            row = db_get_song_stream(vid)
            if row and row.get("stream_sources"):
                cached_ids.append(vid)
            else:
                queued_ids.append(vid)
                if background_tasks:
                    background_tasks.add_task(download_and_upload_loop, vid)
                else:
                    thread = threading.Thread(target=download_and_upload_loop, args=(vid,), daemon=True)
                    thread.start()
            
        return {
            "message": f"Processed {len(raw_ids)} ID(s). {len(queued_ids)} queued, {len(cached_ids)} already cached.",
            "queued": queued_ids,
            "cached": cached_ids
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Failed to add streams to queue: {e}")
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

    try:
        from queue_manager import TASK_QUEUE, LOSSLESS_TASK_QUEUE, QUEUED_SET, LOSSLESS_QUEUED_SET, QUEUED_SET_LOCK, LOSSLESS_QUEUED_LOCK
        with QUEUED_SET_LOCK:
            stream_queued_ids = set(QUEUED_SET)
        with LOSSLESS_QUEUED_LOCK:
            lossless_queued_ids = set(LOSSLESS_QUEUED_SET)
        combined_queued = len(stream_queued_ids | lossless_queued_ids)
        sq_size = TASK_QUEUE.qsize()
        lq_size = LOSSLESS_TASK_QUEUE.qsize()
    except Exception:
        combined_queued = STATS.get("progressing", 0)
        sq_size = 0
        lq_size = 0

    return {
        "status": "online",
        "lossless": lossless_count,
        "m4a": m4a_count,
        "in_queue": combined_queued,
        "queue_details": {
            "stream_queue": sq_size,
            "lossless_queue": lq_size,
            "total_combined_queued": combined_queued
        }
    }

@app.get("/admin", response_class=HTMLResponse)
def get_admin_page():
    try:
        import admin
        return HTMLResponse(content=admin.get_admin_html())
    except Exception:
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import admin
        return HTMLResponse(content=admin.get_admin_html())

@app.get("/", response_class=HTMLResponse)
def dashboard():
    html_content = r"""<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MLC Playground</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link
    href="https://fonts.googleapis.com/css2?family=Bungee&family=Space+Mono:wght@400;700&family=Manrope:wght@400;500;600;700;800&display=swap"
    rel="stylesheet">
  <style>
    :root {
      /* ── Jukebox Console Palette ─────────────────── */
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

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    html {
      scrollbar-color: var(--metal-light) var(--bg-raised);
    }

    body {
      background-color: var(--bg);
      background-image:
        radial-gradient(ellipse 900px 500px at 15% -10%, rgba(255, 183, 51, 0.06), transparent 60%),
        radial-gradient(ellipse 900px 500px at 90% 0%, rgba(99, 230, 234, 0.05), transparent 60%);
      color: var(--cream);
      font-family: var(--font-body);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      overflow-x: hidden;
      -webkit-font-smoothing: antialiased;
    }

    ::selection {
      background: var(--amber-soft);
      color: var(--amber);
    }

    /* Brushed metal texture overlay */
    .metal-bg {
      position: fixed;
      inset: 0;
      z-index: -1;
      opacity: 0.5;
      background-image: repeating-linear-gradient(180deg, rgba(255, 255, 255, 0.012) 0px, rgba(255, 255, 255, 0.012) 1px, transparent 1px, transparent 3px);
      pointer-events: none;
    }

    header {
      border-bottom: 2px solid var(--rule-strong);
      background:
        repeating-linear-gradient(100deg, rgba(255, 255, 255, 0.015) 0px, rgba(255, 255, 255, 0.015) 1px, transparent 1px, transparent 5px),
        var(--metal);
      padding: 1.35rem 2rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 1rem;
    }

    .marquee {
      background: var(--glass-deep);
      border: 3px solid var(--metal-light);
      border-radius: 10px;
      padding: 0.6rem 1.4rem;
      box-shadow: inset 0 2px 10px rgba(0, 0, 0, 0.6), inset 0 0 30px rgba(255, 183, 51, 0.05);
      animation: flicker-in 1.4s ease-out;
    }

    @keyframes flicker-in {
      0% {
        opacity: 0;
      }

      8% {
        opacity: 1;
      }

      12% {
        opacity: 0.2;
      }

      20% {
        opacity: 1;
      }

      26% {
        opacity: 0.4;
      }

      34% {
        opacity: 1;
      }

      100% {
        opacity: 1;
      }
    }

    .marquee h1 {
      font-family: var(--font-display);
      font-size: 1.4rem;
      letter-spacing: 0.02em;
      color: var(--amber);
      text-shadow: 0 0 4px var(--amber-glow), 0 0 16px rgba(255, 183, 51, 0.35);
      display: inline;
    }

    .marquee h1 .sep {
      color: var(--cream-dim);
      font-size: 1rem;
      margin: 0 0.35rem;
      text-shadow: none;
    }

    .marquee h1 em {
      font-style: normal;
      color: var(--cyan);
      text-shadow: 0 0 4px var(--cyan-glow), 0 0 16px rgba(99, 230, 234, 0.35);
      display: inline;
    }

    .marquee p {
      color: var(--cream-dim);
      font-family: var(--font-mono);
      font-size: 0.68rem;
      letter-spacing: 0.04em;
      margin-top: 0.3rem;
      text-transform: uppercase;
    }

    .header-right {
      display: flex;
      align-items: center;
      gap: 0.85rem;
      flex-wrap: wrap;
    }

    .vu-pill {
      border: 1px solid var(--rule-strong);
      padding: 0.4rem 0.85rem;
      border-radius: 6px;
      background: var(--glass);
      display: flex;
      align-items: center;
      gap: 0.5rem;
      font-family: var(--font-mono);
      font-size: 0.72rem;
      color: var(--cream-dim);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      box-shadow: inset 0 1px 4px rgba(0, 0, 0, 0.5);
    }

    .vu-pill .led {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--amber);
      box-shadow: 0 0 6px var(--amber-glow), 0 0 2px var(--amber);
      flex-shrink: 0;
    }

    .vu-pill.cyan-led .led {
      background: var(--cyan);
      box-shadow: 0 0 6px var(--cyan-glow), 0 0 2px var(--cyan);
    }

    .vu-pill strong {
      color: var(--cream);
      font-weight: 700;
      font-size: 0.85rem;
    }

    .chrome-badge {
      font-family: var(--font-mono);
      background: linear-gradient(180deg, var(--metal-light), var(--metal));
      border: 1px solid var(--rule-strong);
      color: var(--cream-dim);
      padding: 0.45rem 0.9rem;
      border-radius: 99px;
      font-size: 0.72rem;
      font-weight: 500;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.08), 0 1px 3px rgba(0, 0, 0, 0.4);
    }

    .app-layout {
      flex: 1;
      display: grid;
      grid-template-columns: 1fr;
      max-width: 1600px;
      margin: 0 auto;
      width: 100%;
    }

    @media (min-width: 1024px) {
      .app-layout {
        grid-template-columns: 350px 1fr;
      }
    }

    /* Sidebar — Selector Panel */
    .sidebar {
      border-right: 2px solid var(--rule);
      background: var(--bg-raised);
      padding: 2rem 1.6rem;
      display: flex;
      flex-direction: column;
      gap: 1.5rem;
      overflow-y: auto;
    }

    .panel-label {
      font-family: var(--font-display);
      font-size: 0.72rem;
      letter-spacing: 0.08em;
      color: var(--amber);
      margin-bottom: 1rem;
      padding-bottom: 0.6rem;
      border-bottom: 2px solid var(--red);
      display: inline-block;
    }

    .endpoint-card {
      background: var(--metal);
      border: 1px solid var(--rule);
      border-radius: 8px;
      padding: 0.9rem;
      margin-bottom: 0.9rem;
      display: flex;
      gap: 0.75rem;
      transition: border-color 0.2s ease, transform 0.15s ease;
    }

    .endpoint-card:hover {
      border-color: var(--amber);
      transform: translateX(2px);
    }

    .endpoint-code {
      flex-shrink: 0;
      width: 34px;
      height: 34px;
      border-radius: 50%;
      background: var(--glass-deep);
      border: 2px solid var(--red);
      color: var(--cream);
      font-family: var(--font-mono);
      font-weight: 700;
      font-size: 0.72rem;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .endpoint-badge {
      display: inline-block;
      font-family: var(--font-mono);
      font-size: 0.62rem;
      font-weight: 700;
      padding: 0.1rem 0.4rem;
      border-radius: 3px;
      text-transform: uppercase;
      background: var(--amber-soft);
      color: var(--amber);
      border: 1px solid rgba(255, 183, 51, 0.3);
      margin-bottom: 0.4rem;
    }

    .endpoint-path {
      font-family: var(--font-mono);
      font-size: 0.78rem;
      font-weight: 700;
      color: var(--cream);
      word-break: break-all;
    }

    .endpoint-desc {
      font-size: 0.73rem;
      color: var(--cream-dim);
      margin-top: 0.4rem;
      line-height: 1.45;
    }

    .code-snippet {
      background: var(--glass-deep);
      border-top: 1px dashed var(--rule-strong);
      border-radius: 0 0 4px 4px;
      padding: 0.5rem 0;
      margin-top: 0.6rem;
      font-family: var(--font-mono);
      font-size: 0.66rem;
      color: var(--cyan);
      overflow-x: auto;
      white-space: pre;
    }

    /* Main workspace */
    .main-content {
      padding: 2.25rem 2.5rem 3rem;
      display: flex;
      flex-direction: column;
      gap: 2.5rem;
      overflow-y: auto;
    }

    /* Search — coin slot */
    .search-wrapper {
      position: relative;
      width: 100%;
    }

    .search-input {
      width: 100%;
      padding: 0.9rem 1rem 0.9rem 2.85rem;
      background: var(--glass-deep);
      border: 2px solid var(--metal-light);
      border-radius: 8px;
      color: var(--cream);
      font-family: var(--font-mono);
      font-size: 0.9rem;
      outline: none;
      box-shadow: inset 0 2px 8px rgba(0, 0, 0, 0.55);
      transition: border-color 0.2s, box-shadow 0.2s;
    }

    .search-input::placeholder {
      color: var(--cream-dim);
      opacity: 0.6;
    }

    .search-input:focus {
      border-color: var(--amber);
      box-shadow: inset 0 2px 8px rgba(0, 0, 0, 0.55), 0 0 0 3px var(--amber-soft);
    }

    .search-icon {
      position: absolute;
      left: 1rem;
      top: 50%;
      transform: translateY(-50%);
      color: var(--cyan);
      width: 16px;
      height: 16px;
    }

    /* Section titles with LED marker */
    .section-title {
      font-family: var(--font-display);
      font-size: 0.85rem;
      letter-spacing: 0.04em;
      color: var(--cream);
      margin-bottom: 1rem;
      display: flex;
      align-items: center;
      gap: 0.6rem;
    }

    .section-title::before {
      content: "";
      display: inline-block;
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--cyan);
      box-shadow: 0 0 6px var(--cyan-glow);
      animation: pulse-led 2.2s ease-in-out infinite;
    }

    @keyframes pulse-led {

      0%,
      100% {
        opacity: 1;
      }

      50% {
        opacity: 0.35;
      }
    }

    /* Song grid — title strip cards */
    .songs-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(270px, 1fr));
      gap: 1.1rem;
    }

    .song-card {
      position: relative;
      background: var(--cream);
      border-bottom: 5px solid var(--red);
      border-radius: 3px;
      padding: 1.1rem 1.2rem;
      cursor: pointer;
      transition: transform 0.18s ease, box-shadow 0.18s ease;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      min-height: 108px;
      box-shadow: 0 3px 0 rgba(0, 0, 0, 0.3), 0 6px 14px rgba(0, 0, 0, 0.35);
      overflow: hidden;
    }

    .song-card::after {
      content: "";
      position: absolute;
      inset: 0;
      background: linear-gradient(115deg, transparent 40%, rgba(255, 255, 255, 0.35) 48%, transparent 56%);
      opacity: 0;
      transition: opacity 0.2s ease;
      pointer-events: none;
    }

    .song-card:hover {
      transform: translateY(-3px) rotate(-0.6deg);
      box-shadow: 0 5px 0 rgba(0, 0, 0, 0.3), 0 12px 22px rgba(0, 0, 0, 0.4);
    }

    .song-card:hover::after {
      opacity: 1;
    }

    .song-title-text {
      font-family: var(--font-display);
      font-size: 0.92rem;
      letter-spacing: 0.01em;
      color: var(--ink);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      line-height: 1.3;
    }

    .song-artist {
      font-family: var(--font-mono);
      font-size: 0.72rem;
      color: var(--ink);
      opacity: 0.62;
      text-transform: uppercase;
      letter-spacing: 0.03em;
      margin-top: 0.3rem;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .song-meta-tags {
      display: flex;
      gap: 0.4rem;
      margin-top: 0.85rem;
    }

    .meta-tag {
      font-family: var(--font-mono);
      font-size: 0.6rem;
      font-weight: 700;
      padding: 0.18rem 0.45rem;
      border-radius: 99px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }

    .tag-lyrics {
      background: var(--ink);
      color: var(--cream);
    }

    .tag-canvas {
      background: var(--red);
      color: var(--cream);
    }

    .tag-stream {
      background: var(--cyan);
      color: var(--glass-deep);
    }

    /* Sandbox / service panel */
    .detail-container {
      display: grid;
      grid-template-columns: 1fr;
      gap: 1.75rem;
      background: var(--bg-raised);
      border: 2px solid var(--metal-light);
      border-radius: 14px;
      padding: 1.75rem;
      position: relative;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
    }

    @media (min-width: 1200px) {
      .detail-container {
        grid-template-columns: 1fr 260px;
      }
    }

    .lyrics-panel {
      display: flex;
      flex-direction: column;
      min-height: 380px;
    }

    .lyrics-header {
      border-bottom: 1px solid var(--rule-strong);
      padding-bottom: 0.9rem;
      margin-bottom: 1.25rem;
      display: flex;
      align-items: baseline;
      gap: 0.6rem;
    }

    .lyrics-header .led {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--amber);
      box-shadow: 0 0 6px var(--amber-glow);
      flex-shrink: 0;
    }

    .lyrics-header h2 {
      font-family: var(--font-display);
      font-size: 1.1rem;
      color: var(--cream);
    }

    .lyrics-header p {
      color: var(--cyan);
      font-family: var(--font-mono);
      font-size: 0.78rem;
      margin-top: 0.15rem;
    }

    .lyrics-body {
      flex: 1;
      background: var(--glass-deep);
      border: 1px solid var(--rule);
      border-radius: 8px;
      padding: 1.1rem 1.2rem;
      font-family: var(--font-mono);
      font-size: 0.88rem;
      line-height: 1.85;
      color: var(--cream);
      overflow-y: auto;
      max-height: 400px;
      white-space: pre-wrap;
      box-shadow: inset 0 2px 10px rgba(0, 0, 0, 0.5);
    }

    /* Canvas viewport bezel */
    .canvas-panel {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 1.5rem;
    }

    .canvas-wrapper {
      width: 100%;
      aspect-ratio: 9 / 16;
      border-radius: 10px;
      overflow: hidden;
      background: #000;
      border: 4px solid var(--metal-light);
      position: relative;
      max-width: 210px;
      box-shadow: inset 0 0 0 2px var(--glass-deep), 0 4px 14px rgba(0, 0, 0, 0.5);
    }

    .canvas-video {
      width: 100%;
      height: 100%;
      object-fit: cover;
    }

    .canvas-fallback {
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      color: var(--cyan);
      text-align: center;
      padding: 1.5rem;
      background:
        repeating-linear-gradient(180deg, rgba(99, 230, 234, 0.03) 0px, rgba(99, 230, 234, 0.03) 1px, transparent 1px, transparent 3px),
        var(--glass-deep);
      font-family: var(--font-mono);
      font-size: 0.7rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }

    .canvas-fallback svg {
      width: 22px;
      height: 22px;
      margin-bottom: 0.6rem;
      opacity: 0.6;
    }

    /* Console / diagnostic CRT */
    .sandbox-console {
      grid-column: 1 / -1;
      border-top: 1px solid var(--rule-strong);
      padding-top: 1.5rem;
      margin-top: 0.25rem;
    }

    .console-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 0.75rem;
    }

    .console-title {
      font-family: var(--font-display);
      font-size: 0.75rem;
      letter-spacing: 0.04em;
      color: var(--cream-dim);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .console-output {
      background:
        repeating-linear-gradient(180deg, rgba(255, 183, 51, 0.025) 0px, rgba(255, 183, 51, 0.025) 1px, transparent 1px, transparent 3px),
        var(--glass-deep);
      border: 1px solid var(--rule);
      border-radius: 8px;
      padding: 1rem 1.1rem;
      font-family: var(--font-mono);
      font-size: 0.74rem;
      color: var(--amber);
      text-shadow: 0 0 3px rgba(255, 183, 51, 0.25);
      max-height: 180px;
      overflow-y: auto;
      white-space: pre-wrap;
      box-shadow: inset 0 2px 10px rgba(0, 0, 0, 0.5);
    }

    .no-selection {
      text-align: center;
      padding: 4.5rem 2rem;
      color: var(--cream-dim);
      border: 2px dashed var(--metal-light);
      border-radius: 12px;
      font-family: var(--font-mono);
      font-size: 0.85rem;
      background: var(--bg-raised);
    }

    .no-selection strong {
      display: block;
      font-family: var(--font-display);
      color: var(--amber);
      font-size: 1rem;
      margin-bottom: 0.6rem;
      letter-spacing: 0.02em;
    }

    .hidden {
      display: none !important;
    }

    /* Shimmer loader */
    .shimmer {
      background: linear-gradient(90deg, var(--cream) 25%, #e8dcc0 50%, var(--cream) 75%);
      background-size: 200% 100%;
      animation: loading 1.4s infinite linear;
    }

    @keyframes loading {
      0% {
        background-position: 200% 0;
      }

      100% {
        background-position: -200% 0;
      }
    }

    /* Ticket toast */
    .toast {
      position: fixed;
      bottom: 2rem;
      right: 2rem;
      background: var(--cream);
      border-left: 4px solid var(--red);
      color: var(--ink);
      padding: 0.85rem 1.25rem;
      border-radius: 4px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.45);
      z-index: 10000;
      transform: translateY(150%);
      transition: transform 0.3s cubic-bezier(0.16, 1, 0.3, 1);
      font-family: var(--font-mono);
      font-size: 0.8rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .toast.show {
      transform: translateY(0);
    }

    /* Scrollbars */
    .sidebar::-webkit-scrollbar,
    .lyrics-body::-webkit-scrollbar,
    .console-output::-webkit-scrollbar {
      width: 8px;
    }

    .sidebar::-webkit-scrollbar-track,
    .lyrics-body::-webkit-scrollbar-track,
    .console-output::-webkit-scrollbar-track {
      background: transparent;
    }

    .sidebar::-webkit-scrollbar-thumb,
    .lyrics-body::-webkit-scrollbar-thumb,
    .console-output::-webkit-scrollbar-thumb {
      background: var(--metal-light);
      border-radius: 4px;
    }
  </style>
</head>

<body>
  <div class="metal-bg"></div>

  <header>
    <div class="marquee">
      <h1>MLC<span class="sep">/</span><em>PLAYGROUND</em></h1>
      <p>Fallback cache · synced lyrics &amp; canvas loops</p>
    </div>
    <div class="header-right">
      <div class="vu-pill">
        <span class="led"></span> Lyrics <strong id="stats-lyrics">···</strong>
      </div>
      <div class="vu-pill cyan-led">
        <span class="led"></span> Canvas <strong id="stats-canvas">···</strong>
      </div>
      <div class="vu-pill" style="border-color: var(--cyan); color: var(--cyan); margin-left: 0.5rem;">
        <span class="led" style="background: var(--cyan); box-shadow: 0 0 6px var(--cyan);"></span> Stream <strong id="stats-stream">···</strong>
      </div>
      <div class="vu-pill" style="border-color: var(--amber); color: var(--amber); margin-left: 0.5rem;">
        <span class="led" style="background: var(--amber); box-shadow: 0 0 6px var(--amber-glow);"></span> Lossless <strong id="stats-lossless">···</strong>
      </div>
      <div class="chrome-badge">BASE: shashwatidr-filestore.hf.space</div>
    </div>
  </header>

  <div class="app-layout">
    <!-- Sidebar / API Docs -->
    <aside class="sidebar">
      <div>
        <span class="panel-label">Selector Panel</span>

        <!-- Endpoint A1 -->
        <div class="api-endpoint-card endpoint-card">
          <div class="endpoint-code">A1</div>
          <div style="flex:1; min-width:0;">
            <span class="endpoint-badge">GET</span>
            <div class="endpoint-path">/api/lyrics/:id</div>
            <div class="endpoint-desc">Retrieves synced and plain-text lyrics for a given YouTube video ID. Requires
              song metadata query parameters to fetch live if not cached.</div>
            <div class="code-snippet">curl shashwatidr-filestore.hf.space/api/lyrics/dQw4w9WgXcQ</div>
          </div>
        </div>

        <!-- Endpoint A2 -->
        <div class="api-endpoint-card endpoint-card">
          <div class="endpoint-code">A2</div>
          <div style="flex:1; min-width:0;">
            <span class="endpoint-badge">GET</span>
            <div class="endpoint-path">/api/canvas</div>
            <div class="endpoint-desc">Retrieves loop video configuration and CDN file stream URL for a given YouTube
              ID.</div>
            <div class="code-snippet">curl "shashwatidr-filestore.hf.space/api/canvas?id=dQw4w9WgXcQ"</div>
          </div>
        </div>

        <!-- Endpoint A3 -->
        <div class="api-endpoint-card endpoint-card">
          <div class="endpoint-code">A3</div>
          <div style="flex:1; min-width:0;">
            <span class="endpoint-badge">GET</span>
            <div class="endpoint-path">/api/canvas/stream/:id</div>
            <div class="endpoint-desc">Redirects to the direct Hugging Face hosted CDN video file for low-overhead
              client streaming.</div>
          </div>
        </div>

        <!-- Endpoint A4 -->
        <div class="api-endpoint-card endpoint-card">
          <div class="endpoint-code">A4</div>
          <div style="flex:1; min-width:0;">
            <span class="endpoint-badge" style="background: var(--cyan); color: var(--glass-deep);">GET</span>
            <div class="endpoint-path">/api/stream</div>
            <div class="endpoint-desc">Retrieves audio stream configuration and CDN stream file URL for a given YouTube ID.</div>
            <div class="code-snippet">curl "shashwatidr-filestore.hf.space/api/stream?id=dQw4w9WgXcQ"</div>
          </div>
        </div>

        <!-- Endpoint A5 -->
        <div class="api-endpoint-card endpoint-card">
          <div class="endpoint-code">A5</div>
          <div style="flex:1; min-width:0;">
            <span class="endpoint-badge" style="background: var(--cyan); color: var(--glass-deep);">GET</span>
            <div class="endpoint-path">/api/stream/listen/:id</div>
            <div class="endpoint-desc">Redirects to the direct Hugging Face hosted LFS audio file.</div>
          </div>
        </div>

        <!-- Endpoint B1 -->
        <div class="api-endpoint-card endpoint-card">
          <div class="endpoint-code">B1</div>
          <div style="flex:1; min-width:0;">
            <span class="endpoint-badge">GET</span>
            <div class="endpoint-path">/api/search</div>
            <div class="endpoint-desc">Queries cached database entries by song title, artists, or video ID.</div>
            <div class="code-snippet">curl "shashwatidr-filestore.hf.space/api/search?q=Never+Gonna+Give"</div>
          </div>
        </div>

        <!-- Endpoint B2 -->
        <div class="api-endpoint-card endpoint-card">
          <div class="endpoint-code">B2</div>
          <div style="flex:1; min-width:0;">
            <span class="endpoint-badge">GET</span>
            <div class="endpoint-path">/api/stats</div>
            <div class="endpoint-desc">Retrieves total cached counts of lyrics and canvas video loops.</div>
            <div class="code-snippet">curl "shashwatidr-filestore.hf.space/api/stats"</div>
          </div>
        </div>

        <!-- Sync & Queue Monitor -->
        <div style="margin-top: 2rem; border-top: 2px solid var(--rule-strong); padding-top: 1.5rem;">
          <span class="panel-label" style="border-color: var(--cyan); color: var(--cyan);">Sync & Queue Monitor</span>
          
          <button onclick="triggerSync()" style="width: 100%; padding: 0.6rem; background: var(--cyan); border: none; border-radius: 6px; color: var(--glass-deep); font-family: var(--font-display); font-size: 0.75rem; cursor: pointer; margin-bottom: 0.5rem; box-shadow: 0 0 8px var(--cyan-soft); font-weight: bold;">
            Trigger Stream Sync
          </button>

          <button onclick="triggerLosslessBackup()" style="width: 100%; padding: 0.6rem; background: var(--amber); border: none; border-radius: 6px; color: var(--ink); font-family: var(--font-display); font-size: 0.75rem; cursor: pointer; margin-bottom: 1rem; box-shadow: 0 0 8px var(--amber-soft); font-weight: bold;">
            Start Lossless Backup
          </button>

          <div style="display: flex; flex-direction: column; gap: 0.5rem; font-family: var(--font-mono); font-size: 0.7rem; color: var(--cream-dim);">
            <div style="display: flex; justify-content: space-between;">
              <span>Queue / Progressing:</span>
              <strong style="color: var(--amber);" id="val-progressing">0</strong>
            </div>
            <div style="display: flex; justify-content: space-between;">
              <span>Cached (Done):</span>
              <strong style="color: var(--cyan);" id="val-done">0</strong>
            </div>
            <div style="display: flex; justify-content: space-between;">
              <span>Failed:</span>
              <strong style="color: var(--red);" id="val-failed">0</strong>
            </div>
            <div style="display: flex; justify-content: space-between;">
              <span>Upload Rate:</span>
              <strong style="color: var(--cream);" id="val-rate">0.0 / min</strong>
            </div>
            <div style="display: flex; justify-content: flex-start; gap: 0.25rem; flex-direction: column; margin-top: 0.5rem; border-top: 1px dashed var(--rule); padding-top: 0.5rem;">
              <div>DL: <span id="val-dl-speed" style="color: var(--cyan);">0.0 KB/s</span> (Total: <span id="val-total-dl">0.0 MB</span>)</div>
              <div>UL: <span id="val-ul-speed" style="color: var(--amber);">0.0 KB/s</span> (Total: <span id="val-total-ul">0.0 MB</span>)</div>
            </div>
          </div>
        </div>
      </div>
    </aside>

    <!-- Main Workspace -->
    <main class="main-content">
      <!-- Search -->
      <div class="search-wrapper">
        <input type="text" class="search-input" id="searchBar" placeholder="SEARCH BY NAME, ARTIST, OR VIDEO ID...">
        <svg class="search-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"
          xmlns="http://www.w3.org/2000/svg">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
            d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path>
        </svg>
      </div>

      <!-- Featured Grid -->
      <div>
        <h3 class="section-title">Now Stocked</h3>
        <div class="songs-grid" id="songsGrid">
          <!-- Rendered song cards -->
        </div>
      </div>

      <!-- Playback Sandbox Details -->
      <div>
        <h3 class="section-title">Test Deck</h3>
        <div id="noSelection" class="no-selection">
          <strong>No title loaded</strong>
          Press a title strip above to run a live cache lookup and view the response payload.
        </div>

        <div id="detailContainer" class="detail-container hidden">
          <!-- Lyrics Explorer -->
          <div class="lyrics-panel">
            <div class="lyrics-header">
              <span class="led"></span>
              <div>
                <h2 id="detailTitle">Song Title</h2>
                <p id="detailArtist">Artist Name</p>
              </div>
            </div>
            <div class="lyrics-body" id="lyricsBody">
              <!-- Synced lyrics content -->
            </div>
          </div>

          <!-- Canvas Player Loop -->
          <div class="canvas-panel">
            <div class="canvas-wrapper">
              <video class="canvas-video hidden" id="canvasVideo" autoplay loop muted playsinline></video>
              <div class="canvas-fallback" id="canvasFallback">
                <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                    d="M15 10l4.553-2.276A1 1 0 0121 8.618v6.764a1 1 0 01-1.447.894L15 14M5 18h8a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z">
                  </path>
                </svg>
                <p>No Signal — Canvas Not Cached</p>
              </div>
            </div>
          </div>

          <!-- Stream Player -->
          <div class="stream-panel" style="margin-top: 1rem;">
            <div class="stream-wrapper" style="background: var(--glass); border: 1px solid var(--rule); border-radius: 8px; padding: 1rem; display: flex; flex-direction: column; gap: 0.5rem;">
              <span class="panel-label" style="font-size: 0.7rem; text-transform: uppercase; color: var(--cyan); letter-spacing: 0.05em; font-weight: 600; display: block; margin-bottom: 0.25rem;">Audio Stream</span>
              <audio id="streamAudio" controls style="width: 100%; height: 32px; outline: none; display: none;"></audio>
              <div id="streamFallback" style="text-align: center; color: var(--cream-dim); font-size: 0.75rem; padding: 0.5rem 0;">
                No Audio Cache
              </div>
              <div id="streamLinkContainer" style="display: none; margin-top: 0.25rem;">
                <a id="streamLink" href="#" target="_blank" style="color: var(--cyan); text-decoration: none; font-size: 0.75rem; word-break: break-all; font-family: var(--font-mono);">[HF Resolve Link]</a>
              </div>
            </div>
          </div>

          <!-- API Sandbox Console -->
          <div class="sandbox-console">
            <div class="console-header" style="display: flex; gap: 1rem; justify-content: flex-start; border-bottom: 1px solid var(--rule-strong); padding-bottom: 0.5rem; margin-bottom: 0.75rem;">
              <button onclick="switchTab('api')" id="tab-api" style="background: none; border: none; color: var(--amber); font-family: var(--font-display); font-size: 0.72rem; cursor: pointer; border-bottom: 2px solid var(--amber); padding-bottom: 0.25rem;">Diagnostic Readout</button>
              <button onclick="switchTab('logs')" id="tab-logs" style="background: none; border: none; color: var(--cream-dim); font-family: var(--font-display); font-size: 0.72rem; cursor: pointer; padding-bottom: 0.25rem;">Server Logs & Queue</button>
            </div>
            
            <div class="console-output" id="consoleOutput">
              Standing by for request...
            </div>
            
            <div class="console-output hidden" id="logsQueueOutput" style="display: grid; grid-template-columns: 1.5fr 1fr; gap: 1.5rem; max-height: 250px; overflow-y: auto;">
              <div style="border-right: 1px solid var(--rule-strong); padding-right: 0.5rem;">
                <div style="font-family: var(--font-display); font-size: 0.6rem; color: var(--cyan); margin-bottom: 0.5rem; text-transform: uppercase;">Live Logs</div>
                <div id="liveLogsContainer" style="font-family: var(--font-mono); font-size: 0.68rem; line-height: 1.4; color: var(--cream); overflow-y: auto; max-height: 200px;"></div>
              </div>
              <div>
                <div style="font-family: var(--font-display); font-size: 0.6rem; color: var(--cyan); margin-bottom: 0.5rem; text-transform: uppercase;">Active Stream Jobs</div>
                <div id="activeJobsContainer" style="font-family: var(--font-mono); font-size: 0.68rem; line-height: 1.4; color: var(--cream); overflow-y: auto; max-height: 200px;"></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </main>
  </div>

  <div class="toast" id="toast">
    <svg style="width: 16px; height: 16px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
    </svg>
    <span id="toastMsg">Success</span>
  </div>

  <script>
    const searchBar = document.getElementById('searchBar');
    const songsGrid = document.getElementById('songsGrid');
    const noSelection = document.getElementById('noSelection');
    const detailContainer = document.getElementById('detailContainer');
    const detailTitle = document.getElementById('detailTitle');
    const detailArtist = document.getElementById('detailArtist');
    const lyricsBody = document.getElementById('lyricsBody');
    const canvasVideo = document.getElementById('canvasVideo');
    const canvasFallback = document.getElementById('canvasFallback');
    const consoleOutput = document.getElementById('consoleOutput');
    const toast = document.getElementById('toast');
    const toastMsg = document.getElementById('toastMsg');

    let allSongs = [];

    function logConsole(url, method, status, responseData) {
      consoleOutput.innerHTML = `[HTTP] ${method} ${url}\n[STATUS] ${status}\n[RESPONSE]\n${JSON.stringify(responseData, null, 2)}`;
      consoleOutput.scrollTop = 0;
    }

    async function loadFeaturedSongs() {
      songsGrid.innerHTML = Array(4).fill(0).map(() => `
        <div class="song-card shimmer">
          <div style="height: 0.95rem; width: 60%; background: rgba(0,0,0,0.08); margin-bottom: 0.5rem; border-radius: 4px;"></div>
          <div style="height: 0.8rem; width: 40%; background: rgba(0,0,0,0.08); border-radius: 4px;"></div>
        </div>
      `).join('');

      try {
        const res = await fetch('/api/search?limit=12');
        if (res.ok) {
          allSongs = await res.json();
          renderSongs(allSongs);
        } else {
          songsGrid.innerHTML = '<p style="grid-column: 1/-1; text-align: center; color: var(--cream-dim); font-family: var(--font-mono);">Failed to load cached songs.</p>';
        }
      } catch (err) {
        songsGrid.innerHTML = '<p style="grid-column: 1/-1; text-align: center; color: var(--cream-dim); font-family: var(--font-mono);">Error connecting to fallback worker.</p>';
      }
    }

    function renderSongs(songs) {
      if (songs.length === 0) {
        songsGrid.innerHTML = '<p style="grid-column: 1/-1; text-align: center; color: var(--cream-dim); font-family: var(--font-mono);">No songs found in database.</p>';
        return;
      }

      songsGrid.innerHTML = songs.map(song => {
        const hasLyrics = !!song.lyrics_sources;
        const hasCanvas = !!song.canvas_sources;
        const hasStream = song.stream_sources && song.stream_sources.startsWith('3');
        return `
          <div class="song-card" onclick="selectSong('${song.id}', '${encodeURIComponent(song.name)}', '${encodeURIComponent(song.artists)}')">
            <div>
              <div class="song-title-text" title="${song.name}">${song.name}</div>
              <div class="song-artist" title="${song.artists}">${song.artists || 'Unknown Artist'}</div>
            </div>
            <div class="song-meta-tags">
              ${hasLyrics ? '<span class="meta-tag tag-lyrics">Lyrics</span>' : ''}
              ${hasCanvas ? '<span class="meta-tag tag-canvas">Canvas</span>' : ''}
              ${hasStream ? '<span class="meta-tag tag-stream">Stream</span>' : ''}
            </div>
          </div>
        `;
      }).join('');
    }

    async function selectSong(id, name, artist) {
      name = decodeURIComponent(name);
      artist = decodeURIComponent(artist);

      noSelection.classList.add('hidden');
      detailContainer.classList.remove('hidden');
      detailTitle.textContent = name;
      detailArtist.textContent = artist;
      lyricsBody.innerHTML = `<span style="color: var(--cream-dim)">Fetching lyrics...</span>`;
      canvasVideo.classList.add('hidden');
      canvasFallback.classList.remove('hidden');
      consoleOutput.textContent = "Initializing request...";

      // Fetch lyrics
      try {
        const targetUrl = `/api/lyrics/${id}?name=${encodeURIComponent(name)}&artist=${encodeURIComponent(artist)}`;
        const lyricsRes = await fetch(targetUrl);
        if (lyricsRes.ok) {
          const data = await lyricsRes.json();
          let text = data.syncedLyrics || data.plainLyrics || "No lyric content available.";
          lyricsBody.innerHTML = text;
          showToast("Lyrics fetched!");
          logConsole(`shashwatidr-filestore.hf.space${targetUrl}`, 'GET', lyricsRes.status, data);
        } else {
          lyricsBody.innerHTML = `<span style="color: var(--red)">Failed to read file. Status ${lyricsRes.status}</span>`;
          const errorData = await lyricsRes.json().catch(() => ({}));
          logConsole(`shashwatidr-filestore.hf.space${targetUrl}`, 'GET', lyricsRes.status, errorData);
        }
      } catch (e) {
        lyricsBody.innerHTML = `<span style="color: var(--red)">Error loading lyrics.</span>`;
      }

      // Fetch canvas
      try {
        const targetCanvasUrl = `/api/canvas?id=${id}&name=${encodeURIComponent(name)}&artist=${encodeURIComponent(artist)}`;
        const canvasRes = await fetch(targetCanvasUrl);
        if (canvasRes.ok) {
          const data = await canvasRes.json();
          if (data.url) {
            canvasVideo.src = data.url;
            canvasVideo.classList.remove('hidden');
            canvasFallback.classList.add('hidden');
          }
        }
      } catch (e) {
        // Fallback remains visible
      }

      // Fetch stream
      try {
        const streamAudio = document.getElementById('streamAudio');
        const streamFallback = document.getElementById('streamFallback');
        const streamLinkContainer = document.getElementById('streamLinkContainer');
        const streamLink = document.getElementById('streamLink');
        
        streamAudio.style.display = 'none';
        streamLinkContainer.style.display = 'none';
        streamFallback.style.display = 'block';
        streamFallback.textContent = "Checking audio cache...";
        
        const targetStreamUrl = `/api/stream?id=${id}`;
        const streamRes = await fetch(targetStreamUrl);
        if (streamRes.ok) {
          const data = await streamRes.json();
          if (data.url) {
            streamAudio.src = data.url;
            streamAudio.style.display = 'block';
            streamFallback.style.display = 'none';
            streamLink.href = data.url;
            streamLink.textContent = data.url;
            streamLinkContainer.style.display = 'block';
          }
        } else {
          streamFallback.textContent = "No Audio Cache";
        }
      } catch (e) {
        document.getElementById('streamFallback').textContent = "Error loading stream";
      }
    }

    // Search bar event listener
    let searchTimeout;
    searchBar.addEventListener('input', (e) => {
      clearTimeout(searchTimeout);
      const query = e.target.value.trim();
      searchTimeout = setTimeout(async () => {
        if (!query) {
          renderSongs(allSongs);
          return;
        }
        songsGrid.innerHTML = `<div style="grid-column: 1/-1; text-align: center; color: var(--cream-dim); font-family: var(--font-mono);">Searching fallback db...</div>`;
        try {
          const res = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
          if (res.ok) {
            const results = await res.json();
            renderSongs(results);
          }
        } catch (err) {
          // Ignore
        }
      }, 300);
    });

    async function triggerSync() {
      try {
        const res = await fetch('/api/stream/sync', { method: 'POST' });
        const data = await res.json();
        showToast(data.message);
      } catch (e) {
        showToast("Failed to start sync");
      }
    }

    function switchTab(tab) {
      const tabApi = document.getElementById('tab-api');
      const tabLogs = document.getElementById('tab-logs');
      const consoleOutput = document.getElementById('consoleOutput');
      const logsQueueOutput = document.getElementById('logsQueueOutput');
      
      if (tab === 'api') {
        tabApi.style.color = 'var(--amber)';
        tabApi.style.borderBottom = '2px solid var(--amber)';
        tabLogs.style.color = 'var(--cream-dim)';
        tabLogs.style.borderBottom = 'none';
        consoleOutput.classList.remove('hidden');
        logsQueueOutput.classList.add('hidden');
      } else {
        tabLogs.style.color = 'var(--amber)';
        tabLogs.style.borderBottom = '2px solid var(--amber)';
        tabApi.style.color = 'var(--cream-dim)';
        tabApi.style.borderBottom = 'none';
        consoleOutput.classList.add('hidden');
        logsQueueOutput.classList.remove('hidden');
      }
    }

    async function loadStats() {
      try {
        const res = await fetch('/api/stats');
        if (res.ok) {
          const stats = await res.json();
          document.getElementById('stats-lyrics').textContent = stats.lyrics;
          document.getElementById('stats-canvas').textContent = stats.canvas;
          if (stats.stream !== undefined) {
            document.getElementById('stats-stream').textContent = stats.stream;
          }
          
          // Queue monitor values
          if (stats.stats) {
            document.getElementById('val-progressing').textContent = stats.stats.progressing;
            document.getElementById('val-done').textContent = stats.stats.done;
            document.getElementById('val-failed').textContent = stats.stats.failed;
          }
          if (stats.avg_uploads_per_min !== undefined) {
            document.getElementById('val-rate').textContent = stats.avg_uploads_per_min.toFixed(1) + ' / min';
          }
          
          // Network stats
          if (stats.network_stats) {
            const dlSpeed = stats.network_stats.last_download_speed_kbps;
            document.getElementById('val-dl-speed').textContent = dlSpeed > 1024 
              ? (dlSpeed / 1024).toFixed(1) + ' MB/s' 
              : dlSpeed.toFixed(1) + ' KB/s';
              
            const ulSpeed = stats.network_stats.last_upload_speed_kbps;
            document.getElementById('val-ul-speed').textContent = ulSpeed > 1024 
              ? (ulSpeed / 1024).toFixed(1) + ' MB/s' 
              : ulSpeed.toFixed(1) + ' KB/s';
              
            document.getElementById('val-total-dl').textContent = stats.network_stats.total_downloaded_mb.toFixed(1) + ' MB';
            document.getElementById('val-total-ul').textContent = stats.network_stats.total_uploaded_mb.toFixed(1) + ' MB';
          }
          
          // Logs
          if (stats.logs) {
            const liveLogsContainer = document.getElementById('liveLogsContainer');
            liveLogsContainer.innerHTML = stats.logs.map(line => {
              let color = 'var(--cream)';
              if (line.includes('[ERROR]')) color = 'var(--red)';
              else if (line.includes('[WARNING]')) color = 'var(--amber)';
              return `<div style="color: ${color}; margin-bottom: 0.15rem; white-space: pre-wrap;">${line}</div>`;
            }).join('');
          }
          
          // Jobs
          if (stats.jobs) {
            const activeJobsContainer = document.getElementById('activeJobsContainer');
            const sortedJobs = Object.entries(stats.jobs).sort((a, b) => b[1].timestamp - a[1].timestamp);
            if (sortedJobs.length === 0) {
              activeJobsContainer.innerHTML = '<div style="color: var(--cream-dim); font-style: italic;">No active tasks</div>';
            } else {
              activeJobsContainer.innerHTML = sortedJobs.map(([id, job]) => {
                let badgeColor = 'var(--amber)';
                if (job.status === 'done') badgeColor = 'var(--cyan)';
                if (job.status === 'failed') badgeColor = 'var(--red)';
                return `<div style="display: flex; justify-content: space-between; margin-bottom: 0.2rem;">
                  <span style="word-break: break-all; margin-right: 0.5rem;">${id}</span>
                  <span style="color: ${badgeColor}; font-weight: bold;">${job.status}</span>
                </div>`;
              }).join('');
            }
          }
        }
      } catch (e) {
        // Ignore
      }
    }

    function showToast(msg) {
      toastMsg.textContent = msg;
      toast.classList.add('show');
      setTimeout(() => {
        toast.classList.remove('show');
      }, 3000);
    }

    // Load initial songs & stats
    loadFeaturedSongs();
    loadStats();
    setInterval(loadStats, 2000);
  </script>
</body>

</html>
"""
    return HTMLResponse(content=html_content)
