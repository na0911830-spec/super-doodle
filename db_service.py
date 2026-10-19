import os
import shutil
import tempfile
import time
import json
import threading
import httpx
from logger_config import log

# Enforce zero shard/chunk disk cache on HuggingFace Xet to prevent 10GB+ cache accumulation
os.environ.setdefault("HF_XET_SHARD_CACHE_SIZE_LIMIT", "0")
os.environ.setdefault("HF_XET_CHUNK_CACHE_SIZE_BYTES", "0")
os.environ.setdefault("HF_XET_RECONSTRUCTION_DOWNLOAD_BUFFER_LIMIT", "33554432")
os.environ.setdefault("HF_XET_LOG_DIR_MAX_SIZE", "10000000")
os.environ.setdefault("HF_HUB_DISABLE_DISK_INTEGRITY_CHECK", "1")

HF_TOKEN = os.environ.get("HFTOKEN")
BUCKET_ID = "shashwatIDR/stream"

HF_UPLOAD_LOCK = threading.Lock()

def safe_batch_bucket_files(bucket_id: str, add=None, copy=None, delete=None, token=None):
    """
    Safely execute batch_bucket_files with thread synchronization,
    preventing race conditions on the underlying XetSession. If a session gets tainted
    (e.g., Previous task error / I/O error), abort_xet_session is invoked and retried.
    """
    from huggingface_hub import batch_bucket_files
    from huggingface_hub.utils._xet import abort_xet_session

    with HF_UPLOAD_LOCK:
        try:
            return batch_bucket_files(bucket_id, add=add, copy=copy, delete=delete, token=token)
        except Exception as e:
            err_str = str(e)
            if "Previous task error" in err_str or "os error 2" in err_str or "I/O error" in err_str or "BrokenPipe" in err_str:
                log.warning(f"Tainted Xet session encountered ({e}). Aborting session and retrying upload...")
                try:
                    abort_xet_session()
                except Exception:
                    pass
                time.sleep(0.5)
                return batch_bucket_files(bucket_id, add=add, copy=copy, delete=delete, token=token)
            raise

def cleanup_upload_cache(force: bool = False):
    """
    Purge stale local Xet/HuggingFace cache files and temporary artifacts.
    Only purges files/directories that are older than 60 seconds (unless force=True)
    to avoid deleting active chunk/staging data while parallel uploads are in progress.
    """
    try:
        now = time.time()
        target_dirs = set()
        try:
            from huggingface_hub.constants import HF_XET_CACHE, HF_HOME
            if HF_XET_CACHE:
                target_dirs.add(HF_XET_CACHE)
            if HF_HOME:
                target_dirs.add(os.path.join(HF_HOME, "xet"))
        except Exception:
            pass

        home_dir = os.path.expanduser("~")
        target_dirs.add(os.path.join(home_dir, ".cache", "huggingface", "xet"))

        env_xet = os.environ.get("HF_XET_CACHE")
        if env_xet:
            target_dirs.add(env_xet)
        env_home = os.environ.get("HF_HOME")
        if env_home:
            target_dirs.add(os.path.join(env_home, "xet"))

        for cdir in target_dirs:
            if os.path.exists(cdir):
                for item in os.listdir(cdir):
                    item_path = os.path.join(cdir, item)
                    try:
                        # Do not touch files/folders created within the last 60 seconds unless forced
                        if not force:
                            mtime = os.path.getmtime(item_path)
                            if (now - mtime) < 60:
                                continue

                        if os.path.islink(item_path) or os.path.isfile(item_path):
                            os.remove(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path, ignore_errors=True)
                    except Exception:
                        pass
                os.makedirs(cdir, exist_ok=True)

        temp_dir = tempfile.gettempdir()
        if os.path.exists(temp_dir):
            for t_item in os.listdir(temp_dir):
                if t_item.startswith("xet") or (t_item.startswith("tmp") and t_item.endswith(".tmp")):
                    t_path = os.path.join(temp_dir, t_item)
                    try:
                        if not force:
                            mtime = os.path.getmtime(t_path)
                            if (now - mtime) < 60:
                                continue
                        if os.path.isfile(t_path) or os.path.islink(t_path):
                            os.remove(t_path)
                        elif os.path.isdir(t_path):
                            shutil.rmtree(t_path, ignore_errors=True)
                    except Exception:
                        pass
    except Exception:
        pass

BASE_URL_CACHE = {
    1: "https://huggingface.co/buckets/allnewuser/lyrics/resolve",
    2: "https://velamhere-img.hf.space",
    3: "https://velamhere-img.hf.space/song",
    4: "https://huggingface.co/buckets/shashwatIDR/lossless/resolve"
}
BASE_URL_CACHE_LOCK = threading.Lock()

DB_CACHE = {}
DB_CACHE_LOCK = threading.Lock()
DB_CACHE_DIRTY = False
DB_BACKUP_INTERVAL = int(os.environ.get("DB_BACKUP_INTERVAL", "180"))

STATS = {
    "total": 0,
    "done": 0,
    "progressing": 0,
    "failed": 0
}

def load_db_cache(force_hf: bool = True):
    """Load all records from HF db_backup.json as primary source."""
    global DB_CACHE, BASE_URL_CACHE
    hf_loaded = False

    try:
        url = f"https://huggingface.co/buckets/{BUCKET_ID}/resolve/db_backup.json"
        headers = {}
        if HF_TOKEN:
            headers["Authorization"] = f"Bearer {HF_TOKEN}"
            
        log.info(f"Fetching complete database from Hugging Face db_backup.json ({url})...")
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            resp = client.get(url, headers=headers)
            
        if resp.status_code == 200:
            data = resp.json()
            songs_list = data.get("songs", [])
            
            if songs_list:
                with DB_CACHE_LOCK:
                    DB_CACHE.clear()
                    DB_CACHE.update({s["id"]: s for s in songs_list if s.get("id")})
                
                base_urls_list = data.get("base_urls", [])
                with BASE_URL_CACHE_LOCK:
                    BASE_URL_CACHE.clear()
                    for item in base_urls_list:
                        if isinstance(item, dict):
                            BASE_URL_CACHE[int(item["id"])] = item["url"]
                        elif isinstance(item, (list, tuple)) and len(item) >= 2:
                            BASE_URL_CACHE[int(item[0])] = item[1]
                    BASE_URL_CACHE[2] = "https://velamhere-img.hf.space"
                        
                log.info(f"Successfully loaded ALL {len(DB_CACHE)} songs from HF db_backup.json!")
                hf_loaded = True
            else:
                log.warning("HF db_backup.json had empty songs list.")
    except Exception as hf_ex:
        log.error(f"Failed to load dataset from HF backup: {hf_ex}")

    with DB_CACHE_LOCK:
        empty_cache = (len(DB_CACHE) == 0)

    if empty_cache:
        log.warning("Cache is empty! Auto-reconstructing from storage buckets...")
        reconstruct_from_buckets(save_to_hf=True)

def load_db_stats():
    """Load stats from DB cache."""
    try:
        with DB_CACHE_LOCK:
            total_count = len(DB_CACHE)
            done_count = sum(1 for song in DB_CACHE.values() if song.get("stream_sources"))
            
        STATS["done"] = done_count
        STATS["total"] = total_count
        log.info(f"Loaded DB stats from cache: Total Songs = {total_count}, Cached Streams = {done_count}")
    except Exception as e:
        log.error(f"Failed to load DB stats: {e}")

def sync_cache_with_buckets(save_backup: bool = True) -> dict:
    """
    Fetches the index of all files in both storage buckets (lossless and stream),
    matches against DB_CACHE, and removes any non-existing records while adding
    bucket tracks that are missing in the cache.
    """
    from huggingface_hub import HfApi

    stream_bucket = BUCKET_ID  # shashwatIDR/stream
    lossless_bucket = "shashwatIDR/lossless"
    api = HfApi(token=HF_TOKEN)

    lossless_files = {}
    stream_files = {}
    lossless_scan_ok = False
    stream_scan_ok = False

    # 1. Scan lossless bucket
    try:
        log.info(f"Bucket Sync: scanning lossless bucket '{lossless_bucket}'...")
        for item in api.list_bucket_tree(lossless_bucket):
            fname = getattr(item, "path", None) or getattr(item, "name", None) or str(item)
            fname = os.path.basename(fname).strip()
            if not fname or fname in [".git", ".gitattributes", ".DS_Store"]:
                continue
            if "." in fname:
                vid, ext = fname.rsplit(".", 1)
            else:
                vid, ext = fname, "flac"
            vid = vid.strip()
            if vid:
                lossless_files[vid] = ext.lower()
        lossless_scan_ok = True
        log.info(f"Bucket Sync: indexed {len(lossless_files)} tracks from lossless bucket.")
    except Exception as e:
        log.error(f"Bucket Sync: error scanning lossless bucket '{lossless_bucket}': {e}")

    # 2. Scan stream bucket
    try:
        log.info(f"Bucket Sync: scanning stream bucket '{stream_bucket}'...")
        for item in api.list_bucket_tree(stream_bucket):
            fname = getattr(item, "path", None) or getattr(item, "name", None) or str(item)
            fname = os.path.basename(fname).strip()
            if not fname or fname in ["db_backup.json", ".git", ".gitattributes", ".DS_Store"]:
                continue
            if "." in fname:
                vid, ext = fname.rsplit(".", 1)
            else:
                vid, ext = fname, "m4a"
            vid = vid.strip()
            if vid:
                stream_files[vid] = ext.lower()
        stream_scan_ok = True
        log.info(f"Bucket Sync: indexed {len(stream_files)} tracks from stream bucket.")
    except Exception as e:
        log.error(f"Bucket Sync: error scanning stream bucket '{stream_bucket}': {e}")

    # 3. Match and update DB_CACHE
    lossless_confirmed = 0
    lossless_removed = 0
    lossless_added = 0
    stream_confirmed = 0
    stream_removed = 0
    stream_added = 0

    with DB_CACHE_LOCK:
        for vid, song in DB_CACHE.items():
            # Lossless reconciliation
            if lossless_scan_ok:
                l_val = str(song.get("lossless") or "").strip()
                is_marked_lossless = bool(l_val and l_val != "0")
                if is_marked_lossless:
                    if vid in lossless_files:
                        song["lossless"] = f"4:{lossless_files[vid]}"
                        lossless_confirmed += 1
                    else:
                        # Non-existing file on bucket -> remove from cache
                        song["lossless"] = "0"
                        lossless_removed += 1
                else:
                    if vid in lossless_files:
                        song["lossless"] = f"4:{lossless_files[vid]}"
                        lossless_added += 1

            # Stream reconciliation
            if stream_scan_ok:
                s_val = str(song.get("stream_sources") or "").strip()
                is_marked_stream = bool(s_val and s_val != "0" and s_val.lower() != "none")
                if is_marked_stream:
                    if vid in stream_files:
                        song["stream_sources"] = stream_files[vid]
                        stream_confirmed += 1
                    else:
                        # Non-existing file on bucket -> remove from cache
                        song["stream_sources"] = None
                        stream_removed += 1
                else:
                    if vid in stream_files:
                        song["stream_sources"] = stream_files[vid]
                        stream_added += 1

        # Add any bucket files not yet in DB_CACHE
        all_bucket_vids = set()
        if lossless_scan_ok:
            all_bucket_vids.update(lossless_files.keys())
        if stream_scan_ok:
            all_bucket_vids.update(stream_files.keys())

        for vid in all_bucket_vids:
            if vid not in DB_CACHE:
                DB_CACHE[vid] = {
                    "id": vid,
                    "name": None,
                    "artists": None,
                    "stream_sources": stream_files.get(vid),
                    "lossless": f"4:{lossless_files[vid]}" if vid in lossless_files else "0",
                    "lyrics_sources": None,
                    "canvas_sources": None
                }

    log.info(
        f"Bucket Match & Sync Complete: "
        f"Lossless [Confirmed: {lossless_confirmed}, Removed non-existing: {lossless_removed}, Added: {lossless_added}, Bucket Total: {len(lossless_files)}]; "
        f"Streams [Confirmed: {stream_confirmed}, Removed non-existing: {stream_removed}, Added: {stream_added}, Bucket Total: {len(stream_files)}]."
    )

    changes_made = (lossless_removed > 0 or stream_removed > 0 or lossless_added > 0 or stream_added > 0)
    if changes_made:
        global DB_CACHE_DIRTY
        DB_CACHE_DIRTY = True

    if save_backup and changes_made:
        log.info("Sanitized database cache with true bucket files. Uploading updated db_backup.json to HF...")
        try:
            trigger_immediate_backup()
        except Exception as b_err:
            log.warning(f"Immediate backup failed after sync: {b_err}")

    return {
        "status": "success",
        "lossless_bucket_tracks": len(lossless_files),
        "lossless_confirmed": lossless_confirmed,
        "lossless_removed": lossless_removed,
        "lossless_added": lossless_added,
        "stream_bucket_tracks": len(stream_files),
        "stream_confirmed": stream_confirmed,
        "stream_removed": stream_removed,
        "stream_added": stream_added,
        "total_songs_in_cache": len(DB_CACHE)
    }

def init_db():
    """Initialize in-memory database cache from Hugging Face json backup and sync with buckets."""
    load_db_cache()
    try:
        sync_cache_with_buckets(save_backup=True)
    except Exception as s_err:
        log.error(f"Bucket synchronization on startup encountered an error: {s_err}")
    load_db_stats()

def db_backup_loop():
    global DB_CACHE_DIRTY
    log.info(f"Starting database backup daemon thread (interval: {DB_BACKUP_INTERVAL}s, dirty-checked)...")
    while True:
        try:
            time.sleep(DB_BACKUP_INTERVAL)
            if not DB_CACHE_DIRTY:
                continue

            with DB_CACHE_LOCK:
                songs = list(DB_CACHE.values())
            
            # SAFEGUARD: Never overwrite backup with empty data
            if not songs:
                log.warning("DB backup skipped: DB_CACHE is empty. Refusing to overwrite db_backup.json.")
                continue
            
            with BASE_URL_CACHE_LOCK:
                base_urls = [{"id": k, "url": v} for k, v in BASE_URL_CACHE.items()]

            backup_data = {
                "songs": songs,
                "base_urls": base_urls
            }
            json_bytes = json.dumps(backup_data, default=str, separators=(',', ':')).encode('utf-8')
            
            safe_batch_bucket_files(
                BUCKET_ID,
                add=[(json_bytes, "db_backup.json")],
                token=HF_TOKEN
            )
            del json_bytes
            cleanup_upload_cache()
            DB_CACHE_DIRTY = False
            log.info(f"Database backup ({len(songs)} songs) successfully uploaded to Hugging Face (db_backup.json).")
        except Exception as e:
            log.error(f"Failed to backup database to Hugging Face: {e}")

def fetch_correct_metadata(video_id: str) -> tuple[str | None, str | None]:
    """Fetch track metadata (title, artist) using upstream metadata provider API."""
    worker_api = os.environ.get("METADATA_API_BASE", os.environ.get("WORKER_API_BASE", "https://beautiful-junie-scoder-53c5187c.koyeb.app"))
    url = f"{worker_api.rstrip('/')}/?id={video_id}"
    try:
        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                matched = data.get("matched_track", {})
                yt = data.get("ytmusic", {})
                title = matched.get("title") or yt.get("title") or None
                artist = matched.get("artist") or yt.get("artist") or None
                if title:
                    return title.strip(), (artist.strip() if artist else None)
    except Exception as e:
        log.warning(f"Metadata API fetch failed for {video_id}: {e}")
    return None, None

def db_save_stream_source(video_id: str, value: str = "m4a"):
    global DB_CACHE_DIRTY
    with DB_CACHE_LOCK:
        if video_id in DB_CACHE:
            DB_CACHE[video_id]["stream_sources"] = value
        else:
            DB_CACHE[video_id] = {
                "id": video_id,
                "name": None,
                "artists": None,
                "stream_sources": value,
                "lossless": "0",
                "lyrics_sources": None,
                "canvas_sources": None
            }
    DB_CACHE_DIRTY = True
    log.info(f"Updated stream_sources to {value} for {video_id}")

def db_save_lossless_source(video_id: str, value: str = "4:flac"):
    global DB_CACHE_DIRTY
    with DB_CACHE_LOCK:
        if video_id in DB_CACHE:
            DB_CACHE[video_id]["lossless"] = value
        else:
            DB_CACHE[video_id] = {
                "id": video_id,
                "name": None,
                "artists": None,
                "stream_sources": None,
                "lossless": value,
                "lyrics_sources": None,
                "canvas_sources": None
            }
    DB_CACHE_DIRTY = True
    log.info(f"Updated lossless to '{value}' for {video_id}")

def trigger_immediate_backup():
    """Trigger immediate dump of in-memory JSON to HF db_backup.json."""
    global DB_CACHE_DIRTY
    try:
        with DB_CACHE_LOCK:
            songs = list(DB_CACHE.values())
        if not songs:
            return
        with BASE_URL_CACHE_LOCK:
            base_urls = [{"id": k, "url": v} for k, v in BASE_URL_CACHE.items()]
        
        backup_data = {
            "songs": songs,
            "base_urls": base_urls
        }
        json_bytes = json.dumps(backup_data, default=str, separators=(',', ':')).encode('utf-8')
        safe_batch_bucket_files(
            BUCKET_ID,
            add=[(json_bytes, "db_backup.json")],
            token=HF_TOKEN
        )
        del json_bytes
        cleanup_upload_cache()
        DB_CACHE_DIRTY = False
    except Exception as e:
        log.warning(f"Immediate JSON backup failed: {e}")

def db_remove_lossless_source(video_id: str, mark_unavailable: bool = True):
    """Mark lossless as '0' (unavailable) or None from in-memory cache and json backup."""
    global DB_CACHE_DIRTY
    val = "0" if mark_unavailable else None
    with DB_CACHE_LOCK:
        if video_id in DB_CACHE:
            DB_CACHE[video_id]["lossless"] = val
        else:
            DB_CACHE[video_id] = {
                "id": video_id,
                "name": None,
                "artists": None,
                "lossless": val,
                "stream_sources": None,
                "lyrics_sources": None,
                "canvas_sources": None
            }
    DB_CACHE_DIRTY = True
    log.info(f"Marked lossless as '{val}' for {video_id}")

def delete_files_from_lossless_bucket(filenames: list[str]):
    """Delete a list of audio files from HF bucket shashwatIDR/lossless."""
    if not filenames:
        return
    try:
        lossless_bucket = "shashwatIDR/lossless"
        safe_batch_bucket_files(
            lossless_bucket,
            delete=filenames,
            token=HF_TOKEN
        )
        cleanup_upload_cache()
        log.info(f"Deleted {len(filenames)} invalid audio files from bucket '{lossless_bucket}'.")
    except Exception as e:
        log.error(f"Failed to delete files from HF bucket 'shashwatIDR/lossless': {e}")

def reconstruct_from_buckets(save_to_hf: bool = True) -> dict:
    """
    Reconstructs the entire database cache by inspecting all audio file names
    in both storage buckets (shashwatIDR/stream and shashwatIDR/lossless).
    Keeps title, artist, lyrics, canvas as null/None for now.
    """
    from huggingface_hub import HfApi, batch_bucket_files

    stream_bucket = BUCKET_ID  # shashwatIDR/stream
    lossless_bucket = "shashwatIDR/lossless"
    api = HfApi(token=HF_TOKEN)

    log.info("Starting complete database reconstruction from buckets...")
    stream_count = 0
    lossless_count = 0
    reconstructed = {}

    # 1. Scan stream bucket (e.g. {video_id}.m4a, {video_id}.webm, etc.)
    try:
        log.info(f"Reconstruction: scanning stream bucket '{stream_bucket}'...")
        for item in api.list_bucket_tree(stream_bucket):
            fname = getattr(item, "path", None) or getattr(item, "name", None) or str(item)
            fname = os.path.basename(fname).strip()
            if not fname or fname in ["db_backup.json", ".git", ".gitattributes", ".DS_Store"]:
                continue
            
            if "." in fname:
                vid, ext = fname.rsplit(".", 1)
            else:
                vid, ext = fname, "m4a"

            vid = vid.strip()
            if not vid:
                continue

            stream_count += 1
            reconstructed[vid] = {
                "id": vid,
                "name": None,
                "artists": None,
                "stream_sources": ext.lower() if ext else "m4a",
                "lossless": "0",
                "lyrics_sources": None,
                "canvas_sources": None
            }
        log.info(f"Reconstruction: indexed {stream_count} tracks from stream bucket.")
    except Exception as e:
        log.error(f"Error scanning stream bucket '{stream_bucket}': {e}")

    # 2. Scan lossless bucket (e.g. {video_id}.flac, {video_id}.mp3)
    lossless_scan_ok = False
    try:
        log.info(f"Reconstruction: scanning lossless bucket '{lossless_bucket}'...")
        for item in api.list_bucket_tree(lossless_bucket):
            fname = getattr(item, "path", None) or getattr(item, "name", None) or str(item)
            fname = os.path.basename(fname).strip()
            if not fname or fname in [".git", ".gitattributes", ".DS_Store"]:
                continue

            if "." in fname:
                vid, ext = fname.rsplit(".", 1)
            else:
                vid, ext = fname, "flac"

            vid = vid.strip()
            if not vid:
                continue

            lossless_count += 1
            lossless_val = f"4:{ext.lower()}"
            if vid in reconstructed:
                reconstructed[vid]["lossless"] = lossless_val
            else:
                reconstructed[vid] = {
                    "id": vid,
                    "name": None,
                    "artists": None,
                    "stream_sources": None,
                    "lossless": lossless_val,
                    "lyrics_sources": None,
                    "canvas_sources": None
                }
        lossless_scan_ok = True
        log.info(f"Reconstruction: indexed {lossless_count} tracks from lossless bucket.")
    except Exception as e:
        log.error(f"Error scanning lossless bucket '{lossless_bucket}': {e}")

    # 3. Update DB_CACHE
    with DB_CACHE_LOCK:
        for vid, entry in reconstructed.items():
            if vid in DB_CACHE:
                existing = DB_CACHE[vid]
                if existing.get("name"):
                    entry["name"] = existing["name"]
                if existing.get("artists"):
                    entry["artists"] = existing["artists"]
                if existing.get("lyrics_sources"):
                    entry["lyrics_sources"] = existing["lyrics_sources"]
                if existing.get("canvas_sources"):
                    entry["canvas_sources"] = existing["canvas_sources"]
                if existing.get("stream_sources") and not entry.get("stream_sources"):
                    entry["stream_sources"] = existing["stream_sources"]
                if not lossless_scan_ok:
                    if existing.get("lossless") and existing["lossless"] != "0":
                        entry["lossless"] = existing["lossless"]
            DB_CACHE[vid] = entry

        total_songs = len(DB_CACHE)

    # 4. Save reconstructed dataset to Hugging Face db_backup.json
    if save_to_hf and total_songs > 0:
        try:
            with DB_CACHE_LOCK:
                songs = list(DB_CACHE.values())
            with BASE_URL_CACHE_LOCK:
                base_urls = [{"id": k, "url": v} for k, v in BASE_URL_CACHE.items()]
            if not base_urls:
                base_urls = [
                    {"id": 1, "url": "https://velamhere-img.hf.space"},
                    {"id": 2, "url": "https://velamhere-img.hf.space"}
                ]
            backup_data = {
                "songs": songs,
                "base_urls": base_urls
            }
            json_bytes = json.dumps(backup_data, default=str, separators=(',', ':')).encode('utf-8')
            safe_batch_bucket_files(
                stream_bucket,
                add=[(json_bytes, "db_backup.json")],
                token=HF_TOKEN
            )
            del json_bytes
            cleanup_upload_cache()
            DB_CACHE_DIRTY = False
            log.info(f"Reconstruction successfully saved {len(songs)} tracks to HF db_backup.json!")
        except Exception as e:
            log.error(f"Failed to upload reconstructed db_backup.json to HF: {e}")

    return {
        "status": "success",
        "stream_tracks_indexed": stream_count,
        "lossless_tracks_indexed": lossless_count,
        "total_songs_in_cache": total_songs
    }
