import os
import time
import json
import codecs
import shutil
import subprocess
import threading
import re
from urllib.parse import urljoin
from typing import Optional
import httpx
from logger_config import log, FATAL_ERRORS
from db_service import cleanup_upload_cache

def get_worker_api_base() -> str:
    raw = (os.environ.get("WORKER_API_BASE") or "").strip()
    if not raw:
        raw = "https://beautiful-junie-scoder-53c5187c.koyeb.app"
    if not raw.startswith("http://") and not raw.startswith("https://"):
        raw = f"https://{raw}"
    return raw.rstrip("/")

def get_render_api_base() -> str:
    raw = (os.environ.get("RENDER_API_BASE") or "").strip()
    if not raw:
        raw = "https://flac-6c8l.onrender.com"
    if not raw.startswith("http://") and not raw.startswith("https://"):
        raw = f"https://{raw}"
    return raw.rstrip("/")

WORKER_API_BASE = get_worker_api_base()
RENDER_API_BASE = get_render_api_base()
LOSSLESS_BUCKET_ID = "shashwatIDR/lossless"
HF_TOKEN = os.environ.get("HFTOKEN")

def get_ffmpeg_binary() -> str:
    return (
        shutil.which("ffmpeg")
        or ("/opt/homebrew/bin/ffmpeg" if os.path.exists("/opt/homebrew/bin/ffmpeg") else None)
        or ("/usr/bin/ffmpeg" if os.path.exists("/usr/bin/ffmpeg") else None)
        or "ffmpeg"
    )

# --- 45s Keep-Alive Daemon for Render Instance ---
def _render_keepalive_loop():
    while True:
        try:
            base = get_render_api_base()
            url = f"{base}/health"
            with httpx.Client(timeout=15.0) as client:
                res = client.get(url)
                log.debug(f"[KeepAlive] Render instance ping ({url}): HTTP {res.status_code}")
        except Exception as e:
            log.debug(f"[KeepAlive] Render ping exception: {e}")
        time.sleep(45)

_keepalive_thread = threading.Thread(target=_render_keepalive_loop, daemon=True, name="RenderKeepAlive")
_keepalive_thread.start()

def fetch_worker_lossless_info(video_id: str) -> dict:
    """Fetch track metadata and stream URLs from primary Qobuz audio service API."""
    url = f"{get_worker_api_base()}/?id={video_id}"
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") in ["success", "partial_error"]:
                    return data
            else:
                log.warning(f"Qobuz API returned HTTP {resp.status_code} for {video_id}")
    except Exception as e:
        log.error(f"Failed to fetch Qobuz API for ID {video_id}: {e}")
    return {}

def fetch_render_fallback_info(video_id: str) -> dict:
    """Fetch track metadata and manifest URLs from the Render Tidal fallback API."""
    url = f"{get_render_api_base()}/?id={video_id}"
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") in ["success", "partial_error"]:
                    return data
            else:
                log.warning(f"Render Fallback API returned HTTP {resp.status_code} for {video_id}")
    except Exception as e:
        log.error(f"Failed to fetch Render Fallback API for ID {video_id}: {e}")
    return {}

def resolve_highest_flac_m3u8(manifest_url: str) -> str:
    """
    Given a master or media HLS m3u8 manifest URL:
    - If it is a master playlist, inspect all variant streams with CODECS containing 'flac'.
    - Sort variants by BANDWIDTH descending to pick the highest available (e.g. 96kHz / 24-bit over 44.1kHz / 16-bit).
    - Return the best variant m3u8 URL.
    - If already a media playlist or only 1 variant, returns manifest_url.
    """
    try:
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            resp = client.get(manifest_url)
            if resp.status_code != 200:
                log.warning(f"Failed to fetch manifest content from {manifest_url}: HTTP {resp.status_code}")
                return manifest_url
            content = resp.text

        lines = [line.strip() for line in content.splitlines() if line.strip()]
        variants = []
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith("#EXT-X-STREAM-INF:"):
                inf = line
                uri = ""
                if i + 1 < len(lines) and not lines[i + 1].startswith("#"):
                    uri = lines[i + 1]
                    i += 1

                codecs_match = re.search(r'CODECS="([^"]+)"', inf, re.IGNORECASE)
                codec_val = codecs_match.group(1).lower() if codecs_match else ""

                # Strictly FLAC
                if "flac" in codec_val or not codec_val:
                    bw_match = re.search(r'BANDWIDTH=(\d+)', inf)
                    bw = int(bw_match.group(1)) if bw_match else 0
                    full_uri = urljoin(manifest_url, uri)
                    variants.append({"bandwidth": bw, "uri": full_uri, "codec": codec_val})
            i += 1

        if variants:
            # Sort descending by bandwidth to pick highest quality (e.g. 96kHz / 24-bit)
            variants.sort(key=lambda x: x["bandwidth"], reverse=True)
            best_variant = variants[0]
            log.debug(f"Selected highest FLAC variant: bandwidth={best_variant['bandwidth']}, codec={best_variant['codec']}")
            return best_variant["uri"]
    except Exception as e:
        log.warning(f"Error parsing master m3u8 playlist: {e}")

    return manifest_url

def convert_m3u8_to_flac_bytes(manifest_url: str) -> Optional[bytes]:
    """Convert an HLS m3u8 playlist URL to pure FLAC bytes using ffmpeg without putting streaming load on Render."""
    target_url = resolve_highest_flac_m3u8(manifest_url)
    ffmpeg_bin = get_ffmpeg_binary()
    cmd = [
        ffmpeg_bin,
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", target_url,
        "-c:a", "copy",
        "-f", "flac",
        "-",
    ]
    log.debug(f"Converting Tidal m3u8 stream to FLAC via ffmpeg copy (highest FLAC variant)...")
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate(timeout=120)
        if proc.returncode == 0 and len(out) > 0 and out[:4] == b"fLaC":
            return out
        else:
            log.debug(f"ffmpeg copy failed (rc={proc.returncode}), attempting flac decode transcode: {err.decode('utf-8', errors='ignore')[-200:]}")
            cmd_transcode = [
                ffmpeg_bin,
                "-reconnect", "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max", "5",
                "-i", target_url,
                "-c:a", "flac",
                "-f", "flac",
                "-",
            ]
            proc2 = subprocess.Popen(cmd_transcode, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out2, err2 = proc2.communicate(timeout=120)
            if proc2.returncode == 0 and len(out2) > 0 and out2[:4] == b"fLaC":
                return out2
            log.debug(f"ffmpeg transcode failed (rc={proc2.returncode}): {err2.decode('utf-8', errors='ignore')[-300:]}")
    except Exception as e:
        log.error(f"ffmpeg error converting m3u8 to FLAC: {e}")
    return None

def upload_audio_to_hf_bucket(audio_bytes: bytes, video_id: str, ext: str, correct_title: str, correct_artist: str, format_id: int = 6) -> dict:
    remote_filename = f"{video_id}.{ext}"
    log.info(f"Uploading {len(audio_bytes)} bytes to HF bucket '{LOSSLESS_BUCKET_ID}' as {remote_filename}...")
    try:
        from db_service import safe_batch_bucket_files
        safe_batch_bucket_files(
            LOSSLESS_BUCKET_ID,
            add=[(audio_bytes, remote_filename)],
            token=HF_TOKEN,
        )
        del audio_bytes
        cleanup_upload_cache()
        log.info(f"Successfully uploaded {ext.upper()} audio to HF bucket for {video_id}")
        return {
            "success": True,
            "ext": ext,
            "format_id": format_id,
            "title": correct_title,
            "artist": correct_artist,
        }
    except Exception as e:
        if "audio_bytes" in locals():
            del audio_bytes
        try:
            from huggingface_hub.utils._xet import abort_xet_session
            abort_xet_session()
        except Exception:
            pass
        cleanup_upload_cache()
        log.error(f"Failed to upload {remote_filename} to HF bucket: {e}")
        return {
            "success": False,
            "title": correct_title,
            "artist": correct_artist,
        }

def fetch_lossless_audio(video_id: str, song_name: str = "", artist_name: str = "") -> Optional[dict]:
    """
    Fetch and download/convert FLAC audio for video_id from Qobuz or Render Tidal HLS fallback.
    Returns dict with audio_bytes and metadata on success, or None on failure.
    Does NOT upload to Hugging Face (used by parallel batch processors for grouped uploads).
    """
    worker_data = fetch_worker_lossless_info(video_id)
    
    correct_title = song_name
    correct_artist = artist_name

    if worker_data:
        ytmusic = worker_data.get("ytmusic", {})
        matched_track = worker_data.get("matched_track", {})
        streams = worker_data.get("streams", {})

        correct_title = matched_track.get("title") or ytmusic.get("title") or song_name
        correct_artist = matched_track.get("artist") or ytmusic.get("artist") or artist_name

        priority_keys = ["flac_hires_192", "flac_hires_96", "flac_cd"]
        selected_stream = None
        for key in priority_keys:
            if key in streams and streams[key].get("url"):
                selected_stream = streams[key]
                break

        if selected_stream:
            stream_url = selected_stream["url"]
            format_id = selected_stream.get("format_id", 6)
            mime_type = selected_stream.get("mime_type", "audio/flac")

            if "flac" in mime_type.lower():
                try:
                    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
                        resp = client.get(stream_url)
                        if resp.status_code == 200:
                            audio_bytes = resp.content
                            if len(audio_bytes) > 0 and audio_bytes[:4] == b"fLaC":
                                return {
                                    "video_id": video_id,
                                    "audio_bytes": audio_bytes,
                                    "ext": "flac",
                                    "format_id": format_id,
                                    "title": correct_title,
                                    "artist": correct_artist,
                                    "source": "qobuz",
                                }
                except Exception as e:
                    log.debug(f"Error downloading Qobuz stream for {video_id}: {e}")

    # Fallback to Render Tidal API Instance
    render_data = fetch_render_fallback_info(video_id)
    if render_data:
        r_ytmusic = render_data.get("ytmusic", {})
        r_matched = render_data.get("matched_track", {})
        r_streams = render_data.get("streams", {})

        correct_title = r_matched.get("title") or r_ytmusic.get("title") or correct_title
        correct_artist = r_matched.get("artist") or r_ytmusic.get("artist") or correct_artist

        manifest_url = (
            r_streams.get("flac_hires", {}).get("manifest_url")
            or r_streams.get("flac", {}).get("manifest_url")
            or render_data.get("manifest_url")
        )

        if manifest_url:
            flac_bytes = convert_m3u8_to_flac_bytes(manifest_url)
            if flac_bytes and len(flac_bytes) > 0 and flac_bytes[:4] == b"fLaC":
                return {
                    "video_id": video_id,
                    "audio_bytes": flac_bytes,
                    "ext": "flac",
                    "format_id": 6,
                    "title": correct_title,
                    "artist": correct_artist,
                    "source": "render_tidal_m3u8",
                }

    return None

def process_qobuz_lossless(video_id: str, song_name: str = "", artist_name: str = "") -> dict:
    """
    Fetch metadata, download FLAC, and upload to Hugging Face bucket (standalone single-track).
    """
    result = fetch_lossless_audio(video_id, song_name, artist_name)
    if result:
        upload_res = upload_audio_to_hf_bucket(
            result["audio_bytes"],
            video_id,
            result["ext"],
            result["title"],
            result["artist"],
            result["format_id"],
        )
        if upload_res.get("success"):
            upload_res["source"] = result.get("source")
            return upload_res

    return {
        "success": False,
        "title": song_name,
        "artist": artist_name,
    }
