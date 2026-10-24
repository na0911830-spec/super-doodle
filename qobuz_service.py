import os
import time
import json
import codecs
import shutil
import subprocess
import threading
import re
from urllib.parse import urljoin, quote
from typing import Optional, List, Dict, Any, Tuple
import concurrent.futures
import xml.etree.ElementTree as ET
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

def get_velixir_tidal_api_base() -> str:
    raw = (os.environ.get("VELIXIR_TIDAL_API_BASE") or os.environ.get("TIDAL_API_BASE") or "").strip()
    if not raw:
        raw = "https://eljohnzcaddon.velixir.run/c57e7b0ef2031b2f1fcd"
    if not raw.startswith("http://") and not raw.startswith("https://"):
        raw = f"https://{raw}"
    return raw.rstrip("/")

WORKER_API_BASE = get_worker_api_base()
RENDER_API_BASE = get_render_api_base()
VELIXIR_TIDAL_API_BASE = get_velixir_tidal_api_base()
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

# --- Velixir Tidal API & DASH MPD Conversion Engine ---

def clean_for_match(s: str) -> str:
    """Normalize string for strict song title and artist comparison."""
    if not s:
        return ""
    # Strip parenthetical metadata like (Official Video), [Remastered]
    s = re.sub(r"[\(\[\{].*?[\)\]\}]", "", s)
    s = s.lower()
    s = re.sub(r"\b(ft\.|feat\.|featuring|official|video|audio|lyrics|lyric|remastered|remaster|version)\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def calculate_similarity(s1: str, s2: str) -> float:
    """Word-set Jaccard similarity with substring containment heuristics."""
    c1 = clean_for_match(s1)
    c2 = clean_for_match(s2)
    if not c1 or not c2:
        return 0.0
    if c1 == c2:
        return 1.0
    if c1 in c2 or c2 in c1:
        return 0.85
    w1 = set(c1.split())
    w2 = set(c2.split())
    inter = w1 & w2
    union = w1 | w2
    return len(inter) / len(union) if union else 0.0

def search_velixir_tidal(query: str) -> list[dict]:
    """Search tracks on Velixir Tidal API endpoint."""
    base = get_velixir_tidal_api_base()
    url = f"{base}/search?q={quote(query)}"
    try:
        with httpx.Client(timeout=25.0, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("tracks") or []
            else:
                log.warning(f"Velixir Tidal search returned HTTP {resp.status_code} for query '{query}'")
    except Exception as e:
        log.error(f"Error querying Velixir Tidal search API: {e}")
    return []

def find_best_velixir_tidal_match(target_title: str, target_artist: str = "", candidates: list[dict] = None) -> Optional[dict]:
    """
    Score and select the optimal Tidal track match:
    1. Exact or high-confidence title match
    2. Artist similarity and containment check
    3. Strict priority for HI_RES_LOSSLESS and LOSSLESS audio quality
    """
    if not candidates:
        return None

    t_clean = clean_for_match(target_title)
    if not t_clean:
        return candidates[0] if candidates else None

    best_match = None
    highest_score = -1.0

    for track in candidates:
        cand_title = track.get("title") or ""
        cand_artist = track.get("artist") or ""
        c_clean = clean_for_match(cand_title)

        title_sim = calculate_similarity(target_title, cand_title)
        is_title_contained = bool(t_clean and c_clean and (t_clean in c_clean or c_clean in t_clean))
        is_title_exact = bool(t_clean and c_clean and t_clean == c_clean)

        # Discard tracks that fail title matching
        if not is_title_exact and not is_title_contained and title_sim < 0.35:
            continue

        # Artist scoring
        artist_sim = 0.5
        if target_artist and cand_artist:
            a_clean = clean_for_match(target_artist)
            ca_clean = clean_for_match(cand_artist)
            if a_clean == ca_clean:
                artist_sim = 1.0
            elif a_clean in ca_clean or ca_clean in a_clean:
                artist_sim = 0.85
            else:
                artist_sim = calculate_similarity(target_artist, cand_artist)
                if artist_sim < 0.2:
                    artist_sim = 0.1

        # Audio Quality Bonus: Hi-Res FLAC > Lossless FLAC > Low / Atmos
        audio_q = (track.get("audioQuality") or "").upper()
        q_label = (track.get("quality") or "").lower()
        is_hires = bool(track.get("hires"))

        quality_bonus = 0.0
        if "HI_RES" in audio_q or "hi-res" in q_label or is_hires:
            quality_bonus = 0.35
        elif "LOSSLESS" in audio_q or "lossless" in q_label:
            quality_bonus = 0.25
        elif "ATMOS" in audio_q or "atmos" in q_label:
            quality_bonus = 0.05

        total_score = (title_sim * 0.40) + (artist_sim * 0.30) + quality_bonus
        if is_title_exact:
            total_score += 0.15

        if total_score > highest_score:
            highest_score = total_score
            best_match = track

    if highest_score >= 0.50:
        log.debug(f"Velixir Tidal best match for '{target_title} - {target_artist}': {best_match.get('title')} by {best_match.get('artist')} (score: {highest_score:.2f}, ID: {best_match.get('id')})")
        return best_match

    return None

def fetch_velixir_tidal_stream_info(track_id: str) -> Optional[dict]:
    """Fetch stream manifest / URL details for track_id from Velixir Tidal API."""
    base = get_velixir_tidal_api_base()
    url = f"{base}/stream/{track_id}"
    try:
        with httpx.Client(timeout=25.0, follow_redirects=True) as client:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("encrypted"):
                    log.warning(f"Velixir Tidal stream for track {track_id} is encrypted; skipping.")
                    return None
                return data
            else:
                log.warning(f"Velixir Tidal stream API returned HTTP {resp.status_code} for {track_id}")
    except Exception as e:
        log.error(f"Error fetching Velixir Tidal stream info for {track_id}: {e}")
    return None

def convert_mpd_to_flac_bytes(mpd_url: str) -> Optional[bytes]:
    """
    Convert a DASH MPD manifest URL to pure FLAC bytes using ffmpeg.
    1. Tries direct ffmpeg stream conversion first.
    2. If ffmpeg lacks DASH demuxer, downloads initialization chunk (0.mp4)
       and media segments concurrently, concatenates fMP4, and pipes to ffmpeg.
    """
    ffmpeg_bin = get_ffmpeg_binary()

    # 1. Direct ffmpeg attempt
    cmd_direct = [
        ffmpeg_bin,
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", mpd_url,
        "-c:a", "copy",
        "-f", "flac",
        "-",
    ]
    try:
        proc = subprocess.Popen(cmd_direct, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate(timeout=90)
        if proc.returncode == 0 and len(out) > 0 and out[:4] == b"fLaC":
            log.info(f"Direct ffmpeg MPD copy succeeded: {len(out)} bytes FLAC")
            return out
    except Exception as e:
        log.debug(f"Direct ffmpeg MPD failed: {e}")

    # 2. Resilient MPD chunk download & pipe conversion
    log.debug(f"Parsing MPD manifest from {mpd_url} for chunk extraction...")
    try:
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            resp = client.get(mpd_url)
            if resp.status_code != 200:
                log.warning(f"Failed to fetch MPD manifest: HTTP {resp.status_code}")
                return None
            xml_text = resp.text

        tree = ET.fromstring(xml_text)
        seg_template = tree.find(".//{*}SegmentTemplate")
        if seg_template is None:
            seg_template = tree.find(".//SegmentTemplate")
        if seg_template is None:
            log.warning("No SegmentTemplate found in MPD manifest.")
            return None

        init_url = seg_template.attrib.get("initialization", "")
        media_template = seg_template.attrib.get("media", "")
        start_num = int(seg_template.attrib.get("startNumber", "1"))

        timeline = seg_template.find("{*}SegmentTimeline")
        if timeline is None:
            timeline = seg_template.find("SegmentTimeline")

        segment_count = 0
        if timeline is not None:
            for s in timeline.findall("{*}S") or timeline.findall("S"):
                r = int(s.attrib.get("r", "0"))
                segment_count += 1 + r

        if segment_count <= 0 or not init_url or not media_template:
            log.warning(f"Invalid SegmentTemplate parameters: count={segment_count}, init={bool(init_url)}, media={bool(media_template)}")
            return None

        # Download initialization chunk (0.mp4)
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            init_resp = client.get(init_url)
            if init_resp.status_code != 200:
                log.warning(f"Failed to download MPD initialization chunk: HTTP {init_resp.status_code}")
                return None
            init_bytes = init_resp.content

        # Concurrently fetch media segments
        def fetch_seg(num: int) -> Tuple[int, bytes]:
            url = media_template.replace("$Number$", str(num))
            try:
                with httpx.Client(timeout=35.0, follow_redirects=True) as cl:
                    r = cl.get(url)
                    if r.status_code == 200:
                        return num, r.content
            except Exception as se:
                log.debug(f"Error fetching segment {num}: {se}")
            return num, b""

        seg_dict: Dict[int, bytes] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(fetch_seg, n) for n in range(start_num, start_num + segment_count)]
            for f in concurrent.futures.as_completed(futures):
                n, b = f.result()
                seg_dict[n] = b

        # Validate and assemble fMP4 stream
        full_fmp4 = bytearray(init_bytes)
        for n in range(start_num, start_num + segment_count):
            chunk = seg_dict.get(n)
            if not chunk:
                log.warning(f"Missing MPD segment #{n} of {segment_count}")
                return None
            full_fmp4.extend(chunk)

        # Pipe assembled fMP4 stream into ffmpeg copy
        cmd_pipe = [
            ffmpeg_bin,
            "-i", "-",
            "-c:a", "copy",
            "-f", "flac",
            "-",
        ]
        proc = subprocess.Popen(cmd_pipe, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate(input=bytes(full_fmp4), timeout=120)
        if proc.returncode == 0 and len(out) > 0 and out[:4] == b"fLaC":
            log.info(f"Successfully converted fMP4 to FLAC via ffmpeg copy: {len(out)} bytes")
            return out

        # Fallback to ffmpeg transcode if copy had muxing constraints
        log.debug("ffmpeg copy failed; falling back to flac transcode...")
        cmd_transcode = [
            ffmpeg_bin,
            "-i", "-",
            "-c:a", "flac",
            "-f", "flac",
            "-",
        ]
        proc2 = subprocess.Popen(cmd_transcode, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out2, err2 = proc2.communicate(input=bytes(full_fmp4), timeout=120)
        if proc2.returncode == 0 and len(out2) > 0 and out2[:4] == b"fLaC":
            log.info(f"Successfully converted fMP4 to FLAC via ffmpeg transcode: {len(out2)} bytes")
            return out2

        log.error(f"ffmpeg conversion failed: {err2.decode('utf-8', errors='ignore')[-300:]}")
    except Exception as e:
        log.error(f"Error converting MPD to FLAC: {e}")

    return None

def fetch_velixir_tidal_lossless(video_id: str, song_name: str = "", artist_name: str = "") -> Optional[dict]:
    """
    Search Velixir Tidal API, find best match track, resolve MPD stream,
    convert to pure FLAC bytes, and return lossless payload.
    """
    title = (song_name or "").strip()
    artist = (artist_name or "").strip()

    if not title or title.lower() == "unknown song":
        try:
            from db_service import fetch_correct_metadata
            m_title, m_artist = fetch_correct_metadata(video_id)
            if m_title:
                title = m_title
            if m_artist:
                artist = m_artist
        except Exception:
            pass

    if not title:
        return None

    # Try title + artist search first, then fallback to title only
    candidates = []
    if artist:
        candidates = search_velixir_tidal(f"{title} {artist}")
    if not candidates:
        candidates = search_velixir_tidal(title)
    if not candidates:
        log.debug(f"Velixir Tidal search found no candidates for '{title} - {artist}'")
        return None

    best_track = find_best_velixir_tidal_match(title, artist, candidates)
    if not best_track or not best_track.get("id"):
        return None

    track_id = str(best_track["id"])
    log.info(f"Found best Velixir Tidal match for {video_id} ('{title}'): ID {track_id} ({best_track.get('title')} - {best_track.get('artist')})")
    stream_info = fetch_velixir_tidal_stream_info(track_id)
    if not stream_info:
        return None

    stream_url = stream_info.get("url") or ""
    if not stream_url:
        return None

    flac_bytes = None
    if ".mpd" in stream_url or stream_info.get("format") == "dash":
        flac_bytes = convert_mpd_to_flac_bytes(stream_url)
    elif ".m3u8" in stream_url:
        flac_bytes = convert_m3u8_to_flac_bytes(stream_url)
    else:
        try:
            with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                resp = client.get(stream_url)
                if resp.status_code == 200 and len(resp.content) > 0 and resp.content[:4] == b"fLaC":
                    flac_bytes = resp.content
        except Exception as e:
            log.debug(f"Direct stream download failed: {e}")

    if flac_bytes and len(flac_bytes) > 0 and flac_bytes[:4] == b"fLaC":
        return {
            "video_id": video_id,
            "audio_bytes": flac_bytes,
            "ext": "flac",
            "format_id": 6,
            "title": best_track.get("title") or title,
            "artist": best_track.get("artist") or artist,
            "source": "velixir_tidal_dash",
        }

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
            if ".mpd" in manifest_url:
                flac_bytes = convert_mpd_to_flac_bytes(manifest_url)
            else:
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

    # Fallback to Velixir Tidal API (Search + Best Match + MPD DASH FLAC)
    velixir_res = fetch_velixir_tidal_lossless(video_id, correct_title, correct_artist)
    if velixir_res:
        return velixir_res

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
