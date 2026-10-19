#!/usr/bin/env python3
"""
Muzo Audio Indexer - Storage Bucket Reconstruction Tool

Scans all audio files across:
  1) shashwatIDR/stream
  2) shashwatIDR/lossless
Indexes all video IDs, tracks, formats, and uploads a fresh db_backup.json to Hugging Face.
"""
import os
import sys

_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURR_DIR not in sys.path:
    sys.path.insert(0, _CURR_DIR)

from db_service import sync_cache_with_buckets, log

if __name__ == "__main__":
    print("=" * 60)
    print("Starting Storage Bucket Database Reconciliation & Sync...")
    print("=" * 60)
    try:
        result = sync_cache_with_buckets(save_backup=True)
        print("=" * 60)
        print("Reconstruction Finished Successfully:")
        for k, v in result.items():
            print(f"  {k}: {v}")
        print("=" * 60)
    except Exception as e:
        print(f"Error during reconstruction: {e}")
        sys.exit(1)
