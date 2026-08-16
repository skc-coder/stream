#!/usr/bin/env python3
"""
stream.testuk.org Batch Downloader & Real-Time Uploader to Google Drive via Rclone
"""

import os
import re
import sys
import time
import json
import shutil
import urllib.parse
import configparser
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# Base URLs and Headers
BASE_SITE = "https://stream.testuk.org"
PROXY_BASE = "https://proxy.streamvideo.co.in/fetch/api.penpencil.co"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": f"{BASE_SITE}/",
}

STAGING_DIR = "staging"
READY_DIR = "ready_for_upload"
INDEX_CACHE_FILE = ".index_cache.json"
STATE_FILE = ".pipeline_state.json"

# Load Configuration
def load_config():
    config = configparser.ConfigParser()
    if os.path.exists("config.ini"):
        config.read("config.ini")
    
    session = config.get("auth", "session", fallback="")
    session_expiry = config.get("auth", "session_expiry", fallback="")
    rclone_target = config.get("rclone", "remote_path", fallback="gdrive:stream/")
    transfers = config.getint("rclone", "transfers", fallback=6)
    poll_interval = config.getint("rclone", "poll_interval_seconds", fallback=10)
    max_workers = config.getint("concurrency", "max_download_workers", fallback=3)
    max_files_per_minute = config.getint("concurrency", "max_files_per_minute", fallback=5)

    return {
        "session": session,
        "session_expiry": session_expiry,
        "rclone_target": rclone_target,
        "transfers": transfers,
        "poll_interval": poll_interval,
        "max_workers": max_workers,
        "max_files_per_minute": max_files_per_minute,
    }

def sanitize_filename(name):
    if not name:
        return "Untitled"
    # Remove invalid path characters
    clean = re.sub(r'[\\/*?:"<>|]', '_', name)
    return clean.strip()

# State Tracking
class PipelineState:
    def __init__(self, filepath=STATE_FILE):
        self.filepath = filepath
        self.completed = set()
        self.load()

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.completed = set(data.get("completed", []))
            except Exception as e:
                print(f"[State] Warning loading state file: {e}")

    def is_completed(self, item_id):
        return item_id in self.completed

    def mark_completed(self, item_id):
        self.completed.add(item_id)
        self.save()

    def save(self):
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump({"completed": list(self.completed)}, f, indent=2)
        except Exception as e:
            print(f"[State] Warning saving state file: {e}")

# API Harvester
class BatchHarvester:
    def __init__(self, session_cookie, session_expiry=""):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        cookies = {"session": session_cookie}
        if session_expiry:
            cookies["session_expiry"] = session_expiry
        self.session.cookies.update(cookies)

    def parse_batch_url(self, url):
        parsed = urllib.parse.urlparse(url.strip())
        params = urllib.parse.parse_qs(parsed.query)
        batch_id = params.get("batchId", [None])[0]
        batch_name = params.get("batchName", [None])[0]

        if not batch_id:
            # Try path parsing if any
            match = re.search(r'batchId=([a-f0-9]+)', url)
            if match:
                batch_id = match.group(1)

        return batch_id, batch_name

    def request_with_retry(self, url, max_retries=5, initial_delay=2):
        delay = initial_delay
        for attempt in range(max_retries):
            res = self.session.get(url)
            if res.status_code == 429:
                retry_after = res.headers.get("Retry-After")
                sleep_time = int(retry_after) if retry_after and retry_after.isdigit() else delay
                print(f"[Rate Limit] HTTP 429 encountered. Backing off for {sleep_time}s (Attempt {attempt+1}/{max_retries})...")
                time.sleep(sleep_time)
                delay *= 2
                continue
            res.raise_for_status()
            return res
        res.raise_for_status()
        return res

    def fetch_batch_subjects(self, batch_id):
        url = f"{PROXY_BASE}/v3/batches/{batch_id}/details"
        res = self.request_with_retry(url)
        data = res.json()
        
        subjects = []
        # Exclude Khazana, Announcements, Tests tab entries if labeled
        raw_subjects = data.get("data", {}).get("subjects", [])
        batch_name = data.get("data", {}).get("name", "Unknown Batch")

        for sub in raw_subjects:
            subject_name = sub.get("subject", "").strip()
            # Exclusion rules: ignore "Khazana", "Announcements", "Tests" tabs
            lower_name = subject_name.lower()
            if any(ex in lower_name for ex in ["khazana", "announcement", "announcements"]):
                print(f"[Harvester] Excluding subject: {subject_name}")
                continue

            subjects.append({
                "subject_id": sub.get("_id"),
                "subject_name": subject_name,
            })
        return batch_name, subjects

    def fetch_subject_topics(self, batch_id, subject_id):
        topics = []
        page = 1
        while True:
            url = f"{PROXY_BASE}/v2/batches/{batch_id}/subject/{subject_id}/topics?page={page}"
            res = self.request_with_retry(url)
            data = res.json()
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                topics.append({
                    "topic_id": item.get("_id"),
                    "topic_name": item.get("name", "").strip(),
                })
            page += 1
            time.sleep(0.3)
        return topics

    def fetch_topic_contents(self, batch_id, subject_id, topic_id, content_type):
        contents = []
        page = 1
        while True:
            url = f"{PROXY_BASE}/v2/batches/{batch_id}/subject/{subject_id}/contents?page={page}&contentType={content_type}&tag={topic_id}"
            res = self.request_with_retry(url)
            data = res.json()
            items = data.get("data", [])
            if not items:
                break
            contents.extend(items)
            page += 1
            time.sleep(0.3)
        return contents

    def fetch_video_stream_details(self, batch_id, subject_id, schedule_id):
        url = f"{BASE_SITE}/schedule-details?batchId={batch_id}&subjectId={subject_id}&scheduleId={schedule_id}&tap=video"
        res = self.request_with_retry(url)
        html = res.text

        match = re.search(r'const MEDIA_TOKEN = "([^"]+)";', html)
        if not match:
            print(f"[Harvester] Could not find MEDIA_TOKEN for scheduleId {schedule_id}")
            return None, []

        media_token = match.group(1)
        encoded_token = urllib.parse.quote(media_token, safe="")
        
        stream_url_req = f"{BASE_SITE}/v1/videos/video-url-details?mediaToken={encoded_token}&videoContainerType=DASH"
        stream_res = self.request_with_retry(stream_url_req)
        stream_data = stream_res.json()

        dash_url = stream_data.get("url")
        keys = stream_data.get("keys", []) # ["KEY_ID:KEY"]
        return dash_url, keys

# Downloader Worker
def download_item(item, harvester, pipeline_state):
    item_id = item["id"]
    if pipeline_state.is_completed(item_id):
        print(f"[Skip] Already completed: {item['rel_path']}")
        return

    rel_dir = os.path.dirname(item["rel_path"])
    file_name = os.path.basename(item["rel_path"])

    staging_dir = os.path.join(STAGING_DIR, rel_dir)
    ready_dir = os.path.join(READY_DIR, rel_dir)

    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(ready_dir, exist_ok=True)

    staging_file = os.path.join(staging_dir, file_name)
    ready_file = os.path.join(ready_dir, file_name)

    if os.path.exists(ready_file):
        pipeline_state.mark_completed(item_id)
        return

    item_type = item["type"]
    print(f"[Download Starting] ({item_type.upper()}) {item['rel_path']}")

    try:
        if item_type == "note":
            url = item["url"]
            res = harvester.session.get(url, stream=True)
            res.raise_for_status()
            with open(staging_file, "wb") as f:
                for chunk in res.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        
        elif item_type == "video":
            dash_url, keys = harvester.fetch_video_stream_details(
                item["batch_id"], item["subject_id"], item["schedule_id"]
            )
            if not dash_url:
                print(f"[Error] Stream details missing for {item['rel_path']}")
                return

            cmd = ["yt-dlp", dash_url]
            for key in keys:
                cmd.extend(["--key", key])
            cmd.extend(["-o", staging_file, "--no-warning", "--quiet"])

            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                print(f"[yt-dlp Error] {proc.stderr}")
                return

        # Atomic move from staging to ready_for_upload
        shutil.move(staging_file, ready_file)
        pipeline_state.mark_completed(item_id)
        print(f"[Download Complete] {item['rel_path']} -> Ready for upload.")

    except Exception as e:
        print(f"[Download Failed] {item['rel_path']}: {e}")
        if os.path.exists(staging_file):
            try:
                os.remove(staging_file)
            except Exception:
                pass

# Background Uploader Process
def start_uploader_process(rclone_target, transfers, poll_interval):
    cmd = [
        "rclone", "move", READY_DIR, rclone_target,
        "--delete-empty-src-dirs",
        "--transfers", str(transfers)
    ]
    print(f"[Uploader Worker] Spawning background uploader loop (Target: {rclone_target})...")
    
    # We run an inline loop inside a python subprocess or thread
    while True:
        try:
            if os.path.exists(READY_DIR) and os.listdir(READY_DIR):
                print(f"[Uploader] Executing rclone move...")
                subprocess.run(cmd, check=False)
        except Exception as e:
            print(f"[Uploader Error] {e}")
        time.sleep(poll_interval)

def main():
    config = load_config()
    session_cookie = config["session"]
    if not session_cookie:
        print("[Error] session cookie missing in config.ini")
        sys.exit(1)

    harvester = BatchHarvester(session_cookie, config["session_expiry"])
    pipeline_state = PipelineState()

    os.makedirs(STAGING_DIR, exist_ok=True)
    os.makedirs(READY_DIR, exist_ok=True)

    if not os.path.exists("courses.txt"):
        print("[Error] courses.txt not found!")
        sys.exit(1)

    with open("courses.txt", "r", encoding="utf-8") as f:
        course_urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    print(f"[Harvester] Processing {len(course_urls)} courses from courses.txt...")

    all_download_items = []

    for course_url in course_urls:
        batch_id, batch_name_override = harvester.parse_batch_url(course_url)
        if not batch_id:
            print(f"[Warning] Could not extract batchId from URL: {course_url}")
            continue

        print(f"\n--- Fetching Batch {batch_id} ---")
        batch_name, subjects = harvester.fetch_batch_subjects(batch_id)
        if batch_name_override:
            batch_name = batch_name_override
        
        batch_folder = sanitize_filename(batch_name)

        for sub in subjects:
            subject_id = sub["subject_id"]
            subject_folder = sanitize_filename(sub["subject_name"])
            print(f"  > Subject: {subject_folder}")

            topics = harvester.fetch_subject_topics(batch_id, subject_id)
            for topic in topics:
                topic_id = topic["topic_id"]
                topic_folder = sanitize_filename(topic["topic_name"])
                print(f"    > Chapter/Topic: {topic_folder}")

                # Fetch Notes
                notes = harvester.fetch_topic_contents(batch_id, subject_id, topic_id, "notes")
                for note in notes:
                    attachments = note.get("homeworkIds", []) or note.get("attachments", [])
                    note_title = sanitize_filename(note.get("topic") or note.get("name") or "Note")
                    
                    if not attachments and ("url" in note or "link" in note):
                        direct_url = note.get("url") or note.get("link")
                        if direct_url:
                            rel_path = os.path.join("stream", batch_folder, subject_folder, topic_folder, f"{note_title}.pdf")
                            all_download_items.append({
                                "id": f"note_{note.get('_id')}",
                                "type": "note",
                                "url": direct_url,
                                "rel_path": rel_path
                            })

                    # Attachment validation rule
                    for att in attachments:
                        base_url = att.get("baseUrl", "")
                        key = att.get("key", "")
                        if not key or key.strip() == "":
                            # Skip dummy root domain URLs to prevent 403
                            continue
                        
                        pdf_url = urllib.parse.urljoin(base_url, key)
                        att_name = att.get("name", "")
                        file_label = sanitize_filename(att_name) if att_name else note_title
                        pdf_name = f"{file_label}.pdf" if not file_label.endswith(".pdf") else file_label
                        rel_path = os.path.join("stream", batch_folder, subject_folder, topic_folder, pdf_name)
                        
                        all_download_items.append({
                            "id": f"note_{note.get('_id')}_{key}",
                            "type": "note",
                            "url": pdf_url,
                            "rel_path": rel_path
                        })

                # Fetch Videos
                videos = harvester.fetch_topic_contents(batch_id, subject_id, topic_id, "videos")
                for vid in videos:
                    schedule_id = vid.get("_id")
                    vid_title = sanitize_filename(vid.get("topic", "Lecture"))
                    video_name = f"{vid_title}.mp4"
                    rel_path = os.path.join("stream", batch_folder, subject_folder, topic_folder, video_name)

                    all_download_items.append({
                        "id": f"video_{schedule_id}",
                        "type": "video",
                        "batch_id": batch_id,
                        "subject_id": subject_id,
                        "schedule_id": schedule_id,
                        "rel_path": rel_path
                    })

    # Save index cache
    with open(INDEX_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(all_download_items, f, indent=2)

    print(f"\n[Harvester] Discovered {len(all_download_items)} total downloadable items.")

    # Start Uploader in separate daemon thread
    import threading
    uploader_thread = threading.Thread(
        target=start_uploader_process,
        args=(config["rclone_target"], config["transfers"], config["poll_interval"]),
        daemon=True
    )
    uploader_thread.start()

    # Parallel Download Producer with Per-Minute Rate Limiter
    max_rate = config.get("max_files_per_minute", 5)
    print(f"[Downloader Pool] Starting parallel download with {config['max_workers']} workers (Rate limit: {max_rate} files/min)...")
    
    with ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        futures = []
        submission_timestamps = []

        for item in all_download_items:
            item_id = item["id"]
            if pipeline_state.is_completed(item_id):
                print(f"[Skip] Already completed: {item['rel_path']}")
                continue

            if max_rate > 0:
                now = time.time()
                # Remove timestamps older than 60 seconds
                submission_timestamps = [t for t in submission_timestamps if now - t < 60]
                if len(submission_timestamps) >= max_rate:
                    sleep_needed = 60 - (now - submission_timestamps[0]) + 0.5
                    if sleep_needed > 0:
                        print(f"[Rate Limiter] Reached max {max_rate} downloads/min. Throttling for {sleep_needed:.1f}s...")
                        time.sleep(sleep_needed)
                    submission_timestamps = [t for t in submission_timestamps if time.time() - t < 60]

                submission_timestamps.append(time.time())

            futures.append(executor.submit(download_item, item, harvester, pipeline_state))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[Worker Exception] {e}")

    print("\n[Pipeline Complete] All downloads finished! Waiting for uploader worker to flush remaining files...")
    # Final flush check
    time.sleep(config["poll_interval"] + 2)
    cmd = [
        "rclone", "move", READY_DIR, config["rclone_target"],
        "--delete-empty-src-dirs",
        "--transfers", str(config["transfers"])
    ]
    subprocess.run(cmd, check=False)
    print("[Done] Real-time upload fully synchronized.")

if __name__ == "__main__":
    main()
