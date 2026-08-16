# stream.testuk.org Downloader & Real-Time Rclone Uploader

Automation pipeline to harvest courses, DRM-decrypted video lectures (ClearKey DASH), and PDF study materials from `stream.testuk.org`, downloading them concurrently into a staging area and uploading them in real-time to Google Drive via `rclone`.

---

## 🛠️ Prerequisites

- **Python 3.8+**
- **[yt-dlp](https://github.com/yt-dlp/yt-dlp)** installed and available in system PATH.
- **[rclone](https://rclone.org/)** configured with your Google Drive remote (e.g. `gdrive:`).

---

## 🚀 Setup & Installation

```bash
# 1. Clone the repository
git clone <repository_url>
cd stream

# 2. Install dependencies
pip install -r requirements.txt
```

---

## ⚙️ Configuration

### 1. `config.ini`
Set your authentication cookie and rclone target directory:

```ini
[auth]
session = YOUR_SESSION_COOKIE_HERE
session_expiry = 1786887774943

[rclone]
remote_path = gdrive:stream/
transfers = 6
poll_interval_seconds = 10

[concurrency]
max_download_workers = 3
```

### 2. `courses.txt`
Add batch URLs (one per line) that you want to harvest:

```text
https://stream.testuk.org/subjects?batchId=65d898a7b8b10a00187a4f9c&batchName=Neev+2025
```

---

## 🏃 Running the Pipeline

```bash
python main.py
```

### How It Works

1. **API Harvester:** Fetches batch details, excluding unwanted sections ("Khazana", "Announcements", "Notices", etc.).
2. **Parallel Downloader:** Downloads DRM videos (decrypted on-the-fly via `yt-dlp`) and validated PDFs into `staging/`.
3. **Atomic Move:** Once a file download is verified, it is moved from `staging/` to `ready_for_upload/`.
4. **Real-Time Uploader:** Background worker polls `ready_for_upload/` every 10s and moves completed files directly to Google Drive via `rclone move ready_for_upload/ gdrive:stream/ --delete-empty-src-dirs --transfers 6`.
5. **State & Resume:** Saves progress in `.pipeline_state.json` so interrupted runs resume seamlessly without re-downloading existing files.
