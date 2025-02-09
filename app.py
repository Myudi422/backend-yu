import os
import signal
import subprocess
import uuid
import json
import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import gdown
import psutil
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger

app = FastAPI()

# Izinkan semua origin (untuk development)
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Global Event Loop & Helper Update ---
event_loop = None

def kill_orphan_ffmpeg():
    # Kumpulkan semua PID ffmpeg yang sedang dikelola
    managed_pids = {stream["process"].pid for stream in streams.values()}
    # Iterasi seluruh proses sistem yang bernama 'ffmpeg'
    for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
        try:
            if proc.info["name"] == "ffmpeg" and proc.pid not in managed_pids:
                print(f"Found orphan ffmpeg process {proc.pid}, terminating...")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                    print(f"Orphan process {proc.pid} terminated.")
                except psutil.TimeoutExpired:
                    proc.kill()
                    print(f"Orphan process {proc.pid} killed.")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

def schedule_update():
    if event_loop:
        asyncio.run_coroutine_threadsafe(broadcast_update(), event_loop)

# -------------------------------
# Model Request & Response
# -------------------------------
class DownloadRequest(BaseModel):
    drive_url: str
    custom_name: str | None = None

class DownloadResponse(BaseModel):
    file_name: str

class StreamRequest(BaseModel):
    file: str
    youtube_key: str
    platform: str
    custom_rtmp_url: Optional[str] = None

class StreamResponse(BaseModel):
    id: str
    file: str
    youtube_key: str
    active: bool


class ScheduledStreamRequest(BaseModel):
    file: str
    youtube_key: str
    platform: str
    custom_rtmp_url: Optional[str] = None
    schedule_time: str

class ScheduledStreamResponse(BaseModel):
    id: str
    file: str
    youtube_key: str
    schedule_time: str

# -------------------------------
# Penyimpanan Data In-Memory
# -------------------------------
downloaded_files = []  # Hanya digunakan untuk tracking download, bukan untuk validasi file
streams = {}
scheduled_streams = {}

DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# -------------------------------
# WebSocket Manager
# -------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                print("Error sending message:", e)

manager = ConnectionManager()


@app.on_event("shutdown")
def shutdown_event():
    # Hentikan semua proses ffmpeg yang tercatat di streams
    for stream_id, stream in list(streams.items()):
        process = stream["process"]
        try:
            if process.poll() is None:  # Proses masih berjalan
                process.terminate()
                process.wait(timeout=5)
                print(f"Terminated ffmpeg process for stream {stream_id}")
        except Exception as e:
            print(f"Error terminating ffmpeg process for stream {stream_id}: {e}")
    scheduler.shutdown()
    # Panggil fungsi untuk menghentikan proses orphan
    kill_orphan_ffmpeg()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    files = [f for f in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]
    await websocket.send_text(json.dumps({
        "files": files,
        "streams": [
            {
                "id": s["id"],
                "file": s["file"],
                "youtube_key": s["youtube_key"],
                "active": s["active"]
            }
            for s in streams.values()
        ],
        "scheduled_streams": [
            {
                "id": s["id"],
                "file": s["file"],
                "youtube_key": s["youtube_key"],
                "schedule_time": s["schedule_time"]
            }
            for s in scheduled_streams.values()
        ]
    }))
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

async def broadcast_update():
    files = [f for f in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]
    data = {
        "files": files,
        "streams": [
            {
                "id": s["id"],
                "file": s["file"],
                "youtube_key": s["youtube_key"],
                "active": s["active"]
            }
            for s in streams.values()
        ],
        "scheduled_streams": [
            {
                "id": s["id"],
                "file": s["file"],
                "youtube_key": s["youtube_key"],
                "schedule_time": s["schedule_time"]
            }
            for s in scheduled_streams.values()
        ]
    }
    await manager.broadcast(json.dumps(data))

# -------------------------------
# APScheduler Setup
# -------------------------------
scheduler = BackgroundScheduler()

@app.on_event("startup")
def startup_event():
    global event_loop
    event_loop = asyncio.get_event_loop()
    scheduler.start()
    # Ubah interval pengecekan menjadi 2 detik agar lebih responsif
    scheduler.add_job(check_streams_status, 'interval', seconds=2)

# -------------------------------
# Endpoint Download & Daftar File
# -------------------------------
@app.post("/api/download", response_model=DownloadResponse)
def download_file(req: DownloadRequest):
    file_id = str(uuid.uuid4())
    extension = ".mp4"
    custom_name = req.custom_name.strip() if req.custom_name else file_id
    file_name = f"{custom_name}{extension}"
    output_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    try:
        gdown.download(req.drive_url, output_path, quiet=False)
        downloaded_files.append(file_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    schedule_update()
    return DownloadResponse(file_name=file_name)

@app.get("/api/files")
def get_files():
    files = [f for f in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]
    return {"files": files}

@app.delete("/api/files/{filename}")
def delete_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    try:
        os.remove(file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")
    schedule_update()
    return {"detail": "File deleted successfully."}


def monitor_ffmpeg(process, stream_id):
    # Baca output stderr secara terus-menerus
    for line in iter(process.stderr.readline, b''):
        decoded_line = line.decode('utf-8')
        # Contoh: jika menemukan kata "error" di log, hentikan proses
        if "error" in decoded_line.lower():
            print(f"Error terdeteksi pada stream {stream_id}: {decoded_line}")
            try:
                process.terminate()
            except Exception as e:
                print(f"Gagal menghentikan stream {stream_id}: {e}")
            break

def get_video_info(file_path: str) -> dict:
    command = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height,avg_frame_rate",
        "-of", "json",
        file_path
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        info = json.loads(result.stdout)
        return info.get("streams", [{}])[0]
    except Exception as e:
        print("Error mendapatkan info video:", e)
        return {}


# -------------------------------
# Helper: Menjalankan ffmpeg untuk Streaming
# -------------------------------
def start_ffmpeg_stream(file_path: str, stream_key: str, stream_id: str, platform: str, custom_rtmp_url: str = None):
    info = get_video_info(file_path)
    width = info.get("width", 1280)
    height = info.get("height", 720)
    
    if width > 1920:
        scale_filter = "scale=1920:-2"
    else:
        scale_filter = None
    
    if width >= 1920:
        video_bitrate = "2500k"
    elif width >= 1280:
        video_bitrate = "1500k"
    else:
        video_bitrate = "1000k"
    
    # Tentukan target RTMP URL berdasarkan platform
    if platform == "youtube":
        target_url = f"rtmp://a.rtmp.youtube.com/live2/{stream_key}"
    elif platform == "facebook":
        target_url = f"rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}"
    elif platform == "other":
        if not custom_rtmp_url:
            raise ValueError("Custom RTMP URL is required for platform 'other'.")
        # Pastikan URL kustom tidak diakhiri oleh slash
        if custom_rtmp_url.endswith("/"):
            custom_rtmp_url = custom_rtmp_url[:-1]
        target_url = f"{custom_rtmp_url}/{stream_key}"
    else:
        raise ValueError("Invalid platform specified.")
    
    command = [
        "ffmpeg",
        "-re",
        "-stream_loop", "-1",
        "-i", file_path,
    ]
    
    if scale_filter:
        command.extend(["-vf", scale_filter])
    
    command.extend([
        "-vcodec", "libx264",
        "-preset", "veryfast",
        "-b:v", video_bitrate,
        "-maxrate", video_bitrate,
        "-bufsize", "2000k",
        "-g", "48",
        "-keyint_min", "48",
        "-c:a", "aac",
        "-b:a", "128k",
        "-f", "flv",
        target_url
    ])
    
    print("Running command:", " ".join(command))
    
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    threading.Thread(target=monitor_ffmpeg, args=(process, stream_id), daemon=True).start()
    return process



# -------------------------------
# Endpoint Streaming Langsung (Now)
# -------------------------------
@app.post("/api/streams", response_model=StreamResponse)
def create_stream(req: StreamRequest):
    file_path = os.path.join(DOWNLOAD_DIR, req.file)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    for stream in streams.values():
        if stream["file"] == req.file and stream["youtube_key"] == req.youtube_key:
            raise HTTPException(status_code=400, detail="This stream is already running.")
    stream_id = str(uuid.uuid4())
    process = start_ffmpeg_stream(file_path, req.youtube_key, stream_id, req.platform, req.custom_rtmp_url)
    streams[stream_id] = {
        "id": stream_id,
        "file": req.file,
        "youtube_key": req.youtube_key,
        "platform": req.platform,
        "custom_rtmp_url": req.custom_rtmp_url,
        "process": process,
        "active": True
    }
    schedule_update()
    return StreamResponse(id=stream_id, file=req.file, youtube_key=req.youtube_key, active=True)

@app.get("/api/streams", response_model=list[StreamResponse])
def list_streams():
    return [
        StreamResponse(
            id=stream["id"],
            file=stream["file"],
            youtube_key=stream["youtube_key"],
            active=stream["active"]
        )
        for stream in streams.values()
    ]

@app.patch("/api/streams/{stream_id}/toggle", response_model=StreamResponse)
def toggle_stream(stream_id: str):
    if stream_id not in streams:
        raise HTTPException(status_code=404, detail="Stream tidak ditemukan.")
    stream = streams[stream_id]
    process = stream["process"]
    try:
        if stream["active"]:
            os.kill(process.pid, signal.SIGSTOP)
            stream["active"] = False
        else:
            os.kill(process.pid, signal.SIGCONT)
            stream["active"] = True
    except ProcessLookupError:
        # Jika proses sudah tidak ada, set status non aktif
        stream["active"] = False
    schedule_update()
    return StreamResponse(
        id=stream["id"],
        file=stream["file"],
        youtube_key=stream["youtube_key"],
        active=stream["active"]
    )

@app.delete("/api/streams/{stream_id}", response_model=dict)
def delete_stream(stream_id: str):
    if stream_id not in streams:
        raise HTTPException(status_code=404, detail="Stream not found.")
    stream = streams.pop(stream_id)
    process = stream["process"]
    process.terminate()
    process.wait()
    schedule_update()
    return {"detail": "Stream stopped and removed."}

# -------------------------------
# Endpoint Streaming Terjadwal
# -------------------------------
@app.post("/api/scheduled", response_model=ScheduledStreamResponse)
def schedule_stream(req: ScheduledStreamRequest):
    file_path = os.path.join(DOWNLOAD_DIR, req.file)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    for s in scheduled_streams.values():
        if s["file"] == req.file and s["youtube_key"] == req.youtube_key:
            raise HTTPException(status_code=400, detail="This stream is already scheduled.")
    schedule_id = str(uuid.uuid4())
    try:
        dt_wib = datetime.fromisoformat(req.schedule_time)
        dt_wib = dt_wib.replace(tzinfo=ZoneInfo("Asia/Jakarta"))
        dt_utc = dt_wib.astimezone(timezone.utc)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid datetime format. Use ISO format.")
    trigger = DateTrigger(run_date=dt_utc)
    scheduler.add_job(
        func=job_start_scheduled_stream,
        trigger=trigger,
        args=[schedule_id, req.file, req.youtube_key, req.platform, req.custom_rtmp_url],
        id=schedule_id
    )
    scheduled_streams[schedule_id] = {
        "id": schedule_id,
        "file": req.file,
        "youtube_key": req.youtube_key,
        "platform": req.platform,
        "custom_rtmp_url": req.custom_rtmp_url,
        "schedule_time": req.schedule_time,
        "job": scheduler.get_job(schedule_id)
    }
    schedule_update()
    return ScheduledStreamResponse(id=schedule_id, file=req.file, youtube_key=req.youtube_key, schedule_time=req.schedule_time)

def job_start_scheduled_stream(schedule_id: str, file: str, youtube_key: str, platform: str, custom_rtmp_url: str):
    if schedule_id not in scheduled_streams:
        return
    scheduled_streams.pop(schedule_id, None)
    file_path = os.path.join(DOWNLOAD_DIR, file)
    if not os.path.exists(file_path):
        return
    stream_id = str(uuid.uuid4())
    process = start_ffmpeg_stream(file_path, youtube_key, stream_id, platform, custom_rtmp_url)
    streams[stream_id] = {
        "id": stream_id,
        "file": file,
        "youtube_key": youtube_key,
        "platform": platform,
        "custom_rtmp_url": custom_rtmp_url,
        "process": process,
        "active": True
    }
    schedule_update()

@app.get("/api/scheduled", response_model=list[ScheduledStreamResponse])
def list_scheduled_streams():
    return [
        ScheduledStreamResponse(
            id=s["id"],
            file=s["file"],
            youtube_key=s["youtube_key"],
            schedule_time=s["schedule_time"]
        )
        for s in scheduled_streams.values()
    ]

@app.delete("/api/scheduled/{schedule_id}", response_model=dict)
def delete_scheduled_stream(schedule_id: str):
    # Jika schedule_id tidak ditemukan, anggap sudah berjalan atau sudah dihapus.
    if schedule_id not in scheduled_streams:
        return {"detail": "Scheduled stream sudah berjalan atau tidak ditemukan."}
    try:
        scheduler.remove_job(schedule_id)
    except Exception as e:
        # Jika terjadi error pada remove_job, kita bisa mengabaikannya
        pass
    scheduled_streams.pop(schedule_id, None)
    schedule_update()
    return {"detail": "Scheduled stream dihapus."}


@app.post("/api/scheduled/{schedule_id}/start", response_model=StreamResponse)
def start_scheduled_stream(schedule_id: str):
    if schedule_id not in scheduled_streams:
        raise HTTPException(status_code=404, detail="Scheduled stream not found.")
    scheduler.remove_job(schedule_id)
    scheduled = scheduled_streams.pop(schedule_id)
    file = scheduled["file"]
    youtube_key = scheduled["youtube_key"]
    platform = scheduled["platform"]
    custom_rtmp_url = scheduled.get("custom_rtmp_url")
    file_path = os.path.join(DOWNLOAD_DIR, file)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    stream_id = str(uuid.uuid4())
    process = start_ffmpeg_stream(file_path, youtube_key, stream_id, platform, custom_rtmp_url)
    streams[stream_id] = {
        "id": stream_id,
        "file": file,
        "youtube_key": youtube_key,
        "platform": platform,
        "custom_rtmp_url": custom_rtmp_url,
        "process": process,
        "active": True
    }
    schedule_update()
    return StreamResponse(id=stream_id, file=file, youtube_key=youtube_key, active=True)


# -------------------------------
# Background Job: Cek Status Proses ffmpeg
# -------------------------------
def check_streams_status():
    to_remove = []
    for stream_id, stream in list(streams.items()):
        process = stream["process"]
        if process.poll() is not None:
            # Jika proses sudah selesai atau error, tandai untuk dihapus
            to_remove.append(stream_id)
    for stream_id in to_remove:
        streams.pop(stream_id, None)
    if to_remove:
        schedule_update()

@app.get("/api/server-stats")
def get_server_stats():
    cpu_usage = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    uptime_seconds = time.time() - psutil.boot_time()
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    active_streams_count = len(streams)
    scheduled_streams_count = len(scheduled_streams)
    downloaded_files_count = len(downloaded_files)
    return {
        "cpu_percent": cpu_usage,
        "memory": {
            "total": mem.total,
            "available": mem.available,
            "used": mem.used,
            "percent": mem.percent
        },
        "disk": {
            "total": disk.total,
            "used": disk.used,
            "free": disk.free,
            "percent": disk.percent
        },
        "uptime": uptime_str,
        "active_streams": active_streams_count,
        "scheduled_streams": scheduled_streams_count,
        "downloaded_files": downloaded_files_count
    }
