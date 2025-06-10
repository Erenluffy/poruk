import os
import time
import logging
import shutil
import re
import asyncio
import json
import subprocess
from pathlib import Path
from os import path as ospath, remove as osremove, makedirs
from typing import Optional, Dict, Callable, List, Tuple, Union
from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, RPCError, MessageNotModified, BadRequest
import humanize
import psutil
import signal
from functools import wraps
from asyncio.subprocess import PIPE
from PIL import Image
from datetime import datetime
import pytz
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConnectionFailure
from collections import defaultdict

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

API_ID = 24500584
API_HASH = "449da69cf4081dc2cc74eea828d0c490"
BOT_TOKEN = "7859842889:AAG5jD89VW5xEo9qXT8J0OsB-byL5aJTqZM"
MAX_CONCURRENT_TASKS = 1
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB
MAX_QUEUE_SIZE = 50
BOT_OWNER_ID = 1047253913
MONGODB_URI = "mongodb+srv://erenyeagermikasa84:pkbOXb3ulzi9cEFd@cluster0.ingt8mt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGODB_NAME = "Cluster0"

mongo_client = AsyncIOMotorClient(MONGODB_URI)
db = mongo_client[MONGODB_NAME]

user_settings = db["user_settings"]
active_tasks_db = db["active_tasks"]
task_queue_db = db["task_queue"]

semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
message_cleanup_queue = defaultdict(list)
user_active_messages = {}
processing_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
processing_status = defaultdict(bool)
current_progress_messages = {}
progress_lock = asyncio.Lock()

ENCODING_PRESETS = {
    "ultrafast": {
        "speed": "ultrafast",
        "crf": 24,
        "description": "Fastest encoding, lower quality"
    },
    "superfast": {
        "speed": "superfast",
        "crf": 23,
        "description": "Very fast encoding, decent quality"
    },
    "veryfast": {
        "speed": "veryfast",
        "crf": 22,
        "description": "Fast encoding, good quality"
    },
    "faster": {
        "speed": "faster",
        "crf": 21,
        "description": "Balanced speed and quality"
    },
    "fast": {
        "speed": "fast",
        "crf": 20,
        "description": "Good quality, moderate speed"
    },
    "medium": {
        "speed": "medium",
        "crf": 19,
        "description": "Better quality, slower"
    },
    "slow": {
        "speed": "slow",
        "crf": 18,
        "description": "Best quality, slow encoding"
    },
    "slower": {
        "speed": "slower",
        "crf": 17,
        "description": "Highest quality, very slow"
    },
    "veryslow": {
        "speed": "veryslow",
        "crf": 16,
        "description": "Ultimate quality, extremely slow"
    }
}

bot = Client(
    "video_encoder_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=200,
    sleep_threshold=30,
    max_concurrent_transmissions=5,
    in_memory=True
)

for folder in ["downloads", "encoded", "thumbnails", "watermarks"]:
    makedirs(folder, exist_ok=True)

def handle_floodwait(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            logger.warning(f"FloodWait: Sleeping for {e.value} seconds")
            await asyncio.sleep(e.value)
            return await func(*args, **kwargs)
        except MessageNotModified:
            return None
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            raise
    return wrapper

async def get_user_settings(chat_id: int) -> Dict:
    try:
        settings = await user_settings.find_one({"chat_id": chat_id})
        return settings or {
            "chat_id": chat_id,
            "quality": "4K",
            "preset": "medium",
            "crf": 19,
            "metadata": {"title": ""},
            "status": "idle",
            "created_at": time.time(),
            "last_update": time.time(),
            "use_watermark": False  
        }
    except ConnectionFailure:
        logger.error("Failed to connect to MongoDB")
        return {
            "chat_id": chat_id,
            "quality": "4K",
            "preset": "medium",
            "crf": 19,
            "metadata": {"title": ""},
            "status": "idle",
            "created_at": time.time(),
            "last_update": time.time(),
            "use_watermark": False 
        }

async def update_user_settings(chat_id: int, update_data: Dict):
    try:
        await user_settings.update_one(
            {"chat_id": chat_id},
            {"$set": update_data},
            upsert=True
        )
    except ConnectionFailure:
        logger.error("Failed to update MongoDB")

async def cleanup_user_messages(chat_id: int):
    if chat_id not in message_cleanup_queue:
        return
        
    try:
        for msg_id in message_cleanup_queue[chat_id]:
            try:
                await bot.delete_messages(chat_id, msg_id)
            except Exception as e:
                logger.warning(f"Failed to delete message {msg_id}: {str(e)}")
                
        message_cleanup_queue[chat_id] = []
    except Exception as e:
        logger.error(f"Cleanup messages failed: {str(e)}")

async def add_to_cleanup_queue(chat_id: int, message_id: int):
    message_cleanup_queue[chat_id].append(message_id)

async def handle_thumbnail(chat_id: int, message: Message) -> Optional[str]:
    try:
        folder = "thumbnails"
        makedirs(folder, exist_ok=True)
        
        if message.photo:
            file_path = ospath.join(folder, f"thumb_{chat_id}.jpg")
            await message.download(file_path)
            
            try:
                with Image.open(file_path) as img:
                    img = img.convert("RGB")
                    img.thumbnail((320, 320))
                    img.save(file_path, "JPEG", quality=85)
            except Exception as e:
                logger.warning(f"Thumbnail processing error: {str(e)}")
                
            await update_user_settings(chat_id, {"thumbnail": file_path})
            return file_path
            
    except Exception as e:
        logger.error(f"Thumbnail handling failed: {str(e)}")
    return None

async def handle_watermark(chat_id: int, message: Message) -> Optional[str]:
    try:
        folder = "watermarks"
        makedirs(folder, exist_ok=True)
        file_path = ospath.join(folder, f"wm_{chat_id}.png")
        
        if message.photo or (message.document and message.document.mime_type == "image/png"):
            await message.download(file_path)
            
            try:
                with Image.open(file_path) as img:
                    if img.mode != 'RGBA':
                        img = img.convert("RGBA")
                    img.save(file_path, "PNG")
            except Exception as e:
                logger.warning(f"Watermark processing error: {str(e)}")
                
            await update_user_settings(chat_id, {"watermark": file_path})
            return file_path
            
    except Exception as e:
        logger.error(f"Watermark handling failed: {str(e)}")
    return None

class VideoEncoder:
    async def get_video_info(self, file_path: str) -> dict:
        try:
            cmd = [
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=duration:stream=bit_rate,width,height,duration,codec_name,index,codec_type,tags:format_tags=title',
                '-of', 'json',
                file_path
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=PIPE,
                stderr=PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_msg = stderr.decode('utf-8')
                logger.error(f"FFprobe error: {error_msg}")
                raise RuntimeError(f"FFprobe error: {error_msg}")
            
            info = json.loads(stdout.decode('utf-8'))
            return self._parse_video_info(info)
            
        except Exception as e:
            logger.error(f"Error getting video info: {str(e)}")
            raise ValueError(f"Could not get video info: {str(e)}")

    def _parse_video_info(self, info: dict) -> dict:
        metadata = info.get('format', {}).get('tags', {})
        streams = info.get('streams', [])
        
        video_streams = [s for s in streams if s.get('codec_type') == 'video']
        audio_streams = [s for s in streams if s.get('codec_type') == 'audio']
        subtitle_streams = [s for s in streams if s.get('codec_type') == 'subtitle']
        
        duration = float(info.get('format', {}).get('duration', 0))
        if duration <= 0 and video_streams:
            duration = float(video_streams[0].get('duration', 0))
            
        return {
            'metadata': metadata,
            'duration': duration,
            'video_streams': video_streams,
            'audio_streams': audio_streams,
            'subtitle_streams': subtitle_streams,
            'original_info': info
        }

    def _build_ffmpeg_command(self, input_path: str, metadata: Dict[str, str], 
                            watermark_path: Optional[str], output_path: str, 
                            preset: str, crf: int, use_watermark: bool = True) -> List[str]:
        """Build FFmpeg command for 4K encoding with watermark and metadata"""
        cmd = [
            'ffmpeg', '-hide_banner', '-loglevel', 'error',
            '-i', input_path
        ]

        if watermark_path and ospath.exists(watermark_path) and use_watermark:
            cmd.extend(['-i', watermark_path])
            watermark_scale = "scale=iw*0.15:-1"  # 15% of input width
            filter_complex = (
                f"[0:v]scale=3840:2160:flags=bicubic+accurate_rnd+full_chroma_inp[scaled];"
                f"[1:v]{watermark_scale},format=rgba,colorchannelmixer=aa=0.5[wm];"
                f"[scaled][wm]overlay=10:10[outv]"
            )


            cmd.extend([
                '-filter_complex', filter_complex,
                '-map', '[outv]',
            ])
        else:
            cmd.extend([
                '-vf', 'scale=3840:2160:flags=bicubic+accurate_rnd+full_chroma_inp',
                '-map', '0:v?',
            ])

        # Add all audio and subtitle streams
        cmd.extend([
            '-map', '0:a?',
            '-map', '0:s?',
            '-c:v', 'libx264',
            '-preset', preset,
            '-tune', 'animation',
            '-pix_fmt', 'yuv420p',
            '-profile:v', 'high',
            '-crf', str(crf),
            '-x264-params', 'no-sao=1:aq-mode=3:deblock=-1,-1:psy-rd=1.5:ref=3:bframes=5',
            '-c:a', 'libopus',
            '-b:a', '192k',
            '-vbr', 'on',
            '-compression_level', '10',
            '-frame_duration', '60',
            '-application', 'audio',
            '-c:s', 'copy',
        ])

        # Add metadata for all streams
        title = metadata.get('title', 'Untitled')
        cmd.extend(['-metadata', f'title={title}'])

        # Add metadata for each video stream
        cmd.extend(['-metadata:s:v:0', f'title={title} (Video)'])

        # Add metadata for each audio stream
        audio_streams = subprocess.check_output([
            'ffprobe', '-v', 'error', '-select_streams', 'a', '-show_entries',
            'stream=index', '-of', 'csv=p=0', input_path
        ]).decode().strip().splitlines()

        for i, _ in enumerate(audio_streams):
            cmd.extend(['-metadata:s:a:' + str(i), f'title={title} (Audio {i + 1})'])

        # Add metadata for each subtitle stream
        subtitle_streams = subprocess.check_output([
            'ffprobe', '-v', 'error', '-select_streams', 's', '-show_entries',
            'stream=index', '-of', 'csv=p=0', input_path
        ]).decode().strip().splitlines()

        for i, _ in enumerate(subtitle_streams):
            cmd.extend(['-metadata:s:s:' + str(i), f'title={title} (Subtitles {i + 1})'])

        cmd.extend(['-y', output_path])
        return cmd

    async def encode_with_progress(
    self,
    input_path: str,
    output_path: str,
    metadata: Dict[str, str],
    watermark_path: Optional[str],
    preset: str,
    crf: int,
    progress_callback: Optional[Callable[[float, str], None]] = None,
    rename_pattern: Optional[str] = None,
    use_watermark: bool = True
) -> str:
        try:
            video_info = await self.get_video_info(input_path)
            if video_info['duration'] <= 0:
                raise ValueError("Invalid video duration")

            cmd = self._build_ffmpeg_command(
                input_path=input_path,
                metadata=metadata,
                watermark_path=watermark_path,
                output_path=output_path,
                preset=preset,
                crf=crf,
                use_watermark=use_watermark
            )

            logger.info(f"Running FFmpeg command: {' '.join(cmd)}")

            # Create temporary progress file
            progress_file = f"progress_{int(time.time())}.log"
            with open(progress_file, 'w') as f:
                f.write("")

            cmd.extend(['-progress', progress_file])

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=PIPE,
                stderr=PIPE
            )

            start_time = time.time()
            last_update = start_time
            
            async def monitor_progress():
                while True:
                    try:
                        if not ospath.exists(progress_file):
                            await asyncio.sleep(1)
                            continue
                            
                        with open(progress_file, 'r') as f:
                            lines = f.readlines()
                            if not lines:
                                await asyncio.sleep(0.5)
                                continue
                                
                            progress_data = {}
                            for line in lines:
                                if '=' in line:
                                    key, value = line.strip().split('=', 1)
                                    progress_data[key] = value
                                    
                            if 'out_time_ms' in progress_data:
                                current_time_ms = int(progress_data['out_time_ms'])
                                current_progress = min(99.9, (current_time_ms / 1_000_000 / video_info['duration']) * 100)
                                
                                # Handle speed value (may contain 'x' suffix)
                                speed_str = progress_data.get('speed', '1.0')
                                try:
                                    speed = float(speed_str.rstrip('x')) if 'x' in speed_str else float(speed_str)
                                except (ValueError, AttributeError):
                                    speed = 1.0
                                
                                fps = float(progress_data.get('fps', '0')) if 'fps' in progress_data else 0
                                bitrate = progress_data.get('bitrate', 'N/A')
                                
                                eta = (video_info['duration'] - (current_time_ms/1_000_000)) / speed if speed > 0 else 0
                                
                                if progress_callback:
                                    try:
                                        elapsed = time.time() - start_time
                                        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                                        eta_str = f"{int(eta // 60)}m {int(eta % 60)}s" if eta > 0 else "calculating..."
                                        
                                        progress_text = (
                                            f"⚙️ Encoding ({speed:.1f}x) | FPS: {fps:.1f}\n"
                                            f"📊 Bitrate: {bitrate} | ⏳ ETA: {eta_str}\n"
                                            f"🕒 Elapsed: {elapsed_str}"
                                        )
                                        
                                        await progress_callback(current_progress, progress_text)
                                        last_update = time.time()
                                    except Exception as e:
                                        logger.warning(f"Progress callback error: {str(e)}")
                                        
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.warning(f"Progress monitoring error: {str(e)}")
                        await asyncio.sleep(1)

            progress_task = asyncio.create_task(monitor_progress())

            await process.wait()
            progress_task.cancel()
            
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
                
            if ospath.exists(progress_file):
                osremove(progress_file)

            if process.returncode != 0:
                error_msg = (await process.stderr.read()).decode('utf-8')
                logger.error(f"FFmpeg failed with return code {process.returncode}\nError output:\n{error_msg}")
                raise RuntimeError(f"FFmpeg encoding failed: {error_msg}")

            if not ospath.exists(output_path) or ospath.getsize(output_path) == 0:
                raise RuntimeError("Encoding produced invalid output file")

            if progress_callback:
                elapsed = time.time() - start_time
                elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                await progress_callback(100.0, f"✅ Encoding complete! | Time: {elapsed_str}")

            return output_path
        except Exception as e:
            logger.error(f"Encoding failed: {str(e)}")
            if ospath.exists(output_path):
                try:
                    osremove(output_path)
                except Exception as e:
                    logger.warning(f"Failed to clean up output file: {str(e)}")
            raise

    def apply_rename(self, filename: str, new_name: Optional[str]) -> str:
        """Rename filename using user-provided name (preserves extension)."""
        if not new_name:
            return filename

        try:
            
            _, ext = ospath.splitext(filename)

            new_base = re.sub(r'[^\w\-_(). ]', '', new_name).strip()
            new_base = re.sub(r'\s+', ' ', new_base)  # Collapse multiple spaces

            return f"{new_base}{ext}"
        except Exception as e:
            logger.warning(f"Filename renaming failed: {e}")
            return filename

async def process_file(
    chat_id: int,
    file_path: str,
    metadata: Dict[str, str],
    thumbnail_path: Optional[str],
    watermark_path: Optional[str],
    preset: str,
    crf: int,
    rename_pattern: Optional[str] = None,
    use_watermark: bool = True
) -> Optional[str]:
    try:
        encoder = VideoEncoder()
        video_info = await encoder.get_video_info(file_path)
        
        original_name = ospath.splitext(ospath.basename(file_path))[0]
        if rename_pattern:
            original_name = encoder.apply_rename(original_name, rename_pattern)
        
        output_filename = f"{original_name}_4K.mkv"
        final_output = ospath.join("encoded", output_filename)
        makedirs(ospath.dirname(final_output), exist_ok=True)

        start_time = time.time()
        
        await update_progress(
            chat_id,
            f"⚙️ Starting encoding\nPreset: {preset}, CRF: {crf}\nFile: {ospath.basename(file_path)}",
            progress=0,
            stage='encode',
            start_time=start_time
        )

        encoded_path = await encoder.encode_with_progress(
            input_path=file_path,
            output_path=final_output,
            metadata=metadata,
            watermark_path=watermark_path,
            preset=preset,
            crf=crf,
            progress_callback=lambda p, t: update_progress(
                chat_id,
                f"⚙️ Encoding to 4K\nPreset: {preset}, CRF: {crf}\n{t}",
                progress=p,
                stage='encode',
                start_time=start_time
            ),
            rename_pattern=rename_pattern,
            use_watermark=use_watermark
        )

        await update_progress(
            chat_id,
            f"✅ Encoding complete\nFile: {ospath.basename(encoded_path)}",
            progress=100,
            stage='encode',
            start_time=start_time
        )

        return encoded_path
    except Exception as e:
        logger.error(f"Processing failed for {file_path}: {str(e)}", exc_info=True)
        await update_progress(
            chat_id,
            f"❌ Encoding failed: {str(e)}",
            stage='encode'
        )
        return None

async def download_file_with_progress(
    chat_id: int,
    message: Message,
    file_info: Dict
) -> Optional[str]:
    try:
        download_dir = ospath.join("downloads", f"dl_{chat_id}_{int(time.time())}")
        makedirs(download_dir, exist_ok=True)
        
        file_path = ospath.join(download_dir, file_info['name'])
        start_time = time.time()
        last_bytes = 0
        speed_history = []
        
        async def progress_callback(current, total):
            nonlocal last_bytes, speed_history
            
            progress = (current / total) * 100
            elapsed = time.time() - start_time
            
            current_speed = (current - last_bytes) / (time.time() - current_progress_messages[chat_id]['last_update'])
            speed_history.append(current_speed)
            
            if len(speed_history) > 5:
                speed_history.pop(0)
                
            avg_speed = sum(speed_history) / len(speed_history) if speed_history else current_speed
            
            speed_str = humanize.naturalsize(avg_speed) + "/s"
            
            status = (
                f"📥 Downloading: {file_info['name']}\n"
                f"🚀 Speed: {speed_str}\n"
                f"📦 Size: {humanize.naturalsize(current)}/{humanize.naturalsize(total)}"
            )
            
            await update_progress(
                chat_id,
                status,
                progress=progress,
                stage='download',
                start_time=start_time
            )
            
            last_bytes = current

        await update_progress(
            chat_id,
            f"📥 Starting download: {file_info['name']}",
            progress=0,
            stage='download',
            start_time=start_time
        )
        
        await message.download(file_path, progress=progress_callback)
        
        await update_progress(
            chat_id,
            f"✅ Download complete: {file_info['name']}",
            progress=100,
            stage='download',
            start_time=start_time
        )
        
        if not ospath.exists(file_path) or ospath.getsize(file_path) == 0:
            logger.error("Downloaded file is empty or doesn't exist")
            return None
            
        file_info['path'] = file_path
        
        await update_user_settings(chat_id, {
            "files": [file_info],
            "download_path": download_dir
        })
        
        return file_path
    except Exception as e:
        logger.error(f"Download failed: {str(e)}", exc_info=True)
        await update_progress(
            chat_id,
            f"❌ Download failed: {str(e)}",
            stage='download'
        )
        return None

async def upload_file_with_progress(
    chat_id: int,
    file_path: str,
    thumbnail_path: Optional[str],
    metadata: Dict[str, str],
    start_time: float
) -> bool:
    try:
        file_size = ospath.getsize(file_path)
        file_name = ospath.basename(file_path)
        
        last_bytes = 0
        last_update = start_time
        speed_history = []
        
        await update_progress(
            chat_id,
            f"📤 Preparing to upload: {file_name}\n"
            f"📦 Size: {humanize.naturalsize(file_size)}",
            progress=0,
            stage='upload',
            start_time=start_time
        )
        
        async def progress_callback(current, total):
            nonlocal last_bytes, last_update, speed_history
            
            now = time.time()
            if now - last_update < 2.0:  # Throttle updates
                return
                
            progress = (current / total) * 100
            elapsed = now - start_time
            
            current_speed = (current - last_bytes) / (now - last_update)
            speed_history.append(current_speed)
            
            if len(speed_history) > 5:
                speed_history.pop(0)
                
            avg_speed = sum(speed_history) / len(speed_history) if speed_history else current_speed
            
            remaining_bytes = total - current
            eta = remaining_bytes / avg_speed if avg_speed > 0 else 0
            
            speed_str = humanize.naturalsize(avg_speed) + "/s"
            elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
            eta_str = humanize.naturaldelta(eta) if eta > 0 else "soon"
            
            await update_progress(
                chat_id,
                f"📤 Uploading: {file_name}\n"
                f"🚀 Speed: {speed_str}\n"
                f"⏳ ETA: {eta_str}\n"
                f"🕒 Elapsed: {elapsed_str}",
                progress=progress,
                stage='upload'
            )
            
            last_bytes = current
            last_update = now

        await bot.send_document(
            chat_id=chat_id,
            document=file_path,
            thumb=thumbnail_path,
            progress=progress_callback
        )
            
        return True
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        return False

async def start_processing(chat_id: int):
    if processing_status.get(chat_id, False):
        return
        
    processing_status[chat_id] = True
    total_start_time = time.time()
    
    try:
        async with semaphore:
            session = await get_user_settings(chat_id)
            if not session:
                await update_progress(chat_id, "❌ Session expired", force_new=True)
                return
            
            files = session.get("files", [])
            
            if not files:
                await update_progress(chat_id, "❌ No valid files to process", force_new=True)
                return

            file_info = files[0]
            file_path = file_info['path']
            
            encoded_path = await process_file(
                chat_id=chat_id,
                file_path=file_path,
                metadata=session.get("metadata", {}),
                thumbnail_path=session.get("thumbnail"),
                watermark_path=session.get("watermark"),
                preset=session.get("preset", "medium"),
                crf=session.get("crf", 19),
                rename_pattern=session.get("rename"),
                use_watermark=session.get("use_watermark", True)
            )
            
            if not encoded_path:
                return
                
            upload_start_time = time.time()
            success = await upload_file_with_progress(
                chat_id=chat_id,
                file_path=encoded_path,
                thumbnail_path=session.get("thumbnail"),
                metadata=session.get("metadata", {}),
                start_time=upload_start_time
            )
            
            if success:
                total_time = time.time() - total_start_time
                hours, remainder = divmod(total_time, 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                
                await update_progress(
                    chat_id,
                    f"🎉 Processing Complete!\n\n"
                    f"⏱️ Total time taken: {time_str}\n"
                    f"📊 Final file size: {humanize.naturalsize(ospath.getsize(encoded_path))}\n\n"
                    f"Thank you for using our service!",
                    force_new=True
                )
                await asyncio.sleep(5)  
            
            
            try:
                osremove(encoded_path)
                if session.get("download_path"):
                    shutil.rmtree(session["download_path"], ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to clean up files: {str(e)}")
                
    except Exception as e:
        logger.error(f"Start processing failed: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Failed to process: {str(e)}", force_new=True)
    finally:
        processing_status[chat_id] = False

@handle_floodwait
async def update_progress(
    chat_id: int,
    text: str,
    progress: Optional[float] = None,
    stage: str = None,
    start_time: Optional[float] = None,
    force_new: bool = False
) -> None:
    async with progress_lock:
        try:
            if (chat_id not in current_progress_messages or 
                current_progress_messages[chat_id].get('stage') != stage or
                force_new):
                
                if chat_id in current_progress_messages:
                    try:
                        await bot.delete_messages(
                            chat_id, 
                            current_progress_messages[chat_id]['message_id']
                        )
                    except Exception:
                        pass
                
                msg = await bot.send_message(
                    chat_id, 
                    text,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{chat_id}")
                    ]])
                )
                current_progress_messages[chat_id] = {
                    'message_id': msg.id,
                    'stage': stage,
                    'last_update': time.time(),
                    'start_time': start_time or time.time()
                }
                return
            
            message_info = current_progress_messages[chat_id]
            
            if (time.time() - message_info['last_update'] < 2.0 and 
                progress not in [0, 100]):
                return
                
            if progress is not None:
                progress_bar_length = 10
                filled_length = int(progress / 100 * progress_bar_length)
                progress_bar = "[" + "⬢" * filled_length + "⬡" * (progress_bar_length - filled_length) + "]"
                
                elapsed = time.time() - message_info['start_time']
                elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                
                if 0 < progress < 100:
                    eta = (elapsed / progress) * (100 - progress) if progress > 0 else 0
                    eta_str = humanize.naturaldelta(eta) if eta > 0 else "calculating..."
                    text = (
                        f"{text}\n\n"
                        f"{progress_bar} {progress:.1f}%\n"
                        f"⏱️ Elapsed: {elapsed_str}\n"
                        f"⏳ ETA: {eta_str}"
                    )
                else:
                    text = (
                        f"{text}\n\n"
                        f"{progress_bar} {progress:.1f}%\n"
                        f"⏱️ Time taken: {elapsed_str}"
                    )

            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_info['message_id'],
                    text=text,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{chat_id}")
                    ]])
                )
                current_progress_messages[chat_id]['last_update'] = time.time()
            except (MessageNotModified, BadRequest):
                pass
            except Exception as e:
                logger.warning(f"Progress update error: {str(e)}")

        except Exception as e:
            logger.error(f"Progress update failed: {str(e)}")

async def collect_settings(chat_id: int):
    try:
        session = await get_user_settings(chat_id)
        if not session:
            await bot.send_message(chat_id, "⚠️ Session expired. Please start again with /encode.")
            return

        has_thumb = "✅" if session.get("thumbnail") and ospath.exists(session["thumbnail"]) else "❌"
        has_wm = "✅" if session.get("watermark") and ospath.exists(session["watermark"]) else "❌"
        has_title = "✅" if session.get("metadata", {}).get("title") else "❌"
        has_rename = "✅" if session.get("rename") else "❌"
        current_preset = session.get("preset", "medium")
        current_crf = session.get("crf", 19)
        use_wm = "✅" if session.get("use_watermark", True) else "❌"

        buttons = [
            [
                InlineKeyboardButton(f"{has_title} Title", callback_data=f"set_title_{chat_id}"),
                InlineKeyboardButton(f"{has_rename} Rename", callback_data=f"set_rename_{chat_id}")
            ],
            [
                InlineKeyboardButton(f"{has_thumb} Thumbnail", callback_data=f"set_thumb_{chat_id}"),
                InlineKeyboardButton(f"{has_wm} Watermark", callback_data=f"set_wm_{chat_id}")
            ],
            [
                InlineKeyboardButton(f"⚙️ Preset: {current_preset}", callback_data=f"set_preset_{chat_id}"),
                InlineKeyboardButton(f"🎚️ CRF: {current_crf}", callback_data=f"set_crf_{chat_id}")
            ],
            [
                InlineKeyboardButton(f"{use_wm} Use Watermark", callback_data=f"toggle_wm_{chat_id}")
            ],
            [
                InlineKeyboardButton("🚀 Start Processing", callback_data=f"confirm_processing_{chat_id}")
            ]
        ]

        msg = await bot.send_message(
            chat_id=chat_id,
            text=(
                "⚙️ Encoding Settings\n\n"
                "Configure your options before processing:\n"
                f"• Title: {'Set' if has_title == '✅' else 'Not set'}\n"
                f"• Rename: {'Set' if has_rename == '✅' else 'Not set'}\n"
                f"• Thumbnail: {'Uploaded' if has_thumb == '✅' else 'Not uploaded'}\n"
                f"• Watermark: {'Uploaded' if has_wm == '✅' else 'Not uploaded'}\n"
                f"• Use Watermark: {'Enabled' if use_wm == '✅' else 'Disabled'}\n"
                f"• Encoding Preset: {current_preset}\n"
                f"• CRF Quality: {current_crf}\n\n"
                "_You can modify any setting below before continuing._"
            ),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
            
        user_active_messages[chat_id] = msg.id

    except Exception as e:
        logger.error(f"Failed to send settings menu to {chat_id}: {e}", exc_info=True)
        await bot.send_message(chat_id, "❌ Failed to load settings menu. Please try again.")

@bot.on_message(filters.command("start") & filters.private)
@handle_floodwait
async def start_handler(client: Client, message: Message):
    try:
        await message.reply(
            "🎬 Video Encoder Bot\n\n"
            "Reply to a video file with /encode to start processing\n\n"
            "This bot will encode your videos to 4K quality with customizable settings.\n\n"
            "⚙️ Features:\n"
            "- Multiple encoding presets\n"
            "- Custom CRF quality settings\n"
            "- Watermark support (with toggle)\n"
            "- Thumbnail support\n"
            "- Title and filename customization\n\n"
            "Use /encode to get started!"
        )
    except Exception as e:
        logger.error(f"Start error: {e}")

@bot.on_message(filters.command("encode") & filters.private & filters.reply)
@handle_floodwait
async def encode_handler(client: Client, message: Message):
    try:
        chat_id = message.chat.id
        
        if not message.reply_to_message.media:
            await message.reply("❌ Please reply to a video file with /encode")
            return

        file_info = None
        if message.reply_to_message.video:
            file_info = {
                'name': message.reply_to_message.video.file_name or f"video_{message.reply_to_message.video.file_unique_id}.mp4",
                'size': message.reply_to_message.video.file_size,
                'is_video': True,
                'message_id': message.reply_to_message.id 
            }
        elif message.reply_to_message.document:
            file_info = {
                'name': message.reply_to_message.document.file_name or f"file_{message.reply_to_message.document.file_unique_id}",
                'size': message.reply_to_message.document.file_size,
                'is_video': message.reply_to_message.document.mime_type.startswith('video/'),
                'message_id': message.reply_to_message.id 
            }
        
        if not file_info:
            await message.reply("❌ Unsupported file type. Please reply to a video file.")
            return

        if file_info['size'] > MAX_FILE_SIZE:
            await message.reply(
                f"❌ File size exceeds limit ({humanize.naturalsize(MAX_FILE_SIZE)}). "
                f"Your file: {humanize.naturalsize(file_info['size'])}"
            )
            return

        await update_user_settings(chat_id, {
            "files": [file_info],
            "status": "awaiting_settings",
            "created_at": time.time(),
            "metadata": {"title": ""},
            "quality": "4K",
            "awaiting": None,
            "preset": "medium",
            "crf": 19,
            "use_watermark": True,
            "original_message_id": message.reply_to_message.id  
        })

        await collect_settings(chat_id)

    except Exception as e:
        logger.error(f"Error in encode handler: {str(e)}", exc_info=True)
        error_msg = (
            "❌ Failed to process file\n\n"
            f"Error: {str(e)}\n\n"
            "Please try again or contact support if this persists."
        )
        await message.reply(error_msg)

@bot.on_message(filters.private & (filters.text | filters.photo | filters.document))
async def handle_user_input(client: Client, message: Message):
    chat_id = message.chat.id

    session = await get_user_settings(chat_id)
    if not session or "awaiting" not in session:
        return

    awaiting = session["awaiting"]

    if awaiting == "title" and message.text:
        clean_title = re.sub(r'[^\w\-_\. ]', '', message.text)
        await update_user_settings(chat_id, {
            "metadata.title": clean_title,
            "awaiting": None
        })
        await message.reply(f"✅ Title set: {clean_title}")
        return await collect_settings(chat_id)

    elif awaiting == "thumb" and message.photo:
        thumb_path = await handle_thumbnail(chat_id, message)
        if thumb_path:
            await update_user_settings(chat_id, {
                "thumbnail": thumb_path,
                "awaiting": None
            })
            await message.reply("✅ Thumbnail saved!")
            return await collect_settings(chat_id)

    elif awaiting == "wm" and (message.photo or (message.document and message.document.mime_type == "image/png")):
        wm_path = await handle_watermark(chat_id, message)
        if wm_path:
            await update_user_settings(chat_id, {
                "watermark": wm_path,
                "awaiting": None
            })
            await message.reply("✅ Watermark saved!")
            return await collect_settings(chat_id)

    elif awaiting == "rename" and message.text:
        pattern = message.text.strip()
        await update_user_settings(chat_id, {
            "rename": pattern,
            "awaiting": None
        })
        await message.reply(f"✅ Rename pattern set:\n`{pattern}`", quote=True)
        return await collect_settings(chat_id)
        
    elif awaiting == "crf" and message.text:
        try:
            crf = int(message.text.strip())
            if 14 <= crf <= 30:
                await update_user_settings(chat_id, {
                    "crf": crf,
                    "awaiting": None
                })
                await message.reply(f"✅ CRF set to {crf}")
                return await collect_settings(chat_id)
            else:
                await message.reply("❌ CRF must be between 14 and 30")
        except ValueError:
            await message.reply("❌ Please enter a valid number between 14 and 30")

@bot.on_callback_query(filters.regex(r"^set_(title|thumb|wm|rename|preset|crf)_(\d+)$"))
async def set_item_handler(client: Client, query: CallbackQuery):
    chat_id = int(query.data.split("_")[2])
    item_type = query.data.split("_")[1]
    
    session = await get_user_settings(chat_id)
    if not session:
        return await query.answer("Session expired!", show_alert=True)
    
    if item_type == "preset":
        buttons = []
        for preset, data in ENCODING_PRESETS.items():
            buttons.append([
                InlineKeyboardButton(
                    f"{'✅' if session.get('preset') == preset else '⬜'} {preset.capitalize()}",
                    callback_data=f"select_preset_{chat_id}_{preset}"
                )
            ])
        
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"back_to_settings_{chat_id}")])
        
        await query.message.edit_text(
            "⚙️ Select Encoding Preset\n\n"
            "Presets determine the speed/quality tradeoff:\n"
            "• Faster presets = quicker encoding, larger files\n"
            "• Slower presets = better compression, smaller files\n\n"
            "Current selection: " + session.get("preset", "medium"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        await query.answer()
        return
        
    elif item_type == "crf":
        await update_user_settings(chat_id, {"awaiting": "crf"})
        await query.message.edit_text(
            "🎚️ Set CRF Quality (14-30)\n\n"
            "Lower values = better quality, larger files\n"
            "Higher values = smaller files, lower quality\n\n"
            "Recommended: 18-22 for high quality\n"
            "Current value: " + str(session.get("crf", 19)) + "\n\n"
            "Please send a number between 18 and 30:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data=f"back_to_settings_{chat_id}")
            ]])
        )
        await query.answer()
        return
    
    await update_user_settings(chat_id, {"awaiting": item_type})
    
    if item_type == "title":
        text = "📝 Please send the new title for your video:"
    elif item_type == "thumb":
        text = "🖼️ Please send an image to use as thumbnail (JPEG recommended):"
    elif item_type == "wm":
        text = "💧 Please send a PNG image to use as watermark (transparent background recommended):"
    elif item_type == "rename":
        text = (
            "🔄 Please send the rename pattern for filename modification\n\n"
            "Format: old_text:new_text|another_old:another_new\n"
            "Example: episode : EP|season : S (replaces 'episode' with 'EP' and 'season' with 'S')"
        )
    
    buttons = [
        [InlineKeyboardButton("❌ Skip", callback_data=f"skip_{item_type}_{chat_id}")]
    ]
    
    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await query.answer()

@bot.on_callback_query(filters.regex(r"^select_preset_(\d+)_(\w+)$"))
async def select_preset_handler(client: Client, query: CallbackQuery):
    try:
        chat_id = int(query.data.split("_")[2])
        preset = query.data.split("_")[3]
        
        await update_user_settings(chat_id, {
            "preset": preset,
            "awaiting": None
        })
        
        await query.answer(f"Preset set to {preset}")
        await collect_settings(chat_id)
        
    except Exception as e:
        logger.error(f"Error in select_preset_handler: {str(e)}")
        await query.answer("Error occurred, please try again", show_alert=True)

@bot.on_callback_query(filters.regex(r"^back_to_settings_(\d+)$"))
async def back_to_settings_handler(client: Client, query: CallbackQuery):
    try:
        chat_id = int(query.data.split("_")[-1])
        await query.answer()
        await collect_settings(chat_id)
    except Exception as e:
        logger.error(f"Error in back_to_settings_handler: {str(e)}")
        await query.answer("Error occurred, please try again", show_alert=True)

@bot.on_callback_query(filters.regex(r"^skip_(thumb|wm|title|rename)_(\d+)$"))
async def skip_handler(client: Client, query: CallbackQuery):
    chat_id = int(query.data.split("_")[2])
    item_type = query.data.split("_")[1]
    
    session = await get_user_settings(chat_id)
    if not session:
        return await query.answer("Session expired!", show_alert=True)
    
    await update_user_settings(chat_id, {"awaiting": None})
    await query.answer(f"Skipped {item_type} setting")
    await collect_settings(chat_id)

@bot.on_callback_query(filters.regex(r"^toggle_wm_(\d+)$"))
async def toggle_watermark_handler(client: Client, query: CallbackQuery):
    chat_id = int(query.data.split("_")[2])
    session = await get_user_settings(chat_id)
    
    current = session.get("use_watermark", True)
    await update_user_settings(chat_id, {"use_watermark": not current})
    
    await query.answer(f"Watermark {'enabled' if not current else 'disabled'}")
    await collect_settings(chat_id)

@bot.on_callback_query(filters.regex(r"^confirm_processing_(\d+)$"))
@handle_floodwait
async def confirm_processing_handler(client: Client, query: CallbackQuery):
    try:
        chat_id = int(query.data.split("_")[-1])
        
        if processing_status.get(chat_id, False):
            await query.answer("Processing is already in progress!", show_alert=True)
            return
        
        session = await get_user_settings(chat_id)
        if not session or "files" not in session:
            await query.answer("Session expired! Please start again with /encode", show_alert=True)
            return
        
        original_message = None
        if "original_message_id" in session:
            try:
                original_message = await client.get_messages(chat_id, session["original_message_id"])
            except Exception as e:
                logger.warning(f"Couldn't get original message by ID: {str(e)}")
        
        if not original_message and query.message.reply_to_message:
            original_message = query.message.reply_to_message
        
        if not original_message:
            async for message in client.search_messages(chat_id, limit=10):
                if message.video or (message.document and message.document.mime_type.startswith('video/')):
                    original_message = message
                    break
        
        if not original_message or not original_message.media:
            await query.answer("Original file not found! Please send the file again", show_alert=True)
            await update_progress(
                chat_id,
                "❌ Original file not found. Please send the file again with /encode",
                force_new=True
            )
            return
        
        await query.answer("Starting download...")
        await bot.delete_messages(chat_id, query.message.id)
        
        try:
            await update_progress(
                chat_id,
                "📥 Starting download...",
                force_new=True
            )
            
            file_info = session["files"][0]
            file_path = await download_file_with_progress(
                chat_id=chat_id,
                message=original_message,
                file_info=file_info
            )
            
            if not file_path:
                await update_progress(
                    chat_id,
                    "❌ Failed to download file. Please try again",
                    force_new=True
                )
                return
                
            await processing_queue.put(chat_id)
            await update_progress(
                chat_id,
                "⏳ Your file has been added to the processing queue\n\n"
                f"Position in queue: {processing_queue.qsize()}\n"
                "You will be notified when processing starts.",
                force_new=True
            )
        except asyncio.QueueFull:
            await update_progress(
                chat_id,
                "❌ Queue is full. Please try again later.",
                force_new=True
            )
        except Exception as e:
            logger.error(f"Error in confirm processing: {str(e)}", exc_info=True)
            await update_progress(
                chat_id,
                f"❌ Error starting processing: {str(e)}",
                force_new=True
            )
    except Exception as e:
        logger.error(f"Error in confirm_processing_handler: {str(e)}", exc_info=True)
        await query.answer("An error occurred, please try again", show_alert=True)

@bot.on_callback_query(filters.regex(r"^cancel_(\d+)$"))
async def cancel_processing_handler(client: Client, query: CallbackQuery):
    chat_id = int(query.data.split("_")[1])
    
    try:
        if chat_id in list(processing_queue._queue):
            processing_queue._queue.remove(chat_id)
        
        await update_user_settings(chat_id, {"status": "cancelled"})
        
        session = await get_user_settings(chat_id)
        if session:
            if "download_path" in session:
                shutil.rmtree(session["download_path"], ignore_errors=True)
            if "encoded_path" in session:
                try:
                    osremove(session["encoded_path"])
                except Exception:
                    pass
        
        await query.answer("Processing cancelled")
        await update_progress(
            chat_id,
            "⏹️ Processing cancelled by user",
            stage='cancelled'
        )
    except Exception as e:
        logger.error(f"Error cancelling processing: {str(e)}")
        await query.answer("Failed to cancel processing", show_alert=True)

async def task_queue_manager():
    """Simplified queue manager that processes tasks one at a time"""
    while True:
        try:
            chat_id = await processing_queue.get()
            
            try:
                await start_processing(chat_id)
            except Exception as e:
                logger.error(f"Error processing queue item {chat_id}: {str(e)}", exc_info=True)
                await update_progress(
                    chat_id,
                    f"❌ Processing failed: {str(e)}",
                    force_new=True
                )
            finally:
                processing_queue.task_done()
                
        except Exception as e:
            logger.error(f"Queue manager error: {str(e)}")
            await asyncio.sleep(5)

@bot.on_message(filters.command("status") & filters.private)
@handle_floodwait
async def status_handler(client: Client, message: Message):
    chat_id = message.chat.id
    session = await get_user_settings(chat_id)
    if session:
        status = session.get("status", "unknown")
        
        status_msg = (
            f"🔄 Current Status: {status.capitalize()}\n"
            f"📊 Queue position: {processing_queue.qsize()}\n"
            f"⏱️ Session started: {humanize.naturaltime(datetime.now()) - datetime.fromtimestamp(session.get('created_at', time.time()))}"
        )
        
        await message.reply(status_msg)
    else:
        await message.reply("ℹ️ No active session. Reply to a file with /encode to start.")

@bot.on_message(filters.command("cancel") & filters.private)
@handle_floodwait
async def cancel_handler(client: Client, message: Message):
    chat_id = message.chat.id
    
    try:
        if chat_id in list(processing_queue._queue):
            processing_queue._queue.remove(chat_id)
    except ValueError:
        pass
    
    session = await get_user_settings(chat_id)
    if session:
        if "download_path" in session:
            shutil.rmtree(session["download_path"], ignore_errors=True)
        await update_user_settings(chat_id, {"status": "cancelled"})
    
    await message.reply("⏹️ Operation cancelled")

async def memory_monitor():
    """Monitor memory usage and log it periodically"""
    while True:
        try:
            mem = psutil.virtual_memory()
            logger.info(
                f"Memory usage: {humanize.naturalsize(mem.used)} / {humanize.naturalsize(mem.total)} "
                f"({mem.percent}%) | Tasks in queue: {processing_queue.qsize()}"
            )
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Memory monitor error: {str(e)}")
            await asyncio.sleep(60)

async def cleanup_temp_files():
    """Periodically clean up temporary files"""
    while True:
        await asyncio.sleep(3600)
        try:
            now = time.time()
            active_files = []
            
            async for session in user_settings.find({}):
                if session.get("thumbnail"):
                    active_files.append(session["thumbnail"])
                if session.get("watermark"):
                    active_files.append(session["watermark"])
                if session.get("download_path"):
                    active_files.append(session["download_path"])
            
            for folder in ["thumbnails", "watermarks", "downloads", "encoded"]:
                if not ospath.exists(folder):
                    continue
                    
                for item in os.listdir(folder):
                    item_path = ospath.join(folder, item)
                    try:
                        if item_path in active_files:
                            continue
                            
                        if ospath.isfile(item_path) and now - ospath.getmtime(item_path) > 86400:
                            osremove(item_path)
                        elif ospath.isdir(item_path) and now - ospath.getmtime(item_path) > 86400:
                            shutil.rmtree(item_path, ignore_errors=True)
                    except Exception as e:
                        logger.warning(f"Cleanup failed for {item_path}: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")

async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown"""
    logger.info(f"Received exit signal {signal.name}...")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    
    if bot.is_connected:
        await bot.stop()
    
    loop.stop()

async def main():
    """Simplified main function"""
    try:
        try:
            await db.command('ping')
            logger.info("Connected to MongoDB successfully")
        except ConnectionFailure:
            logger.error("Failed to connect to MongoDB")
            return

        await bot.start()
        logger.info("Bot started successfully")
        
        queue_task = asyncio.create_task(task_queue_manager())
        mem_task = asyncio.create_task(memory_monitor())
        cleanup_task = asyncio.create_task(cleanup_temp_files())
        
        await idle()
        
    except Exception as e:
        logger.critical(f"Bot crashed: {str(e)}", exc_info=True)
    finally:
        queue_task.cancel()
        mem_task.cancel()
        cleanup_task.cancel()
        
        await asyncio.gather(queue_task, mem_task, cleanup_task, return_exceptions=True)
        
        if await bot.is_connected():
            await bot.stop()
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
