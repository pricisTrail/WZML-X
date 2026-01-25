#!/usr/bin/env python3
from asyncio import create_subprocess_exec, create_subprocess_shell
from asyncio.subprocess import PIPE
from os import path as ospath
from secrets import token_hex
from logging import getLogger
from re import search as re_search
from time import time

from bot import download_dict_lock, download_dict, non_queued_dl, queue_dict_lock, DOWNLOAD_DIR
from bot.helper.telegram_helper.message_utils import sendStatusMessage
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.ext_utils.bot_utils import sync_to_async, async_to_sync
from bot.helper.ext_utils.task_manager import is_queued, stop_duplicate_check, limit_checker

LOGGER = getLogger(__name__)


class FFmpegDownloadStatus:
    def __init__(self, obj, listener, gid):
        self.__obj = obj
        self.__listener = listener
        self.__gid = gid

    def gid(self):
        return self.__gid

    def progress_raw(self):
        return self.__obj.progress

    def progress(self):
        return f'{round(self.__obj.progress, 2)}%'

    def speed(self):
        return self.__obj.speed_string

    def name(self):
        return self.__obj.name

    def size(self):
        return self.__obj.size_string

    def eta(self):
        return self.__obj.eta_string

    def status(self):
        return "Downloading"

    def processed_bytes(self):
        return self.__obj.downloaded_string

    def listener(self):
        return self.__listener

    def download(self):
        return self.__obj


class FFmpegDownloader:
    def __init__(self, listener):
        self.__listener = listener
        self.__gid = ''
        self.__is_cancelled = False
        self.__downloading = False
        self.__process = None
        self.name = ''
        self.progress = 0
        self.downloaded_bytes = 0
        self.size = 0
        self.speed = 0
        self.eta = 0
        self._last_update = time()

    @property
    def speed_string(self):
        if self.speed > 0:
            return f"{self.speed:.2f} KB/s"
        return "N/A"

    @property
    def size_string(self):
        if self.size > 0:
            return self._format_size(self.size)
        return "N/A"

    @property
    def downloaded_string(self):
        return self._format_size(self.downloaded_bytes)

    @property
    def eta_string(self):
        if self.eta > 0:
            return self._format_time(self.eta)
        return "N/A"

    @staticmethod
    def _format_size(size_bytes):
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    @staticmethod
    def _format_time(seconds):
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        else:
            return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"

    def _parse_ffmpeg_progress(self, line):
        """Parse ffmpeg output for progress info"""
        # ffmpeg outputs time in format: time=00:01:23.45
        if 'time=' in line:
            try:
                time_match = re_search(r'time=(\d+):(\d+):(\d+\.?\d*)', line)
                if time_match:
                    hours = int(time_match.group(1))
                    minutes = int(time_match.group(2))
                    seconds = float(time_match.group(3))
                    current_time = hours * 3600 + minutes * 60 + seconds
                    
                    # Estimate progress based on processed time
                    # For HLS streams, we estimate based on fragment count if available
                    if hasattr(self, '_duration') and self._duration > 0:
                        self.progress = min((current_time / self._duration) * 100, 99)
                    
                    # Parse speed if available
                    speed_match = re_search(r'speed=\s*(\d+\.?\d*)x', line)
                    if speed_match:
                        self.speed = float(speed_match.group(1)) * 100  # Approximate KB/s

                    # Parse size
                    size_match = re_search(r'size=\s*(\d+)(\w+)', line)
                    if size_match:
                        size_val = int(size_match.group(1))
                        size_unit = size_match.group(2).lower()
                        if 'kb' in size_unit or 'kib' in size_unit:
                            self.downloaded_bytes = size_val * 1024
                        elif 'mb' in size_unit or 'mib' in size_unit:
                            self.downloaded_bytes = size_val * 1024 * 1024
                        elif 'gb' in size_unit or 'gib' in size_unit:
                            self.downloaded_bytes = size_val * 1024 * 1024 * 1024
                        else:
                            self.downloaded_bytes = size_val
            except Exception as e:
                LOGGER.debug(f"Error parsing ffmpeg progress: {e}")

    async def __onDownloadStart(self, from_queue=False):
        async with download_dict_lock:
            download_dict[self.__listener.uid] = FFmpegDownloadStatus(
                self, self.__listener, self.__gid)
        if not from_queue:
            await self.__listener.onDownloadStart()
            await sendStatusMessage(self.__listener.message)

    async def __onDownloadComplete(self):
        await self.__listener.onDownloadComplete()

    async def __onDownloadError(self, error):
        self.__is_cancelled = True
        await self.__listener.onDownloadError(error)

    async def add_download(self, link, path, name):
        """Download HLS stream using ffmpeg"""
        self.__gid = token_hex(5)
        
        # Generate output filename
        if name:
            if not name.endswith('.mp4'):
                name = f"{name}.mp4"
            self.name = name
        else:
            self.name = f"hls_video_{self.__gid}.mp4"
        
        output_path = ospath.join(path, self.name)
        
        # Check for duplicates
        msg, button = await stop_duplicate_check(self.name, self.__listener)
        if msg:
            await self.__listener.onDownloadError(msg, button)
            return

        # Check queue
        added_to_queue, event = await is_queued(self.__listener.uid)
        if added_to_queue:
            LOGGER.info(f"Added to Queue/Download: {self.name}")
            async with download_dict_lock:
                download_dict[self.__listener.uid] = QueueStatus(
                    self.name, self.size, self.__gid, self.__listener, 'dl')
            await event.wait()
            async with download_dict_lock:
                if self.__listener.uid not in download_dict:
                    return
            LOGGER.info(f'Start Queued Download with FFmpeg: {self.name}')
            await self.__onDownloadStart(True)
        else:
            await self.__onDownloadStart()
            LOGGER.info(f'Download with FFmpeg: {self.name}')

        async with queue_dict_lock:
            non_queued_dl.add(self.__listener.uid)

        try:
            # Create directory if needed
            from aiofiles.os import makedirs
            await makedirs(path, exist_ok=True)
            
            # FFmpeg command for HLS download
            # -y: overwrite output
            # -i: input URL
            # -c copy: copy streams without re-encoding
            # -bsf:a aac_adtstoasc: fix AAC audio for MP4 container
            cmd = [
                'ffmpeg', '-y',
                '-headers', 'Accept-Encoding: identity',
                '-i', link,
                '-c', 'copy',
                '-bsf:a', 'aac_adtstoasc',
                output_path
            ]
            
            LOGGER.info(f"FFmpeg command: {' '.join(cmd)}")
            
            self.__downloading = True
            self.__process = await create_subprocess_exec(
                *cmd,
                stdout=PIPE,
                stderr=PIPE
            )
            
            # Read stderr for progress (ffmpeg outputs progress to stderr)
            while True:
                if self.__is_cancelled:
                    self.__process.kill()
                    await self.__onDownloadError("Download cancelled by user!")
                    return
                
                line = await self.__process.stderr.readline()
                if not line:
                    break
                
                line_str = line.decode('utf-8', errors='ignore').strip()
                if line_str:
                    self._parse_ffmpeg_progress(line_str)
                    LOGGER.debug(f"FFmpeg: {line_str}")
            
            await self.__process.wait()
            
            if self.__is_cancelled:
                return
            
            if self.__process.returncode == 0:
                # Check if file exists and has size
                if ospath.exists(output_path) and ospath.getsize(output_path) > 0:
                    self.progress = 100
                    self.size = ospath.getsize(output_path)
                    self.downloaded_bytes = self.size
                    await self.__onDownloadComplete()
                else:
                    await self.__onDownloadError("Download failed: Output file is empty or missing")
            else:
                # Get error output
                stderr_output = await self.__process.stderr.read()
                error_msg = stderr_output.decode('utf-8', errors='ignore') if stderr_output else "Unknown error"
                # Truncate long error messages
                if len(error_msg) > 500:
                    error_msg = error_msg[:500] + "..."
                await self.__onDownloadError(f"FFmpeg error (code {self.__process.returncode}): {error_msg}")
                
        except Exception as e:
            LOGGER.error(f"FFmpeg download error: {e}")
            await self.__onDownloadError(str(e))

    async def cancel_download(self):
        self.__is_cancelled = True
        LOGGER.info(f"Cancelling FFmpeg Download: {self.name}")
        if self.__process:
            try:
                self.__process.kill()
            except:
                pass
        if not self.__downloading:
            await self.__listener.onDownloadError("Download Cancelled by User!")


def is_hls_url(url):
    """Check if URL is an HLS/m3u8 stream"""
    url_lower = url.lower()
    return '.m3u8' in url_lower or '/hls/' in url_lower or 'playlist' in url_lower and ('.m3u8' in url_lower or '/hls' in url_lower)
