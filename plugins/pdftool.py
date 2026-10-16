# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ==================== REXY V14 PRO — PDF TOOLS ====================

from __future__ import annotations
import os
import io
import re
import uuid
import logging
import asyncio
import time
import html
import zipfile
import tempfile
import shutil
import img2pdf
import hashlib
from dataclasses import dataclass, field
from functools import wraps
from typing import Optional, Callable, Awaitable, Union, Any, Dict
from concurrent.futures import ThreadPoolExecutor

from pyrogram import Client, filters, StopPropagation
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified, MessageIdInvalid, ChatWriteForbidden, UserIsBlocked, PeerIdInvalid
from PIL import Image
from cachetools import TTLCache
import fitz

from helper.database import DARKXSIDE78
from plugins.admin import safe_edit, safe_get_users, close_btn

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗖𝗢𝗥𝗔𝗧𝗢𝗥𝗦 & 𝗛𝗘𝗟𝗣𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def retry_on_flood(func):
    """Decorator to automatically handle FloodWait for PDF API calls."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        while True:
            try:
                return await func(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"[pdf] FloodWait in {func.__name__}: sleeping {e.value}s")
                await asyncio.sleep(e.value)
            except Exception:
                raise
    return wrapper

@retry_on_flood
async def safe_edit_id(client: Client, chat_id: int, msg_id: int, text: str, **kwargs):
    """Safely edit a message by ID, automatically switching between text and caption."""
    try:
        return await client.edit_message_text(chat_id, msg_id, text, **kwargs)
    except Exception:
        try:
            return await client.edit_message_caption(chat_id, msg_id, caption=text, **kwargs)
        except Exception as e:
            logger.debug(f"safe_edit_id failed: {e}")
            return False

@retry_on_flood
async def safe_send_photo(client: Client, chat_id: int, photo: str, caption: str, markup=None):
    return await client.send_photo(chat_id, photo, caption=caption, reply_markup=markup, parse_mode=ParseMode.HTML)

@retry_on_flood
async def safe_edit_message(client: Client, chat_id: int, message_id: int, text: str, markup=None):
    """Smart edit helper that handles both text messages and media captions."""
    try:
        # Try as text message first
        return await client.edit_message_text(chat_id, message_id, text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        # If it fails (likely because it's a media message), try as caption
        try:
            return await client.edit_message_caption(chat_id, message_id, caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
        except MessageNotModified:
            pass
        except Exception as e2:
            logger.error(f"[pdf] safe_edit_message fault: {e2} (original: {e})")

@retry_on_flood
async def safe_send_document(client: Client, chat_id: int, document: str, caption: str, markup=None):
    return await client.send_document(chat_id, document, caption=caption, reply_markup=markup, parse_mode=ParseMode.HTML)

# ═══════════════════════════════════════════════════════════
#  | 𝗦𝗘𝗥𝗩𝗜𝗖𝗘 𝗟𝗔𝗬𝗘𝗥 |
# ═══════════════════════════════════════════════════════════

class ProgressManager:
    """Centralized progress updater with hash checking and floodwait mitigation."""
    def __init__(self, message: Message):
        self.message = message
        self.last_text_hash = None
        self.last_update_time = 0.0

    async def update(self, text: str, force: bool = False):
        now = time.monotonic()
        if not force and now - self.last_update_time < 3.0:
            return

        text_hash = hashlib.md5(text.encode()).hexdigest()
        if text_hash == self.last_text_hash:
            return

        try:
            await safe_edit(self.message, text)
            self.last_text_hash = text_hash
            self.last_update_time = now
        except MessageNotModified:
            self.last_text_hash = text_hash
            self.last_update_time = now
        except Exception as e:
            logger.debug(f"ProgressManager edit failed: {e}")

@dataclass(order=True)
class PDFJob:
    priority: int
    created_at: float
    user_id: int = field(compare=False)
    operation: str = field(compare=False)
    filename: str = field(compare=False)
    kwargs: dict = field(compare=False)
    client: Client = field(compare=False)
    message: Message = field(compare=False)
    success_text: str = field(compare=False)
    file_id: str = field(compare=False)
    file_size: int = field(compare=False)

_PDF_QUEUE = asyncio.PriorityQueue()
_BACKGROUND_TASKS: set[asyncio.Task] = set()

async def _pdf_worker():
    """Background worker for processing PDF jobs sequentially from the PriorityQueue in-memory."""
    while True:
        job: PDFJob = await _PDF_QUEUE.get()
        try:
            processing_msg = await job.message.reply("<b>Pʀᴏᴄᴇssɪɴɢ, ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ...</b>", parse_mode=ParseMode.HTML)
            progress = ProgressManager(processing_msg)
            
            await progress.update("<b>Dᴏᴡɴʟᴏᴀᴅɪɴɢ sᴏᴜʀᴄᴇ PDF...</b>", force=True)
            
            # Download directly into memory buffer using file_id to survive deleted replies
            dl_buffer = await job.client.download_media(job.file_id, in_memory=True)
            dl_bytes = dl_buffer.getvalue()
            
            await progress.update("<b>Aᴘᴘʟʏɪɴɢ ᴄʜᴀɴɢᴇs...</b>", force=True)
            
            if job.operation == "add_banner":
                out_bytes = await pdf_svc.add_banner(dl_bytes, **job.kwargs)
            elif job.operation == "replace_banner":
                out_bytes = await pdf_svc.replace_banner(dl_bytes, **job.kwargs)
            else:
                out_bytes = dl_bytes
                
            await progress.update("<b>Uᴘʟᴏᴀᴅɪɴɢ PDF...</b>", force=True)
            
            # Create a bytes IO object and set the proper filename attribute for Pyrogram
            out_io = io.BytesIO(out_bytes)
            out_io.name = job.filename
            
            await safe_send_document(job.client, job.message.chat.id, out_io, f"<b>{job.success_text}</b>")
            await processing_msg.delete()
        except Exception as e:
            logger.error(f"PDF Job failed: {e}", exc_info=True)
            try:
                await safe_edit(processing_msg, f"<b>❌ Fᴀɪʟᴇᴅ:</b> <code>{e}</code>")
            except: pass
        finally:
            _PDF_QUEUE.task_done()

_cbz_semaphore: asyncio.Semaphore = None  # type: ignore

def _get_cbz_semaphore() -> asyncio.Semaphore:
    global _cbz_semaphore
    if _cbz_semaphore is None:
        _cbz_semaphore = asyncio.Semaphore(2)
    return _cbz_semaphore

_PDF_WORKERS: int = 2  # Number of concurrent PDF worker coroutines

def _fitz_add_link(page, url: str):
    """Add a full-page clickable URI link using fitz."""
    if not url:
        return
    try:
        page.insert_link({"kind": fitz.LINK_URI, "from": page.rect, "uri": url})
    except Exception as e:
        logger.warning(f"fitz_add_link failed: {e}")

# ═══════════════════════════════════════════════════════════
#  | 𝗘𝗫𝗘𝗖𝗨𝗧𝗢𝗥𝗦 & 𝗖𝗔𝗖𝗛𝗜𝗡𝗚 |
# ═══════════════════════════════════════════════════════════

_CBZ_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="cbz_pool")
_PDF_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pdf_pool")

# Banner Cache: TTLCache automatically expires old variants and protects against memory leaks
_BANNER_CACHE = TTLCache(maxsize=128, ttl=3600)
_BANNER_CACHE_TTL = 3600 # 1 hour

def log_timing(op_name: str):
    """Decorator to measure and log execution time of heavy operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter()
            try:
                res = func(*args, **kwargs)
                duration = time.perf_counter() - t0
                logger.info(f"[timing] {op_name} succeeded in {duration:.3f}s")
                return res
            except Exception as e:
                duration = time.perf_counter() - t0
                logger.error(f"[timing] {op_name} failed after {duration:.3f}s: {e}")
                raise
        return wrapper
    return decorator

# Hard Limits for Pixel-Bomb Protection
MAX_PIXELS = 80_000_000 # 80 MegaPixels
MAX_DIMENSION = 12000    # Max width or height

def _validate_image(path: str):
    """Ensure an image file is not a memory bomb (path-based, used by CBZ extractor)."""
    with Image.open(path) as img:
        w, h = img.size
        if w * h > MAX_PIXELS:
            raise ValueError(f"Image too large ({w*h/1e6:.1f}MP > 80MP limit)")
        if w > MAX_DIMENSION or h > MAX_DIMENSION:
            raise ValueError(f"Image dimensions exceed limit ({w}x{h})")

def _preprocess_image(img_bytes: bytes) -> bytes:
    """Resize huge dimensions, convert to RGB, optimize JPEG."""
    try:
        with Image.open(io.BytesIO(img_bytes)) as img:
            w, h = img.size
            if w > 4000 or h > 4000:
                img.thumbnail((4000, 4000), Image.Resampling.LANCZOS)
            
            if img.mode != 'RGB':
                img = img.convert('RGB')
                
            out = io.BytesIO()
            img.save(out, format="JPEG", quality=85, optimize=True)
            return out.getvalue()
    except Exception as e:
        logger.warning(f"Image preprocessing failed: {e}")
        return img_bytes

def _render_banner_page(image_bytes: bytes, target_width: float, target_height: float) -> bytes:
    """Adaptive fit (contain) with black background – entire image visible, centered."""
    try:
        opt_bytes = _preprocess_image(image_bytes)
        
        # Use PIL to get dimensions to avoid fitz decode overhead
        with Image.open(io.BytesIO(opt_bytes)) as im:
            img_w, img_h = im.size
        
        doc = fitz.open()
        page = doc.new_page(width=target_width, height=target_height)
        
        # Set page background to black
        page.draw_rect(page.rect, fill=(0, 0, 0))  # RGB black
        
        # Contain fit: scale so entire image fits inside the page
        scale = min(target_width / img_w, target_height / img_h)
        new_w = img_w * scale
        new_h = img_h * scale
        
        # Center the scaled image
        x_offset = (target_width - new_w) / 2
        y_offset = (target_height - new_h) / 2
        
        fit_rect = fitz.Rect(x_offset, y_offset, x_offset + new_w, y_offset + new_h)
        page.insert_image(fit_rect, stream=opt_bytes)
        
        pdf_bytes = doc.write()
        doc.close()
        return pdf_bytes
    except Exception as e:
        logger.error(f"Render banner failed: {e}")
        with fitz.open("jpeg", image_bytes) as doc:
            return doc.convert_to_pdf()

def _get_cached_banner_pdf(image_bytes: bytes, identifier: str, target_width: float, target_height: float) -> fitz.Document:
    """Returns a fitz.Document from cache or converts a new one adaptively."""
    cache_key = f"{identifier}:{int(target_width)}x{int(target_height)}"
    
    pdf_bytes = _BANNER_CACHE.get(cache_key)
    if pdf_bytes is not None:
        return fitz.open("pdf", pdf_bytes)
            
    pdf_bytes = _render_banner_page(image_bytes, target_width, target_height)
    _BANNER_CACHE[cache_key] = pdf_bytes
    return fitz.open("pdf", pdf_bytes)

class PDFService:
    @staticmethod
    def _run_sync(func, *args, executor=None, **kwargs):
        """Run a synchronous function in the specified executor."""
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(executor, lambda: func(*args, **kwargs))

    @staticmethod
    @log_timing("add_banner")
    def _add_banner_sync(pdf_bytes: bytes, page_args: list, banner_bytes: Optional[bytes] = None, banner_last_bytes: Optional[bytes] = None, banner_url: Optional[str] = None, banner_id: str = None, banner_last_id: str = None) -> bytes:
        """Optimized PDF banner addition using PyMuPDF and caching."""
        doc = fitz.open("pdf", pdf_bytes)
        if len(doc) == 0:
            res = doc.write(deflate=True)
            doc.close()
            return res
            
        target_width = doc[0].rect.width
        target_height = doc[0].rect.height
        
        if "first" in page_args and banner_bytes:
            with _get_cached_banner_pdf(banner_bytes, banner_id, target_width, target_height) as bpdf:
                doc.insert_pdf(bpdf, start_at=0)
                if banner_url:
                    _fitz_add_link(doc[0], banner_url)
        
        if "last" in page_args:
            b_bytes = banner_last_bytes or banner_bytes
            b_id = banner_last_id or banner_id
            if b_bytes:
                with _get_cached_banner_pdf(b_bytes, b_id, target_width, target_height) as bpdf:
                    doc.insert_pdf(bpdf, start_at=len(doc))
                    if banner_url:
                        _fitz_add_link(doc[len(doc)-1], banner_url)
        
        res = doc.write(deflate=True)
        doc.close()
        return res

    @classmethod
    async def add_banner(cls, pdf_input: Union[bytes, str], pdf_output: Optional[str] = None, **kwargs) -> Optional[bytes]:
        if isinstance(pdf_input, str):
            # Path-based call (legacy/file_rename path)
            def _path_wrapper():
                with open(pdf_input, "rb") as f:
                    in_bytes = f.read()
                
                # Extract banner bytes from paths if provided
                banner_path = kwargs.get("banner_path")
                banner_bytes = kwargs.get("banner_bytes")
                if banner_path and os.path.exists(banner_path):
                    with open(banner_path, "rb") as f:
                        banner_bytes = f.read()
                        
                banner_last_path = kwargs.get("banner_last_path")
                banner_last_bytes = kwargs.get("banner_last_bytes")
                if banner_last_path and os.path.exists(banner_last_path):
                    with open(banner_last_path, "rb") as f:
                        banner_last_bytes = f.read()
                
                # Prep clean kwargs for sync function
                clean_kwargs = {
                    "page_args": kwargs.get("page_args", []),
                    "banner_bytes": banner_bytes,
                    "banner_last_bytes": banner_last_bytes,
                    "banner_url": kwargs.get("banner_url"),
                    "banner_id": kwargs.get("banner_id"),
                    "banner_last_id": kwargs.get("banner_last_id")
                }
                
                out_bytes = cls._add_banner_sync(in_bytes, **clean_kwargs)
                
                if pdf_output:
                    with open(pdf_output, "wb") as f:
                        f.write(out_bytes)
                return out_bytes
                
            return await cls._run_sync(_path_wrapper, executor=_PDF_POOL)
        else:
            # Bytes-based call (worker path)
            return await cls._run_sync(cls._add_banner_sync, pdf_input, executor=_PDF_POOL, **kwargs)

    @staticmethod
    @log_timing("replace_banner")
    def _replace_banner_sync(pdf_bytes: bytes, page_args: list, banner_bytes: Optional[bytes] = None, banner_last_bytes: Optional[bytes] = None, banner_url: Optional[str] = None, banner_id: str = None, banner_last_id: str = None) -> bytes:
        """Optimized PDF banner replacement using PyMuPDF and caching."""
        doc = fitz.open("pdf", pdf_bytes)
        num_pages = len(doc)
        if num_pages == 0:
            res = doc.write(deflate=True)
            doc.close()
            return res
            
        target_width = doc[0].rect.width
        target_height = doc[0].rect.height
        
        if "last" in page_args and num_pages > 0:
            doc.delete_page(num_pages - 1)
            b_bytes = banner_last_bytes or banner_bytes
            b_id = banner_last_id or banner_id
            if b_bytes:
                with _get_cached_banner_pdf(b_bytes, b_id, target_width, target_height) as bpdf:
                    doc.insert_pdf(bpdf, start_at=len(doc))
                    if banner_url:
                        _fitz_add_link(doc[len(doc)-1], banner_url)
                    
        if "first" in page_args and len(doc) > 0:
            doc.delete_page(0)
            if banner_bytes:
                with _get_cached_banner_pdf(banner_bytes, banner_id, target_width, target_height) as bpdf:
                    doc.insert_pdf(bpdf, start_at=0)
                    if banner_url:
                        _fitz_add_link(doc[0], banner_url)
        
        res = doc.write(deflate=True)
        doc.close()
        return res

    @classmethod
    async def replace_banner(cls, pdf_input: Union[bytes, str], pdf_output: Optional[str] = None, **kwargs) -> Optional[bytes]:
        if isinstance(pdf_input, str):
            # Path-based call (legacy/file_rename path)
            def _path_wrapper():
                with open(pdf_input, "rb") as f:
                    in_bytes = f.read()
                
                # Extract banner bytes from paths if provided
                banner_path = kwargs.get("banner_path")
                banner_bytes = kwargs.get("banner_bytes")
                if banner_path and os.path.exists(banner_path):
                    with open(banner_path, "rb") as f:
                        banner_bytes = f.read()
                        
                banner_last_path = kwargs.get("banner_last_path")
                banner_last_bytes = kwargs.get("banner_last_bytes")
                if banner_last_path and os.path.exists(banner_last_path):
                    with open(banner_last_path, "rb") as f:
                        banner_last_bytes = f.read()
                
                # Prep clean kwargs for sync function
                clean_kwargs = {
                    "page_args": kwargs.get("page_args", []),
                    "banner_bytes": banner_bytes,
                    "banner_last_bytes": banner_last_bytes,
                    "banner_url": kwargs.get("banner_url"),
                    "banner_id": kwargs.get("banner_id"),
                    "banner_last_id": kwargs.get("banner_last_id")
                }
                
                out_bytes = cls._replace_banner_sync(in_bytes, **clean_kwargs)
                
                if pdf_output:
                    with open(pdf_output, "wb") as f:
                        f.write(out_bytes)
                return out_bytes
                
            return await cls._run_sync(_path_wrapper, executor=_PDF_POOL)
        else:
            # Bytes-based call (worker path)
            return await cls._run_sync(cls._replace_banner_sync, pdf_input, executor=_PDF_POOL, **kwargs)

    @staticmethod
    @log_timing("extract_pages")
    def _extract_pages_sync(input_path: str, first_out: str, last_out: str):
        """Ultra-fast page extraction using PyMuPDF's direct rendering."""
        with fitz.open(input_path) as doc:
            num_pages = len(doc)
            if num_pages > 0:
                doc[0].get_pixmap(matrix=fitz.Matrix(0.5, 0.5)).save(first_out)
                doc[num_pages - 1].get_pixmap(matrix=fitz.Matrix(0.5, 0.5)).save(last_out)

    @classmethod
    async def extract_pages(cls, input_path: str, first_out: str, last_out: str):
        # We don't need a semaphore here since it's just extracting bytes, but let's limit concurrent processing
        await cls._run_sync(cls._extract_pages_sync, input_path, first_out, last_out, executor=_PDF_POOL)

    @staticmethod
    @log_timing("lock_pdf")
    def _lock_pdf_sync(input_path: str, output_path: str, password: str):
        """Ultra-fast AES-256 encryption using PyMuPDF."""
        with fitz.open(input_path) as doc:
            doc.save(
                output_path, 
                encryption=fitz.PDF_ENCRYPT_AES_256, 
                user_pw=password, 
                owner_pw=password,
                permissions=fitz.PDF_PERM_ACCESSIBILITY | fitz.PDF_PERM_PRINT
            )

    @classmethod
    async def lock_pdf(cls, input_path: str, output_path: str, password: str):
        await cls._run_sync(cls._lock_pdf_sync, input_path, output_path, password, executor=_PDF_POOL)

    @staticmethod
    @log_timing("cbz_to_pdf")
    def _cbz_to_pdf_sync(input_path: str, output_path: str, progress_callback: Callable = None):
        """
        Peak Optimized CBZ to PDF conversion with pixel-bomb protection.
        """
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
            
        if not zipfile.is_zipfile(input_path):
            raise ValueError("Invalid CBZ: File is not a ZIP archive.")

        MAX_IMAGES = 500
        
        with tempfile.TemporaryDirectory(prefix="cbz_") as temp_dir:
            image_files = []
            try:
                with zipfile.ZipFile(input_path, 'r') as zip_ref:
                    # Protection: Check total uncompressed size
                    total_uncompressed_size = sum(file.file_size for file in zip_ref.infolist())
                    if total_uncompressed_size > 1536 * 1024 * 1024:
                        raise ValueError("CBZ archive is too large (> 1.5GB).")
                    
                    valid_exts = ('.jpg', '.jpeg', '.png', '.webp')
                    members = [m for m in zip_ref.namelist() if m.lower().endswith(valid_exts)]
                    
                    if not members:
                        raise ValueError("No valid images found in archive.")
                    
                    if len(members) > MAX_IMAGES:
                        raise ValueError(f"Too many images in archive (Max {MAX_IMAGES}).")

                    # Extract and validate one-by-one
                    for i, member in enumerate(members):
                        try:
                            extracted_path = zip_ref.extract(member, temp_dir)
                            _validate_image(extracted_path)
                            image_files.append(extracted_path)
                            
                            # Trigger progress update every 5 images
                            if progress_callback and (i % 5 == 0 or i == len(members) - 1):
                                progress_callback(i + 1, len(members))
                        except Exception as e:
                            logger.warning(f"Failed to extract/validate {member}: {e}")
            except Exception as e:
                raise ValueError(f"Failed to process CBZ: {e}")

            if not image_files:
                raise ValueError("Could not extract any valid images.")

            image_files.sort(key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f)])
            
            try:
                with open(output_path, "wb") as f:
                    f.write(img2pdf.convert(image_files))
            except Exception as e:
                logger.error(f"img2pdf conversion failed: {e}")
                raise ValueError(f"PDF generation failed: {e}")

    @classmethod
    async def cbz_to_pdf(cls, input_path: str, output_path: str, msg: Message = None):
        """Asynchronous wrapper for CBZ to PDF conversion with strict concurrency and progress UI."""
        last_edit_time = 0.0
        loop = asyncio.get_running_loop()

        def sync_progress_callback(current, total):
            nonlocal last_edit_time
            now = time.monotonic()
            if msg and (now - last_edit_time >= 5.0 or current == total):
                last_edit_time = now
                text = f"<b>Cᴏɴᴠᴇʀᴛɪɴɢ CBZ ᴛᴏ PDF...</b>\n<blockquote>◍ Pᴀɢᴇs: <code>{current} / {total}</code></blockquote>"
                asyncio.run_coroutine_threadsafe(safe_edit(msg, text), loop)

        async with _get_cbz_semaphore():
            try:
                await asyncio.wait_for(
                    cls._run_sync(cls._cbz_to_pdf_sync, input_path, output_path, sync_progress_callback, executor=_CBZ_POOL),
                    timeout=600.0
                )
            except asyncio.TimeoutError:
                logger.error(f"[pdf] CBZ conversion timed out: {input_path}")
                raise TimeoutError("CBZ to PDF conversion timed out (Max 10 mins).")

pdf_svc = PDFService()

async def async_remove(filepath: str):
    def _remove():
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
            except:
                pass
    await asyncio.get_running_loop().run_in_executor(None, _remove)

# ═══════════════════════════════════════════════════════════
#  | 𝗕𝗔𝗖𝗞𝗚𝗥𝗢𝗨𝗡𝗗 𝗧𝗔𝗦𝗞𝗦 |
# ═══════════════════════════════════════════════════════════

@dataclass(slots=True)
class PendingState:
    state: str
    msg_id: int
    chat_id: int
    ts: float

_pdf_pending_states: dict[int, PendingState] = {}
_CMD_LAST_USED: dict[tuple[int, str], float] = {}

async def _evict_old_pending_states():
    """Cleans up stale pending input states."""
    while True:
        try:
            await asyncio.sleep(300)
            now = time.time()
            expired = [uid for uid, data in _pdf_pending_states.items() if now - data.ts > 600]
            for uid in expired:
                _pdf_pending_states.pop(uid, None)
            if expired:
                logger.debug(f"[pdf] Evicted {len(expired)} stale pending states")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[pdf] Pending state eviction error: {e}")

async def _evict_old_cooldowns():
    """Cleans up the command cooldown dictionary."""
    while True:
        try:
            await asyncio.sleep(600)
            now = time.monotonic()
            expired = [k for k, v in _CMD_LAST_USED.items() if now - v > 600]
            for k in expired:
                _CMD_LAST_USED.pop(k, None)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[pdf] Cooldown eviction error: {e}")

def _on_cooldown(user_id: int, cmd: str) -> bool:
    key = (user_id, cmd)
    now = time.monotonic()
    if now - _CMD_LAST_USED.get(key, 0.0) < 3.0:
        return True
    _CMD_LAST_USED[key] = now
    return False

async def _evict_old_banner_cache():
    """Periodically prunes the banner memory cache."""
    while True:
        try:
            await asyncio.sleep(1800) # Every 30 mins
            _BANNER_CACHE.expire()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[pdf] Banner cache eviction error: {e}")

# ═══════════════════════════════════════════════════════════
#  | 𝗨𝗜 𝗕𝗨𝗜𝗟𝗗𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def build_pdf_panel_keyboard(user_id: int, prefs: dict) -> InlineKeyboardMarkup:
    pdf_banner_on = prefs.get("pdf_banner_mode", False)
    banner_placement = prefs.get("pdf_banner_placement", "first")
    cbz_on = prefs.get("cbz_to_pdf_mode", False)
    
    btn_on = InlineKeyboardButton("Tᴜʀɴ ON ◉" if pdf_banner_on else "Tᴜʀɴ ON", callback_data=f"pdf_pan_toggle:1:{user_id}")
    btn_off = InlineKeyboardButton("Tᴜʀɴ OFF" if pdf_banner_on else "Tᴜʀɴ OFF ◉", callback_data=f"pdf_pan_toggle:0:{user_id}")
    
    btn_first = InlineKeyboardButton("Fɪʀsᴛ ◉" if banner_placement == "first" else "Fɪʀsᴛ", callback_data=f"pdf_pan_place:first:{user_id}")
    btn_last = InlineKeyboardButton("Lᴀsᴛ ◉" if banner_placement == "last" else "Lᴀsᴛ", callback_data=f"pdf_pan_place:last:{user_id}")
    btn_both = InlineKeyboardButton("Bᴏᴛʜ ◉" if banner_placement == "both" else "Bᴏᴛʜ", callback_data=f"pdf_pan_place:both:{user_id}")

    btn_cbz_on  = InlineKeyboardButton("CBZ ᴛᴏ PDF ON ◉" if cbz_on  else "CBZ ᴛᴏ PDF ON",  callback_data=f"pdf_cbz_toggle:1:{user_id}")
    btn_cbz_off = InlineKeyboardButton("CBZ ᴛᴏ PDF OFF"   if cbz_on  else "CBZ ᴛᴏ PDF OFF ◉", callback_data=f"pdf_cbz_toggle:0:{user_id}")
    
    buttons = [
        [btn_on, btn_off],
        [btn_first, btn_last, btn_both],
        [btn_cbz_on, btn_cbz_off],
        [
            InlineKeyboardButton("Sᴇᴛ URL", callback_data=f"pdf_pan_set_url:{user_id}"),
            InlineKeyboardButton("Dᴇʟᴇᴛᴇ URL", callback_data=f"pdf_pan_del_url:{user_id}")
        ],
        [
            InlineKeyboardButton("Sᴇᴛ Iᴍᴀɢᴇ", callback_data=f"pdf_pan_set_img_menu:{user_id}"),
            InlineKeyboardButton("Dᴇʟᴇᴛᴇ Iᴍᴀɢᴇ", callback_data=f"pdf_pan_del_img_menu:{user_id}")
        ],
        [
            InlineKeyboardButton("Vɪᴇᴡ Bᴀɴɴᴇʀ", callback_data=f"pdf_pan_view:{user_id}"),
            InlineKeyboardButton("Pᴀssᴡᴏʀᴅ", callback_data=f"pdf_pan_password:{user_id}")
        ],
        [InlineKeyboardButton("Cʟᴏsᴇ", callback_data="close_msg")]
    ]
    return InlineKeyboardMarkup(buttons)

def build_pdf_password_menu(user_id: int, prefs: dict) -> InlineKeyboardMarkup:
    lock_on = prefs.get("pdf_lock_mode", False)
    has_pass = bool(prefs.get("pdf_lock_password"))
    
    btn_lock_on  = InlineKeyboardButton("Lᴏᴄᴋ ON ◉" if lock_on else "Lᴏᴄᴋ ON",  callback_data=f"pdf_lock_toggle:1:{user_id}")
    btn_lock_off = InlineKeyboardButton("Lᴏᴄᴋ OFF"  if lock_on else "Lᴏᴄᴋ OFF ◉", callback_data=f"pdf_lock_toggle:0:{user_id}")
    
    rows = [
        [
            InlineKeyboardButton("Sᴇᴛ Pᴀss", callback_data=f"pdf_pass_set:{user_id}"),
            InlineKeyboardButton("Dᴇʟ Pᴀss" if has_pass else "Dᴇʟ Pᴀss ✗", callback_data=f"pdf_pass_del:{user_id}"),
            InlineKeyboardButton("Vɪᴇᴡ Pᴀss", callback_data=f"pdf_pass_view:{user_id}")
        ],
        [btn_lock_on, btn_lock_off],
        [InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"pdf_pan_back:{user_id}")]
    ]
    return InlineKeyboardMarkup(rows)

def build_pdf_image_menu(user_id: int, is_delete: bool = False) -> InlineKeyboardMarkup:
    action = "del" if is_delete else "set"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Fɪʀsᴛ Pᴀɢᴇ Iᴍᴀɢᴇ", callback_data=f"pdf_img_{action}:first:{user_id}"),
            InlineKeyboardButton("Lᴀsᴛ Pᴀɢᴇ Iᴍᴀɢᴇ", callback_data=f"pdf_img_{action}:last:{user_id}")
        ],
        [InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"pdf_pan_back:{user_id}")]
    ])

def format_pdf_panel_text(prefs: dict) -> str:
    pdf_banner_on = prefs.get("pdf_banner_mode", False)
    banner_placement = prefs.get("pdf_banner_placement", "first").upper()
    url = prefs.get("pdf_banner_url")
    first_img = prefs.get("pdf_banner")
    last_img = prefs.get("pdf_banner_last")
    
    url_text = f"<code>{url}</code>" if url else "Not set"
    
    lock_on = prefs.get("pdf_lock_mode", False)
    lock_pass = prefs.get("pdf_lock_password")
    
    img_text = "Not set"
    if first_img and last_img:
        img_text = "Set (First & Last)"
    elif first_img:
        img_text = "Set (First only)"
    elif last_img:
        img_text = "Set (Last only)"
        
    lock_text = "ON" if lock_on else "OFF"
    pass_text = f"<code>{'*' * min(len(lock_pass), 8)}</code>" if lock_pass else "Not set"
        
    cbz_on = prefs.get("cbz_to_pdf_mode", False)
    cbz_text = "ON" if cbz_on else "OFF"
        
    return (
        "<blockquote><b>✦ 𝗣𝗗𝗙 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦</b></blockquote>\n"
        "<blockquote><b>›› Mᴀɴᴀɢᴇ PDF ʙᴀɴɴᴇʀ sᴛᴀᴛᴜs ᴅɪʀᴇᴄᴛʟʏ ғʀᴏᴍ ᴛʜɪs ᴘᴀɴᴇʟ.</b></blockquote>\n\n"
        f"◍ Bᴀɴɴᴇʀ : {'ON' if pdf_banner_on else 'OFF'}\n"
        f"◍ Pᴏsɪᴛɪᴏɴ : {banner_placement}\n"
        f"◍ URL : {url_text}\n"
        f"◍ Iᴍᴀɢᴇ : {img_text}\n"
        f"◍ Lᴏᴄᴋ : {lock_text}\n"
        f"◍ Pᴀssᴡᴏʀᴅ : {pass_text}\n"
        f"◍ CBZ → PDF : {cbz_text}\n\n"
        "<i><blockquote expandable>≡ Usᴇ ᴛʜᴇ ʙᴜᴛᴛᴏɴs ʙᴇʟᴏᴡ ᴛᴏ ᴛᴜʀɴ ʙᴀɴɴᴇʀ ᴏɴ ᴏʀ ᴏғғ, ᴄʜᴏᴏsᴇ ᴛʜᴇ ᴘᴏsɪᴛɪᴏɴ, sᴀᴠᴇ ᴏʀ ᴅᴇʟᴇᴛᴇ URL, ᴀɴᴅ sᴀᴠᴇ ᴏʀ ᴅᴇʟᴇᴛᴇ ᴛʜᴇ ʙᴀɴɴᴇʀ ɪᴍᴀɢᴇ.</blockquote></i>"
    )

# ═══════════════════════════════════════════════════════════
#  | 𝗛𝗔𝗡𝗗𝗟𝗘𝗥𝗦 |
# ═══════════════════════════════════════════════════════════

def _check_owner(query: CallbackQuery) -> bool:
    try:
        owner_id = int(query.data.split(":")[-1])
        return query.from_user.id == owner_id
    except (ValueError, TypeError, IndexError):
        return False

BANNER_PIC = "https://i.ibb.co/RTTJWQY4/photo-2026-05-02-16-05-08-7635326201489260572.jpg"

@Client.on_message(
    filters.command(["pdf_panel", "pdftools", "modepdf", "pdfmode", "pdf_mode"]) 
    & filters.private # Fix 1: Private only
)
async def pdf_panel_cmd(client: Client, message: Message):
    user_id = message.from_user.id

    if _on_cooldown(user_id, "pdf_panel"):
        return await message.reply_text("⧗ Pʟᴇᴀsᴇ sʟᴏᴡ ᴅᴏᴡɴ...", quote=True)

    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(user_id, prefs)

    try:
        await safe_send_photo(client, message.chat.id, BANNER_PIC, text, keyboard)
    except Exception as e:
        logger.warning(f"[pdf] Banner photo failed ({e}), falling back to text.")
        try:
            await client.send_message(message.chat.id, text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception as e2:
            logger.error(f"[pdf] pdf_panel_cmd fallback also failed: {e2}")

@Client.on_callback_query(filters.regex(r"^pdf_pan_toggle:(0|1):(\d+)$"))
async def pdf_pan_toggle_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
        
    mode = bool(int(query.matches[0].group(1)))
    user_id = query.from_user.id
    
    # UI Optimization: Fetch once, update locally
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    prefs["pdf_banner_mode"] = mode
    await DARKXSIDE78.set_pdf_banner_mode(user_id, mode)
    
    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(user_id, prefs)
    
    await safe_edit(query.message, text, reply_markup=keyboard)
    await query.answer(f"PDF Bᴀɴɴᴇʀ: {'ON' if mode else 'OFF'}")

@Client.on_callback_query(filters.regex(r"^pdf_cbz_toggle:(0|1):(\d+)$"))
async def pdf_cbz_toggle_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)

    mode = bool(int(query.matches[0].group(1)))
    user_id = query.from_user.id
    
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    prefs["cbz_to_pdf_mode"] = mode
    await DARKXSIDE78.set_cbz_to_pdf_mode(user_id, mode)

    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(user_id, prefs)

    await safe_edit(query.message, text, reply_markup=keyboard)
    await query.answer("CBZ → PDF : " + ("ON ✓" if mode else "OFF ✓"))

@Client.on_callback_query(filters.regex(r"^pdf_pan_place:(first|last|both):(\d+)$"))
async def pdf_pan_place_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
        
    placement = query.matches[0].group(1)
    user_id = query.from_user.id
    
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    prefs["pdf_banner_placement"] = placement
    await DARKXSIDE78.set_pdf_banner_placement(user_id, placement)
    
    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(user_id, prefs)
    
    await safe_edit(query.message, text, reply_markup=keyboard)
    await query.answer(f"Pʟᴀᴄᴇᴍᴇɴᴛ sᴇᴛ ᴛᴏ {placement.upper()}")



@Client.on_callback_query(filters.regex(r"^pdf_pan_set_url:(\d+)$"))
async def pdf_pan_set_url_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ !", show_alert=True)
        
    user_id = query.from_user.id
    _pdf_pending_states[user_id] = PendingState(
        state="url", 
        msg_id=query.message.id, 
        chat_id=query.message.chat.id,
        ts=time.time()
    )
    
    text = (
        "<b>✦ 𝗦𝗘𝗧 𝗕𝗔𝗡𝗡𝗘𝗥 𝗨𝗥𝗟</b>\n"
        "<blockquote>›› Sᴇɴᴅ ᴛʜᴇ URL ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴏᴘᴇɴ ᴡʜᴇɴ ᴜsᴇʀs ᴛᴀᴘ ᴛʜᴇ PDF ʙᴀɴɴᴇʀ ɪᴍᴀɢᴇ.</blockquote>\n"
        "<blockquote>ⓘ 𝗥𝗨𝗟𝗘𝗦 :</blockquote>\n"
        "<blockquote>• Iᴛ ᴍᴜsᴛ sᴛᴀʀᴛ ᴡɪᴛʜ http:// ᴏʀ https://\n"
        "• Yᴏᴜʀ ɴᴇxᴛ ᴛᴇxᴛ ᴍᴇssᴀɢᴇ ᴡɪʟʟ ʙᴇ sᴀᴠᴇᴅ ᴀs ᴛʜᴇ ʙᴀɴɴᴇʀ URL.</blockquote>"
    )
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("✕ Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]])
    await safe_edit(query.message, text, reply_markup=markup)

@Client.on_callback_query(filters.regex(r"^pdf_pan_del_url:(\d+)$"))
async def pdf_pan_del_url_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
        
    user_id = query.from_user.id
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    prefs["pdf_banner_url"] = None
    await DARKXSIDE78.set_pdf_banner_url(user_id, None)
    
    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(user_id, prefs)
    await safe_edit(query.message, text, reply_markup=keyboard)
    await query.answer("URL Dᴇʟᴇᴛᴇᴅ")

@Client.on_callback_query(filters.regex(r"^pdf_pan_(set|del)_img_menu:(\d+)$"))
async def pdf_pan_img_menu_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
        
    user_id = query.from_user.id
    action = query.matches[0].group(1)
    
    keyboard = build_pdf_image_menu(user_id, is_delete=(action == "del"))
    await safe_edit(query.message, "<b>Cʜᴏᴏsᴇ ᴡʜɪᴄʜ ɪᴍᴀɢᴇ ᴛᴏ ᴍᴏᴅɪғʏ:</b>", reply_markup=keyboard)

@Client.on_callback_query(filters.regex(r"^pdf_pan_back:(\d+)$"))
async def pdf_pan_back_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
        
    user_id = query.from_user.id
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(user_id, prefs)
    await safe_edit_message(client, query.message.chat.id, query.message.id, text, keyboard)

@Client.on_callback_query(filters.regex(r"^pdf_img_del:(first|last):(\d+)$"))
async def pdf_img_del_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
        
    user_id = query.from_user.id
    target = query.matches[0].group(1)
    
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    if target == "first":
        prefs["pdf_banner"] = None
        await DARKXSIDE78.set_pdf_banner(user_id, None)
    else:
        prefs["pdf_banner_last"] = None
        await DARKXSIDE78.set_pdf_banner_last(user_id, None)
        
    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(user_id, prefs)
    await safe_edit(query.message, text, reply_markup=keyboard)
    await query.answer(f"{target.capitalize()} Iᴍᴀɢᴇ Dᴇʟᴇᴛᴇᴅ")

@Client.on_callback_query(filters.regex(r"^pdf_img_set:(first|last):(\d+)$"))
async def pdf_img_set_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
        
    user_id = query.from_user.id
    target = query.matches[0].group(1)
    
    _pdf_pending_states[user_id] = PendingState(
        state=f"image_{target}", 
        msg_id=query.message.id, 
        chat_id=query.message.chat.id,
        ts=time.time()
    )
    text = (
        f"<b>✦ 𝗦𝗘𝗧 {target.upper()} 𝗣𝗔𝗚𝗘 𝗜𝗠𝗔𝗚𝗘</b>\n\n"
        "<blockquote>›› Pʟᴇᴀsᴇ sᴇɴᴅ ᴛʜᴇ ᴘʜᴏᴛᴏ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴜsᴇ ᴀs ʏᴏᴜʀ ʙᴀɴɴᴇʀ.</blockquote>"
    )
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]])
    await safe_edit_message(client, query.message.chat.id, query.message.id, text, markup)

@Client.on_callback_query(filters.regex(r"^pdf_pan_cancel_input$"))
async def pdf_pan_cancel_input_cb(client: Client, query: CallbackQuery):
    _pdf_pending_states.pop(query.from_user.id, None)
    prefs = await DARKXSIDE78.get_user_prefs_batch(query.from_user.id)
    text = format_pdf_panel_text(prefs)
    keyboard = build_pdf_panel_keyboard(query.from_user.id, prefs)
    await safe_edit(query.message, text, reply_markup=keyboard)
    await query.answer("Iɴᴘᴜᴛ ᴄᴀɴᴄᴇʟᴇᴅ.")

@Client.on_message(filters.private & (filters.text | filters.photo), group=-100)
async def pdf_pending_inpup_handler(client: Client, message: Message):
    user_id = message.from_user.id
    state_data = _pdf_pending_states.get(user_id)
    if not state_data:
        return

    state = state_data.state
    msg_id = state_data.msg_id
    chat_id = state_data.chat_id
    
    try:
        await message.delete()
    except:
        pass

    if state == "url":
        if not message.text:
            await safe_edit_id(client, chat_id, msg_id, "<blockquote>✗ Yᴏᴜ ᴍᴜsᴛ sᴇɴᴅ ᴀ ᴛᴇxᴛ URL. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ᴏʀ ᴄᴀɴᴄᴇʟ.</blockquote>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]]))
            raise StopPropagation
            
        if message.text.startswith(("http://", "https://")):
            await DARKXSIDE78.set_pdf_banner_url(user_id, message.text)
            _pdf_pending_states.pop(user_id, None)
            
            prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
            prefs["pdf_banner_url"] = message.text
            text = format_pdf_panel_text(prefs)
            keyboard = build_pdf_panel_keyboard(user_id, prefs)
            await safe_edit_id(client, chat_id, msg_id, f"<blockquote>✓ URL Sᴀᴠᴇᴅ !</blockquote>\n<blockquote>{text}</blockquote>", reply_markup=keyboard)
            raise StopPropagation
        else:
            await safe_edit_id(client, chat_id, msg_id, "<blockquote>✗ Iɴᴠᴀʟɪᴅ URL ғᴏʀᴍᴀᴛ. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.</blockquote>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]]))
            raise StopPropagation
            
    elif state.startswith("image_"):
        target = state.split("_")[1]
        if not message.photo:
            await safe_edit_id(client, chat_id, msg_id, "<blockquote>✗ Yᴏᴜ ᴍᴜsᴛ sᴇɴᴅ ᴀ ᴘʜᴏᴛᴏ. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ᴏʀ ᴄᴀɴᴄᴇʟ.</blockquote>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]]))
            raise StopPropagation
            
        prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
        if target == "first":
            prefs["pdf_banner"] = message.photo.file_id
            await DARKXSIDE78.set_pdf_banner(user_id, message.photo.file_id)
        else:
            prefs["pdf_banner_last"] = message.photo.file_id
            await DARKXSIDE78.set_pdf_banner_last(user_id, message.photo.file_id)
            
        _pdf_pending_states.pop(user_id, None)
        
        text = format_pdf_panel_text(prefs)
        keyboard = build_pdf_panel_keyboard(user_id, prefs)
        await safe_edit_id(client, chat_id, msg_id, f"<blockquote>✓ {target.capitalize()} Iᴍᴀɢᴇ Sᴀᴠᴇᴅ !</blockquote>\n<blockquote>{text}</blockquote>", reply_markup=keyboard)
        raise StopPropagation

    elif state == "password":
        if not message.text:
            await safe_edit_message(client, chat_id, msg_id, "<blockquote>✗ Sᴇɴᴅ ᴛʜᴇ ᴘᴀssᴡᴏʀᴅ ᴀs ᴛᴇxᴛ.</blockquote>", InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]]))
            raise StopPropagation
        
        pw = message.text.strip()
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]])
        
        if " " in pw:
            await safe_edit_message(client, chat_id, msg_id, "<blockquote>✗ Pᴀssᴡᴏʀᴅ ᴄᴀɴɴᴏᴛ ᴄᴏɴᴛᴀɪɴ sᴘᴀᴄᴇs. Tʀʏ ᴀɢᴀɪɴ.</blockquote>", cancel_kb)
            raise StopPropagation
        if len(pw) < 4:
            await safe_edit_message(client, chat_id, msg_id, "<blockquote>✗ Pᴀssᴡᴏʀᴅ ᴍᴜsᴛ ʙᴇ ᴀᴛ ʟᴇᴀsᴛ 4 ᴄʜᴀʀᴀᴄᴛᴇʀs.</blockquote>", cancel_kb)
            raise StopPropagation
        if len(pw) > 32:
            await safe_edit_message(client, chat_id, msg_id, "<blockquote>✗ Pᴀssᴡᴏʀᴅ ᴍᴜsᴛ ʙᴇ 32 ᴄʜᴀʀᴀᴄᴛᴇʀs ᴏʀ ꜰᴇᴡᴇʀ.</blockquote>", cancel_kb)
            raise StopPropagation

        await DARKXSIDE78.set_pdf_lock_password(user_id, pw)
        _pdf_pending_states.pop(user_id, None)

        prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
        prefs["pdf_lock_password"] = pw
        keyboard = build_pdf_password_menu(user_id, prefs)
        lock_on = prefs.get("pdf_lock_mode", False)
        text = (
            "<blockquote><b>⛊ 𝗣𝗗𝗙 𝗟𝗢𝗖𝗞 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦</b></blockquote>\n\n"
            f"• Lᴏᴄᴋ : {'<b>ON</b>' if lock_on else 'OFF'}\n"
            "• Pᴀssᴡᴏʀᴅ : ✓ Set\n\n"
            "<i><blockquote>≡ Sᴇᴛ ᴀ ᴘᴀssᴡᴏʀᴅ ᴛᴏ ʟᴏᴄᴋ ᴏᴜᴛᴘᴜᴛ PDFs. Tᴜʀɴ ʟᴏᴄᴋ ON ᴛᴏ ᴀᴘᴘʟʏ ɪᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴅᴜʀɪɴɢ ʀᴇɴᴀᴍᴇ.</blockquote></i>"
        )
        await safe_edit_id(client, chat_id, msg_id, text, reply_markup=keyboard)
        raise StopPropagation

@Client.on_callback_query(filters.regex(r"^pdf_pan_view:(\d+)$"))
async def pdf_pan_view_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
        
    user_id = query.from_user.id
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    
    first_img = prefs.get("pdf_banner")
    last_img = prefs.get("pdf_banner_last")
    
    if not first_img and not last_img:
        return await query.answer("✗ Nᴏ ʙᴀɴɴᴇʀ ɪᴍᴀɢᴇs sᴇᴛ!", show_alert=True)
        
    if first_img:
        await safe_send_photo(client, query.message.chat.id, first_img, "<b>Fɪʀsᴛ Pᴀɢᴇ Bᴀɴɴᴇʀ</b>")
    if last_img:
        await safe_send_photo(client, query.message.chat.id, last_img, "<b>Lᴀsᴛ Pᴀɢᴇ Bᴀɴɴᴇʀ</b>")
        
    await query.answer()

@Client.on_callback_query(filters.regex(r"^pdf_pan_password:(\d+)$"))
async def pdf_pan_password_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
    
    user_id = query.from_user.id
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    keyboard = build_pdf_password_menu(user_id, prefs)
    lock_on = prefs.get("pdf_lock_mode", False)
    has_pass = bool(prefs.get("pdf_lock_password"))
    
    text = (
        "<blockquote><b>⛊ 𝗣𝗗𝗙 𝗟𝗢𝗖𝗞 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦</b></blockquote>\n\n"
        f"• Lᴏᴄᴋ : {'<b>ON</b>' if lock_on else 'OFF'}\n"
        f"• Pᴀssᴡᴏʀᴅ : {'✓ Set' if has_pass else '✗ Not set'}\n\n"
        "<i><blockquote>≡ Sᴇᴛ ᴀ ᴘᴀssᴡᴏʀᴅ ᴛᴏ ʟᴏᴄᴋ ᴏᴜᴛᴘᴜᴛ PDFs. Tᴜʀɴ ʟᴏᴄᴋ ON ᴛᴏ ᴀᴘᴘʟʏ ɪᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴅᴜʀɪɴɢ ʀᴇɴᴀᴍᴇ.</blockquote></i>"
    )
    await safe_edit_message(client, query.message.chat.id, query.message.id, text, keyboard)
    await query.answer()

@Client.on_callback_query(filters.regex(r"^pdf_lock_toggle:(0|1):(\d+)$"))
async def pdf_lock_toggle_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
    
    user_id = query.from_user.id
    mode = query.matches[0].group(1) == "1"
    
    if mode:
        prefs_check = await DARKXSIDE78.get_user_prefs_batch(user_id)
        if not prefs_check.get("pdf_lock_password"):
            return await query.answer("✗ Sᴇᴛ ᴀ ᴘᴀssᴡᴏʀᴅ ꜰɪʀsᴛ ʙᴇꜰᴏʀᴇ ᴇɴᴀʙʟɪɴɢ ʟᴏᴄᴋ!", show_alert=True)
    
    await DARKXSIDE78.set_pdf_lock_mode(user_id, mode)
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    keyboard = build_pdf_password_menu(user_id, prefs)
    lock_on = prefs.get("pdf_lock_mode", False)
    has_pass = bool(prefs.get("pdf_lock_password"))
    text = (
        "<blockquote><b>⛊ 𝗣𝗗𝗙 𝗟𝗢𝗖𝗞 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦</b></blockquote>\n\n"
        f"• Lᴏᴄᴋ : {'<b>ON</b>' if lock_on else 'OFF'}\n"
        f"• Pᴀssᴡᴏʀᴅ : {'✓ Set' if has_pass else '✗ Not set'}\n\n"
        "<i><blockquote>≡ Sᴇᴛ ᴀ ᴘᴀssᴡᴏʀᴅ ᴛᴏ ʟᴏᴄᴋ ᴏᴜᴛᴘᴜᴛ PDFs. Tᴜʀɴ ʟᴏᴄᴋ ON ᴛᴏ ᴀᴘᴘʟʏ ɪᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴅᴜʀɪɴɢ ʀᴇɴᴀᴍᴇ.</blockquote></i>"
    )
    await safe_edit_message(client, query.message.chat.id, query.message.id, text, keyboard)
    await query.answer("Lᴏᴄᴋ " + ("ᴇɴᴀʙʟᴇᴅ ✓" if mode else "ᴅɪsᴀʙʟᴇᴅ ✓"))

@Client.on_callback_query(filters.regex(r"^pdf_pass_set:(\d+)$"))
async def pdf_pass_set_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
    
    user_id = query.from_user.id
    _pdf_pending_states[user_id] = PendingState(
        state="password", 
        msg_id=query.message.id, 
        chat_id=query.message.chat.id,
        ts=time.time()
    )
    text = (
        "<blockquote><b>✦ 𝗦𝗘𝗧 𝗣𝗗𝗙 𝗣𝗔𝗦𝗦𝗪𝗢𝗥𝗗</b></blockquote>\n"
        "<blockquote>›› Sᴇɴᴅ ʏᴏᴜʀ ᴅᴇsɪʀᴇᴅ PDF ʟᴏᴄᴋ ᴘᴀssᴡᴏʀᴅ ᴀs ᴀ ᴛᴇxᴛ ᴍᴇssᴀɢᴇ.</blockquote>\n"
        "<blockquote>ⓘ 𝗥𝗨𝗟𝗘𝗦 :</blockquote>\n"
        "<blockquote>• M\u026a\u0274 4–32 \u1d04\u029c\u1d00\u0280\u1d00\u1d04\u1d1b\u1d07\u0280s ʟᴏɴɢ\n"
        "• Nᴏ sᴘᴀᴄᴇs ᴀʟʟᴏᴡᴇᴅ</blockquote>"
    )
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("Cᴀɴᴄᴇʟ", callback_data="pdf_pan_cancel_input")]])
    await safe_edit_message(client, query.message.chat.id, query.message.id, text, markup)

@Client.on_callback_query(filters.regex(r"^pdf_pass_del:(\d+)$"))
async def pdf_pass_del_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
    
    user_id = query.from_user.id
    prefs_check = await DARKXSIDE78.get_user_prefs_batch(user_id)
    if not prefs_check.get("pdf_lock_password"):
        return await query.answer("✗ Nᴏ ᴘᴀssᴡᴏʀᴅ ɪs sᴇᴛ !", show_alert=True)
    
    await DARKXSIDE78.set_pdf_lock_password(user_id, None)
    await DARKXSIDE78.set_pdf_lock_mode(user_id, False)
    
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    keyboard = build_pdf_password_menu(user_id, prefs)
    text = (
        "<blockquote><b>⛊ 𝗣𝗗𝗙 𝗟𝗢𝗖𝗞 𝗦𝗘𝗧𝗧𝗜𝗡𝗚𝗦</b></blockquote>\n\n"
        "• Lᴏᴄᴋ : OFF\n"
        "• Pᴀssᴡᴏʀᴅ : ✗ Not set\n\n"
        "<i><blockquote>≡ Sᴇᴛ ᴀ ᴘᴀssᴡᴏʀᴅ ᴛᴏ ʟᴏᴄᴋ ᴏᴜᴛᴘᴜᴛ PDFs. Tᴜʀɴ ʟᴏᴄᴋ ON ᴛᴏ ᴀᴘᴘʟʏ ɪᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴅᴜʀɪɴɢ ʀᴇɴᴀᴍᴇ.</blockquote></i>"
    )
    await safe_edit_message(client, query.message.chat.id, query.message.id, text, keyboard)
    await query.answer("✓ Pᴀssᴡᴏʀᴅ ᴅᴇʟᴇᴛᴇᴅ. Lᴏᴄᴋ ᴅɪsᴀʙʟᴇᴅ.")

@Client.on_callback_query(filters.regex(r"^pdf_pass_view:(\d+)$"))
async def pdf_pass_view_cb(client: Client, query: CallbackQuery):
    if not _check_owner(query):
        return await query.answer("Yᴏᴜ ᴄᴀɴ'ᴛ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ ᴀɴᴏᴛʜᴇʀ ᴜsᴇʀ!", show_alert=True)
    
    user_id = query.from_user.id
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    password = prefs.get("pdf_lock_password")
    
    if not password:
        return await query.answer("✗ Nᴏ ᴘᴀssᴡᴏʀᴅ ɪs sᴇᴛ!", show_alert=True)
    
    await query.answer(f"⛊ Yᴏᴜʀ ᴘᴀssᴡᴏʀᴅ : {password}", show_alert=True)

# ═══════════════════════════════════════════════════════════
#  | 𝗣𝗗𝗙 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦 (𝗘𝗫𝗘𝗖𝗨𝗧𝗜𝗢𝗡) |
# ═══════════════════════════════════════════════════════════

def _parse_pages(arg: str) -> list[int]:
    if arg == "none":
        return []
    pages = []
    for part in arg.split(","):
        part = part.strip()
        if part == "first":
            pages.append(0)
        elif part == "last":
            pages.append(-1)
        elif part.isdigit():
            pages.append(int(part) - 1)
        else:
            raise ValueError(f"Invalid option: {part}")
    return pages

async def _process_pdf_wrapper(client: Client, message: Message, processing_func: Callable, success_text: str):
    replied = message.reply_to_message
    if not replied or not replied.document or not replied.document.file_name.lower().endswith(".pdf"):
        return await message.reply("<b>ⓘ Rᴇᴘʟʏ ᴛᴏ ᴀ PDF ꜰɪʟᴇ</b>", parse_mode=ParseMode.HTML)
        
    file_size = replied.document.file_size or 0
    
    # Enforce strict 200MB limit to prevent Out-of-Memory crashes
    if file_size > 200 * 1024 * 1024:
        return await message.reply("<b>❌ Eʀʀᴏʀ: Tʜᴇ PDF ғɪʟᴇ sɪᴢᴇ ᴇxᴄᴇᴇᴅs ᴛʜᴇ 𝟸𝟶𝟶MB ʟɪᴍɪᴛ.</b>", parse_mode=ParseMode.HTML)
        
    priority = 10 if file_size < 10*1024*1024 else 20
    filename = replied.document.file_name.replace(".pdf", "_out.pdf")
    file_id = replied.document.file_id
    
    # We construct the job and add it to the queue instead of running directly.
    job = PDFJob(
        priority=priority,
        created_at=time.time(),
        user_id=message.from_user.id,
        operation=processing_func.__name__,
        filename=filename,
        kwargs=processing_func.kwargs if hasattr(processing_func, "kwargs") else {},
        client=client,
        message=message,
        success_text=success_text,
        file_id=file_id,
        file_size=file_size
    )
    await _PDF_QUEUE.put(job)
    await message.reply("<b>Pʟᴀᴄᴇᴅ ɪɴ Qᴜᴇᴜᴇ...</b>", parse_mode=ParseMode.HTML)

@Client.on_message(filters.command(["set_pdf_remove", "setpdfremove", "pdf_remove"]) & filters.private)
async def set_pdf_remove_cmd(client: Client, message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply(
            "<b>Usᴀɢᴇ:</b> <code>/pdf_remove &lt;pages&gt;</code>\n"
            "E.g.: <code>/pdf_remove 2</code> or <code>first,last</code> or <code>none</code>",
            parse_mode=ParseMode.HTML
        )
        
    try:
        remove_pages = _parse_pages(args[1].strip().lower())
        await DARKXSIDE78.set_pdf_remove_pages(message.from_user.id, remove_pages)
        pretty = ", ".join([("First" if p == 0 else "Last" if p == -1 else str(p+1)) for p in remove_pages]) if remove_pages else "Disabled"
        await message.reply(f"<b>✅ PDF ᴀᴜᴛᴏ-ʀᴇᴍᴏᴠᴇ ᴘᴀɢᴇs sᴇᴛ ᴛᴏ:</b> <code>{pretty}</code>", parse_mode=ParseMode.HTML)
    except ValueError as e:
        await message.reply(f"<b>❌ {str(e)}</b>", parse_mode=ParseMode.HTML)

@Client.on_message(filters.command("banner_place") & filters.reply & filters.private)
async def pdf_replace_banner(client: Client, message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply("<b>Sᴘᴇᴄɪꜰʏ ᴡʜɪᴄʜ ᴘᴀɢᴇ(s): <code>first</code>, <code>last</code>, or <code>both</code></b>", parse_mode=ParseMode.HTML)
        
    page_args = [x.strip().lower() for x in args[1].split(",") if x.strip() in ("first", "last", "both")]
    if "both" in page_args:
        page_args = ["first", "last"]
        
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    banner_file_id = prefs.get("pdf_banner")
    banner_last_file_id = prefs.get("pdf_banner_last")
    banner_url = prefs.get("pdf_banner_url")
    
    if not banner_file_id and not banner_last_file_id:
        return await message.reply("<b>❌ Yᴏᴜ ʜᴀᴠᴇ ɴᴏ PDF ʙᴀɴɴᴇʀs sᴇᴛ.</b>", parse_mode=ParseMode.HTML)
        
    def dummy_func(): pass
    dummy_func.__name__ = "replace_banner"
    
    banner_bytes = None
    banner_last_bytes = None
    if banner_file_id:
        b1_io = await client.download_media(banner_file_id, in_memory=True)
        banner_bytes = b1_io.getvalue()
    if banner_last_file_id and "last" in page_args:
        b2_io = await client.download_media(banner_last_file_id, in_memory=True)
        banner_last_bytes = b2_io.getvalue()
        
    dummy_func.kwargs = {
        "page_args": page_args,
        "banner_bytes": banner_bytes,
        "banner_last_bytes": banner_last_bytes,
        "banner_url": banner_url,
        "banner_id": banner_file_id,
        "banner_last_id": banner_last_file_id
    }
            
    await _process_pdf_wrapper(client, message, dummy_func, f"Rᴇᴘʟᴀᴄᴇᴅ ᴘᴀɢᴇs: {', '.join(page_args)} ᴡɪᴛʜ ʙᴀɴɴᴇʀ.")

@Client.on_message(filters.command("addpdf") & filters.reply & filters.private)
async def pdf_add_banner_cmd(client: Client, message: Message):
    user_id = message.from_user.id
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply("<b>Sᴘᴇᴄɪꜰʏ ᴡʜᴇʀᴇ ᴛᴏ ᴀᴅᴅ: <code>first</code>, <code>last</code>, or <code>both</code></b>", parse_mode=ParseMode.HTML)
        
    page_args = [x.strip().lower() for x in args[1].split(",") if x.strip() in ("first", "last", "both")]
    if "both" in page_args:
        page_args = ["first", "last"]
        
    prefs = await DARKXSIDE78.get_user_prefs_batch(user_id)
    banner_file_id = prefs.get("pdf_banner")
    banner_last_file_id = prefs.get("pdf_banner_last")
    banner_url = prefs.get("pdf_banner_url")
    
    if not banner_file_id and not banner_last_file_id:
        return await message.reply("<b>❌ Yᴏᴜ ʜᴀᴠᴇ ɴᴏ PDF ʙᴀɴɴᴇʀs sᴇᴛ.</b>", parse_mode=ParseMode.HTML)
        
    def dummy_func(): pass
    dummy_func.__name__ = "add_banner"
    
    banner_bytes = None
    banner_last_bytes = None
    if banner_file_id:
        b1_io = await client.download_media(banner_file_id, in_memory=True)
        banner_bytes = b1_io.getvalue()
    if banner_last_file_id and "last" in page_args:
        b2_io = await client.download_media(banner_last_file_id, in_memory=True)
        banner_last_bytes = b2_io.getvalue()
        
    dummy_func.kwargs = {
        "page_args": page_args,
        "banner_bytes": banner_bytes,
        "banner_last_bytes": banner_last_bytes,
        "banner_url": banner_url,
        "banner_id": banner_file_id,
        "banner_last_id": banner_last_file_id
    }
            
    await _process_pdf_wrapper(client, message, dummy_func, f"Aᴅᴅᴇᴅ ʙᴀɴɴᴇʀ ᴘᴀɢᴇ(s): {', '.join(page_args)}")

@Client.on_message(filters.command("pdf_extract") & filters.reply & filters.private)
async def pdf_extractor_cmd(client: Client, message: Message):
    replied = message.reply_to_message
    if not replied or not replied.document or not replied.document.file_name.lower().endswith(".pdf"):
        return await message.reply("<b>⚠️ Rᴇᴘʟʏ ᴛᴏ ᴀ PDF ꜰɪʟᴇ</b>", parse_mode=ParseMode.HTML)
        
    processing_msg = await message.reply("<b>Pʀᴏᴄᴇssɪɴɢ...</b>", parse_mode=ParseMode.HTML)
    input_path = await replied.download()
    
    first_img_path = input_path.replace(".pdf", "_first.jpg")
    last_img_path = input_path.replace(".pdf", "_last.jpg")
    
    try:
        await pdf_svc.extract_pages(input_path, first_img_path, last_img_path)
        await safe_send_photo(client, message.chat.id, first_img_path, "<b>Fɪʀsᴛ Pᴀɢᴇ Iᴍᴀɢᴇ</b>")
        await safe_send_photo(client, message.chat.id, last_img_path, "<b>Lᴀsᴛ Pᴀɢᴇ Iᴍᴀɢᴇ</b>")
        await processing_msg.delete()
    except Exception as e:
        logger.error(f"Extract failed: {e}", exc_info=True)
        await safe_edit(processing_msg, f"<b>❌ Fᴀɪʟᴇᴅ:</b> <code>{e}</code>")
    finally:
        await async_remove(input_path)
        await async_remove(first_img_path)
        await async_remove(last_img_path)

async def init(*args, **kwargs):
    """Starts background maintenance tasks with GC protection."""
    loop = asyncio.get_running_loop()
    
    # Store strong references to background tasks to protect them from GC
    task_pending = loop.create_task(_evict_old_pending_states())
    _BACKGROUND_TASKS.add(task_pending)
    task_pending.add_done_callback(_BACKGROUND_TASKS.discard)
    
    task_cooldowns = loop.create_task(_evict_old_cooldowns())
    _BACKGROUND_TASKS.add(task_cooldowns)
    task_cooldowns.add_done_callback(_BACKGROUND_TASKS.discard)
    
    task_cache = loop.create_task(_evict_old_banner_cache())
    _BACKGROUND_TASKS.add(task_cache)
    task_cache.add_done_callback(_BACKGROUND_TASKS.discard)
    
    for _ in range(_PDF_WORKERS):
        t = loop.create_task(_pdf_worker())
        _BACKGROUND_TASKS.add(t)
        t.add_done_callback(_BACKGROUND_TASKS.discard)
        
    logger.info(f"[pdf] Background maintenance tasks and {_PDF_WORKERS} workers started with GC protection.")

__all__ = ["pdf_svc", "init"]

logger.info("[✓] PDF Tool Module Hardened & Loaded")
