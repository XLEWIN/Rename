#┌─ 𝗥𝗘𝗫𝗬 𝗪𝗢𝗥𝗞𝗦𝗛𝗢𝗣 ───────────────────────────────┐
#│  ʀᴇ-ᴄᴏᴅᴇ ᴏᴘᴛɪᴍɪᴢᴀᴛɪᴏɴ ᴅᴏɴᴇ [v3 PRO]          │      @NyxRexy
#└──────────────────────────────────────────┘

import asyncio
import html
import logging
import os
import re
import time
import uuid
from collections import OrderedDict
from typing import Optional

import aiofiles
from pymediainfo import MediaInfo
from pyrogram import Client, filters
from pyrogram.enums import ChatAction, ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegraph import Telegraph

from plugins.access_control import check_ban_status
from plugins.admin import safe_edit, close_btn
from plugins.regex import PeakTokenEngineV2_REXY

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  | 𝗖𝗢𝗡𝗙𝗜𝗚 |
# ─────────────────────────────────────────────────────────────

_TELEGRAPH_AUTHOR     = "WAGURI MediaInfo"
_TELEGRAPH_AUTHOR_URL = "https://t.me/NyxRexy"
_TELEGRAPH_IMAGE      = "https://i.ibb.co/LzDM2SYp/photo-2026-05-17-16-48-19-7640903611660107800.jpg"
_TEMP_DIR             = "mediainfo"

# FIX #1 — limit is in BYTES, not megabytes
_STREAM_LIMIT_BYTES   = 15 * 1024 * 1024   # 15 MiB — enough for most container headers
_SMALL_FILE_THRESHOLD = 10 * 1024 * 1024   # files ≤10 MiB → download fully

_COOLDOWN_SECS        = 10.0

# MIME → extension fallback for extension-less files
_MIME_EXT: dict[str, str] = {
    "video/x-matroska": ".mkv",
    "video/mp4":        ".mp4",
    "video/x-msvideo":  ".avi",
    "video/quicktime":  ".mov",
    "video/webm":       ".webm",
    "audio/mpeg":       ".mp3",
    "audio/flac":       ".flac",
    "audio/ogg":        ".ogg",
    "audio/x-wav":      ".wav",
    "audio/aac":        ".aac",
    "audio/opus":       ".opus",
}

_telegraph: Optional[Telegraph] = None
_cooldown_map: dict[int, float] = {}
_img_verified: bool = False
_img_checked: bool  = False

# ── Section styled headers
_HEADERS: dict[str, str] = {
    "general":  "| |  𝗚𝗘𝗡𝗘𝗥𝗔𝗟 𝗜𝗡𝗙𝗢  | |",
    "video":    "| |  𝗩𝗜𝗗𝗘𝗢 𝗜𝗡𝗙𝗢  | |",
    "audio":    "| |  𝗔𝗨𝗗𝗜𝗢 𝗜𝗡𝗙𝗢  | |",
    "subtitle": "| |  𝗦𝗨𝗕𝗧𝗜𝗧𝗟𝗘 𝗜𝗡𝗙𝗢  | |",
    "image":    "| |  𝗜𝗠𝗔𝗚𝗘 𝗜𝗡𝗙𝗢  | |",
    "chapters": "| |  𝗖𝗛𝗔𝗣𝗧𝗘𝗥𝗦  | |",
}
_MAIN_HEADER = "❐ 𝗠𝗘𝗗𝗜𝗔 𝗜𝗡𝗙𝗢𝗥𝗠𝗔𝗧𝗜𝗢𝗡"
_SEP2 = "━" * 38

# ── Keys to ALWAYS exclude
_EXCLUDE_KEYS: frozenset = frozenset({
    "Count", "Count of stream of this kind", "Kind of stream",
    "Stream identifier", "StreamOrder", "IsStreamable",
    "Folder name", "File name extension", "File name", "File extension",
    "File last modification date", "File last modification date (local)",
    "Format/Url", "Codec ID/Url", "Format/Extensions usually used",
    "Internet media type", "Commercial name",
    "colour_description_present", "colour_description_present_Source",
    "colour_range_Source", "colour_primaries_Source",
    "transfer_characteristics_Source", "matrix_coefficients_Source",
    "Stored_Width", "Sampled_Width", "Sampled_Height",
    "Frame count", "Samples count", "Proportion of this stream",
    "Chapters_Pos_Begin", "Chapters_Pos_End",
    "Delay, origin",
    "ARTIST", "AUTHOR", "CUSTOM_TAG",
    "Video_Format_List", "Video_Format_WithHint_List", "Codecs Video",
    "Audio_Format_List", "Audio_Format_WithHint_List", "Audio codecs",
    "Audio_Language_List", "Audio_Channels_Total",
    "Text_Format_List", "Text_Format_WithHint_List", "Text codecs",
    "Text_Language_List",
    "Count of video streams", "Count of audio streams",
    "Count of text streams", "Count of menu streams",
    "Encoded_Library_Name", "Encoded_Library_Version",
    "Pixel aspect ratio",
})

_SOFT_EXCLUDE: frozenset = frozenset({
    "Delay relative to video",
})

# ── Key label remap
_KEY_LABELS: dict[str, str] = {
    "Complete name":             "File name",
    "Format profile":            "Profile",
    "Format version":            "Version",
    "Format settings":           "Settings",
    "Codec ID/Info":             "Codec Info",
    "Overall bit rate":          "Overall bitrate",
    "Overall bit rate mode":     "Bitrate mode",
    "Nominal bit rate":          "Nominal bitrate",
    "Bit rate":                  "Bitrate",
    "Bit rate mode":             "Bitrate mode",
    "Bit depth":                 "Bit depth",
    "Channel(s)":                "Channels",
    "Channel layout":            "Ch. layout",
    "Sampling rate":             "Sample rate",
    "Compression mode":          "Compression",
    "Frame rate mode":           "FPS mode",
    "Frame rate":                "Frame rate",
    "Scan type":                 "Scan type",
    "Scan order":                "Scan order",
    "Writing application":       "Muxer",
    "Writing library":           "Enc. library",
    "Encoded date":              "Encoded date",
    "Tagged date":               "Tagged date",
    "Default":                   "Default",
    "Forced":                    "Forced",
    "colour_range":              "Colour range",
    "colour_primaries":          "Colour primaries",
    "transfer_characteristics":  "Transfer char.",
    "matrix_coefficients":       "Matrix coeff.",
    "mastering_display_color_primaries": "HDR Primaries",
    "mastering_display_luminance":       "HDR Luminance",
    "maximum_content_light_level":       "MaxCLL",
    "maximum_frameaverage_light_level":  "MaxFALL",
    "HDR format":                "HDR format",
    "HDR format compatibility":  "HDR compat.",
    "Number of dynamic objects": "Dynamic objects",
    "Codec ID":                  "Codec ID",
    "Stream size":               "Stream size",
    "Duration":                  "Duration",
    "Language":                  "Language",
    "Title":                     "Title",
    "Unique ID":                 "Unique ID",
    "File size":                 "File size",
    "Format":                    "Format",
    "Width":                     "Width",
    "Height":                    "Height",
}

# FIX #6 — metadata keys where WAGURI/source tags legitimately appear
_STATUS_FIELDS: frozenset = frozenset({
    "Writing application", "Writing library",
    "Encoded_Library", "Encoded_Library_Name",
    "Encoded_Library_Settings", "Encoded_Application",
})


# ─────────────────────────────────────────────────────────────
#  | 𝗛𝗘𝗟𝗣𝗘𝗥𝗦 |
# ─────────────────────────────────────────────────────────────

def _get_telegraph() -> Telegraph:
    global _telegraph
    if _telegraph is None:
        tg = Telegraph()
        tg.create_account(short_name="WAGURI_Bot")
        _telegraph = tg
    return _telegraph


def _on_cooldown(user_id: int) -> tuple[bool, float]:
    now = time.monotonic()
    last = _cooldown_map.get(user_id, 0.0)
    remaining = _COOLDOWN_SECS - (now - last)
    if remaining > 0:
        return True, remaining
    _cooldown_map[user_id] = now
    return False, 0.0


def _fmt_size(n: int) -> str:
    if not n:
        return "N/A"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} PB"


def _is_raw_numeric(v: str) -> bool:
    try:
        float(v.replace(" ", "").replace(",", ""))
        return True
    except ValueError:
        return False


def _best(key: str, values: list[str]) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    if key.lower() == "language":
        long_v = [v for v in values if len(v) > 3 and not _is_raw_numeric(v)]
        if long_v:
            return long_v[0]
    for v in values:
        if not _is_raw_numeric(v) and not v.startswith("http"):
            return v
    return values[0]


def _label(key: str) -> str:
    return _KEY_LABELS.get(key, key)


def _sniff_ext(file_name: str, mime_type: str) -> str:
    """Return best extension for temp file. FIX #3."""
    ext = os.path.splitext(file_name)[1]
    if ext and len(ext) <= 5:
        return ext
    return _MIME_EXT.get(mime_type, "")


# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗘𝗗𝗜𝗔𝗜𝗡𝗙𝗢 𝗣𝗔𝗥𝗦𝗘𝗥 |
# ─────────────────────────────────────────────────────────────

def _get_mediainfo_sections(
    path: str, real_name: str
) -> tuple[list[tuple[str, str, list[tuple[str, str]]]], str, dict]:
    """Parse pymediainfo into clean sections. FIX #4 — parse_speed=0."""
    try:
        # parse_speed=0 forces MediaInfo to do a deep read, extracting more
        # from truncated / partial files.
        raw = MediaInfo.parse(path, output="", parse_speed=0) or ""
    except Exception as e:
        raise RuntimeError(f"pymediainfo failed: {e}")

    # FIX #6 — only look for status tags inside real metadata fields
    meta_values: set[str] = set()
    for line in raw.splitlines():
        if " : " in line:
            k, v = line.split(" : ", 1)
            if k.strip() in _STATUS_FIELDS:
                meta_values.add(v.strip())

    is_waguri      = any(t in v for v in meta_values for t in ("SHADOWEDTOMB", "ST_CORE", "ST_IRONCLAD"))
    has_source_tags = any(t in v for v in meta_values for t in ("Lavf", "x264", "x265", "HandBrake", "FFmpeg"))

    if is_waguri:
        file_status = "⧗ WAGURI Sᴛᴀɴᴅᴀʀᴅ"
    elif not has_source_tags:
        file_status = "✦ Pʀᴇᴍɪᴜᴍ Pᴜʀɢᴇᴅ"
    else:
        file_status = "⇄ Rᴀᴡ Sᴏᴜʀᴄᴇ"

    # ── Split into per-stream blocks
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in raw.splitlines():
        if line.strip() == "":
            if current:
                blocks.append(current)
                current = []
        else:
            current.append(line)
    if current:
        blocks.append(current)

    sections: list[tuple[str, str, list[tuple[str, str]]]] = []
    chapters: list[str] = []
    stream_counts: dict[str, int] = {}

    for block in blocks:
        if not block:
            continue

        section_name = block[0].strip()

        # ── Chapter/Menu block → parse separately
        if section_name.lower().startswith("menu"):
            for line in block[1:]:
                line = line.rstrip()
                if " : " in line:
                    ts, title = line.split(" : ", 1)
                    chapters.append(f"{ts.strip()} — {title.strip()}")
            continue

        if section_name.startswith("Text"):
            section_name = section_name.replace("Text", "Subtitle", 1)

        section_key = section_name.lower().split()[0]
        stream_counts[section_key] = stream_counts.get(section_key, 0) + 1
        header = _HEADERS.get(section_key, f"| |  {section_name.upper()}  | |")

        if "#" in section_name:
            num = section_name.split("#")[-1].strip()
            header = header.replace("  | |", f" #{num}  | |")

        raw_fields: list[tuple[str, str]] = []
        for line in block[1:]:
            line = line.rstrip()
            if not line or line != line.lstrip():
                continue
            if " : " in line:
                k, v = line.split(" : ", 1)
                raw_fields.append((k.rstrip(), v.lstrip()))

        grouped: OrderedDict[str, list[str]] = OrderedDict()
        for k, v in raw_fields:
            ks = k.strip()
            if not ks or ks in _EXCLUDE_KEYS or ks.startswith("0x"):
                continue
            if "conformance" in ks.lower() or "compliance" in ks.lower():
                continue
            grouped.setdefault(ks, []).append(v)

        fields: list[tuple[str, str]] = []
        for k, vals in grouped.items():
            val = _best(k, vals)
            if not val:
                continue
            if k in _SOFT_EXCLUDE and _is_raw_numeric(val):
                continue
            if k == "Complete name":
                val = real_name
            fields.append((_label(k), val))

        if fields:
            sections.append((header, section_name, fields))

    extra_meta = {
        "chapters":      chapters,
        "stream_counts": stream_counts,
    }
    return sections, file_status, extra_meta


async def _run_mediainfo(path: str, real_name: str) -> tuple[list, str, dict]:
    return await asyncio.to_thread(_get_mediainfo_sections, path, real_name)


# ─────────────────────────────────────────────────────────────
#  | 𝗦𝗨𝗠𝗠𝗔𝗥𝗬 𝗕𝗨𝗜𝗟𝗗𝗘𝗥 |
# ─────────────────────────────────────────────────────────────

def _extract_section_dict(sections: list, keyword: str, n: int = 0) -> dict:
    hits = [f for _, t, f in sections if keyword in t.lower()]
    return dict(hits[n]) if n < len(hits) else {}


def _build_summary_lines(
    sections: list,
    file_name: str,
    file_size: int,
    mime_type: str,
    file_status: str,
    extra_meta: dict,
) -> list[str]:
    """
    Compact Telegram message — essentials only.
    All deep detail lives on Telegraph.
    FIX #8 — every dynamic value is html.escape()'d.
    FIX #7 — multi-audio track count shown.
    """
    g  = _extract_section_dict(sections, "general")
    vf = _extract_section_dict(sections, "video",   0)
    af = _extract_section_dict(sections, "audio",   0)
    sc = extra_meta.get("stream_counts", {})
    summary: list[str] = []

    # ── Identity
    summary.append(f"<b>◍ Sᴛᴀᴛᴜs  :</b> <code>{html.escape(file_status)}</code>")
    summary.append(f"<b>◍ Fɪʟᴇ    :</b> <code>{html.escape(file_name)}</code>")
    summary.append(f"<b>◍ Sɪᴢᴇ    :</b> <code>{html.escape(_fmt_size(file_size))}</code>")

    if mime_type:
        summary.append(f"<b>◍ Fᴏʀᴍᴀᴛ  :</b> <code>{html.escape(mime_type)}</code>")

    dur = g.get("Duration", "")
    if dur:
        summary.append(f"<b>◍ Dᴜʀ     :</b> <code>{html.escape(dur)}</code>")

    # ── Video — resolution + codec+profile
    w, h = vf.get("Width", ""), vf.get("Height", "")
    if w and h:
        summary.append(f"<b>◍ Rᴇs     :</b> <code>{html.escape(w)} × {html.escape(h)}</code>")

    v_fmt = vf.get("Format", "")
    if v_fmt:
        v_profile = vf.get("Profile", "").split("@")[0].strip()
        v_str = v_fmt + (f" {v_profile}" if v_profile else "")
        summary.append(f"<b>◍ Vɪᴅᴇᴏ   :</b> <code>{html.escape(v_str)}</code>")

    # ── Audio — Format · channels · @ bitrate · sample rate  (FIX #7: + track count)
    a_fmt = af.get("Format", "")
    if a_fmt:
        a_parts = [a_fmt]
        a_ch  = af.get("Channels", "")
        a_chl = af.get("Ch. layout", "")
        if a_ch:
            ch_str = a_ch.replace(" channels", " ch").replace(" channel", " ch")
            if a_chl:
                ch_str += f" ({a_chl})"
            a_parts.append(ch_str)
        a_br = af.get("Bitrate", "")
        if a_br:
            a_parts.append(f"@ {a_br}")
        a_sr = af.get("Sample rate", "")
        if a_sr:
            a_parts.append(a_sr)
        # show total audio track count if > 1
        total_audio = sc.get("audio", 0)
        if total_audio > 1:
            a_parts.append(f"[{total_audio} tracks]")
        summary.append(f"<b>◍ Aᴜᴅɪᴏ   :</b> <code>{html.escape(' · '.join(a_parts))}</code>")

    # ── Stream count bar
    counts = []
    for lbl, key in [("V", "video"), ("A", "audio"), ("S", "subtitle")]:
        n = sc.get(key, 0)
        if n:
            counts.append(f"{n}{lbl}")
    if counts:
        summary.append(f"<b>◍ Sᴛʀᴇᴀᴍs :</b> <code>{html.escape(' · '.join(counts))}</code>")

    return summary


# ─────────────────────────────────────────────────────────────
#  | 𝗧𝗘𝗟𝗘𝗚𝗥𝗔𝗣𝗛 𝗕𝗨𝗜𝗟𝗗𝗘𝗥 |
# ─────────────────────────────────────────────────────────────

async def _verify_banner():
    global _img_verified
    if not _TELEGRAPH_IMAGE:
        return
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.head(_TELEGRAPH_IMAGE, timeout=5) as resp:
                _img_verified = resp.status == 200
    except Exception:
        _img_verified = False


_SEP = "─" * 42


def _build_pre_block(fields: list[tuple[str, str]]) -> str:
    """
    Render fields as a <pre> block — every k and v is html.escape()'d (FIX #2).
    Key column is padded to 28 chars for clean alignment without <table>.
    """
    lines = ""
    for k, v in fields:
        ek = html.escape(k)
        ev = html.escape(v)
        lines += f"{ek:<28}: {ev}\n"
    return f"<pre>{html.escape(_SEP)}\n{lines}{html.escape(_SEP)}</pre>"


def _build_page(
    file_name: str,
    sections: list,
    extra_meta: dict,
    file_size: int,
    mime_type: str,
    file_status: str,
) -> str:
    tc = ""
    if _TELEGRAPH_IMAGE and _img_verified:
        tc += f'<img src="{html.escape(_TELEGRAPH_IMAGE)}"><br>'

    tc += (
        f"<h3>{_MAIN_HEADER}</h3>"
        f"<p><i>Generated by WAGURI Bot · @NyxRexy</i></p>"
        f"<h4>[ ✦ ]  {html.escape(file_name)}</h4>"
    )
    # File summary header block — all values escaped (FIX #2)
    tc += _build_pre_block([
        ("Status",  file_status),
        ("Size",    _fmt_size(file_size)),
        ("MIME",    mime_type or "N/A"),
    ])
    tc += "<br>"

    # ── All parsed sections
    for header, title, fields in sections:
        tc += f"<h4>{html.escape(header)}</h4>"
        tc += _build_pre_block(fields)

    # ── Chapters appendix — FIX #2: escape all chapter text
    chaps = extra_meta.get("chapters", [])
    if chaps:
        tc += f"<h4>{_HEADERS['chapters']}</h4>"
        chap_fields = [
            (c.split(" — ")[0], c.split(" — ", 1)[1] if " — " in c else c)
            for c in chaps
        ]
        tc += _build_pre_block(chap_fields)

    return tc


# ─────────────────────────────────────────────────────────────
#  | 𝗖𝗢𝗠𝗠𝗔𝗡𝗗 𝗛𝗔𝗡𝗗𝗟𝗘𝗥 |
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("mediainfo"))
@check_ban_status
async def mediainfo_command(client: Client, message: Message) -> None:
    global _img_checked
    user_id = message.from_user.id

    if not _img_checked and _TELEGRAPH_IMAGE:
        await _verify_banner()
        _img_checked = True

    # FIX #11 — show remaining seconds dynamically
    on_cd, remaining = _on_cooldown(user_id)
    if on_cd:
        logger.debug(f"[mediainfo] cooldown user={user_id} remaining={remaining:.1f}s")
        return await message.reply_text(
            f"<blockquote>⧗ 𝗥𝗔𝗧𝗘 𝗟𝗜𝗠𝗜𝗧\n\n"
            f"◍ Pʟᴇᴀsᴇ ᴡᴀɪᴛ <code>{remaining:.0f}s</code></blockquote>",
            parse_mode=ParseMode.HTML, quote=True)

    replied = message.reply_to_message
    if not replied:
        return await message.reply_text(
            "<blockquote>ⓘ 𝗨𝗦𝗔𝗚𝗘</blockquote>\n\n"
            "◍ Rᴇᴘʟʏ ᴛᴏ ᴀɴʏ ꜰɪʟᴇ ᴡɪᴛʜ <code>/mediainfo</code>",
            parse_mode=ParseMode.HTML, quote=True)

    media = (replied.document or replied.video or replied.audio
             or replied.voice or replied.animation or replied.video_note)
    if not media:
        return await message.reply_text(
            "<blockquote>✗ Nᴏ sᴜᴘᴘᴏʀᴛᴇᴅ ꜰɪʟᴇ ꜰᴏᴜɴᴅ ɪɴ ʀᴇᴘʟʏ.</blockquote>",
            parse_mode=ParseMode.HTML, quote=True)

    file_name: str = getattr(media, "file_name", None) or f"file_{media.file_unique_id}"
    file_size: int = getattr(media, "file_size", 0) or 0
    mime_type: str = getattr(media, "mime_type", "") or ""

    status_msg = await message.reply_text(
        "<blockquote>⧗ 𝗚𝗘𝗡𝗘𝗥𝗔𝗧𝗜𝗡𝗚 𝗠𝗘𝗗𝗜𝗔𝗜𝗡𝗙𝗢...</blockquote>",
        parse_mode=ParseMode.HTML, quote=True)

    local_path: Optional[str] = None
    try:
        await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)
        os.makedirs(_TEMP_DIR, exist_ok=True)

        # FIX #3 — sniff extension from MIME if filename has none
        ext        = _sniff_ext(file_name, mime_type)
        local_path = os.path.join(_TEMP_DIR, f"{uuid.uuid4().hex}{ext}")

        await safe_edit(status_msg,
            "<blockquote>⧗ 𝗦𝗧𝗥𝗘𝗔𝗠𝗜𝗡𝗚 𝗛𝗘𝗔𝗗𝗘𝗥..</blockquote>",
            parse_mode=ParseMode.HTML)

        # FIX #1 + FIX #3 — smart download size
        # Small files: grab everything. Large files: stream _STREAM_LIMIT_BYTES.
        # stream_media limit is in chunks (~64 KiB each), so convert bytes → chunks.
        CHUNK = 64 * 1024
        if file_size and file_size <= _SMALL_FILE_THRESHOLD:
            dl_limit = None   # full download
        else:
            dl_limit = max(1, _STREAM_LIMIT_BYTES // CHUNK)

        # FIX #12 — FloodWait can occur inside stream_media
        try:
            async with aiofiles.open(local_path, "wb") as f:
                async for chunk in client.stream_media(replied, limit=dl_limit):
                    await f.write(chunk)
        except FloodWait as fw:
            raise fw   # re-raise to outer handler

        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            raise RuntimeError("Stream returned empty data.")

        await safe_edit(status_msg,
            "<blockquote>⧗ 𝗔𝗡𝗔𝗟𝗬𝗦𝗜𝗡𝗚 𝗠𝗘𝗗𝗜𝗔𝗜𝗡𝗙𝗢...</blockquote>",
            parse_mode=ParseMode.HTML)

        # FIX #5 — if parse fails on partial data, retry hint
        sections, file_status, extra_meta = await _run_mediainfo(local_path, file_name)
        if not sections:
            raise RuntimeError(
                "MediaInfo could not parse this file. "
                "The container header may be beyond the first 15 MiB."
            )

        await safe_edit(status_msg,
            "<blockquote>⧗ 𝗣𝗨𝗕𝗟𝗜𝗦𝗛𝗜𝗡𝗚 𝗧𝗢 𝗧𝗘𝗟𝗘𝗚𝗥𝗔𝗣𝗛....</blockquote>",
            parse_mode=ParseMode.HTML)

        page_html = _build_page(
            file_name, sections, extra_meta, file_size, mime_type, file_status
        )
        tg         = await asyncio.to_thread(_get_telegraph)
        safe_title = (file_name[:250] + "…") if len(file_name) > 250 else file_name

        resp = await asyncio.to_thread(
            tg.create_page,
            title=safe_title,
            html_content=page_html,
            author_name=_TELEGRAPH_AUTHOR,
            author_url=_TELEGRAPH_AUTHOR_URL,
        )

        # FIX #14 — guard missing 'path' in Telegraph response
        page_path = resp.get("path") if isinstance(resp, dict) else getattr(resp, "path", None)
        if not page_path:
            raise RuntimeError("Telegraph did not return a valid page URL.")
        page_url = f"https://telegra.ph/{page_path}"

        # ── Compact Telegram summary
        summary_lines = _build_summary_lines(
            sections, file_name, file_size, mime_type, file_status, extra_meta
        )
        caption = (
            "<blockquote>✓ 𝗠𝗘𝗗𝗜𝗔 𝗜𝗡𝗙𝗢</blockquote>\n\n"
            + "\n".join(summary_lines)
            + "\n\n<blockquote>➥ Fᴜʟʟ ᴅᴇᴛᴀɪʟs ᴏɴ Tᴇ𝗟𝗲𝗴𝗿𝗮𝗽𝗵 ↓</blockquote>"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❐ Vɪᴇᴡ Fᴜʟʟ Mᴇᴅɪᴀ Iɴꜰᴏ", url=page_url)],
            [InlineKeyboardButton("✕ Cʟᴏsᴇ", callback_data="close_msg")],
        ])

        await safe_edit(status_msg, caption, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        logger.info(f"[mediainfo] user={user_id} url={page_url}")

    except FloodWait as e:
        await safe_edit(status_msg,
            f"<blockquote>⧗ 𝗙𝗟𝗢𝗢𝗗 𝗪𝗔𝗜𝗧\n\n◍ Wᴀɪᴛ <code>{e.value}s</code></blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=close_btn())
    except Exception as exc:
        logger.error(f"[mediainfo] user={user_id}: {exc}", exc_info=True)
        await safe_edit(status_msg,
            f"<blockquote>✗ 𝗘𝗥𝗥𝗢𝗥\n\n◍ <code>{html.escape(str(exc)[:300])}</code></blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=close_btn())
    finally:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError:
                pass
        # FIX #13 — remove empty temp dir
        try:
            if os.path.isdir(_TEMP_DIR) and not os.listdir(_TEMP_DIR):
                os.rmdir(_TEMP_DIR)
        except OSError:
            pass


# ─────────────────────────────────────────────────────────────
#  | 𝗦𝗠𝗔𝗥𝗧 𝗡𝗔𝗠𝗘 𝗖𝗛𝗘𝗖𝗞𝗘𝗥 |
# ─────────────────────────────────────────────────────────────

_CANONICAL_ORDER = [
    "title", "season", "episode", "episode_start", "episode_end",
    "season_episode", "episode_absolute", "episode_type", "type",
    "part", "volume", "chapter", "year", "quality", "source",
    "video_codec", "hdr_format", "bit_depth", "audio_tracks",
    "language", "languages", "audio_codec", "audio_layout",
    "audio", "group", "edition", "release_flags", "version", "crc"
]


def _resolve_check_input(replied: Message) -> str:
    if not replied:
        return ""
    media = (replied.document or replied.video or replied.audio
             or replied.voice or replied.animation or replied.video_note)
    if media and getattr(media, "file_name", None):
        return media.file_name
    elif replied.text:
        return replied.text
    elif replied.caption:
        return replied.caption
    return ""


def _build_canonical_tokens(engine, parsed_data: dict) -> dict:
    tokens = {k: cand.value for k, cand in engine.resolved.items()}
    for k in ("title", "season_episode", "type", "audio", "audio_full"):
        if parsed_data.get(k):
            tokens[k] = parsed_data[k]
    langs = parsed_data.get("languages")
    if langs:
        tokens["languages"] = langs
    return tokens


def _build_check_token_summary(tokens: dict) -> list[str]:
    lines = []
    for key in _CANONICAL_ORDER:
        val = tokens.get(key)
        if val in ("", None, [], {}, False):
            continue
        if key == "watermark" and re.fullmatch(r"S\d{1,2}E\d{1,4}(?:-\d{1,4})?", str(val), re.I):
            continue
        lines.append(f"◍ <code>{{{key}}}</code> : {html.escape(str(val))}")
    for key, val in tokens.items():
        if key not in _CANONICAL_ORDER:
            if val in ("", None, [], {}, False):
                continue
            if key == "watermark" and re.fullmatch(r"S\d{1,2}E\d{1,4}(?:-\d{1,4})?", str(val), re.I):
                continue
            lines.append(f"◍ <code>{{{key}}}</code> : {html.escape(str(val))}")
    return lines if lines else ["◍ Nᴏ ᴘʟᴀᴄᴇʜᴏʟᴅᴇʀs ᴅᴇᴛᴇᴄᴛᴇᴅ."]


def _build_check_confidence_summary(resolved: dict) -> list[str]:
    high, med, low = [], [], []
    for k, cand in resolved.items():
        conf = cand.confidence
        pct  = int(conf * 100)
        line = f"◍ <code>{{{k}}}</code> → {html.escape(str(cand.value))} <i>({pct}%)</i>"
        if conf >= 0.90:   high.append(line)
        elif conf >= 0.75: med.append(line)
        else:              low.append(line)
    lines = []
    if low:  lines.append("<b><blockquote>›› [ Wᴇᴀᴋ Cᴏɴꜰɪᴅᴇɴᴄᴇ ]</b></blockquote>\n"   + "\n".join(low))
    if med:  lines.append("<b><blockquote>›› [ Mᴇᴅɪᴜᴍ Cᴏɴꜰɪᴅᴇɴᴄᴇ ]</b></blockquote>\n" + "\n".join(med))
    if high: lines.append("<b><blockquote>›› [ Hɪɢʜ Cᴏɴꜰɪᴅᴇɴᴄᴇ ]</b></blockquote>\n"   + "\n".join(high))
    return lines if lines else ["◍ Nᴏ ʀᴇsᴏʟᴠᴇᴅ ᴛᴏᴋᴇɴs ꜰᴏᴜɴᴅ."]


def _get_mode_and_why(tokens: dict, mode: str) -> str:
    if mode == "MANGA":
        if "chapter" in tokens or "volume" in tokens:
            return "MANGA (Cʜᴀᴘᴛᴇʀ/Vᴏʟᴜᴍᴇ ᴅᴇᴛᴇᴄᴛᴇᴅ)"
    elif mode == "SERIES":
        if "season" in tokens or "episode" in tokens:
            return "SERIES (Sᴇᴀsᴏɴ/Eᴘɪsᴏᴅᴇ ᴍᴀʀᴋᴇʀs ᴅᴇᴛᴇᴄᴛᴇᴅ)"
    elif mode == "MOVIE":
        return "MOVIE (Nᴏ ᴇᴘɪsᴏᴅɪᴄ/ᴍᴀɴɢᴀ ᴍᴀʀᴋᴇʀs ᴅᴇᴛᴇᴄᴛᴇᴅ)"
    return mode


def _build_missing_ambiguous(tokens: dict) -> list[str]:
    warns = []
    if "quality"  not in tokens: warns.append("◍ Mɪssɪɴɢ `quality`")
    if "group"    not in tokens: warns.append("◍ Mɪssɪɴɢ `group`")
    if "language" not in tokens and "languages" not in tokens:
        warns.append("◍ Mɪssɪɴɢ `language`")
    if "chapter" in tokens and "episode" in tokens:
        warns.append("◍ Aᴍʙɪɢᴜᴏᴜs: Bᴏᴛʜ Cʜᴀᴘᴛᴇʀ & Eᴘɪsᴏᴅᴇ ᴅᴇᴛᴇᴄᴛᴇᴅ")
    title = str(tokens.get("title", ""))
    if title and len(title) < 4:
        warns.append("◍ Tɪᴛʟᴇ ʙᴇᴄᴀᴍᴇ ᴠᴇʀʏ sʜᴏʀᴛ ᴀꜰᴛᴇʀ ᴄʟᴇᴀɴᴜᴘ")
    return warns


def _add_tech_to_templates(parts: list, tokens: dict) -> tuple[str, str]:
    tech = []
    if "edition"     in tokens: tech.append("{edition}")
    if "quality"     in tokens: tech.append("{quality}")
    elif "resolution" in tokens: tech.append("{resolution}")
    if "source"      in tokens: tech.append("{source}")
    if "video_codec" in tokens: tech.append("{video_codec}")
    if "bit_depth"   in tokens: tech.append("{bit_depth}")

    semantic_audio = None
    if   "audio_tracks"  in tokens:                        semantic_audio = "{audio_tracks}"
    elif "languages"     in tokens and tokens["languages"]: semantic_audio = "{languages}"
    elif "language"      in tokens:                        semantic_audio = "{language}"
    elif "release_flags" in tokens:                        semantic_audio = "{release_flags}"
    elif "audio"         in tokens:                        semantic_audio = "{audio}"

    if semantic_audio:        tech.append(semantic_audio)
    if "audio_codec"  in tokens: tech.append("{audio_codec}")
    if "audio_layout" in tokens: tech.append("{audio_layout}")

    parts_1 = parts.copy()
    parts_2 = parts.copy()
    if tech:
        parts_1.append(f"[{' '.join(tech)}]")
        if parts_2 and not parts_2[-1].endswith("-"):
            parts_2.append("-")
        parts_2.extend([f"[{t}]" for t in tech])
    if "crc" in tokens:
        parts_1.append("[{crc}]")
        parts_2.append("[{crc}]")

    r1 = " ".join(parts_1)
    r2 = " ".join(parts_2)
    if not r1.strip() or r1.strip() == "{title}":
        r1 = "{title} [{quality}]" if "quality" in tokens else "{title}"
        r2 = r1
    return r1, r2


def _build_manga_template(tokens: dict) -> tuple[str, str]:
    parts = []
    if "group"  in tokens: parts.append("[{group}]")
    parts.append("{title}")
    vol_chap = []
    if "volume"  in tokens: vol_chap.append("V{volume}")
    if "chapter" in tokens: vol_chap.append("C{chapter}")
    if vol_chap: parts.append("- " + " ".join(vol_chap))
    if "part"   in tokens: parts.append("Part {part}")
    return _add_tech_to_templates(parts, tokens)


def _build_series_template(tokens: dict) -> tuple[str, str]:
    parts = []
    if "group" in tokens: parts.append("[{group}]")
    parts.append("{title}")
    if "episode_start" in tokens and "episode_end" in tokens:
        if "season" in tokens: parts.append("- S{season}E{episode_start}-{episode_end}")
        else:                  parts.append("- E{episode_start}-{episode_end}")
    elif "season_episode"   in tokens: parts.append("- {season_episode}")
    elif "episode_absolute" in tokens: parts.append("- {episode_absolute}")
    elif "episode"          in tokens: parts.append("- E{episode}")
    if "part"         in tokens: parts.append("Part {part}")
    if "year"         in tokens: parts.append("({year})")
    if "type"         in tokens: parts.append("[{type}]")
    elif "episode_type" in tokens: parts.append("[{episode_type}]")
    return _add_tech_to_templates(parts, tokens)


def _build_movie_template(tokens: dict) -> tuple[str, str]:
    parts = []
    if "group" in tokens: parts.append("[{group}]")
    parts.append("{title}")
    if "year" in tokens: parts.append("({year})")
    return _add_tech_to_templates(parts, tokens)


def _build_check_recommendations(tokens: dict, mode: str) -> tuple[str, str]:
    if mode == "MANGA":    return _build_manga_template(tokens)
    elif mode == "SERIES": return _build_series_template(tokens)
    else:                  return _build_movie_template(tokens)


def _render_template(template: str, tokens: dict, ext: str) -> str:
    def repl(m):
        val = tokens.get(m.group(1))
        return str(val) if val is not None else ""
    res = re.sub(r"\{([^}]+)\}", repl, template)
    res = re.sub(r"\[\s*\]|\(\s*\)", "", res)
    res = re.sub(r"\s+", " ", res).strip(" -")
    if ext and len(ext) < 6 and not res.endswith(ext):
        res += ext
    return res


@Client.on_message(filters.private & filters.command("check"))
@check_ban_status
async def check_command(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    on_cd, remaining = _on_cooldown(user_id)
    if on_cd:
        return await message.reply_text(
            f"<blockquote>⧗ 𝗥𝗔𝗧𝗘 𝗟𝗜𝗠𝗜𝗧\n\n"
            f"◍ Pʟᴇᴀsᴇ ᴡᴀɪᴛ <code>{remaining:.0f}s</code></blockquote>",
            parse_mode=ParseMode.HTML, quote=True)

    replied = message.reply_to_message
    if not replied:
        return await message.reply_text(
            "<blockquote>ⓘ 𝗨𝗦𝗔𝗚𝗘</blockquote>\n\n"
            "◍ Rᴇᴘʟʏ ᴛᴏ ᴀɴʏ ꜰɪʟᴇ (ᴏʀ ᴛᴇxᴛ ᴍᴇssᴀɢᴇ) ᴡɪᴛʜ <code>/check</code> ᴛᴏ ᴀɴᴀʟʏᴢᴇ ɪᴛs ɴᴀᴍɪɴɢ.",
            parse_mode=ParseMode.HTML, quote=True)

    file_name = _resolve_check_input(replied)
    if not file_name:
        return await message.reply_text(
            "<blockquote>✗ Cᴏᴜʟᴅ ɴᴏᴛ ᴇxᴛʀᴀᴄᴛ ᴀ ꜰɪʟᴇɴᴀᴍᴇ ᴛᴏ ᴄʜᴇᴄᴋ.</blockquote>",
            parse_mode=ParseMode.HTML, quote=True)

    status_msg = await message.reply_text(
        "<blockquote>⧗ 𝗔𝗡𝗔𝗟𝗬𝗭𝗜𝗡𝗚 𝗡𝗔𝗠𝗘 𝗦𝗧𝗥𝗨𝗖𝗧𝗨𝗥𝗘...</blockquote>",
        parse_mode=ParseMode.HTML, quote=True)

    try:
        engine      = PeakTokenEngineV2_REXY(file_name)
        parsed_data = engine.parse()
        mode        = parsed_data.get("mode", "UNKNOWN")
        tokens      = _build_canonical_tokens(engine, parsed_data)
        mode_why    = _get_mode_and_why(tokens, mode)
        token_summary = "\n".join(_build_check_token_summary(tokens))
        conf_summary  = "\n\n".join(_build_check_confidence_summary(engine.resolved))
        warns     = _build_missing_ambiguous(tokens)
        warns_str = "\n".join(warns) if warns else "◍ Aʟʟ ɢᴏᴏᴅ! Nᴏ ᴀᴍʙɪɢᴜɪᴛɪᴇs ꜰᴏᴜɴᴅ."
        rec_1, rec_2 = _build_check_recommendations(tokens, mode)
        ext          = os.path.splitext(file_name)[1] if "." in file_name else ""
        preview_1    = _render_template(rec_1, tokens, ext)
        preview_2    = _render_template(rec_2, tokens, ext)

        response = (
            f"<blockquote>✦ 𝗦𝗠𝗔𝗥𝗧 𝗡𝗔𝗠𝗘 𝗗𝗜𝗔𝗚𝗡𝗢𝗦𝗧𝗜𝗖𝗦</blockquote>\n\n"
            f"<b>Oʀɪɢɪɴᴀʟ : </b><code>{html.escape(file_name)}</code>\n\n"
            f"<b>Mᴏᴅᴇ :</b> <code>{mode_why}</code>\n\n────────────────────\n\n"
            f"<blockquote>✎ 𝗥𝗘𝗦𝗢𝗟𝗩𝗘𝗗 𝗧𝗢𝗞𝗘𝗡𝗦 :</blockquote>\n"
            f"{token_summary}\n\n────────────────────\n\n"
            f"<blockquote>❐ 𝗖𝗢𝗡𝗙𝗜𝗗𝗘𝗡𝗖𝗘 𝗕𝗔𝗡𝗗𝗦 :</blockquote>\n"
            f"{conf_summary}\n\n────────────────────\n\n"
            f"<blockquote>ⓘ 𝗠𝗜𝗦𝗦𝗜𝗡𝗚 / 𝗔𝗠𝗕𝗜𝗚𝗨𝗢𝗨𝗦 :</blockquote>\n"
            f"{warns_str}\n\n────────────────────\n\n"
            f"<blockquote>❐ 𝗢𝗣𝗧𝗜𝗢𝗡 𝟭 (𝗚𝗥𝗢𝗨𝗣𝗘𝗗) :</blockquote>\n"
            f"<code>{html.escape(rec_1)}</code>\n"
            f"<blockquote>➥𝗣𝗥𝗘𝗩𝗜𝗘𝗪 :</blockquote>\n"
            f"<code>{html.escape(preview_1)}</code>\n\n────────────────────\n\n"
            f"<blockquote>❐ 𝗢𝗣𝗧𝗜𝗢𝗡 𝟮 (𝗦𝗘𝗣𝗔𝗥𝗔𝗧𝗘𝗗) :</blockquote>\n"
            f"<code>{html.escape(rec_2)}</code>\n"
            f"<blockquote>➥ 𝗣𝗥𝗘𝗩𝗜𝗘𝗪 :</blockquote>\n"
            f"<code>{html.escape(preview_2)}</code>\n\n────────────────────"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✕ Cʟᴏsᴇ", callback_data="close_msg")],
        ])
        await safe_edit(status_msg, response, parse_mode=ParseMode.HTML, reply_markup=keyboard)

    except Exception as exc:
        logger.error(f"[check] user={user_id}: {exc}", exc_info=True)
        await safe_edit(status_msg,
            f"<blockquote>✗ 𝗘𝗥𝗥𝗢𝗥\n\n◍ <code>{html.escape(str(exc)[:300])}</code></blockquote>",
            parse_mode=ParseMode.HTML, reply_markup=close_btn())


logger.info("[✓] MediaInfo Plugin Loaded — pymediainfo PRO v3")

