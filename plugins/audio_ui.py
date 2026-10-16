#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ᴀᴜᴅɪᴏ ᴄᴏɴᴠᴇʀsɪᴏɴ ᴜɪ • /audioset               │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇᴀᴅʏ ʙʏ @NyxRexy                  │
#└──────────────────────────────────────────┘
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any, Final, Optional

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, QueryIdInvalid
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from helper.database import DARKXSIDE78 as db
from helper.utils import handle_floodwait
from plugins.metadata import safe_send_photo

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────

AUDIO_SET_PIC: Final[str] = (
    "https://i.ibb.co/CsS9MPsT/photo-2026-05-17-10-01-15-7640798698493968412.jpg"
)

_VALID_FORMATS: Final[tuple[str, ...]] = ("aac", "mp3", "opus", "flac")
_VALID_BITRATES: Final[tuple[str, ...]] = ("64k", "96k", "128k", "192k", "256k", "320k")

_FORMAT_LABELS: Final[dict[str, str]] = {
    "aac":  "AAC  (ʀᴇᴄᴏᴍᴍᴇɴᴅᴇᴅ)",
    "mp3":  "MP3",
    "opus": "OPUS (ʟᴏᴡ sɪᴢᴇ)",
    "flac": "FLAC (ʟᴏssʟᴇss)",
}

_BITRATE_QUALITY: Final[dict[str, str]] = {
    "64k":  "64k  — ᴠᴏɪᴄᴇ",
    "96k":  "96k  — ᴄᴀsᴜᴀʟ",
    "128k": "128k — ɢᴏᴏᴅ",
    "192k": "192k — ʜɪɢʜ",
    "256k": "256k — ᴠᴇʀʏ ʜɪɢʜ",
    "320k": "320k — ᴍᴀx",
}


# Bitrate numeric map for comparison logic
_BITRATE_BPS: Final[dict[str, int]] = {
    "64k":  64_000,
    "96k":  96_000,
    "128k": 128_000,
    "192k": 192_000,
    "256k": 256_000,
    "320k": 320_000,
}

# Codec name → our format key (ffprobe codec_name → _VALID_FORMATS member)
_CODEC_ALIAS: Final[dict[str, str]] = {
    "aac":       "aac",
    "mp3":       "mp3",
    "mp3float":  "mp3",
    "opus":      "opus",
    "flac":      "flac",
}

# Formats where bitrate arg is meaningless (lossless)
_LOSSLESS_FORMATS: Final[frozenset[str]] = frozenset({"flac"})

# Formats that only work in MKV container
_MKV_ONLY_FORMATS: Final[frozenset[str]] = frozenset({"opus"})


# ─────────────────────────────────────────────────────────────
# State dataclass
# ─────────────────────────────────────────────────────────────

@dataclass
class AudioConvState:
    enabled:        bool
    fmt:            str
    bitrate:        str
    last_src_codec: Optional[str] = None
    last_src_bps:   Optional[int] = None

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

async def _load_state(user_id: int) -> AudioConvState:
    prefs = await db.get_user_prefs_batch(user_id)
    raw_bps = prefs.get("audio_last_src_bps")
    return AudioConvState(
        enabled=bool(prefs.get("audio_conv_enabled", False)),
        fmt=str(prefs.get("audio_format", "aac") or "aac"),
        bitrate=str(prefs.get("audio_bitrate", "128k") or "128k"),
        last_src_codec=prefs.get("audio_last_src_codec") or None,
        last_src_bps=int(raw_bps) if raw_bps else None,
    )


def _build_panel_text(state: AudioConvState) -> str:
    status_label = "Eɴᴀʙʟᴇᴅ ◉" if state.enabled else "Dɪsᴀʙʟᴇᴅ"
    fmt_label    = _FORMAT_LABELS.get(state.fmt, state.fmt.upper())
    br_label     = _BITRATE_QUALITY.get(state.bitrate, state.bitrate)
    # Source info line (only if we have previous probe data)
    src_line = ""
    if state.last_src_codec and state.last_src_bps:
        src_bps_k = state.last_src_bps // 1000
        src_line = (
            f"<b>◈ Lᴀsᴛ Sʀᴄ ▹</b> "
            f"<code>{state.last_src_codec.upper()} @ {src_bps_k}k</code>"
        )
    
    # Downgrade warning
    downgrade_line = ""
    if (
        state.last_src_bps
        and state.fmt not in _LOSSLESS_FORMATS
        and _BITRATE_BPS.get(state.bitrate, 0) >= state.last_src_bps
        and _CODEC_ALIAS.get(state.last_src_codec or "", "") == state.fmt
    ):
        downgrade_line = (
            "⚠ <b>Wᴀʀɴɪɴɢ:</b> <i>ᴛᴀʀɢᴇᴛ ʙɪᴛʀᴀᴛᴇ ɪs ʜɪɢʜᴇʀ ᴛʜᴀɴ sᴏᴜʀᴄᴇ — "
            "ᴘʀᴏᴄᴇss ᴡɪʟʟ ʙᴇ sᴋɪᴘᴘᴇᴅ ᴛᴏ ᴀᴠᴏɪᴅ ᴡᴀsᴛᴇᴅ sᴘᴀᴄᴇ</i>"
        )
    
    # OPUS container warning
    opus_line = ""
    if state.fmt in _MKV_ONLY_FORMATS:
        opus_line = "ⓘ <i>OPUS ᴏɴʟʏ ᴡᴏʀᴋs ɪɴ .ᴍᴋᴠ ᴄᴏɴᴛᴀɪɴᴇʀs</i>"
    
    # FLAC bitrate note
    flac_line = ""
    if state.fmt in _LOSSLESS_FORMATS:
        flac_line = "ⓘ <i>FLAC ɪs ʟᴏssʟᴇss — ʙɪᴛʀᴀᴛᴇ sᴇʟᴇᴄᴛɪᴏɴ ɪɢɴᴏʀᴇᴅ</i>"

    lines = [
        "<b><blockquote>✦ 𝗔𝗨𝗗𝗜𝗢 𝗖𝗢𝗡𝗩𝗘𝗥𝗦𝗜𝗢𝗡 𝗖𝗘𝗡𝗧𝗘𝗥</blockquote></b>",
        "",
        f"<b>◈ Sᴛᴀᴛᴜs ▹</b> <code>{status_label}</code>",
        f"<b>◈ Fᴏʀᴍᴀᴛ ▹</b> <code>{fmt_label}</code>",
        f"<b>◈ Bɪᴛʀᴀᴛᴇ ▹</b> <code>{br_label}</code>"
    ]
    if src_line:
        lines.append(src_line)
    if downgrade_line:
        lines.append(downgrade_line)
    if opus_line:
        lines.append(opus_line)
    if flac_line:
        lines.append(flac_line)

    lines.extend([
        "",
        "<blockquote expandable>◍ <b>Hᴏᴡ ɪᴛ ᴡᴏʀᴋs</b>: ᴡʜᴇɴ ᴇɴᴀʙʟᴇᴅ, ᴛʜᴇ ʙᴏᴛ ʀᴇ‑ᴇɴᴄᴏᴅᴇs ᴛʜᴇ ᴀᴜᴅɪᴏ ᴛʀᴀᴄᴋ "
        "ᴛᴏ ʏᴏᴜʀ ᴄʜᴏsᴇɴ ғᴏʀᴍᴀᴛ / ʙɪᴛʀᴀᴛᴇ ᴡʜɪʟᴇ ᴋᴇᴇᴘɪɴɢ ᴛʜᴇ ᴠɪᴅᴇᴏ sᴛʀᴇᴀᴍ ɪɴᴛᴀᴄᴛ (sᴛʀᴇᴀᴍ ᴄᴏᴘʏ). "
        "Tʜɪs ᴀᴅᴅs ~10–30s ᴛᴏ ᴘʀᴏᴄᴇssɪɴɢ ᴛɪᴍᴇ.</blockquote>"
    ])
    
    return "\n".join(lines)


def _build_main_kb(user_id: int, enabled: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"{'◉ ' if enabled else ''}Oɴ",
                callback_data=f"aud:toggle:on:{user_id}",
            ),
            InlineKeyboardButton(
                f"{'◉ ' if not enabled else ''}Oғғ",
                callback_data=f"aud:toggle:off:{user_id}",
            ),
        ],
        [
            InlineKeyboardButton("🎵 Fᴏʀᴍᴀᴛ",  callback_data=f"aud:fmt_menu:{user_id}"),
            InlineKeyboardButton("⚡ Bɪᴛʀᴀᴛᴇ", callback_data=f"aud:br_menu:{user_id}"),
        ],
        [InlineKeyboardButton("Cʟᴏsᴇ", callback_data=f"aud:close:{user_id}")],
    ])


def _build_fmt_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    row: list[InlineKeyboardButton] = []
    for fmt, label in _FORMAT_LABELS.items():
        row.append(InlineKeyboardButton(label, callback_data=f"aud:save_fmt:{fmt}:{user_id}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"aud:main:{user_id}")])
    return InlineKeyboardMarkup(rows)


def _build_br_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    row: list[InlineKeyboardButton] = []
    for br, label in _BITRATE_QUALITY.items():
        row.append(InlineKeyboardButton(label, callback_data=f"aud:save_br:{br}:{user_id}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"aud:main:{user_id}")])
    return InlineKeyboardMarkup(rows)


def _parse_owner(data: str) -> Optional[int]:
    try:
        return int(data.rsplit(":", 1)[-1])
    except (TypeError, ValueError):
        return None


async def _edit_panel(
    query: CallbackQuery,
    text: str,
    markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """Edit the panel whether it is a photo caption or plain text."""
    if not query.message:
        return
    client = getattr(query.message, "_client", None) or getattr(query, "_client", None)
    if client is None:
        return
    chat_id    = query.message.chat.id
    message_id = query.message.id
    try:
        await client.edit_message_caption(
            chat_id=chat_id,
            message_id=message_id,
            caption=text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )
        return
    except MessageNotModified:
        raise
    except Exception:
        pass
    await client.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        reply_markup=markup,
        parse_mode=ParseMode.HTML,
    )


async def _refresh_panel(query: CallbackQuery, user_id: int) -> None:
    state = await _load_state(user_id)
    try:
        await _edit_panel(
            query,
            _build_panel_text(state),
            _build_main_kb(user_id, state.enabled),
        )
    except MessageNotModified:
        pass


# ─────────────────────────────────────────────────────────────
# Engine Methods
# ─────────────────────────────────────────────────────────────

async def _probe_audio_stream(path: str) -> Optional[tuple[str, int]]:
    """
    Returns (codec_name, bit_rate_bps) for the first audio stream.
    Returns None on any failure — caller must treat as unknown.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-analyzeduration", "100000",
        "-probesize", "500000",
        "-select_streams", "a:0",
        "-show_entries", "stream=codec_name,bit_rate",
        "-of", "json",
        path,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            logger.warning("_probe_audio_stream timed out for %s", path)
            return None
        data = json.loads(stdout)
        streams = data.get("streams", [])
        if not streams:
            return None
        codec = streams[0].get("codec_name") or None
        raw   = streams[0].get("bit_rate")
        bps   = int(raw) if raw else None
        # Do NOT fall back to format-level bitrate — that's the total
        # container bitrate (video+audio), not the audio stream bitrate.
        # If stream-level is absent (normal for MKV), return None for bps
        # so callers treat it as unknown and always transcode.
        if codec:
            return codec, bps or 0
        return None
    except Exception as exc:
        logger.warning("_probe_audio_stream failed for %s: %s", path, exc)
        return None


_libfdk_aac_available: Optional[bool] = None


def _resolve_ffmpeg_bin() -> Optional[str]:
    for key in ("FFMPEG_PATH", "FFMPEG"):
        path = os.environ.get(key)
        if path and os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return shutil.which("ffmpeg")


def _ffmpeg_has_libfdk_aac() -> bool:
    """Cache whether the server's FFmpeg build includes libfdk_aac (3-5× faster AAC)."""
    global _libfdk_aac_available
    if _libfdk_aac_available is not None:
        return _libfdk_aac_available
    ffmpeg = _resolve_ffmpeg_bin()
    if not ffmpeg:
        _libfdk_aac_available = False
        return False
    try:
        proc = subprocess.run(
            [ffmpeg, "-hide_banner", "-codecs"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        _libfdk_aac_available = "libfdk_aac" in (proc.stdout or "")
    except Exception:
        _libfdk_aac_available = False
    if _libfdk_aac_available:
        logger.info("libfdk_aac detected — using fast AAC encoder")
    return _libfdk_aac_available


def get_audio_stream_at(probed_streams: list, track_idx: int) -> Optional[dict]:
    audio_streams = [s for s in probed_streams if s.get("codec_type") == "audio"]
    if track_idx < len(audio_streams):
        return audio_streams[track_idx]
    return None


def can_passthrough_audio_track(
    src_codec: Optional[str],
    src_bps: int,
    target_fmt: str,
    target_br: str,
) -> bool:
    """
    True only when audio is CONFIRMED to already meet or exceed target quality.
    When source bitrate is unknown (src_bps == 0), always encode — we cannot
    guarantee the source is at or below target without proof.
    """
    if not src_codec:
        return False
    return _should_skip_transcode(src_codec, src_bps, target_fmt, target_br)


def _should_skip_transcode(
    src_codec: str,
    src_bps: int,
    target_fmt: str,
    target_br: str,
) -> bool:
    """
    True → skip re-encode, use stream copy instead.
    Condition: same codec family AND target bitrate ≤ source bitrate.
    FLAC (lossless) always transcodes when going lossy.
    """
    if src_codec in _LOSSLESS_FORMATS:
        # Source is lossless → always encode when going to lossy format
        return False
    if target_fmt in _LOSSLESS_FORMATS:
        # Going to lossless → always encode (pointless to skip)
        return False
    # bps=0 means unknown (MKV streams don't store per-stream bitrate)
    # Never skip when we can't confirm source is actually higher quality
    if src_bps == 0:
        return False
    tgt_bps = _BITRATE_BPS.get(target_br, 0)
    same_codec = _CODEC_ALIAS.get(src_codec, "") == target_fmt
    return same_codec and tgt_bps >= src_bps


def _build_skip_reason(src_codec: str, src_bps: int, target_br: str) -> str:
    """Human-readable skip reason for user notification."""
    src_k = src_bps // 1000
    return (
        f"⚡ <b>Sᴋɪᴘᴘᴇᴅ ʀᴇ‑ᴇɴᴄᴏᴅᴇ</b>\n"
        f"◈ Sᴏᴜʀᴄᴇ ▹ <code>{src_codec.upper()} @ {src_k}k</code>\n"
        f"◈ Tᴀʀɢᴇᴛ ▹ <code>{target_br}</code> — ʜɪɢʜᴇʀ ᴛʜᴀɴ sᴏᴜʀᴄᴇ\n"
        f"◈ Aᴄᴛɪᴏɴ ▹ <code>Sᴛʀᴇᴀᴍ ᴄᴏᴘʏ (ʟᴏssʟᴇss ᴘᴀssᴛʜʀᴏᴜɢʜ)</code>"
    )

def _extract_audio_info_from_streams(
    streams: list,
) -> Optional[tuple[str, int]]:
    """
    Extract (codec_name, bit_rate_bps) from already-probed ffprobe stream list.
    Mirrors _probe_audio_stream output format — no subprocess.
    """
    for s in streams:
        if s.get("codec_type") == "audio":
            codec = s.get("codec_name") or None
            raw = s.get("bit_rate")
            # Return 0 for bps if absent — caller must treat as unknown
            bps = int(raw) if raw else 0
            if codec:
                return codec, bps
    return None


async def resolve_audio_transcode(
    user_id: int,
    input_path: str,
    prefs: dict,
    probed_streams: Optional[list] = None,
) -> dict:
    """
    Call this from file_rename.py before launching FFmpeg.

    Returns a dict:
      {
        "skip":        bool,         # True → use stream copy
        "ffmpeg_args": list[str],    # ready-to-splice FFmpeg audio args
        "skip_reason": str | None,   # user-facing text if skipped
        "src_codec":   str | None,
        "src_bps":     int | None,
      }
    """
    enabled = bool(prefs.get("audio_conv_enabled", False))
    target_fmt = str(prefs.get("audio_format", "aac") or "aac")
    target_br  = str(prefs.get("audio_bitrate", "128k") or "128k")

    if not enabled:
        return {
            "skip": True,
            "ffmpeg_args": ["-acodec", "copy"],
            "skip_reason": None,
            "src_codec": None,
            "src_bps": None,
            "target_fmt": target_fmt,
            "target_br": target_br,
        }

    if probed_streams is not None:
        probe = _extract_audio_info_from_streams(probed_streams)
    else:
        probe = await _probe_audio_stream(input_path)
    
    src_codec, src_bps = probe if probe else (None, None)

    # Persist source info for panel display
    if src_codec and src_bps:
        try:
            await db.set_audio_last_src_info(user_id, src_codec, src_bps)
        except Exception as exc:
            logger.warning("Failed to save src audio info: %s", exc)

    # Skip logic
    if src_codec and src_bps and _should_skip_transcode(src_codec, src_bps, target_fmt, target_br):
        return {
            "skip": True,
            "ffmpeg_args": ["-acodec", "copy"],
            "skip_reason": _build_skip_reason(src_codec, src_bps, target_br),
            "src_codec": src_codec,
            "src_bps": src_bps,
            "target_fmt": target_fmt,
            "target_br": target_br,
        }

    # Build encode args
    ffmpeg_args = _build_ffmpeg_audio_args(target_fmt, target_br)
    return {
        "skip": False,
        "ffmpeg_args": ffmpeg_args,
        "skip_reason": None,
        "src_codec": src_codec,
        "src_bps": src_bps,
        "target_fmt": target_fmt,
        "target_br": target_br,
    }


def _build_ffmpeg_audio_args(fmt: str, bitrate: str) -> list[str]:
    """Returns FFmpeg audio stream args for the given format/bitrate."""
    bps_int = _BITRATE_BPS.get(bitrate, 0)
    # MKV muxer doesn't write BPS tags — force them so MediaInfo reads correctly
    bps_tags = (
        ["-metadata:s:a:0", f"BPS={bps_int}", "-metadata:s:a:0", f"BPS-eng={bps_int}"]
        if bps_int else []
    )
    if fmt == "aac":
        if _ffmpeg_has_libfdk_aac():
            return [
                "-acodec", "libfdk_aac",
                "-profile:a", "aac_low",  # fastest libfdk profile (~3-5× native aac)
                "-b:a", bitrate,
                "-ar", "48000",
            ] + bps_tags
        return [
            "-acodec", "aac",
            "-b:a", bitrate,
            "-aac_coder", "fast",   # twoloop is slow; fast uses const quantizer (~25-50x realtime)
            "-cutoff", "18000",     # skip dynamic cutoff calculation overhead
            "-ar", "48000",         # normalize sample rate; avoids mid-stream resampler init
        ] + bps_tags

    if fmt == "mp3":
        return [
            "-acodec", "libmp3lame",
            "-b:a", bitrate,
            "-compression_level", "7",  # 0=slowest/best, 9=fastest; 7 = ~4x faster vs default (2)
        ] + bps_tags

    if fmt == "opus":
        return [
            "-acodec", "libopus",
            "-b:a", bitrate,
            "-application", "voip",         # voip = fastest; "audio" runs full psychoacoustic model
            "-frame_duration", "60",         # 60ms = 3x less frame overhead vs 20ms default
            "-vbr", "on",                    # VBR skips CBR rate-control loop — faster
            "-compression_level", "3",       # 0-10; 3 is fast with good quality vs default 10
        ] + bps_tags

    if fmt == "flac":
        # FLAC: lossless, bitrate is variable — don't write a fixed BPS tag
        # compression_level 0 = Rice coding only, ~6x faster than default 5, ~3% larger file
        return ["-acodec", "flac", "-compression_level", "0"]

    # Fallback
    return ["-acodec", "aac", "-b:a", bitrate, "-aac_coder", "fast"] + bps_tags

# ─────────────────────────────────────────────────────────────
# Command handler
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("audioset") & filters.private)
async def audioset_cmd(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    state   = await _load_state(user_id)
    await safe_send_photo(
        client,
        message.chat.id,
        AUDIO_SET_PIC,
        _build_panel_text(state),
        _build_main_kb(user_id, state.enabled),
    )


# ─────────────────────────────────────────────────────────────
# Callback router
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^aud:"))
async def audio_router(client: Client, query: CallbackQuery) -> None:
    try:
        if not query.data:
            await query.answer("Invalid request.")
            return

        owner_id = _parse_owner(query.data)
        if owner_id is None or not query.from_user or query.from_user.id != owner_id:
            await query.answer("ⓘ ᴛʜɪs ɪs ɴᴏᴛ ʏᴏᴜʀ ᴘᴀɴᴇʟ !", show_alert=True)
            return

        parts  = query.data.split(":")
        action = parts[1]

        # ── Main panel ──────────────────────────────────────────────
        if action == "main":
            await _refresh_panel(query, owner_id)
            return

        # ── Close panel ─────────────────────────────────────────────
        if action == "close":
            try:
                await query.message.delete()
            except Exception:
                pass
            await query.answer("Panel closed.")
            return

        # ── Toggle on/off ────────────────────────────────────────────
        if action == "toggle":
            enabled = parts[2] == "on"
            await db.set_audio_conv_enabled(owner_id, enabled)
            await query.answer(f"✓ Audio Conversion {'Enabled' if enabled else 'Disabled'}")
            await _refresh_panel(query, owner_id)
            return

        # ── Format menu ──────────────────────────────────────────────
        if action == "fmt_menu":
            await _edit_panel(
                query,
                "<blockquote>🎵 𝗔𝗨𝗗𝗜𝗢 𝗙𝗢𝗥𝗠𝗔𝗧</blockquote>\n"
                "<blockquote>≡ ᴄʜᴏᴏsᴇ ᴛʜᴇ ᴏᴜᴛᴘᴜᴛ ᴀᴜᴅɪᴏ ᴄᴏᴅᴇᴄ. <b>AAC</b> ɪs "
                "ʀᴇᴄᴏᴍᴍᴇɴᴅᴇᴅ ғᴏʀ ᴍᴏsᴛ ᴠɪᴅᴇᴏs. <b>OPUS</b> ᴡᴏʀᴋs ɪɴ ᴍᴋᴠ ᴏɴʟʏ.</blockquote>",
                _build_fmt_kb(owner_id),
            )
            return

        # ── Save format ──────────────────────────────────────────────
        if action == "save_fmt" and len(parts) >= 4:
            fmt = parts[2]
            if fmt not in _VALID_FORMATS:
                await query.answer("✗ Invalid format.", show_alert=True)
                return
            await db.set_audio_format(owner_id, fmt)
            await query.answer(f"[ ✓ ] Format set to {fmt.upper()}")
            await _refresh_panel(query, owner_id)
            return

        # ── Bitrate menu ─────────────────────────────────────────────
        if action == "br_menu":
            await _edit_panel(
                query,
                "<blockquote>⚡ 𝗔𝗨𝗗𝗜𝗢 𝗕𝗜𝗧𝗥𝗔𝗧𝗘</blockquote>\n"
                "<blockquote>≡ ʜɪɢʜᴇʀ ʙɪᴛʀᴀᴛᴇ = ʙᴇᴛᴛᴇʀ sᴏᴜɴᴅ ǫᴜᴀʟɪᴛʏ ʙᴜᴛ ʟᴀʀɢᴇʀ ғɪʟᴇ. "
                "<b>128k</b> ɪs ᴛʜᴇ sᴡᴇᴇᴛ sᴘᴏᴛ ғᴏʀ ᴍᴏsᴛ ᴜsᴇʀs.</blockquote>",
                _build_br_kb(owner_id),
            )
            return

        # ── Save bitrate ─────────────────────────────────────────────
        if action == "save_br" and len(parts) >= 4:
            br = parts[2]
            if br not in _VALID_BITRATES:
                await query.answer("✗ Invalid bitrate.", show_alert=True)
                return
            await db.set_audio_bitrate(owner_id, br)
            await query.answer(f"[ ✓ ] Bitrate set to {br}")
            await _refresh_panel(query, owner_id)
            return

        await query.answer("Unknown action.")

    except FloodWait as exc:
        await handle_floodwait(exc.value)
    except QueryIdInvalid:
        logger.warning("Ignored expired audio_ui callback query")
    except MessageNotModified:
        pass
    except Exception as exc:
        logger.error("audio_router error: %s", exc, exc_info=True)
        try:
            await query.answer(f"Error: {str(exc)[:120]}", show_alert=True)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Init hook
# ─────────────────────────────────────────────────────────────

async def init(*args: Any, **kwargs: Any) -> None:
    logger.info("[✓] Audio Conversion UI Module Initialized")


__all__ = [
    "audioset_cmd",
    "audio_router",
    "resolve_audio_transcode",
    "init",
]
