from __future__ import annotations

import asyncio
import io
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Final, List, Optional, Tuple, Union

from PIL import Image, ImageDraw, ImageFont
from pyrogram import Client, StopPropagation, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, QueryIdInvalid
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from helper.database import DARKXSIDE78 as db
from helper.utils import handle_floodwait
from plugins.metadata import safe_send_photo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
INTRO_PIC: Final[str] = (
    "https://i.ibb.co/CsS9MPsT/photo-2026-05-17-10-01-15-7640798698493968412.jpg"
)
DEFAULT_INTRO_TEXT: Final[str] = "── SHADOWEDTOMB ──"
DEFAULT_INTRO_TIMER: Final[str] = "5"
DEFAULT_FONT_SIZE: Final[str] = "60"
MAX_INTRO_TEXT_LEN: Final[int] = 50

HEX_COLOR_RE: Final[re.Pattern[str]]       = re.compile(r"^[0-9A-F]{6}$")
HEX_COLOR_SHORT_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9A-F]{3}$")

# Pre-compiled SRT timestamp pattern for shift operations
_SRT_TIME_RE: Final[re.Pattern[str]] = re.compile(
    r"(?P<sh>\d{2,}):(?P<sm>\d{2}):(?P<ss>\d{2}),(?P<sms>\d{1,3})"
    r"\s*-->\s*"
    r"(?P<eh>\d{2,}):(?P<em>\d{2}):(?P<es>\d{2}),(?P<ems>\d{1,3})"
)

# Pre-compiled VTT timestamp pattern
_VTT_TIME_RE: Final[re.Pattern[str]] = re.compile(
    # WebVTT permits the hours field to be omitted (MM:SS.mmm), which is
    # common in subtitle tracks exported by browsers and streaming tools.
    r"(?:(?P<sh>\d{2,}):)?(?P<sm>\d{2}):(?P<ss>\d{2})\.(?P<sms>\d{1,3})"
    r"\s*-->\s*"
    r"(?:(?P<eh>\d{2,}):)?(?P<em>\d{2}):(?P<es>\d{2})\.(?P<ems>\d{1,3})"
)

# Pre-compiled LRC timestamp pattern
_LRC_TIME_RE: Final[re.Pattern[str]] = re.compile(r"\[(\d{2}):(\d{2}\.\d{2})\]")

# MicroDVD/SUB frame range pattern
_SUB_FRAME_RE: Final[re.Pattern[str]] = re.compile(r"\{(\d+)\}\{(\d+)\}")

# VTT cue ID: any line before a timestamp that is not itself a timestamp
_VTT_CUE_ID_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?!(?:\d{2}:)?\d{2}:\d{2}\.\d{1,3}\s*-->)[^\n]+\n"
    r"(?=(?:\d{2}:)?\d{2}:\d{2}\.\d{1,3}\s*-->)",
    re.MULTILINE,
)

# TTML namespaces
_TTML_NS  = "http://www.w3.org/ns/ttml"
_TTML_TTS = "http://www.w3.org/ns/ttml#styling"

_SHADOW_NAMES: Final[Dict[str, str]] = {
    "0": "Nᴏɴᴇ",
    "2": "Cʟᴀssɪᴄ",
    "8": "Gʟᴏᴡ",
}

_FONT_CANDIDATES: Final[Tuple[str, ...]] = (
    "arial.ttf",
    "Arial.ttf",
    "DejaVuSans.ttf",
    "LiberationSans-Regular.ttf",
    "FreeSans.ttf",
    "sans-serif.ttf",
)

_FONT_SIZE_NAMES: Final[Dict[str, str]] = {
    "40": "Sᴍᴀʟʟ: 40",
    "60": "Mᴇᴅɪᴜᴍ: 60",
    "80": "Lᴀʀɢᴇ: 80",
    "120": "X-Lᴀʀɢᴇ: 120",
}


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class SubtitleFormat(str, Enum):
    ASS      = "ass"
    SSA      = "ssa"
    SRT      = "srt"
    VTT      = "vtt"
    TTML     = "ttml"
    DFXP     = "dfxp"
    LRC      = "lrc"
    SUB      = "sub"
    MOV_TEXT = "mov_text"


class SubColor(Enum):
    CYAN    = ("Cʏᴀɴ",    "&HFFFF00&", "#00FFFF")
    GOLD    = ("Gᴏʟᴅ",    "&H00D7FF&", "#FFD700")
    RUBY    = ("Rᴜʙʏ",    "&H0000FF&", "#FF0000")
    EMERALD = ("Eᴍᴇʀᴀʟᴅ", "&H00FF00&", "#00FF00")
    WHITE   = ("Wʜɪᴛᴇ",   "&HFFFFFF&", "#FFFFFF")
    ORANGE  = ("Oʀᴀɴɢᴇ",  "&H00A5FF&", "#FFA500")
    BLACK   = ("Bʟᴀᴄᴋ",   "&H000000&", "#000000")

    @property
    def label(self) -> str:
        return self.value[0]

    @property
    def ass_hex(self) -> str:
        return self.value[1]

    @property
    def srt_hex(self) -> str:
        return self.value[2]


class SubPos(Enum):
    TOP    = ("Tᴏᴘ",    "8")
    MIDDLE = ("Mɪᴅᴅʟᴇ", "5")
    BOTTOM = ("Bᴏᴛᴛᴏᴍ", "2")

    @property
    def label(self) -> str:
        return self.value[0]

    @property
    def code(self) -> str:
        return self.value[1]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class IntroPanelState:
    enabled:       bool
    text:          str
    timer:         str
    color:         str
    border_enabled: bool
    border_width:  str
    border_color:  str
    pos:           str
    font_size:     str
    shadow:        str


@dataclass(slots=True)
class PendingTextEdit:
    chat_id:           int
    prompt_message_id: Optional[int]
    task:              Optional[asyncio.Task] = field(default=None)


@dataclass
class SubtitleCapabilities:
    supports_font_size: bool
    supports_border:    bool
    supports_shadow:    bool
    supports_position:  bool
    supports_color:     bool
    description:        str


# ---------------------------------------------------------------------------
# State stores
# ---------------------------------------------------------------------------
_pending_text_edits:  Dict[int, PendingTextEdit] = {}
_pending_color_edits: Dict[int, PendingTextEdit] = {}
_pending_size_edits:  Dict[int, PendingTextEdit] = {}

# ---------------------------------------------------------------------------
# Templates  (ASS_TEMPLATE removed — generate_ass_line uses an explicit f-string)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Capabilities registry
# ---------------------------------------------------------------------------
SUBTITLE_CAPABILITIES: Final[Dict[str, SubtitleCapabilities]] = {
    "ass": SubtitleCapabilities(
        supports_font_size=True,
        supports_border=True,
        supports_shadow=True,
        supports_position=True,
        supports_color=True,
        description="ASS/SSA (Full Styling Support)",
    ),
    "ssa": SubtitleCapabilities(
        supports_font_size=True,
        supports_border=True,
        supports_shadow=True,
        supports_position=True,
        supports_color=True,
        description="ASS/SSA (Full Styling Support)",
    ),
    "srt": SubtitleCapabilities(
        supports_font_size=False,
        supports_border=False,
        supports_shadow=False,
        supports_position=True,
        supports_color=True,
        description="SRT (HTML color only; border/font size/shadow dropped)",
    ),
    "vtt": SubtitleCapabilities(
        supports_font_size=False,
        supports_border=False,
        supports_shadow=False,
        supports_position=True,
        supports_color=True,
        description="WebVTT (::cue() color; border/font size/shadow dropped)",
    ),
    "ttml": SubtitleCapabilities(
        supports_font_size=True,
        supports_border=False,   # tts:textOutline is non-standard
        supports_shadow=False,
        supports_position=True,
        supports_color=True,
        description="TTML/DFXP (Netflix/Apple — color+position+size, no border/shadow)",
    ),
    "dfxp": SubtitleCapabilities(
        supports_font_size=True,
        supports_border=False,
        supports_shadow=False,
        supports_position=True,
        supports_color=True,
        description="TTML/DFXP (Netflix/Apple — color+position+size, no border/shadow)",
    ),
    "lrc": SubtitleCapabilities(
        supports_font_size=False,
        supports_border=False,
        supports_shadow=False,
        supports_position=False,
        supports_color=False,
        description="LRC (timing only — plain text, no styling)",
    ),
    "sub": SubtitleCapabilities(
        supports_font_size=False,
        supports_border=False,
        supports_shadow=False,
        supports_position=False,
        supports_color=False,
        description="MicroDVD/SUB (frame-based timing — no styling)",
    ),
    "mov_text": SubtitleCapabilities(
        supports_font_size=False,
        supports_border=False,
        supports_shadow=False,
        supports_position=False,
        supports_color=False,
        description="mov_text (MP4 container — plain text/timing only)",
    ),
}


# ===========================================================================
# Utility / conversion helpers
# ===========================================================================

def sanitize_sub_text(text: str) -> str:
    """Return a single-line, display-safe intro text (max 50 chars)."""
    if not text:
        return ""
    clean = re.sub(r"\s+", " ", text.replace("{", r"\{").replace("}", r"\}")).strip()
    return clean[:MAX_INTRO_TEXT_LEN]


def parse_timer_seconds(raw_value: Union[str, int, None]) -> int:
    """Normalize timer values to a sane positive duration (1–60 s)."""
    try:
        seconds = int(str(raw_value or DEFAULT_INTRO_TIMER).strip())
    except (TypeError, ValueError):
        return int(DEFAULT_INTRO_TIMER)
    return max(1, min(seconds, 60))


def format_ass_time(seconds: int) -> str:
    hrs  = seconds // 3600
    mins = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hrs}:{mins:02d}:{secs:02d}.00"


def format_srt_time(seconds: int) -> str:
    hrs  = seconds // 3600
    mins = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hrs:02d}:{mins:02d}:{secs:02d},000"


def format_vtt_time(seconds: int) -> str:
    hrs  = seconds // 3600
    mins = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hrs:02d}:{mins:02d}:{secs:02d}.000"


def format_ttml_time(seconds: int) -> str:
    """Return an HH:MM:SS.mmm timestamp for TTML begin/end attributes."""
    hrs  = seconds // 3600
    mins = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hrs:02d}:{mins:02d}:{secs:02d}.000"


def normalize_hex_color(raw_value: str) -> str:
    """
    Normalize user-supplied hex color to '#RRGGBB'.
    Accepts '#RRGGBB', 'RRGGBB', '#RGB', or 'RGB'.
    """
    cleaned = (raw_value or "").strip().upper()
    if cleaned.startswith("#"):
        cleaned = cleaned[1:]
    if HEX_COLOR_SHORT_RE.fullmatch(cleaned):
        cleaned = "".join(ch * 2 for ch in cleaned)
    if not HEX_COLOR_RE.fullmatch(cleaned):
        raise ValueError("Invalid hex color")
    return f"#{cleaned}"


def rgb_hex_to_ass(hex_color: str) -> str:
    """Convert '#RRGGBB' into ASS '&HBBGGRR&' format."""
    normalized = normalize_hex_color(hex_color)
    rr, gg, bb = normalized[1:3], normalized[3:5], normalized[5:7]
    return f"&H{bb}{gg}{rr}&"


def ass_to_rgb(ass_color: str) -> Tuple[int, int, int]:
    """Convert '&HBBGGRR&' or '&HAABBGGRR&' into (R, G, B)."""
    cleaned = ass_color.strip().upper().replace("&H", "").replace("&", "")
    if len(cleaned) == 8:
        cleaned = cleaned[2:]
    if len(cleaned) != 6:
        return (255, 255, 255)
    try:
        blue  = int(cleaned[0:2], 16)
        green = int(cleaned[2:4], 16)
        red   = int(cleaned[4:6], 16)
    except ValueError:
        return (255, 255, 255)
    return (red, green, blue)


def ass_to_web_hex(ass_color: str) -> str:
    red, green, blue = ass_to_rgb(ass_color)
    return f"#{red:02X}{green:02X}{blue:02X}"


def escape_srt_text(text: str) -> str:
    return sanitize_sub_text(text)


def escape_ass_text(text: str) -> str:
    return sanitize_sub_text(text)


def resolve_subtitle_format(fmt: Union[str, SubtitleFormat]) -> SubtitleFormat:
    if isinstance(fmt, SubtitleFormat):
        return fmt
    raw = str(fmt).strip().lower()
    alias_map: Dict[str, SubtitleFormat] = {
        "ass":      SubtitleFormat.ASS,
        "ssa":      SubtitleFormat.SSA,
        "srt":      SubtitleFormat.SRT,
        "vtt":      SubtitleFormat.VTT,
        "webvtt":   SubtitleFormat.VTT,
        "ttml":     SubtitleFormat.TTML,
        "dfxp":     SubtitleFormat.DFXP,
        "lrc":      SubtitleFormat.LRC,
        "sub":      SubtitleFormat.SUB,
        "microdvd": SubtitleFormat.SUB,
        "mov_text": SubtitleFormat.MOV_TEXT,
        "movtext":  SubtitleFormat.MOV_TEXT,
    }
    if raw not in alias_map:
        raise ValueError(f"Unsupported subtitle format: {fmt!r}")
    return alias_map[raw]


def detect_subtitle_format(content: str) -> SubtitleFormat:
    """
    Sniff subtitle format from file content signature.
    Falls back to SubtitleFormat.SRT when the content is unrecognized.
    """
    s = content.lstrip("\ufeff").lstrip()
    if s.startswith("WEBVTT"):
        return SubtitleFormat.VTT
    if re.match(r"<\?xml", s) or re.search(r"<tt(?:\s|>)", s, re.IGNORECASE):
        return SubtitleFormat.TTML
    if re.match(r"\[Script Info\]", s):
        header_block = s[:500]
        if re.search(r"ScriptType:\s*v4\.00\b(?!\+)", header_block, re.IGNORECASE):
            return SubtitleFormat.SSA
        return SubtitleFormat.ASS
    if re.match(r"\[(\d{2}:\d{2}\.\d{2})\]", s):
        return SubtitleFormat.LRC
    if re.match(r"\{\d+\}\{\d+\}", s):
        return SubtitleFormat.SUB
    if re.match(r"\d+\r?\n\d{2}:\d{2}:\d{2},", s):
        return SubtitleFormat.SRT
    return SubtitleFormat.SRT


def check_and_log_degradation(codec: str) -> List[str]:
    """Check capabilities of the target codec and return list of dropped style options."""
    caps = SUBTITLE_CAPABILITIES.get(codec.lower(), SUBTITLE_CAPABILITIES["mov_text"])
    dropped: List[str] = []
    if not caps.supports_font_size:
        dropped.append("font_size")
    if not caps.supports_border:
        dropped.append("border")
    if not caps.supports_shadow:
        dropped.append("shadow")
    if not caps.supports_position:
        dropped.append("position")
    if not caps.supports_color:
        dropped.append("color")
    if dropped:
        logger.warning(
            "intro_style_degraded: codec=%s dropped=%s description=%s",
            codec, dropped, caps.description,
        )
    return dropped


# ===========================================================================
# Time-shift helpers
# ===========================================================================

def _ms_to_srt(total_ms: int) -> str:
    ms      = total_ms % 1000
    total_s = total_ms // 1000
    s       = total_s % 60
    m       = (total_s // 60) % 60
    h       = total_s // 3600
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def _timestamp_to_ms(hours: Optional[str], minutes: str, seconds: str, fraction: str) -> int:
    """Convert a subtitle timestamp, including 1--3 digit fractions, to ms."""
    milliseconds = int(fraction.ljust(3, "0")[:3])
    return (int(hours or 0) * 3600 + int(minutes) * 60 + int(seconds)) * 1000 + milliseconds


def _shift_srt_timestamp(match: re.Match, offset_ms: int) -> str:
    """Shift a single SRT HH:MM:SS,mmm --> HH:MM:SS,mmm match by offset_ms milliseconds."""
    g = match.groupdict()
    start = _timestamp_to_ms(g["sh"], g["sm"], g["ss"], g["sms"]) + offset_ms
    end = _timestamp_to_ms(g["eh"], g["em"], g["es"], g["ems"]) + offset_ms
    return f"{_ms_to_srt(max(0, start))} --> {_ms_to_srt(max(0, end))}"


def _ms_to_vtt(total_ms: int) -> str:
    ms      = total_ms % 1000
    total_s = total_ms // 1000
    s       = total_s % 60
    m       = (total_s // 60) % 60
    h       = total_s // 3600
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def _shift_vtt_timestamp(match: re.Match, offset_ms: int) -> str:
    """Shift a single VTT HH:MM:SS.mmm --> HH:MM:SS.mmm match by offset_ms milliseconds."""
    g = match.groupdict()
    start = _timestamp_to_ms(g["sh"], g["sm"], g["ss"], g["sms"]) + offset_ms
    end = _timestamp_to_ms(g["eh"], g["em"], g["es"], g["ems"]) + offset_ms
    return f"{_ms_to_vtt(max(0, start))} --> {_ms_to_vtt(max(0, end))}"


def _shift_lrc_timestamp(match: re.Match, offset_s: int) -> str:
    """Shift a single LRC [MM:SS.cc] timestamp by offset_s seconds."""
    minutes    = int(match.group(1))
    sec_dec    = float(match.group(2))
    total      = minutes * 60 + sec_dec + offset_s
    new_m, new_s = divmod(total, 60)
    return f"[{int(new_m):02d}:{new_s:05.2f}]"


# ===========================================================================
# TTML helpers
# ===========================================================================

def _ttml_align_from_pos(pos: str) -> str:
    return {"8": "top", "5": "center", "2": "bottom"}.get(pos, "bottom")


def _register_ttml_namespaces() -> None:
    ET.register_namespace("",    _TTML_NS)
    ET.register_namespace("tts", _TTML_TTS)
    ET.register_namespace("xml", "http://www.w3.org/XML/1998/namespace")


def generate_ttml_block(
    text: str,
    color_hex: str,
    pos: str,
    duration: int,
    font_size: str,
) -> str:
    """Generate a standalone TTML document with a single intro cue."""
    _register_ttml_namespaces()

    ns_map = {
        "xmlns":     _TTML_NS,
        "xml:lang":  "en",
    }
    root = ET.Element(f"{{{_TTML_NS}}}tt", ns_map)

    head    = ET.SubElement(root, f"{{{_TTML_NS}}}head")
    styling = ET.SubElement(head, f"{{{_TTML_NS}}}styling")
    ET.SubElement(styling, f"{{{_TTML_NS}}}style", {
        "xml:id":                 "intro",
        f"{{{_TTML_TTS}}}color":      color_hex,
        f"{{{_TTML_TTS}}}fontSize":   f"{font_size}px",
        f"{{{_TTML_TTS}}}textAlign":  "center",
        f"{{{_TTML_TTS}}}displayAlign": _ttml_align_from_pos(pos),
    })

    body = ET.SubElement(root, f"{{{_TTML_NS}}}body")
    div  = ET.SubElement(body, f"{{{_TTML_NS}}}div")
    p    = ET.SubElement(div, f"{{{_TTML_NS}}}p", {
        "begin": "00:00:00.000",
        "end":   format_ttml_time(duration),
        "style": "intro",
    })
    p.text = sanitize_sub_text(text)

    return ET.tostring(root, encoding="unicode", xml_declaration=True)


# ===========================================================================
# Generator functions
# ===========================================================================

def generate_ass_line(
    text: str,
    color: str,
    pos: str,
    duration: int = 5,
    b_width: str = "2",
    b_color: str = "&H000000&",
    font_size: str = DEFAULT_FONT_SIZE,
    shadow: str = "0",
) -> str:
    safe_text        = escape_ass_text(text)
    ass_color        = color if color.startswith("&H") else rgb_hex_to_ass(color)
    ass_border_color = b_color if b_color.startswith("&H") else rgb_hex_to_ass(b_color)
    start_t = format_ass_time(0)
    end_t   = format_ass_time(duration)
    f_size = font_size if font_size != "0" else DEFAULT_FONT_SIZE
    style  = (
        f"{{\\an{pos}\\bord{b_width}\\shad{shadow}"
        f"\\fs{f_size}\\c{ass_color}\\3c{ass_border_color}\\4c&H000000&}}{safe_text}"
    )
    return f"Dialogue: 0,{start_t},{end_t},Default,,0,0,0,,{style}"


def generate_srt_block(
    text: str,
    duration: int = 5,
    color: Optional[str] = None,
    pos: Optional[str] = None,
) -> str:
    safe_text = escape_srt_text(text)
    pos_tag   = f"{{\\an{pos}}}" if pos in ("8", "5", "2") else ""
    body      = (
        f'<font color="{color}"><b>{safe_text}</b></font>' if color
        else f"<b>{safe_text}</b>"
    )
    return f"1\n{format_srt_time(0)} --> {format_srt_time(duration)}\n{pos_tag}{body}\n"


def generate_vtt_block(
    text: str,
    duration: int = 5,
    color: Optional[str] = None,
    pos: Optional[str] = None,
) -> str:
    safe_text   = escape_srt_text(text)
    pos_tag     = f"{{\\an{pos}}}" if pos in ("8", "5", "2") else ""
    style_block = f"STYLE\n::cue {{\n  color: {color};\n}}\n\n" if color else ""
    return (
        f"WEBVTT\n\n{style_block}"
        f"00:00:00.000 --> {format_vtt_time(duration)}\n{pos_tag}{safe_text}\n"
    )


def generate_lrc_block(text: str, duration: int = 5) -> str:  # noqa: ARG001
    """Generate a minimal LRC block with a single timed lyric line."""
    return f"[00:00.00]{sanitize_sub_text(text)}\n"


def generate_sub_block(text: str, duration: int = 5, fps: float = 23.976) -> str:
    """Generate a MicroDVD/SUB block with frame-based timing."""
    end_frame = int(duration * fps)
    return f"{{0}}{{{end_frame}}}{sanitize_sub_text(text)}\n"


def generate_mov_text_block(text: str, duration: int = 5) -> str:
    """
    Plain SRT-format string for ffmpeg -c:s mov_text muxing.
    ffmpeg reads this as an SRT-style cue and embeds it in the MP4 track.
    """
    return (
        f"1\n"
        f"{format_srt_time(0)} --> {format_srt_time(duration)}\n"
        f"{sanitize_sub_text(text)}\n"
    )


def generate_intro_subtitle(
    subtitle_format: Union[str, SubtitleFormat],
    text: str,
    color: str,
    pos: str,
    duration: int = 5,
    b_width: str = "2",
    b_color: str = "&H000000&",
    font_size: str = DEFAULT_FONT_SIZE,
    shadow: str = "0",
) -> str:
    resolved = resolve_subtitle_format(subtitle_format)
    if resolved in (SubtitleFormat.ASS, SubtitleFormat.SSA):
        return generate_ass_line(text, color, pos, duration, b_width, b_color, font_size, shadow)
    if resolved is SubtitleFormat.SRT:
        html_color = _resolve_html_color(color)
        return generate_srt_block(text, duration, color=html_color, pos=pos)
    if resolved in (SubtitleFormat.TTML, SubtitleFormat.DFXP):
        html_color = _resolve_html_color(color) or "#FFFFFF"
        return generate_ttml_block(text, html_color, pos, duration, font_size)
    if resolved is SubtitleFormat.LRC:
        return generate_lrc_block(text, duration)
    if resolved is SubtitleFormat.SUB:
        return generate_sub_block(text, duration)
    if resolved is SubtitleFormat.MOV_TEXT:
        return generate_mov_text_block(text, duration)
    html_color = _resolve_html_color(color)
    return generate_vtt_block(text, duration, color=html_color, pos=pos)


# ===========================================================================
# Injectors
# ===========================================================================


def _resolve_html_color(color: str) -> Optional[str]:
    """
    Convert a color value (ASS &H…& or plain hex or "Default") to a web "#RRGGBB" string.
    Returns None when no color tag should be emitted (letting the renderer use its default).
    """
    if not color or color.upper() == "DEFAULT":
        return None
    if color.startswith("&H"):
        return ass_to_web_hex(color)
    try:
        return normalize_hex_color(color)
    except ValueError:
        return None


def inject_into_ass(
    content: str,
    text: str,
    color: str,
    pos: str,
    duration: int,
    b_width: str,
    b_color: str,
    font_size: str,
    shadow: str,
    scale: float = 1.0,
) -> str:
    """Inject styled branding line into an ASS subtitle file with optional scaling."""
    scaled_fs   = str(int(round(float(font_size) * scale))) if font_size and font_size != "0" else DEFAULT_FONT_SIZE
    scaled_bw   = str(int(round(float(b_width)   * scale))) if b_width  and b_width  != "0" else "0"
    scaled_shad = str(int(round(float(shadow)     * scale))) if shadow   else "0"

    branding_line = generate_ass_line(
        text=text, color=color, pos=pos, duration=duration,
        b_width=scaled_bw, b_color=b_color,
        font_size=scaled_fs, shadow=scaled_shad,
    )

    if "[Events]" in content:
        parts = content.split("[Events]", 1)
        lines = parts[1].strip().splitlines()
        if lines and lines[0].strip().startswith("Format:"):
            header = lines[0]
            body   = "\n".join(lines[1:])
        else:
            header = "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text"
            body   = "\n".join(lines)
        return f"{parts[0]}[Events]\n{header}\n{branding_line}\n{body}"
    else:
        return (
            "[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
            f"{branding_line}\n{content}"
        )


def inject_into_srt(
    content: str,
    text: str,
    color: str,
    pos: str,
    duration: int,
    b_width: str,
    b_color: str,
    font_size: str,
    shadow: str,
) -> str:
    """
    Inject subtitle branding into an SRT file.

    Fixes vs. old implementation:
    - Shifts ALL existing cue timestamps forward by `duration` seconds so the
      branding cue (cue 1) does not overlap with the original content.
    - Renumbers existing cues starting at 2.
    - Uses HTML <font color="…"> for color; drops unsupported styles cleanly.
    """
    check_and_log_degradation("srt")

    html_color    = _resolve_html_color(color)
    styled_text   = (
        f'<font color="{html_color}"><b>{sanitize_sub_text(text)}</b></font>'
        if html_color else f"<b>{sanitize_sub_text(text)}</b>"
    )
    pos_tag       = f"{{\\an{pos}}}" if pos in ("8", "5", "2") else ""
    start_t       = format_srt_time(0)
    end_t         = format_srt_time(duration)
    watermark_cue = f"1\n{start_t} --> {end_t}\n{pos_tag}{styled_text}"

    content = content.lstrip("\ufeff").replace("\r\n", "\n").strip()
    if not content:
        return watermark_cue + "\n\n"

    # Guard: strip any pre-existing watermark cue (cue starting at 00:00:00,000)
    # to prevent timestamp cascade on re-injection.
    raw_cues_pre = re.split(r"\n\s*\n", content)
    if raw_cues_pre:
        first_lines = raw_cues_pre[0].strip().split("\n")
        ts_line = (
            first_lines[1]
            if (len(first_lines) >= 2 and re.match(r"^\d+$", first_lines[0]))
            else first_lines[0]
        )
        if re.match(r"00:00:00,000\s*-->", ts_line):
            content = "\n\n".join(raw_cues_pre[1:]).strip()
            if not content:
                return watermark_cue + "\n\n"

    # Shift all existing timestamps forward by duration seconds
    offset_ms = duration * 1000
    shifted   = _SRT_TIME_RE.sub(
        lambda m: _shift_srt_timestamp(m, offset_ms), content
    )

    raw_cues = re.split(r"\n\s*\n", shifted)
    cues: List[str] = []
    idx = 2
    for rc in raw_cues:
        rc = rc.strip()
        if not rc:
            continue
        lines = rc.split("\n")
        if len(lines) >= 2 and re.match(r"^\d+$", lines[0].strip()):
            lines = lines[1:]
        cues.append(f"{idx}\n" + "\n".join(lines))
        idx += 1

    return watermark_cue + "\n\n" + "\n\n".join(cues) + "\n\n"


def inject_into_vtt(
    content: str,
    text: str,
    color: str,
    pos: str,
    duration: int,
    b_width: str,
    b_color: str,
    font_size: str,
    shadow: str,
) -> str:
    """
    Inject subtitle branding into a WebVTT file.

    Fixes vs. old implementation:
    - Strips both numeric AND named cue IDs (was only stripping numeric IDs).
    - Shifts all existing timestamps forward by `duration` seconds.
    - Adds a STYLE block with ::cue() color instead of deprecated <font> tag.
    """
    check_and_log_degradation("vtt")

    html_color  = _resolve_html_color(color)

    # Modern VTT color via ::cue() — no deprecated <font> tag
    style_block   = f"STYLE\n::cue {{\n  color: {html_color};\n}}\n\n" if html_color else ""
    pos_tag       = f"{{\\an{pos}}}" if pos in ("8", "5", "2") else ""
    start_t       = format_vtt_time(0)
    end_t         = format_vtt_time(duration)
    watermark_cue = f"{start_t} --> {end_t}\n{pos_tag}{sanitize_sub_text(text)}"

    content = content.lstrip("\ufeff").replace("\r\n", "\n").strip()
    if content.startswith("WEBVTT"):
        parts   = re.split(r"\n\s*\n", content, 1)
        content = parts[1].strip() if len(parts) > 1 else ""

    if not content:
        return f"WEBVTT\n\n{style_block}{watermark_cue}\n\n"

    # Guard: strip any pre-existing watermark cue (cue starting at 00:00:00.000)
    # to prevent timestamp cascade on re-injection.
    raw_cues_pre = re.split(r"\n\s*\n", content)
    # A generated file starts with STYLE followed by the intro cue.  Find the
    # first actual timing block instead of assuming it is the first block;
    # otherwise reinjection leaves the old cue in place and shifts it.
    for block_index, block in enumerate(raw_cues_pre):
        lines = block.strip().split("\n")
        ts_line = next((line for line in lines if "-->" in line), "")
        if not ts_line:
            continue
        if re.match(r"(?:00:)?00:00\.000\s*-->", ts_line):
            content = "\n\n".join(raw_cues_pre[block_index + 1:]).strip()
            if not content:
                return f"WEBVTT\n\n{style_block}{watermark_cue}\n\n"
        break

    # Shift all existing timestamps forward by duration seconds
    offset_ms = duration * 1000
    shifted   = _VTT_TIME_RE.sub(
        lambda m: _shift_vtt_timestamp(m, offset_ms), content
    )

    # Strip all cue IDs (numeric or named) using the pre-compiled pattern
    shifted = re.sub(_VTT_CUE_ID_RE, "", shifted)

    raw_cues = re.split(r"\n\s*\n", shifted)
    cues: List[str] = []
    for rc in raw_cues:
        rc = rc.strip()
        if not rc:
            continue
        cues.append(rc)

    return f"WEBVTT\n\n{style_block}{watermark_cue}\n\n" + "\n\n".join(cues) + "\n\n"


def inject_into_ttml(
    content: str,
    text: str,
    color: str,
    pos: str,
    duration: int,
    b_width: str,
    b_color: str,
    font_size: str,
    shadow: str,
) -> str:
    """
    Inject subtitle branding into an existing TTML/DFXP file.

    Prepends the intro <p> at position 0 inside the first <div>.
    Existing cue timings are NOT shifted (TTML start/end are absolute).
    Namespace-aware: registers TTML prefixes before parsing so ET doesn't
    mangle them.
    """
    check_and_log_degradation("ttml")

    html_color = _resolve_html_color(color) or "#FFFFFF"
    _register_ttml_namespaces()

    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        logger.warning("inject_into_ttml: XML parse failed (%s); returning standalone block", exc)
        return generate_ttml_block(text, html_color, pos, duration, font_size)

    # DFXP commonly uses the older 2006 namespace.  Use the namespace from
    # the document itself instead of rejecting an otherwise valid TTML file.
    root_ns = root.tag[root.tag.find("{") + 1:root.tag.find("}")] if root.tag.startswith("{") else _TTML_NS
    ns = {"tt": root_ns}
    div = root.find(".//tt:body/tt:div", ns)
    if div is None:
        logger.warning("inject_into_ttml: no <div> found; returning standalone block")
        return generate_ttml_block(text, html_color, pos, duration, font_size)

    intro_p = ET.Element(f"{{{root_ns}}}p", {
        "begin": "00:00:00.000",
        "end":   format_ttml_time(duration),
    })
    intro_p.text = sanitize_sub_text(text)
    div.insert(0, intro_p)

    return ET.tostring(root, encoding="unicode")


def inject_into_lrc(
    content: str,
    text: str,
    duration: int,
    **_: Any,
) -> str:
    """
    Inject branding into an LRC (lyrics) file.
    Prepends the intro line and shifts all existing timestamps by `duration` seconds.
    Strips any [offset:] ID tag first to prevent it from shifting the injected cue.
    """
    # Strip [offset:] tag — it specifies a global timing adjustment that would
    # unintentionally shift the injected branding cue.
    content = re.sub(r"^\[offset:[^\]]*\]\s*\n?", "", content, flags=re.MULTILINE | re.IGNORECASE)
    intro   = generate_lrc_block(text, duration)
    shifted = _LRC_TIME_RE.sub(
        lambda m: _shift_lrc_timestamp(m, duration), content
    )
    return intro + shifted


def inject_into_sub(
    content: str,
    text: str,
    duration: int,
    fps: float = 23.976,
    **_: Any,
) -> str:
    """
    Inject branding into a MicroDVD/SUB file.
    Prepends the intro block and shifts all existing frame numbers by `duration * fps` frames.
    """
    intro_frames = int(duration * fps)

    def shift(m: re.Match) -> str:
        s = int(m.group(1)) + intro_frames
        e = int(m.group(2)) + intro_frames
        return f"{{{s}}}{{{e}}}"

    shifted = _SUB_FRAME_RE.sub(shift, content)
    return generate_sub_block(text, duration, fps) + shifted


# ---------------------------------------------------------------------------
# Unified injector dispatcher (Phase 4)
# ---------------------------------------------------------------------------

#: Map from SubtitleFormat to its injector callable.
#: Each injector accepts (content, **kwargs) where kwargs is the standardized
#: set of branding parameters; unused kwargs are silently absorbed via **_.
INJECTORS: Dict[SubtitleFormat, Callable[..., str]] = {}  # populated below


def _build_injector_map() -> Dict[SubtitleFormat, Callable[..., str]]:
    return {
        SubtitleFormat.ASS:      inject_into_ass,
        SubtitleFormat.SSA:      inject_into_ass,
        SubtitleFormat.SRT:      inject_into_srt,
        SubtitleFormat.VTT:      inject_into_vtt,
        SubtitleFormat.TTML:     inject_into_ttml,
        SubtitleFormat.DFXP:     inject_into_ttml,
        SubtitleFormat.LRC:      inject_into_lrc,
        SubtitleFormat.SUB:      inject_into_sub,
        # mov_text is extracted as SRT before modification.  Returning only
        # the intro cue here discarded every original cue when this dispatcher
        # was used for an MP4 subtitle track.
        SubtitleFormat.MOV_TEXT: inject_into_srt,
    }


INJECTORS = _build_injector_map()


def inject_branding(
    fmt: Union[str, SubtitleFormat],
    content: str,
    *,
    text: str,
    color: str,
    pos: str,
    duration: int,
    b_width: str   = "2",
    b_color: str   = "&H000000&",
    font_size: str = DEFAULT_FONT_SIZE,
    shadow: str    = "0",
    scale: float   = 1.0,
    fps: float     = 23.976,
) -> str:
    """
    Unified branding injector.

    Resolves the format, looks up the correct injector, strips parameters
    that the target format does not support, then calls the injector.

    This replaces the scattered `if fmt == "srt": inject_into_srt(...)` chains
    in callers with a single dispatch point.
    """
    content = content.lstrip("\ufeff")
    resolved = resolve_subtitle_format(fmt)
    fn = INJECTORS.get(resolved)
    if fn is None:
        raise ValueError(f"No injector registered for format: {resolved!r}")

    caps = SUBTITLE_CAPABILITIES.get(resolved.value, SUBTITLE_CAPABILITIES["mov_text"])

    # Build kwargs, dropping parameters the format can't use
    kwargs: Dict[str, Any] = dict(
        text=text, color=color, pos=pos, duration=duration,
        b_width=b_width, b_color=b_color, font_size=font_size, shadow=shadow,
    )
    if not caps.supports_border:
        kwargs.update(b_width="0", b_color="&H000000&")
    if not caps.supports_shadow:
        kwargs["shadow"] = "0"
    if not caps.supports_font_size:
        kwargs["font_size"] = DEFAULT_FONT_SIZE

    # Format-specific extra kwargs
    if resolved in (SubtitleFormat.ASS, SubtitleFormat.SSA):
        kwargs["scale"] = scale
    if resolved is SubtitleFormat.SUB:
        kwargs["fps"] = fps

    return fn(content, **kwargs)


# ===========================================================================
# Fallback ASS builder (no existing subtitles)
# ===========================================================================

def build_fallback_intro_ass(
    text: str,
    color: str,
    pos: str,
    duration: int,
    b_width: str,
    b_color: str,
    font_size: str,
    shadow: str,
) -> str:
    """Create a fully valid standalone ASS subtitle track for videos with no subtitles."""
    raw_b   = b_color.strip().upper().replace("&H", "").replace("&", "")[-6:]
    fb_bcol = f"&H00{raw_b}&"
    branding_line = generate_ass_line(text, color, pos, duration, b_width, b_color, font_size, shadow)
    return (
        "[Script Info]\nScriptType: v4.00+\nPlayResX: 1280\nPlayResY: 720\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
        "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
        "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Default,Arial,20,&H00FFFFFF,&H000000FF,{fb_bcol},&H00000000,"
        f"0,0,0,0,100,100,0,0,1,{b_width},{shadow},{pos},10,10,10,1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
        f"{branding_line}\n"
    )


# ===========================================================================
# UI helpers
# ===========================================================================

def _resolve_color_label(raw_color: str) -> str:
    for item in SubColor:
        if item.ass_hex.upper() == raw_color.upper():
            return item.label
    if raw_color.upper() == "DEFAULT":
        return "Original"
    try:
        return normalize_hex_color(ass_to_web_hex(raw_color))
    except ValueError:
        return raw_color


def _prefs_to_state(prefs: Dict[str, Any]) -> IntroPanelState:
    return IntroPanelState(
        enabled=bool(prefs.get("intro_sub", False)),
        text=sanitize_sub_text(str(prefs.get("intro_text", DEFAULT_INTRO_TEXT))),
        timer=str(parse_timer_seconds(prefs.get("intro_timer", DEFAULT_INTRO_TIMER))),
        color=str(prefs.get("sub_color", "Default")),
        border_enabled=bool(prefs.get("sub_bord_status", False)),
        border_width=str(prefs.get("sub_bord_width", "2")),
        border_color=str(prefs.get("sub_bord_color", "&H000000&")),
        pos=str(prefs.get("sub_pos", "2")),
        font_size=str(prefs.get("sub_font_size", DEFAULT_FONT_SIZE)),
        shadow=str(prefs.get("sub_shadow", "0")),
    )


def build_intro_text(state: IntroPanelState) -> str:
    pos_label = next((p.label for p in SubPos if p.code == state.pos), "Bottom")
    return (
        "<b><blockquote>✦ 𝗕𝗥𝗔𝗡𝗗𝗜𝗡𝗚 & 𝗦𝗧𝗬𝗟𝗜𝗡𝗚 𝗖𝗘𝗡𝗧𝗘𝗥</blockquote></b>\n\n"
        f"<b>◈ Sᴛᴀᴛᴜs ▹</b> <code>{'Eɴᴀʙʟᴇᴅ' if state.enabled else 'Dɪsᴀʙʟᴇᴅ'}</code>\n"
        f"<b>◈ Tᴇxᴛ ▹</b> <code>{state.text}</code>\n"
        f"<b>◈ Tɪᴍᴇʀ ▹</b> <code>{state.timer}s</code>\n"
        f"<b>◈ Cᴏʟᴏʀ ▹</b> <code>{_resolve_color_label(state.color)}</code>\n"
        f"<b>◈ Pᴏsɪᴛɪᴏɴ ▹</b> <code>{pos_label}</code>\n"
        f"<b>◈ Sɪᴢᴇ ▹</b> <code>{_FONT_SIZE_NAMES.get(state.font_size, f'Cᴜsᴛᴏᴍ: {state.font_size}')}</code>\n"
        f"<b>◈ Sʜᴀᴅᴏᴡ ▹</b> <code>{_SHADOW_NAMES.get(state.shadow, 'None')}</code>\n\n"
        "<blockquote expandable>◍ <b>Cᴜsᴛᴏᴍ ɪɴᴛʀᴏ</b> : Tʜᴇ Tᴇxᴛ, Tɪᴍᴇʀ, Pᴏsɪᴛɪᴏɴ, "
        "ᴀɴᴅ Sʜᴀᴅᴏᴡ ᴀᴘᴘʟʏ ᴏɴʟʏ ᴛᴏ ᴛʜᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ.</blockquote>"
    )


# ---------------------------------------------------------------------------
# Keyboard builders
# ---------------------------------------------------------------------------

def build_main_keyboard(user_id: int, enabled: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"Iɴᴛʀᴏ: {'Oɴ ◉' if enabled else 'Oɴ'}",
                callback_data=f"itro:toggle:on:{user_id}",
            ),
            InlineKeyboardButton(
                f"Iɴᴛʀᴏ: {'Oғғ ◉' if not enabled else 'Oғғ'}",
                callback_data=f"itro:toggle:off:{user_id}",
            ),
        ],
        [
            InlineKeyboardButton("Eᴅɪᴛ Tᴇxᴛ", callback_data=f"itro:edit_txt:{user_id}"),
        ],
        [
            InlineKeyboardButton("Sᴇᴛᴛɪɴɢs", callback_data=f"itro:settings_menu:{user_id}"),
            InlineKeyboardButton("Cʟᴏsᴇ",    callback_data=f"itro:close_panel:{user_id}"),
        ],
    ])


def build_settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Sᴇᴛ Tɪᴍᴇʀ", callback_data=f"itro:timer_menu:{user_id}"),
            InlineKeyboardButton("Pᴏsɪᴛɪᴏɴ",  callback_data=f"itro:pos_menu:{user_id}"),
        ],
        [
            InlineKeyboardButton("Cᴏʟᴏʀ",  callback_data=f"itro:gcol_menu:{user_id}"),
            InlineKeyboardButton("Sʜᴀᴅᴏᴡ", callback_data=f"itro:shad_menu:{user_id}"),
        ],
        [
            InlineKeyboardButton("Sɪᴢᴇ",   callback_data=f"itro:size_menu:{user_id}"),
            InlineKeyboardButton("Bᴀᴄᴋ",   callback_data=f"itro:main:{user_id}"),
        ],
    ])


def build_timer_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("3s",  callback_data=f"itro:save_timer:3:{user_id}"),
            InlineKeyboardButton("5s",  callback_data=f"itro:save_timer:5:{user_id}"),
            InlineKeyboardButton("7s",  callback_data=f"itro:save_timer:7:{user_id}"),
            InlineKeyboardButton("10s", callback_data=f"itro:save_timer:10:{user_id}"),
        ],
        [InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:settings_menu:{user_id}")],
    ])


def build_color_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("Oʀɪɢɪɴᴀʟ", callback_data=f"itro:save_gcol:Default:{user_id}")]]
    current_row: List[InlineKeyboardButton] = []
    for color in SubColor:
        if color is SubColor.BLACK:
            continue
        current_row.append(
            InlineKeyboardButton(color.label, callback_data=f"itro:save_gcol:{color.ass_hex}:{user_id}")
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("Cᴜsᴛᴏᴍ ʜᴇx ᴄᴏʟᴏʀ", callback_data=f"itro:edit_col:{user_id}")])
    rows.append([InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:settings_menu:{user_id}")])
    return InlineKeyboardMarkup(rows)


def build_pos_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for pos in SubPos:
        current_row.append(InlineKeyboardButton(pos.label, callback_data=f"itro:save_pos:{pos.code}:{user_id}"))
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:settings_menu:{user_id}")])
    return InlineKeyboardMarkup(rows)


def build_size_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for code, name in _FONT_SIZE_NAMES.items():
        current_row.append(InlineKeyboardButton(name, callback_data=f"itro:save_size:{code}:{user_id}"))
        if len(current_row) == 3:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("Cᴜsᴛᴏᴍ Fᴏɴᴛ Sɪᴢᴇ", callback_data=f"itro:edit_size:{user_id}")])
    rows.append([InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:main:{user_id}")])
    return InlineKeyboardMarkup(rows)


def build_shad_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for code, name in _SHADOW_NAMES.items():
        current_row.append(InlineKeyboardButton(name, callback_data=f"itro:save_shad:{code}:{user_id}"))
        if len(current_row) == 3:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:settings_menu:{user_id}")])
    return InlineKeyboardMarkup(rows)


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def parse_owner_id(data: str) -> Optional[int]:
    try:
        return int(data.rsplit(":", 1)[-1])
    except (TypeError, ValueError):
        return None


async def refresh_intro_panel(query: CallbackQuery, user_id: int) -> None:
    prefs = await db.get_user_prefs_batch(user_id)
    state = _prefs_to_state(prefs)
    try:
        await edit_intro_message(
            query,
            build_intro_text(state),
            reply_markup=build_main_keyboard(user_id, state.enabled),
        )
    except MessageNotModified:
        logger.debug("Intro panel unchanged for user %s", user_id)


async def edit_intro_message(
    query: CallbackQuery,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """Edit the callback source whether the message is media or plain text."""
    if not query.message:
        raise ValueError("Callback query has no attached message")
    client = getattr(query.message, "_client", None) or getattr(query, "_client", None)
    if client is None:
        raise RuntimeError("Client reference missing for message edit")

    chat_id    = query.message.chat.id
    message_id = query.message.id

    try:
        await client.edit_message_caption(
            chat_id=chat_id,
            message_id=message_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
        )
        return
    except MessageNotModified:
        raise
    except Exception as caption_exc:
        logger.debug("Caption edit failed for %s/%s: %s", chat_id, message_id, caption_exc)

    await client.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
    )


def _load_preview_fonts(
    primary_size: int,
) -> Tuple[ImageFont.ImageFont, ImageFont.ImageFont]:
    for font_name in _FONT_CANDIDATES:
        try:
            return (
                ImageFont.truetype(font_name, primary_size),
                ImageFont.truetype(font_name, 24),
            )
        except OSError:
            continue
    return ImageFont.load_default(), ImageFont.load_default()


async def _generate_preview_image(state: IntroPanelState) -> io.BytesIO:
    width, height = 1280, 720
    image = Image.new("RGB", (width, height), color=(15, 15, 15))
    draw  = ImageDraw.Draw(image)

    font_size              = int(state.font_size) if state.font_size != "0" else int(DEFAULT_FONT_SIZE)
    title_font, small_font = _load_preview_fonts(font_size)

    text_color = next(
        (ass_to_rgb(c.ass_hex) for c in SubColor if c.ass_hex.upper() == state.color.upper()),
        None,
    )
    if text_color is None:
        text_color = ass_to_rgb(state.color)
    border_color = ass_to_rgb(state.border_color)

    bbox        = draw.textbbox((0, 0), state.text, font=title_font)
    text_width  = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    x = (width - text_width) // 2

    if state.pos == SubPos.TOP.code:
        y = 40
    elif state.pos == SubPos.MIDDLE.code:
        y = (height - text_height) // 2
    else:
        y = height - text_height - 50

    if state.shadow != "0":
        offset = int(state.shadow) * 2
        draw.text((x + offset, y + offset), state.text, font=title_font, fill=(0, 0, 0))

    if state.border_enabled:
        border_width = int(state.border_width) * 2
        for dx in range(-border_width, border_width + 1):
            for dy in range(-border_width, border_width + 1):
                if dx == 0 and dy == 0:
                    continue
                if dx * dx + dy * dy <= border_width * border_width:
                    draw.text((x + dx, y + dy), state.text, font=title_font, fill=border_color)

    draw.text((x, y), state.text, font=title_font, fill=text_color)
    draw.text((20, 20), f"Preview Mode: {state.timer}s Duration", font=small_font, fill=(100, 100, 100))

    output = io.BytesIO()
    output.name = "preview.png"
    image.save(output, "PNG")
    output.seek(0)
    return output


def _schedule_pending_expiry(store: Dict[int, PendingTextEdit], user_id: int) -> "asyncio.Task[None]":
    async def expire() -> None:
        await asyncio.sleep(60)
        store.pop(user_id, None)

    return asyncio.create_task(expire())


# ===========================================================================
# Pyrogram handlers
# ===========================================================================

@Client.on_message(filters.command("introsub") & filters.private)
async def introsub_cmd(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    prefs   = await db.get_user_prefs_batch(user_id)

    if not prefs.get("is_premium"):
        await message.reply("<blockquote>✦ <b>Iɴᴛʀᴏ Sᴜʙ ɪs ᴀ Pʀᴇᴍɪᴜᴍ ꜰᴇᴀᴛᴜʀᴇ !</b></blockquote>")
        return

    state = _prefs_to_state(prefs)
    await safe_send_photo(
        client,
        message.chat.id,
        INTRO_PIC,
        build_intro_text(state),
        build_main_keyboard(user_id, state.enabled),
    )


@Client.on_callback_query(filters.regex(r"^itro:"))
async def intro_router(client: Client, query: CallbackQuery) -> None:
    try:
        if not query.data:
            await query.answer("Invalid request.")
            return

        owner_id = parse_owner_id(query.data)
        if owner_id is None or not query.from_user or query.from_user.id != owner_id:
            await query.answer("Access denied.", show_alert=True)
            return

        parts = query.data.split(":")
        if len(parts) < 3:
            await query.answer("Invalid request.")
            return

        action = parts[1]

        if action == "main":
            await refresh_intro_panel(query, owner_id)
            return

        if action == "settings_menu":
            prefs = await db.get_user_prefs_batch(owner_id)
            state = _prefs_to_state(prefs)
            await edit_intro_message(
                query,
                build_intro_text(state),
                reply_markup=build_settings_keyboard(owner_id),
            )
            return

        if action == "close_panel":
            try:
                await query.message.delete()
            except Exception:
                pass
            await query.answer("Panel closed.")
            return

        if action == "toggle":
            enabled = parts[2] == "on"
            await db.set_intro_sub(owner_id, enabled)
            await query.answer(f"✓ Intro {'Enabled' if enabled else 'Disabled'}")
            await refresh_intro_panel(query, owner_id)
            return

        if action == "timer_menu":
            await edit_intro_message(
                query,
                "<blockquote>⧗ 𝗜𝗡𝗧𝗥𝗢 𝗦𝗨𝗕 𝗗𝗨𝗥𝗔𝗧𝗜𝗢𝗡</blockquote>\n"
                "<blockquote>≡ ᴄᴏɴᴛʀᴏʟ ʜᴏᴡ ʟᴏɴɢ ᴛʜᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ sᴛᴀʏs ᴏɴ sᴄʀᴇᴇɴ ʙᴇғᴏʀᴇ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ғᴀᴅɪɴɢ ᴏᴜᴛ ᴅᴜʀɪɴɢ ᴘʟᴀʏʙᴀᴄᴋ.</blockquote>",
                reply_markup=build_timer_keyboard(owner_id),
            )
            return

        if action == "save_timer" and len(parts) >= 4:
            timer_value = str(parse_timer_seconds(parts[2]))
            await db.set_intro_timer(owner_id, timer_value)
            await query.answer(f"✓ Timer set to {timer_value}s")
            await refresh_intro_panel(query, owner_id)
            return

        if action == "gcol_menu":
            await edit_intro_message(
                query,
                "<blockquote>❐ 𝗜𝗡𝗧𝗥𝗢 𝗦𝗨𝗕 𝗖𝗢𝗟𝗢𝗥</blockquote>\n"
                "<blockquote>≡ ᴄᴜsᴛᴏᴍɪᴢᴇ ᴛʜᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ᴄᴏʟᴏʀ ᴜsɪɴɢ ᴘʀᴇsᴇᴛ ᴛʜᴇᴍᴇs ᴏʀ ʏᴏᴜʀ ᴏᴡɴ ʜᴇx ᴄᴏʟᴏʀ ғᴏʀ ᴀ ᴄʟᴇᴀɴᴇʀ ᴘᴇʀsᴏɴᴀʟɪᴢᴇᴅ ɪɴᴛʀᴏ sᴛʏʟᴇ.</blockquote>",
                reply_markup=build_color_keyboard(owner_id),
            )
            return

        if action == "save_gcol" and len(parts) >= 4:
            await db.set_sub_color(owner_id, parts[2])
            await query.answer("[ ✓ ] Iɴᴛʀᴏ ᴄᴏʟᴏʀ sᴀᴠᴇᴅ")
            await refresh_intro_panel(query, owner_id)
            return

        if action == "pos_menu":
            await edit_intro_message(
                query,
                "<blockquote>❐ 𝗜𝗡𝗧𝗥𝗢 𝗦𝗨𝗕 𝗣𝗢𝗦𝗜𝗧𝗜𝗢𝗡</blockquote>\n"
                "<blockquote>≡ ᴀᴅᴊᴜsᴛ ᴛʜᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ᴘᴏsɪᴛɪᴏɴ ᴀɴᴅ ᴄʜᴏᴏsᴇ ᴡʜᴇʀᴇ ᴛʜᴇ ɪɴᴛʀᴏ ᴛᴇxᴛ ᴀᴘᴘᴇᴀʀs ᴏɴ sᴄʀᴇᴇɴ ᴅᴜʀɪɴɢ ᴘʟᴀʏʙᴀᴄᴋ.</blockquote>",
                reply_markup=build_pos_keyboard(owner_id),
            )
            return

        if action == "save_pos" and len(parts) >= 4:
            await db.set_sub_pos(owner_id, parts[2])
            await query.answer("[ ✓ ] Pᴏsɪᴛɪᴏɴ ᴜᴘᴅᴀᴛᴇᴅ")
            await refresh_intro_panel(query, owner_id)
            return

        if action == "shad_menu":
            await edit_intro_message(
                query,
                "<blockquote>❐ 𝗜𝗡𝗧𝗥𝗢 𝗦𝗨𝗕 𝗦𝗧𝗬𝗟𝗘</blockquote>\n"
                "<blockquote>≡ ᴄʜᴏᴏsᴇ ʜᴏᴡ ᴛʜᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ᴀᴘᴘᴇᴀʀs ᴡɪᴛʜ ᴄʟᴇᴀɴ sʜᴀᴅᴏᴡ ᴀɴᴅ ɢʟᴏᴡ ᴇғғᴇᴄᴛs ғᴏʀ ᴀ sʜᴀʀᴘᴇʀ ʟᴏᴏᴋ.</blockquote>",
                reply_markup=build_shad_keyboard(owner_id),
            )
            return

        if action == "save_shad" and len(parts) >= 4:
            await db.set_sub_shadow(owner_id, parts[2])
            await query.answer("[ ✓ ] sᴛʏʟᴇ ᴜᴘᴅᴀᴛᴇᴅ")
            await refresh_intro_panel(query, owner_id)
            return

        if action == "size_menu":
            await edit_intro_message(
                query,
                "<blockquote>❐ 𝗜𝗡𝗧𝗥𝗢 𝗦𝗨𝗕 𝗦𝗜𝗭𝗘</blockquote>\n"
                "<blockquote>≡ ᴄʜᴏᴏsᴇ ᴛʜᴇ sɪᴢᴇ ᴏғ ᴛʜᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ᴛᴏ ᴍᴀᴛᴄʜ ʏᴏᴜʀ ᴘʀᴇғᴇʀᴇɴᴄᴇs.</blockquote>",
                reply_markup=build_size_keyboard(owner_id),
            )
            return

        if action == "save_size" and len(parts) >= 4:
            await db.set_sub_font_size(owner_id, parts[2])
            await query.answer("[ ✓ ] sɪᴢᴇ ᴜᴘᴅᴀᴛᴇᴅ")
            await refresh_intro_panel(query, owner_id)
            return

        if action == "edit_txt":
            if not query.message:
                await query.answer("Message context missing.", show_alert=True)
                return
            existing = _pending_text_edits.get(owner_id)
            if existing and existing.task:
                existing.task.cancel()
            entry = PendingTextEdit(
                chat_id=query.message.chat.id,
                prompt_message_id=query.message.id,
            )
            _pending_text_edits[owner_id] = entry
            await edit_intro_message(
                query,
                "<blockquote>❐ 𝗘𝗗𝗜𝗧 𝗜𝗡𝗧𝗥𝗢 𝗧𝗘𝗫𝗧</blockquote>\n"
                "<blockquote>≡ sᴇɴᴅ ʏᴏᴜʀ ᴄᴜsᴛᴏᴍ ɪɴᴛʀᴏ ʙʀᴀɴᴅɪɴɢ ᴛᴇxᴛ ғᴏʀ ᴘᴇʀsᴏɴᴀʟɪᴢᴇᴅ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇs. "
                "(ᴍᴀx 50 ᴄʜᴀʀs • sɪɴɢʟᴇ-ʟɪɴᴇ • ᴀss-sᴀғᴇ)</blockquote>\n",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:main:{owner_id}")]]
                ),
            )
            await query.answer("sᴇɴᴅ ɴᴇᴡ ᴛᴇxᴛ ᴡɪᴛʜɪɴ 60 sᴇᴄᴏɴᴅs.")
            entry.task = _schedule_pending_expiry(_pending_text_edits, owner_id)
            return

        if action == "edit_col":
            if not query.message:
                await query.answer("Message context missing.", show_alert=True)
                return
            existing = _pending_color_edits.get(owner_id)
            if existing and existing.task:
                existing.task.cancel()
            entry = PendingTextEdit(
                chat_id=query.message.chat.id,
                prompt_message_id=query.message.id,
            )
            _pending_color_edits[owner_id] = entry
            await edit_intro_message(
                query,
                "<blockquote>❐ 𝗘𝗗𝗜𝗧 𝗖𝗨𝗦𝗧𝗢𝗠 𝗛𝗘𝗫 𝗖𝗢𝗟𝗢𝗥</blockquote>\n"
                "<blockquote>≡ sᴇɴᴅ ʏᴏᴜʀ ᴄᴜsᴛᴏᴍ ʜᴇx ᴄᴏʟᴏʀ ᴄᴏᴅᴇ ᴛᴏ ᴘᴇʀsᴏɴᴀʟɪᴢᴇ ᴛʜᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ᴀᴘᴘᴇᴀʀᴀɴᴄᴇ. "
                "(ᴇxᴀᴍᴘʟᴇ: #FF00FF ᴏʀ #F0F)</blockquote>",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:main:{owner_id}")]]
                ),
            )
            await query.answer("sᴇɴᴅ ʜᴇx ᴄᴏʟᴏʀ ᴡɪᴛʜɪɴ 60 sᴇᴄᴏɴᴅs.")
            entry.task = _schedule_pending_expiry(_pending_color_edits, owner_id)
            return

        if action == "edit_size":
            if not query.message:
                await query.answer("Message context missing.", show_alert=True)
                return
            existing = _pending_size_edits.get(owner_id)
            if existing and existing.task:
                existing.task.cancel()
            entry = PendingTextEdit(
                chat_id=query.message.chat.id,
                prompt_message_id=query.message.id,
            )
            _pending_size_edits[owner_id] = entry
            await edit_intro_message(
                query,
                "<blockquote>❐ 𝗘𝗗𝗜𝗧 𝗜𝗡𝗧𝗥𝗢 𝗦𝗜𝗭𝗘</blockquote>\n"
                "<blockquote>≡ sᴇɴᴅ ʏᴏᴜʀ ᴄᴜsᴛᴏᴍ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ғᴏɴᴛ sɪᴢᴇ (ᴇ.ɢ. 50). "
                "ᴍᴜsᴛ ʙᴇ ᴀ ɴᴜᴍʙᴇʀ ʙᴇᴛᴡᴇᴇɴ 10 ᴀɴᴅ 500.</blockquote>",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Bᴀᴄᴋ", callback_data=f"itro:main:{owner_id}")]]
                ),
            )
            await query.answer("sᴇɴᴅ ɴᴇᴡ sɪᴢᴇ ᴡɪᴛʜɪɴ 60 sᴇᴄᴏɴᴅs.")
            entry.task = _schedule_pending_expiry(_pending_size_edits, owner_id)
            return

        await query.answer("Unknown action.")

    except FloodWait as exc:
        await handle_floodwait(exc.value)
    except QueryIdInvalid:
        logger.warning("Ignored expired intro callback query")
    except Exception as exc:
        logger.error("intro_router error: %s", exc, exc_info=True)
        try:
            detail = str(exc).strip() or exc.__class__.__name__
            await query.answer(f"Error: {detail[:120]}", show_alert=True)
        except Exception:
            logger.debug("Failed to answer callback with error state", exc_info=True)


# ---------------------------------------------------------------------------
# Pending input filters
# ---------------------------------------------------------------------------

async def _is_pending(_, __, message: Message) -> bool:
    if not message.from_user:
        return False
    pending = _pending_text_edits.get(message.from_user.id)
    return bool(pending and pending.chat_id == message.chat.id)


async def _is_color_pending(_, __, message: Message) -> bool:
    if not message.from_user:
        return False
    pending = _pending_color_edits.get(message.from_user.id)
    return bool(pending and pending.chat_id == message.chat.id)


async def _is_size_pending(_, __, message: Message) -> bool:
    if not message.from_user:
        return False
    pending = _pending_size_edits.get(message.from_user.id)
    return bool(pending and pending.chat_id == message.chat.id)


pending_filter       = filters.create(_is_pending)
color_pending_filter = filters.create(_is_color_pending)
size_pending_filter  = filters.create(_is_size_pending)


@Client.on_message(filters.private & filters.text & pending_filter, group=-100)
async def capture_intro_text(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    try:
        cleaned = sanitize_sub_text(message.text or "")
        if not cleaned:
            await message.reply("✗ Empty text is not allowed.")
            return
        await db.set_intro_text(user_id, cleaned)
        _pending_text_edits.pop(user_id, None)
        await message.reply(
            f"<blockquote>[ ✓ ]𝗜𝗡𝗧𝗥𝗢 𝗧𝗘𝗫𝗧 𝗨𝗣𝗗𝗔𝗧𝗘𝗗</blockquote>\n"
            f"<blockquote>≡ ʏᴏᴜʀ ᴄᴜsᴛᴏᴍ ɪɴᴛʀᴏ ʙʀᴀɴᴅɪɴɢ ᴛᴇxᴛ ʜᴀs ʙᴇᴇɴ ᴜᴘᴅᴀᴛᴇᴅ ᴀɴᴅ ᴡɪʟʟ ɴᴏᴡ ʙᴇ ᴜsᴇᴅ ᴅᴜʀɪɴɢ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ɢᴇɴᴇʀᴀᴛɪᴏɴ.</blockquote>\n"
            f"<blockquote>➥ <code>{cleaned}</code></blockquote>"
        )
    except FloodWait as exc:
        await handle_floodwait(exc.value)
    except Exception as exc:
        logger.error("capture_intro_text error: %s", exc, exc_info=True)
        await message.reply("<blockquote>✗ ғᴀɪʟᴇᴅ ᴛᴏ sᴀᴠᴇ ɪɴᴛʀᴏ ᴛᴇxᴛ.</blockquote>")
    finally:
        raise StopPropagation


@Client.on_message(filters.private & filters.text & color_pending_filter, group=-101)
async def capture_intro_color(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    try:
        normalized = normalize_hex_color(message.text or "")
        ass_color  = rgb_hex_to_ass(normalized)
        await db.set_sub_color(user_id, ass_color)
        _pending_color_edits.pop(user_id, None)
        await message.reply(
            f"<blockquote>[ ✓ ]𝗖𝗨𝗦𝗧𝗢𝗠 𝗖𝗢𝗟𝗢𝗥 𝗨𝗣𝗗𝗔𝗧𝗘𝗗</blockquote>\n"
            f"<blockquote>≡ ᴄᴜsᴛᴏᴍ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ᴄᴏʟᴏʀ ʜᴀs ʙᴇᴇɴ sᴜᴄᴄᴇssғᴜʟʟʏ ᴜᴘᴅᴀᴛᴇᴅ ᴀɴᴅ ᴀᴘᴘʟɪᴇᴅ ᴛᴏ ғᴜᴛᴜʀᴇ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ ɢᴇɴᴇʀᴀᴛɪᴏɴ.</blockquote>\n"
            f"<blockquote>➥ <code>{normalized}</code></blockquote>"
        )
    except ValueError:
        await message.reply("<blockquote>✗ ɪɴᴠᴀʟɪᴅ ʜᴇx ᴄᴏʟᴏʀ. sᴇɴᴅ 3 ᴏʀ 6 ʜᴇx ᴅɪɢɪᴛs</blockquote>")
    except FloodWait as exc:
        await handle_floodwait(exc.value)
    except Exception as exc:
        logger.error("capture_intro_color error: %s", exc, exc_info=True)
        await message.reply("✗ Failed to save custom hex color.")
    finally:
        raise StopPropagation


@Client.on_message(filters.private & filters.text & size_pending_filter, group=-102)
async def capture_intro_size(client: Client, message: Message) -> None:
    if not message.from_user:
        return
    user_id = message.from_user.id
    try:
        size_str = (message.text or "").strip()
        if not size_str.isdigit():
            await message.reply("<blockquote>✗ ɪɴᴠᴀʟɪᴅ sɪᴢᴇ. sᴇɴᴅ ᴀ ɴᴜᴍʙᴇʀ.</blockquote>")
            return
        size_val = int(size_str)
        if size_val < 10 or size_val > 500:
            await message.reply("<blockquote>✗ sɪᴢᴇ ᴍᴜsᴛ ʙᴇ ʙᴇᴛᴡᴇᴇɴ 10 ᴀɴᴅ 500.</blockquote>")
            return
            
        await db.set_sub_font_size(user_id, str(size_val))
        _pending_size_edits.pop(user_id, None)
        await message.reply(
            f"<blockquote>[ ✓ ]𝗖𝗨𝗦𝗧𝗢𝗠 𝗦𝗜𝗭𝗘 𝗨𝗣𝗗𝗔𝗧𝗘𝗗</blockquote>\n"
            f"<blockquote>≡ ᴄᴜsᴛᴏᴍ ɪɴᴛʀᴏ sᴜʙᴛɪᴛʟᴇ sɪᴢᴇ ʜᴀs ʙᴇᴇɴ sᴜᴄᴄᴇssғᴜʟʟʏ ᴜᴘᴅᴀᴛᴇᴅ.</blockquote>\n"
            f"<blockquote>➥ <code>{size_val}</code></blockquote>"
        )
    except FloodWait as exc:
        await handle_floodwait(exc.value)
    except Exception as exc:
        logger.error("capture_intro_size error: %s", exc, exc_info=True)
        await message.reply("✗ Failed to save custom size.")
    finally:
        raise StopPropagation


async def init(client: Any, *args: Any, **kwargs: Any) -> None:
    _pending_text_edits.clear()
    _pending_color_edits.clear()
    _pending_size_edits.clear()
    logger.info("[\u2713] Intro Subtitle Customization Module Initialized")


__all__ = [
    # Enums
    "SubtitleFormat",
    "SubColor",
    "SubPos",
    # Data classes
    "SubtitleCapabilities",
    "IntroPanelState",
    # Constants
    "SUBTITLE_CAPABILITIES",
    "INJECTORS",
    # Utility
    "sanitize_sub_text",
    "parse_timer_seconds",
    "normalize_hex_color",
    "rgb_hex_to_ass",
    "ass_to_rgb",
    "ass_to_web_hex",
    "resolve_subtitle_format",
    "detect_subtitle_format",
    "check_and_log_degradation",
    # Formatters
    "format_ass_time",
    "format_srt_time",
    "format_vtt_time",
    "format_ttml_time",
    # Generators
    "generate_ass_line",
    "generate_srt_block",
    "generate_vtt_block",
    "generate_ttml_block",
    "generate_lrc_block",
    "generate_sub_block",
    "generate_mov_text_block",
    "generate_intro_subtitle",
    # Injectors
    "inject_into_ass",
    "inject_into_srt",
    "inject_into_vtt",
    "inject_into_ttml",
    "inject_into_lrc",
    "inject_into_sub",
    "inject_branding",
    # ASS fallback
    "build_fallback_intro_ass",
    # Pyrogram handlers
    "introsub_cmd",
    "intro_router",
    "capture_intro_text",
    "capture_intro_color",
    "capture_intro_size",
    "init",
]
