
#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʀᴇsɪʟɪᴇɴᴛ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

import os
import re
import logging
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Phase 2 — Source-type tier constants for episode/season conflict resolution.
# Higher value = higher priority; breaks ties before confidence scoring.
# ---------------------------------------------------------------------------
SRC_EXPLICIT = 4  # SxxExx, Season X Episode Y keyword forms
SRC_E_PREFIX = 3  # Standalone E-prefix or EP-prefix
SRC_BRACKET  = 2  # Bracketed standalone number [07] / (07)
SRC_LEADING  = 1  # Bare leading number "060 - Title"
SRC_INFERRED = 0  # Everything else


@dataclass
class Token:
    text: str
    start: int
    end: int
    bracket_type: Optional[str] = None
    kinds: Set[str] = field(default_factory=set)


@dataclass
class Candidate:
    type: str
    value: Any
    raw: str
    start: int
    end: int
    confidence: float
    priority: int = 0
    sortable: Optional[str] = None
    bracket_type: Optional[str] = None
    source_type: int = SRC_INFERRED  # Phase 2 — tier for episode/season resolution


# ---------------------------------------------------------------------------
# Field priorities
# ---------------------------------------------------------------------------
FIELD_PRIORITY: Dict[str, int] = {
    "season":           100,
    "season_start":      99,
    "season_end":        99,
    "episode":           95,
    "episode_start":     95,
    "episode_end":       95,
    "episode_type":      92,
    "episode_absolute":  91,
    "part":              90,
    "chapter":           90,
    "volume":            85,
    "quality":           80,
    "year":              75,
    "alt_title":         72,   # Phase 1 — alternate/translated title
    "group":             70,
    "source":            65,
    "edition":           60,
    "release_flags":     58,
    "video_codec":       50,
    "hdr_format":        48,
    "bit_depth":         45,
    "audio_codec":       40,
    "audio_layout":      35,
    "audio_tracks":      30,
    "language":          25,
    "version":           20,
    "watermark":          5,
}

# ---------------------------------------------------------------------------
# Lookup / mapping tables
# ---------------------------------------------------------------------------
MAP_QUALITY: Dict[str, str] = {
    "4k":    "2160p",
    "uhd":   "2160p",
    "fhd":   "1080p",
    "sd":    "480p",
    "hdrip": "HDrip",
}

MAP_SOURCE: Dict[str, str] = {
    "amzn":    "WEB-DL",
    "nf":      "WEB-DL",
    "hulu":    "WEB-DL",
    "dsnp":    "WEB-DL",
    "atvp":    "WEB-DL",
    "it":      "WEB-DL",
    "itunes":  "WEB-DL",
    "webrip":  "WEB-DL",
    "web-dl":  "WEB-DL",
    "webrdl":  "WEB-DL",
    "bluray":  "BluRay",
    "bdrip":   "BluRay",
    "brrip":   "BluRay",
    "hdtv":    "HDTV",
}

MAP_EPISODE_TYPE: Dict[str, str] = {
    "OVA":     "OVA",
    "SP":      "SP",
    "SPECIAL": "SP",
    "ONA":     "ONA",
    "OAD":     "OAD",
    "NCOP":    "NCOP",
    "NCED":    "NCED",
}

MAP_EPISODE_TYPE_DISPLAY: Dict[str, str] = {
    "SP":   "Special",
    "NCOP": "OP",
    "NCED": "ED",
}

STREAM_LANG_MAP: Dict[str, str] = {
    "eng": "English",  "en": "English",
    "hin": "Hindi",    "hi": "Hindi",
    "jpn": "Japanese", "ja": "Japanese",
    "tam": "Tamil",    "ta": "Tamil",
    "tel": "Telugu",   "te": "Telugu",
    "mal": "Malayalam","ml": "Malayalam",
    "kan": "Kannada",  "kn": "Kannada",
    "ben": "Bengali",  "bn": "Bengali",
    "pan": "Punjabi",  "pa": "Punjabi",
    "kor": "Korean",   "ko": "Korean",
    "spa": "Spanish",  "es": "Spanish",
    "fra": "French",   "fr": "French",
    "ger": "German",   "de": "German",
}

LANG_PATTERNS: Dict[str, str] = {
    "English":   r"^(eng|english)$",
    "Hindi":     r"^(hin|hindi|hin-eng|हिन्दी)$",
    "Japanese":  r"^(jpn|japanese|jap|日本語)$",
    "Tamil":     r"^(tam|tamil|தமிழ்)$",
    "Telugu":    r"^(tel|telugu)$",
    "Malayalam": r"^(mal|malayalam)$",
    "Kannada":   r"^(kan|kannada)$",
    "Bengali":   r"^(ben|bengali)$",
    "Punjabi":   r"^(pan|punjabi)$",
}

TITLE_LANG_WORDS: Dict[str, List[str]] = {
    "English":   ["English"],
    "Hindi":     ["Hindi", "हिन्दी"],
    "Japanese":  ["Japanese", "日本語"],
    "Tamil":     ["Tamil", "தமிழ்"],
    "Telugu":    ["Telugu"],
    "Malayalam": ["Malayalam"],
    "Kannada":   ["Kannada"],
    "Bengali":   ["Bengali"],
    "Punjabi":   ["Punjabi"],
}

AUDIO_BRACKET_LANGS: Set[str] = set(TITLE_LANG_WORDS.keys())

RESOLUTION_VALUES: Set[str] = {"144", "240", "360", "480", "720", "800", "1080", "1440", "2160"}
RESOLUTION_INTS:   Set[int] = {int(v) for v in RESOLUTION_VALUES}  # Phase 2 — integer fast lookup

QUALITY_VALUES:     Set[str] = {"144p", "240p", "360p", "480p", "720p", "800p", "1080p", "1440p", "2160p", "HDrip"}
VIDEO_CODEC_VALUES: Set[str] = {"x264", "x265", "h264", "h265", "hevc", "av1"}
HDR_VALUES:         Set[str] = {"hdr", "hdr10", "hdr10+", "hlg", "dv", "dolbyvision"}
AUDIO_CODEC_VALUES: Set[str] = {"aac", "ac3", "dts", "ddp", "dd", "eac3", "truehd", "flac", "opus", "mp3", "atmos"}
AUDIO_LAYOUT_VALUES: Set[str] = {"2.0", "5.1", "7.1", "stereo", "mono", "multichannel"}
RELEASE_FLAG_VALUES: Set[str] = {"sub", "dub", "esub", "msubs", "multi-sub"}
SOURCE_VALUES:       Set[str] = set(MAP_SOURCE.keys())

WATERMARK_PATTERNS: List[str] = [
    r"(?i)^@\w{3,}$",
    r"(?i)^[A-Z0-9_]+$",
    r"(?i)^BY\s+[A-Z0-9 _-]+$",
]

NOISE_WORDS: Set[str] = {
    "esub", "msubs", "sub", "dub", "multi", "batch", "complete",
    "proper", "repack", "uncut", "raw", "web-dl", "webrip", "bluray",
    "hdrip", "x264", "x265", "h264", "h265", "hevc", "av1",
    "aac", "ac3", "dts", "ddp", "dd", "eac3", "truehd", "flac", "opus",
    "mp3", "atmos", "hdr", "hdr10", "hdr10+", "hlg", "dv", "dolbyvision",
}

# Phase 3a — editorial qualifiers that belong in 'edition', not 'group'
EDITORIAL_WORDS: Set[str] = {
    "uncensored", "director", "directors", "cut", "theatrical",
    "extended", "remastered", "restored", "unrated",
}

# All tokens that indicate a bracket segment is technical (not an alt-title)
_TECHNICAL_TOKENS: Set[str] = (
    QUALITY_VALUES
    | VIDEO_CODEC_VALUES
    | HDR_VALUES
    | AUDIO_CODEC_VALUES
    | AUDIO_LAYOUT_VALUES
    | RELEASE_FLAG_VALUES
    | SOURCE_VALUES
    | NOISE_WORDS
    | {"dual", "multi", "tri", "sub", "dub"}
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def normalize_lang_code(code: str) -> str:
    """Map a BCP-47 / ISO 639 language code to a human-readable name."""
    if not code:
        return ""
    c = code.lower().strip()
    return STREAM_LANG_MAP.get(c, c.title())


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------
class PeakTokenEngineV2_REXY:
    """
    Filename metadata extractor.

    Pipeline:
        _normalize → _tokenize → _classify_tokens → _detect_languages →
        _detect_series_patterns → _detect_episode_only_patterns →
        _detect_part_patterns → _detect_manga_patterns →
        _detect_anime_episode_patterns → _detect_scene_format →
        _detect_batch_patterns → _detect_channel_watermarks →
        _detect_hdr_formats → _detect_extended_audio →
        _score_candidates → _resolve_conflicts →
        _normalize_episode_resolution → _rebuild_title → result dict
    """

    def __init__(self, filename: str) -> None:
        if not isinstance(filename, str) or not filename.strip():
            self.valid = False
            return

        self.valid = True
        self.raw_input = filename
        self.basename = os.path.basename(filename)
        self.stem, self.extension = os.path.splitext(self.basename)
        self.normalized = self._normalize(self.stem)
        self.tokens: List[Token] = []
        self.candidates: List[Candidate] = []
        self.resolved: Dict[str, Candidate] = {}
        self.mode = "MOVIE"
        self.bracket_segments: List[Tuple[str, int, int, str]] = []

    # ------------------------------------------------------------------
    # Phase 4a + general normalization
    # ------------------------------------------------------------------
    def _normalize(self, text: str) -> str:
        name = unicodedata.normalize("NFKC", text)
        name = name.replace("{", "[").replace("}", "]")
        name = re.sub(r"(?i)\bhttps?://\S+\b", " ", name)
        name = re.sub(r"(?i)\bwww\.\S+\b", " ", name)
        name = re.sub(r"[≈≠~±]", " ", name)
        name = re.sub(r"(?i)»\s*BY\s+[A-Za-z0-9 _-]+", " ", name)
        name = re.sub(r"(?i)\bVia\s+@[A-Za-z0-9_]+", " ", name)

        # Strip plain channel tags early (e.g. @XANIME@XANIME_UNIVERSE)
        name = re.sub(r"@[a-zA-Z0-9_]+", " ", name)

        name = re.sub(r"(?i)\bH[.\s]?264\b", "H264", name)
        name = re.sub(r"(?i)\bH[.\s]?265\b", "H265", name)
        name = re.sub(r"(?i)\bDolby[ .]?Vision\b", "DolbyVision", name)
        name = re.sub(r"(?i)\bAAC[.\s]?2[.\s]?0\b", "AAC2.0", name)
        name = re.sub(r"(?i)\bAAC[.\s]?5[.\s]?1\b", "AAC5.1", name)
        name = re.sub(r"(?i)\bDDP[.\s]?(2\.0|5\.1|7\.1)\b", lambda m: f"DDP{m.group(1)}", name)
        name = re.sub(r"(?i)\bDD[.\s]?(2\.0|5\.1|7\.1)\b",  lambda m: f"DD{m.group(1)}", name)
        name = re.sub(r"(?i)\bEAC3[.\s]?(2\.0|5\.1|7\.1)\b", lambda m: f"EAC3{m.group(1)}", name)
        name = re.sub(r"(?i)\bTrueHD[\s.]?(\d+\.?\d*)\b", r"TrueHD \1", name)
        name = re.sub(r"(?i)\bWEB[.\s-]?DL\b", "WEB-DL", name)
        name = re.sub(r"(?i)\bWEB[.\s-]?Rip\b", "WEBRip", name)
        name = re.sub(r"(?i)\bMSubs\b", "MSubs", name)
        name = re.sub(r"(?i)\bE-?Sub\b", "Esub", name)
        name = re.sub(r"(?i)\bDual[ ._-]?Audio\b", "Dual Audio", name)
        name = re.sub(r"(?i)\bMulti[ ._-]?Audio\b", "Multi Audio", name)
        name = re.sub(r"(?i)\bMulti[- ]?audios?\b", "Multi Audio", name)

        # Normalize S01 01 / S1 01 / S01_01 / S01.01 → S01E01
        name = re.sub(
            r"(?i)\bS(\d{1,2})[\s._-]+Ep(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bS(\d{1,2})[\s._]+(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bSeason[\s._-]*(\d{1,2})[\s._-]*Episode[\s._-]*(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bS(?:EASON|S)?[\s._-]*(\d{1,2})[\s._-]*E(?:P|PS|PISODE|PISODES)?[\s._-]*(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bS(\d{1,2})\s+E(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bSS[\s._-]*(\d{1,2})\b",
            lambda m: f"S{int(m.group(1)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bEE[\s._-]*(\d{1,4})\b",
            lambda m: f"E{int(m.group(1)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\b(\d{1,2})x(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bP(?:art|t)?[\s._-]*(\d{1,2})[\s._-]+(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bS(\d{1,2})\s*-\s*(\d{1,4})\b",
            lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}",
            name,
        )
        name = re.sub(
            r"(?i)\bE(?:P|PS|PISODE|PISODES)?[\s._-]*(\d{1,4})(v\d+)?\b",
            lambda m: f"E{int(m.group(1)):02d}{m.group(2) or ''}",
            name,
        )
        name = re.sub(r"(?i)第\s*(\d{1,4})\s*[話话]", lambda m: f"E{int(m.group(1)):02d}", name)
        name = re.sub(
            r"(?i)\bVol(?:ume)?[.\s_-]*(\d+)[.\s_-]*Ch(?:apter)?[.\s_-]*(\d+(?:\.\d+)?)\b",
            lambda m: f"V{int(m.group(1)):02d}C{m.group(2)}",
            name,
        )
        name = re.sub(
            r"(?i)\bS(\d{1,2})-S(\d{1,2})\b",
            lambda m: f" BATCH_S{int(m.group(1)):02d}_S{int(m.group(2)):02d} ",
            name,
        )
        name = re.sub(
            r"\[(\d{1,4})\s*-\s*(\d{1,4})\]",
            lambda m: f" [EP_RANGE_{int(m.group(1)):02d}_{int(m.group(2)):02d}] ",
            name,
        )

        # Phase 4a — protect decimal dots in version/chapter numbers before the
        # general dot-splitting rules below destroy them (e.g. "v1.5", "Ch1.5").
        # Sentinel \x00 never appears in filenames naturally.
        _SENTINEL = "\x00"
        name = re.sub(
            r"(?i)\b(v\d+\.\d+)\b",
            lambda m: m.group(0).replace(".", _SENTINEL),
            name,
        )
        name = re.sub(
            r"(?i)\b(ch?\d+\.\d+)\b",  # matches ch1.5 AND C1.5 (from V01C1.5)
            lambda m: m.group(0).replace(".", _SENTINEL),
            name,
        )

        name = name.replace("_", " ").replace("~", " ")
        name = re.sub(r"[⌯•|]+", " ", name)
        name = re.sub(r"(?<=[A-Za-z])\.(?=[A-Za-z0-9])", " ", name)
        name = re.sub(r"(?<=[0-9])\.(?=[A-Za-z])", " ", name)
        # Fixed: was r"(?<!\d)\.|\\.(?!\d)" — second alt was matching backslash+any
        name = re.sub(r"(?<!\d)\.|\.(?!\d)", " ", name)
        name = re.sub(r"\s+", " ", name).strip()

        # Restore sentinel → original dot
        name = name.replace(_SENTINEL, ".")
        return name

    # ------------------------------------------------------------------
    # Parse pipeline
    # ------------------------------------------------------------------
    def parse(self) -> Dict[str, Any]:
        if not self.valid:
            return {"title": "", "mode": "UNKNOWN"}

        self._tokenize()
        self._classify_tokens()
        self._detect_languages()
        self._detect_series_patterns()
        self._detect_episode_only_patterns()
        self._detect_part_patterns()
        self._detect_manga_patterns()
        self._detect_anime_episode_patterns()
        self._detect_scene_format()
        self._detect_batch_patterns()
        self._detect_channel_watermarks()
        self._detect_hdr_formats()
        self._detect_extended_audio()
        self._score_candidates()
        self._resolve_conflicts()
        self._normalize_episode_resolution()  # Phase 2d

        title = self._rebuild_title()
        langs = sorted(list(set(c.value for c in self.candidates if c.type == "language")))

        res: Dict[str, Any] = {
            "title":    title,
            "mode":     self.mode,
            "parsed":   {},
            "raw":      {},
            "confidence": {},
            "sortable": {},
            "languages": langs,
            "language":  langs[0] if langs else None,
        }

        for cand in sorted(self.resolved.values(), key=lambda x: x.priority, reverse=True):
            res["parsed"][cand.type]     = cand.value
            res["raw"][cand.type]        = cand.raw
            res["confidence"][cand.type] = cand.confidence
            if cand.sortable:
                res["sortable"][cand.type] = cand.sortable
            res[cand.type] = cand.value

        # Phase 1 — surface best alt_title candidate
        alt_cands = sorted(
            [c for c in self.candidates if c.type == "alt_title"],
            key=lambda c: c.confidence,
            reverse=True,
        )
        if alt_cands:
            res["alt_title"] = alt_cands[0].value

        # Phase 3c — collect ALL group candidates so UIs can display all release groups
        group_cands = sorted(
            [c for c in self.candidates if c.type == "group"],
            key=lambda c: c.confidence,
            reverse=True,
        )
        res["groups"] = [c.value for c in group_cands]
        if group_cands:
            res["group"] = group_cands[0].value

        s       = res.get("season")
        e       = res.get("episode")
        e_start = res.get("episode_start")
        e_end   = res.get("episode_end")

        if s and e_start and e_end:
            res["season_episode"] = f"S{s}E{e_start}-{e_end}"
        elif s and e:
            res["season_episode"] = f"S{s}E{e}"
        elif s:
            res["season_episode"] = f"S{s}"
        elif e:
            res["season_episode"] = f"E{e}"

        raw_ep_type = res.get("episode_type", "")
        if raw_ep_type:
            res["type"] = MAP_EPISODE_TYPE_DISPLAY.get(raw_ep_type, raw_ep_type)

        if (
            not res.get("episode_absolute")
            and e and not s
            and self.mode == "SERIES"
            and re.fullmatch(r"\d+", e)
        ):
            res["episode_absolute"] = str(int(e))

        audio = (
            res.get("audio_tracks")
            or res.get("release_flags")
            or res.get("audio_codec")
            or ("AAC" if res.get("audio_hint") else "")
        )

        if audio == "Dual Audio":
            audio = "Dual"
        elif audio == "Multi Audio":
            audio = "Multi"
        elif audio == "Tri Audio":
            audio = "Tri"
        elif audio == "Multi-Sub":
            audio = "Sub"
        elif audio in AUDIO_BRACKET_LANGS:
            audio = "AAC"
        elif not audio:
            audio = "AAC"

        res["audio"] = audio

        if res.get("hdr_format"):
            res["hdr"] = res["hdr_format"]

        codec  = res.get("audio_codec", "")
        layout = res.get("audio_layout", "")
        res["audio_full"] = f"{codec} {layout}".strip() if (codec and layout) else (codec or layout or "")

        if res.get("episode_start") and res.get("episode_end"):
            res["is_batch"]    = True
            res["batch_range"] = f"{res['episode_start']}-{res['episode_end']}"
        else:
            res["is_batch"] = False

        # Phase 4c — shallow copy so callers cannot corrupt the LRU-cached dict
        return dict(res)

    # ------------------------------------------------------------------
    # Phase 4b — Tokenizer with unmatched-bracket fallback
    # ------------------------------------------------------------------
    def _tokenize(self) -> None:
        text = self.normalized
        n    = len(text)
        i    = 0
        pos  = 0

        while i < n:
            if text[i] in "[(":
                start_char = text[i]
                end_char   = "]" if start_char == "[" else ")"

                if i > pos:
                    self._create_tokens_from_text(text[pos:i], pos, None)

                start_idx  = i
                nest_level = 1
                i += 1

                while i < n and nest_level > 0:
                    if text[i] == start_char:
                        nest_level += 1
                    elif text[i] == end_char:
                        nest_level -= 1
                    i += 1

                if nest_level == 0:
                    b_type = "square" if start_char == "[" else "round"
                    inner  = text[start_idx + 1:i - 1]
                    self.bracket_segments.append((inner, start_idx, i, b_type))
                    self._create_tokens_from_text(inner, start_idx + 1, b_type)
                    pos = i
                else:
                    # Phase 4b — unmatched bracket: warn and treat span as plain text
                    logger.warning("Unmatched '%s' in: %s", start_char, self.basename)
                    self._create_tokens_from_text(text[start_idx:], start_idx, None)
                    pos = n
            else:
                i += 1

        if pos < n:
            self._create_tokens_from_text(text[pos:], pos, None)

    def _create_tokens_from_text(self, text: str, offset: int, b_type: Optional[str]) -> None:
        for m in re.finditer(r"\S+", text):
            self.tokens.append(
                Token(
                    text=m.group(0),
                    start=offset + m.start(),
                    end=offset + m.end(),
                    bracket_type=b_type,
                )
            )

    # ------------------------------------------------------------------
    # Token classification
    # ------------------------------------------------------------------
    def _classify_tokens(self) -> None:
        for tok in self.tokens:
            t = tok.text.lower()

            if re.fullmatch(r"\d{3,4}p", t) or t in {"4k", "uhd", "fhd", "sd", "hdrip"}:
                tok.kinds.add("quality")

            if t in VIDEO_CODEC_VALUES:
                tok.kinds.add("video_codec")

            if re.fullmatch(r"(10bit|8bit)", t):
                tok.kinds.add("bit_depth")

            if re.fullmatch(r"[a-f0-9]{6,8}", t):
                tok.kinds.add("crc")

            if t in RELEASE_FLAG_VALUES:
                tok.kinds.add("release_flags")

            if t in SOURCE_VALUES:
                tok.kinds.add("source")

            if re.fullmatch(r"(proper|repack|remux|uncut|extended|complete|batch)", t):
                tok.kinds.add("edition")

            if re.fullmatch(r"aac(2\.0|5\.1)", t):
                tok.kinds.add("audio_codec")
                self.candidates.append(
                    Candidate(
                        type="audio_layout",
                        value=t.replace("aac", ""),
                        raw=tok.text,
                        start=tok.start,
                        end=tok.end,
                        confidence=0.92,
                        priority=FIELD_PRIORITY["audio_layout"],
                        bracket_type=tok.bracket_type,
                    )
                )
            elif re.fullmatch(r"(ddp|dd|eac3)(2\.0|5\.1|7\.1)", t):
                codec_m  = re.match(r"(ddp|dd|eac3)", t)
                layout_m = re.search(r"(2\.0|5\.1|7\.1)", t)
                codec  = codec_m.group(1).upper() if codec_m else t.upper()
                layout = layout_m.group(1) if layout_m else ""
                self.candidates.append(
                    Candidate(
                        type="audio_codec",
                        value=codec,
                        raw=tok.text,
                        start=tok.start,
                        end=tok.end,
                        confidence=0.95,
                        priority=FIELD_PRIORITY["audio_codec"],
                        bracket_type=tok.bracket_type,
                    )
                )
                if layout:
                    self.candidates.append(
                        Candidate(
                            type="audio_layout",
                            value=layout,
                            raw=tok.text,
                            start=tok.start,
                            end=tok.end,
                            confidence=0.95,
                            priority=FIELD_PRIORITY["audio_layout"],
                            bracket_type=tok.bracket_type,
                        )
                    )
            elif t in AUDIO_CODEC_VALUES:
                tok.kinds.add("audio_codec")

            if t in AUDIO_LAYOUT_VALUES:
                tok.kinds.add("audio_layout")

            if re.fullmatch(r"(dual|dual[- ]?audio|multi|multi[- ]?audio|tri|tri[- ]?audio)", t):
                tok.kinds.add("audio_tracks")

            if t in HDR_VALUES:
                tok.kinds.add("hdr_format")

            if re.fullmatch(r"v\d+", t):
                tok.kinds.add("version")

            for kind in tok.kinds:
                val = tok.text

                if kind == "quality":
                    val = MAP_QUALITY.get(val.lower(), val)
                elif kind == "source":
                    val = MAP_SOURCE.get(val.lower(), val)
                elif kind == "release_flags":
                    low = val.lower()
                    if low == "msubs":
                        val = "Multi-Sub"
                    elif low == "esub":
                        val = "Esub"
                elif kind == "audio_codec":
                    low = val.lower()
                    if low.startswith("aac"):
                        val = "AAC"
                    elif low.startswith("ddp"):
                        val = "DDP"
                    elif low.startswith("dd") and not low.startswith("ddp"):
                        val = "DD"
                    elif low.startswith("eac3"):
                        val = "EAC3"
                    elif low == "truehd":
                        val = "TrueHD"
                    elif low == "atmos":
                        val = "Atmos"
                    else:
                        val = val.upper()
                elif kind == "audio_tracks":
                    lookup = {
                        "dual":       "Dual Audio",
                        "dual audio": "Dual Audio",
                        "multi":      "Multi Audio",
                        "multi audio":"Multi Audio",
                        "tri":        "Tri Audio",
                        "tri audio":  "Tri Audio",
                    }
                    val = lookup.get(val.lower(), val)
                elif kind == "hdr_format":
                    lookup = {
                        "dolbyvision": "DolbyVision",
                        "hdr":         "HDR",
                        "hdr10":       "HDR10",
                        "hdr10+":      "HDR10+",
                        "hlg":         "HLG",
                        "dv":          "DV",
                    }
                    val = lookup.get(val.lower(), val)

                self.candidates.append(
                    Candidate(
                        type=kind,
                        value=val,
                        raw=tok.text,
                        start=tok.start,
                        end=tok.end,
                        confidence=0.9 if tok.bracket_type else 0.75,
                        priority=FIELD_PRIORITY.get(kind, 0),
                        bracket_type=tok.bracket_type,
                    )
                )

        # Post-process: merge "Multi"/"Dual"/"Tri" + "Audio" split tokens into
        # a single audio_tracks candidate. This prevents orphaned "Audio" token
        # from leaking into the title when normalization already split the phrase.
        _audio_heads = {"multi", "dual", "tri"}
        _merged_spans: set = set()
        for i, tok in enumerate(self.tokens):
            if tok.text.lower() in _audio_heads and i + 1 < len(self.tokens):
                next_tok = self.tokens[i + 1]
                if next_tok.text.lower() == "audio":
                    combined = f"{tok.text.title()} Audio"
                    self.candidates.append(
                        Candidate(
                            type="audio_tracks",
                            value=combined,
                            raw=f"{tok.text} {next_tok.text}",
                            start=tok.start,
                            end=next_tok.end,
                            confidence=0.97 if tok.bracket_type else 0.85,
                            priority=FIELD_PRIORITY["audio_tracks"],
                            bracket_type=tok.bracket_type,
                        )
                    )
                    _merged_spans.add(next_tok.start)
        # Remove the orphaned "Audio" token so it won't appear in the title
        if _merged_spans:
            self.tokens = [t for t in self.tokens if t.start not in _merged_spans]

    # ------------------------------------------------------------------
    # Language detection
    # ------------------------------------------------------------------
    def _detect_languages(self) -> None:
        for tok in self.tokens:
            for lang, pat in LANG_PATTERNS.items():
                if re.fullmatch(pat, tok.text, re.I):
                    self.candidates.append(
                        Candidate(
                            type="language",
                            value=lang,
                            raw=tok.text,
                            start=tok.start,
                            end=tok.end,
                            confidence=0.86,
                            priority=FIELD_PRIORITY["language"],
                            bracket_type=tok.bracket_type,
                        )
                    )
                    if tok.bracket_type == "square" and lang in AUDIO_BRACKET_LANGS:
                        self.candidates.append(
                            Candidate(
                                type="audio_hint",
                                value=lang,
                                raw=tok.text,
                                start=tok.start,
                                end=tok.end,
                                confidence=0.96,
                                priority=FIELD_PRIORITY["audio_tracks"],
                                bracket_type=tok.bracket_type,
                            )
                        )

    # ------------------------------------------------------------------
    # Phase 2 — Series/season patterns (SRC_EXPLICIT)
    # ------------------------------------------------------------------
    def _detect_series_patterns(self) -> None:
        text = self.normalized

        # [S01] [07] or [S01] [07-12]
        for m in re.finditer(r"(?i)\[S(\d{1,2})\]\s*\[(\d{1,4})(?:\s*-\s*(\d{1,4}))?\]", text):
            season = m.group(1).zfill(2)
            ep1    = m.group(2).zfill(2)
            ep2    = m.group(3)
            self._add_cand("season", season, m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)
            if ep2:
                ep2 = ep2.zfill(2)
                self._add_cand("episode_start", ep1, m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)
                self._add_cand("episode_end",   ep2, m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)
                self._add_cand("episode", f"{ep1}-{ep2}", m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)
            else:
                self._add_cand("episode", ep1, m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)

        # [S01 E07]
        for m in re.finditer(r"(?i)\[S(\d{1,2})\s*E(\d{1,4})\]", text):
            self._add_cand("season",  m.group(1).zfill(2), m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)
            self._add_cand("episode", m.group(2).zfill(2), m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)

        # [S01-E07]
        for m in re.finditer(r"(?i)\[S(\d{1,2})-E(\d{1,4})\]", text):
            self._add_cand("season",  m.group(1).zfill(2), m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)
            self._add_cand("episode", m.group(2).zfill(2), m, 1.0, bracket_type="square", source_type=SRC_EXPLICIT)

        # S01E07 or S01E07-E12
        for m in re.finditer(r"(?i)\bS(\d{1,2})E(\d{1,4})(?:[- ]?E?(\d{1,4}))?\b", text):
            self._add_cand("season", m.group(1).zfill(2), m, 1.0, source_type=SRC_EXPLICIT)
            ep1 = m.group(2).zfill(2)
            ep2 = m.group(3)
            if ep2:
                ep2 = ep2.zfill(2)
                self._add_cand("episode_start", ep1, m, 1.0, source_type=SRC_EXPLICIT)
                self._add_cand("episode_end",   ep2, m, 1.0, source_type=SRC_EXPLICIT)
                self._add_cand("episode", f"{ep1}-{ep2}", m, 1.0, source_type=SRC_EXPLICIT)
            else:
                self._add_cand("episode", ep1, m, 1.0, source_type=SRC_EXPLICIT)

        # 1x07 style
        for m in re.finditer(r"(?i)\b(\d{1,2})x(\d{1,4})\b", text):
            self._add_cand("season",  m.group(1).zfill(2), m, 0.98, source_type=SRC_EXPLICIT)
            self._add_cand("episode", m.group(2).zfill(2), m, 0.98, source_type=SRC_EXPLICIT)

        # Standalone S01
        for m in re.finditer(r"(?i)\bS(\d{1,2})\b", text):
            self._add_cand("season", m.group(1).zfill(2), m, 0.96, source_type=SRC_EXPLICIT)

    # ------------------------------------------------------------------
    # Phase 2 — Episode-only patterns (SRC_E_PREFIX)
    # ------------------------------------------------------------------
    def _detect_episode_only_patterns(self) -> None:
        text = self.normalized

        for m in re.finditer(r"(?i)\bE(\d{1,4})(v\d+)?\b", text):
            self._add_cand("episode", m.group(1).zfill(2), m, 0.97, source_type=SRC_E_PREFIX)
            if m.group(2):
                self.candidates.append(
                    Candidate(
                        type="version",
                        value=m.group(2).lower(),
                        raw=m.group(0),
                        start=m.start(),
                        end=m.end(),
                        confidence=0.85,
                        priority=FIELD_PRIORITY["version"],
                    )
                )

        # Phase 2 – Emergency fallback for Ep-prefix (e.g., "Ep01")
        for m in re.finditer(r"(?i)\bEp(\d{1,4})\b", text):
            self._add_cand("episode", m.group(1).zfill(2), m, 0.93, source_type=SRC_E_PREFIX)

        for m in re.finditer(r"(?i)(?:^|[\s\-])(\d{1,3})v(\d+)\b", text):
            self._add_cand("episode", m.group(1).zfill(2), m, 0.82, source_type=SRC_E_PREFIX)
            self.candidates.append(
                Candidate(
                    type="version",
                    value=f"v{m.group(2)}",
                    raw=m.group(0),
                    start=m.start(),
                    end=m.end(),
                    confidence=0.82,
                    priority=FIELD_PRIORITY["version"],
                )
            )

        for m in re.finditer(r"(?i)\b(OVA|SP|ONA|OAD|NCOP|NCED)(?:[\s._-]?(\d{1,2}))?\b", text):
            kind_raw    = m.group(1).upper()
            kind_mapped = MAP_EPISODE_TYPE.get(kind_raw, kind_raw)
            self._add_cand("episode_type", kind_mapped, m, 0.95)
            if m.group(2):
                self._add_cand("episode", m.group(2).zfill(2), m, 0.85, source_type=SRC_E_PREFIX)

    # ------------------------------------------------------------------
    # Part / cour patterns
    # ------------------------------------------------------------------
    def _detect_part_patterns(self) -> None:
        text = self.normalized
        for m in re.finditer(r"\b(?:Part|Pt)[.\s_-]*(\d{1,2})\b", text, re.I):
            self._add_cand("part", m.group(1), m, 0.92)
        for m in re.finditer(r"\bCour[.\s_-]*(\d{1,2})\b", text, re.I):
            self._add_cand("part", m.group(1), m, 0.90)

    # ------------------------------------------------------------------
    # Manga patterns
    # ------------------------------------------------------------------
    def _detect_manga_patterns(self) -> None:
        text = self.normalized
        for m in re.finditer(r"\bCh(?:apter)?[.\- ]+(\d+(?:\.\d+)?)\b", text, re.I):
            self._add_cand("chapter", m.group(1), m, 1.0)
        for m in re.finditer(r"\bV(\d+)[.\- ]?C(\d+(?:\.\d+)?)\b", text, re.I):
            self._add_cand("volume",  m.group(1).zfill(2), m, 1.0)
            self._add_cand("chapter", m.group(2), m, 1.0)
        for m in re.finditer(r"\bVol[.\- ]?(\d+)\b", text, re.I):
            self._add_cand("volume", m.group(1).zfill(2), m, 1.0)

    # ------------------------------------------------------------------
    # Phase 2a — Anime episode patterns (SRC_BRACKET / SRC_LEADING)
    # ------------------------------------------------------------------
    def _detect_anime_episode_patterns(self) -> None:
        text = self.normalized

        for m in re.finditer(r"\bEP_RANGE_(\d{2})_(\d{2})\b", text):
            self._add_cand("episode_start", m.group(1), m, 0.96, source_type=SRC_BRACKET)
            self._add_cand("episode_end",   m.group(2), m, 0.96, source_type=SRC_BRACKET)
            self._add_cand("episode", f"{m.group(1)}-{m.group(2)}", m, 0.96, source_type=SRC_BRACKET)

        # Standalone bracketed episode number: [07] or (07)
        for m in re.finditer(r"(?<!\d)([\[\(])(\d{1,3})(v\d+)?([\]\)])(?!\d)", text, re.I):
            open_b  = m.group(1)
            val     = m.group(2)
            close_b = m.group(4)

            if open_b == "[" and close_b != "]":
                continue
            if open_b == "(" and close_b != ")":
                continue

            ival = int(val)
            if ival in RESOLUTION_INTS or 1950 <= ival <= 2030:
                continue

            nearby = text[max(0, m.start() - 12):min(len(text), m.end() + 12)].lower()
            if any(k in nearby for k in ["bit", "ch", "aac", "dts", "ac3", "fps"]):
                continue

            # Phase 2a — cross-check: skip if a quality/codec candidate is within ±40 chars
            w_lo = max(0, m.start() - 40)
            w_hi = min(len(text), m.end() + 40)
            if any(
                c.type in {"quality", "video_codec"} and w_lo <= c.start and c.end <= w_hi
                for c in self.candidates
            ):
                continue

            if 1 <= ival <= 999:
                b_type = "square" if open_b == "[" else "round"
                self._add_cand("episode", f"{ival:02d}", m, 0.93, bracket_type=b_type, source_type=SRC_BRACKET)

        
        # Bare leading episode number: "060 - Title" or "60. Title"
        for m in re.finditer(r"(?:^|\s)(\d{1,4})(?:\s*[-\u2013\u2014.]\s*)(?=[A-Za-z\[\(])", text):
            val = int(m.group(1))
            if 1 <= val <= 1999 and val not in RESOLUTION_INTS:
                if 1900 <= val <= 2030:
                    nearby = text[max(0, m.start() - 40):min(len(text), m.end() + 40)].lower()
                    if not any(k in nearby for k in ["2160p","1080p","720p","480p","bluray","web-dl","webrip","hdrip","x264","x265"]):
                        continue
                self._add_cand("episode", f"{val:02d}", m, 0.88, source_type=SRC_LEADING)


        # Bare number before a bracket, '@', or end-of-string (e.g., "001 @channel")
        for m in re.finditer(r"(?:^|\s)(\d{1,4})(?=\s*(?:\[|@|$))", text):
            val = int(m.group(1))
            if 1 <= val <= 1999 and val not in RESOLUTION_INTS:
                if 1900 <= val <= 2030:
                    nearby = text[max(0, m.start() - 40):min(len(text), m.end() + 40)].lower()
                    if not any(k in nearby for k in ["2160p","1080p","720p","480p","bluray","web-dl","webrip","hdrip","x264","x265"]):
                        continue
                self._add_cand("episode", f"{val:02d}", m, 0.75, source_type=SRC_LEADING)

        # Episode range in brackets: [01-12]
        for m in re.finditer(r"[\[\(](\d{1,3})\s*-\s*(\d{1,3})[\]\)]", text):
            a, b = int(m.group(1)), int(m.group(2))
            if 1 <= a < b <= 1999:
                b_type = "square" if text[m.start()] == "[" else "round"
                self._add_cand("episode_start", f"{a:02d}", m, 0.88, bracket_type=b_type, source_type=SRC_BRACKET)
                self._add_cand("episode_end",   f"{b:02d}", m, 0.88, bracket_type=b_type, source_type=SRC_BRACKET)
                self._add_cand("episode", f"{a:02d}-{b:02d}", m, 0.88, bracket_type=b_type, source_type=SRC_BRACKET)

    # ------------------------------------------------------------------
    # Phase 3b — Scene group detection (hardened)
    # ------------------------------------------------------------------
    def _detect_scene_format(self) -> None:
        text = self.normalized
        m = re.search(r"-([A-Z][A-Za-z0-9]{1,15})$", text)
        if not m:
            return
        token = m.group(1)
        # Groups containing digits (e.g. "-2049") are only valid when a technical
        # token (quality / source / codec) has already been detected before them,
        # preventing year-suffixed titles like "Blade Runner 2049" from being cut.
        if re.search(r"\d", token) and not self._has_technical_before(m.start()):
            return
        self._add_cand("group", token, m, 0.92)

    # ------------------------------------------------------------------
    # Batch patterns
    # ------------------------------------------------------------------
    def _detect_batch_patterns(self) -> None:
        text = self.normalized
        for m in re.finditer(r"\bBATCH_S(\d{2})_S(\d{2})\b", text):
            self._add_cand("season_start", m.group(1), m, 0.90)
            self._add_cand("season_end",   m.group(2), m, 0.90)
            self._add_cand("season",       m.group(1), m, 0.90)
            self.mode = "SERIES"

    # ------------------------------------------------------------------
    # Phase 1b + 3a + 3b — Channel watermarks, alt-title, editorial routing
    # ------------------------------------------------------------------
    def _detect_channel_watermarks(self) -> None:
        for inner, start, end, b_type in self.bracket_segments:
            content = inner.strip()
            low     = content.lower()

            # Skip segments that are clearly already-classified structured tokens
            if re.fullmatch(r"(?i)S\d{1,2}E\d{1,4}(?:-\d{1,4})?", content):
                continue
            if re.fullmatch(r"(?i)E\d{1,4}", content):
                continue
            if re.fullmatch(r"(?i)\d{1,4}", content):
                continue
            if content in QUALITY_VALUES:
                continue
            if low in NOISE_WORDS:
                continue
            if low in {"dual", "multi", "tri"}:
                continue

            # Phase 1b — alt-title: round brackets with ≥2 capitalized words and no
            # technical tokens. Example: (My Star) in "Oshi no Ko (My Star) S02...".
            if b_type == "round":
                words      = content.split()
                cap_count  = sum(1 for w in words if w and w[0].isupper())
                has_tech   = any(w.lower() in _TECHNICAL_TOKENS for w in words)
                if cap_count >= 2 and not has_tech and len(words) >= 2:
                    self.candidates.append(
                        Candidate(
                            type="alt_title",
                            value=content,
                            raw=content,
                            start=start,
                            end=end,
                            confidence=0.85,
                            priority=FIELD_PRIORITY["alt_title"],
                            bracket_type=b_type,
                        )
                    )
                    continue

            # Phase 3a — editorial qualifiers → route to 'edition', not 'group'
            if any(w.lower() in EDITORIAL_WORDS for w in content.split()):
                self.candidates.append(
                    Candidate(
                        type="edition",
                        value=content,
                        raw=content,
                        start=start,
                        end=end,
                        confidence=0.88,
                        priority=FIELD_PRIORITY["edition"],
                        bracket_type=b_type,
                    )
                )
                continue

            if any(re.fullmatch(pat, content, re.I) for pat in WATERMARK_PATTERNS):
                self.candidates.append(
                    Candidate(
                        type="watermark",
                        value=content,
                        raw=content,
                        start=start,
                        end=end,
                        confidence=0.99,
                        priority=FIELD_PRIORITY["watermark"],
                        bracket_type=b_type,
                    )
                )
                continue

            if re.search(
                r"s\d{1,2}e\d{1,4}|\d{3,4}p|aac|ac3|dts|ddp|eac3|truehd|x264|x265|h264|h265|hevc",
                low,
            ):
                continue

            if len(content.split()) <= 4 and not any(
                word.lower() in NOISE_WORDS for word in content.split()
            ):
                self.candidates.append(
                    Candidate(
                        type="group",
                        value=content,
                        raw=content,
                        start=start,
                        end=end,
                        confidence=0.80,
                        priority=FIELD_PRIORITY["group"],
                        bracket_type=b_type,
                    )
                )

    # ------------------------------------------------------------------
    # HDR format detection
    # ------------------------------------------------------------------
    def _detect_hdr_formats(self) -> None:
        text = self.normalized
        for m in re.finditer(r"\b(HDR10\+?|HLG|DolbyVision|DV)\b", text, re.I):
            val = m.group(1)
            val = "DolbyVision" if val.lower() == "dolbyvision" else val.upper()
            self._add_cand("hdr_format", val, m, 0.95)

    # ------------------------------------------------------------------
    # Extended audio detection
    # ------------------------------------------------------------------
    def _detect_extended_audio(self) -> None:
        text = self.normalized
        for m in re.finditer(r"\b(TrueHD)\s+(7\.1|5\.1|2\.0)\s*(Atmos)?\b", text, re.I):
            self._add_cand("audio_codec", "TrueHD", m, 0.95)
            self._add_cand("audio_layout", m.group(2), m, 0.95)
            if m.group(3):
                self._add_cand("audio_codec", "Atmos", m, 0.70)
        for m in re.finditer(r"\b(DDP|DD|EAC3)(2\.0|5\.1|7\.1)\b", text, re.I):
            self._add_cand("audio_codec",  m.group(1).upper(), m, 0.95)
            self._add_cand("audio_layout", m.group(2), m, 0.95)
        for m in re.finditer(r"\b(FLAC|OPUS|MP3)\b", text, re.I):
            self._add_cand("audio_codec", m.group(1).upper(), m, 0.90)

    # ------------------------------------------------------------------
    # Year scoring
    # ------------------------------------------------------------------
    def _score_candidates(self) -> None:
        text = self.normalized
        for m in re.finditer(r"(?<!\d)((?:19|20)\d{2})(?!\d)", text):
            score  = 0.35
            nearby = text[max(0, m.start() - 24):min(len(text), m.end() + 24)].lower()
            if any(k in nearby for k in ["2160p", "1080p", "720p", "480p", "bluray", "web-dl", "webrip", "hdrip"]):
                score += 0.35
            self.candidates.append(
                Candidate(
                    type="year",
                    value=m.group(1),
                    raw=m.group(0),
                    start=m.start(),
                    end=m.end(),
                    confidence=score,
                    priority=FIELD_PRIORITY["year"],
                )
            )

    # ------------------------------------------------------------------
    # Phase 2b/2c — Conflict resolution with source_type priority tiers
    # ------------------------------------------------------------------
    def _resolve_conflicts(self) -> None:
        if any(c.type in {"episode", "episode_start", "episode_end", "season"} for c in self.candidates):
            self.mode = "SERIES"
        else:
            self.mode = "MOVIE"

        best_per_type: Dict[str, Candidate] = {}
        for cand in self.candidates:
            current = best_per_type.get(cand.type)
            if current is None:
                best_per_type[cand.type] = cand
            elif self._candidate_beats(cand, current):
                best_per_type[cand.type] = cand

        self.resolved = best_per_type

        if "chapter" in self.resolved:
            self.mode = "MANGA"

    def _candidate_beats(self, challenger: Candidate, current: Candidate) -> bool:
        """
        Three-key comparison for conflict resolution:
          1. source_type — for episode/season fields, higher tier wins unconditionally.
          2. effective confidence = raw confidence + 0.05 bracket bonus.
          3. span length — longer raw match preferred as final tiebreaker.
        """
        ep_fields = {
            "episode", "episode_start", "episode_end",
            "season", "season_start", "season_end",
        }
        if challenger.type in ep_fields:
            if challenger.source_type != current.source_type:
                return challenger.source_type > current.source_type

        ch_w = challenger.confidence + (0.05 if challenger.bracket_type else 0.0)
        cu_w = current.confidence    + (0.05 if current.bracket_type    else 0.0)
        if ch_w != cu_w:
            return ch_w > cu_w

        return (challenger.end - challenger.start) > (current.end - current.start)

    # ------------------------------------------------------------------
    # Phase 2d — Post-resolution episode/range normalization
    # ------------------------------------------------------------------
    def _normalize_episode_resolution(self) -> None:
        """
        If both episode_start and episode_end were resolved, force the episode
        field to reflect the full range and ensure is_batch consistency.
        """
        ep_start = self.resolved.get("episode_start")
        ep_end   = self.resolved.get("episode_end")
        if ep_start and ep_end:
            range_val = f"{ep_start.value}-{ep_end.value}"
            if "episode" in self.resolved:
                self.resolved["episode"].value = range_val
            else:
                self.resolved["episode"] = Candidate(
                    type="episode",
                    value=range_val,
                    raw=ep_start.raw,
                    start=ep_start.start,
                    end=ep_end.end,
                    confidence=ep_start.confidence,
                    priority=FIELD_PRIORITY["episode"],
                    source_type=ep_start.source_type,
                    bracket_type=ep_start.bracket_type,
                )

    # ------------------------------------------------------------------
    # Phase 3b helper — check if any technical candidate precedes a position
    # ------------------------------------------------------------------
    def _has_technical_before(self, pos: int) -> bool:
        tech_types = {"quality", "source", "video_codec", "audio_codec", "hdr_format"}
        return any(c.end <= pos and c.type in tech_types for c in self.candidates)

    # ------------------------------------------------------------------
    # Phase 1 — Title reconstruction with trailing noise collapse
    # ------------------------------------------------------------------
    def _strip_title_language_noise(self, title: str) -> str:
        langs: Set[str] = set()
        if "audio_hint" in self.resolved:
            langs.add(self.resolved["audio_hint"].value)
        if "language" in self.resolved:
            langs.add(self.resolved["language"].value)
        for lang in langs:
            for word in TITLE_LANG_WORDS.get(lang, []):
                title = re.sub(rf"(?i)(?:^|\s){re.escape(word)}(?:\s|$)", " ", title)
        return title

    def _rebuild_title(self) -> str:
        text = self.normalized
        mask = [False] * len(text)

        # alt_title is intentionally excluded — its span belongs in the title string
        removable_types: Set[str] = {
            "season", "season_start", "season_end",
            "episode", "episode_start", "episode_end", "episode_type", "episode_absolute",
            "part", "chapter", "volume",
            "quality", "year",
            "group", "watermark",
            "source", "edition", "release_flags", "video_codec", "bit_depth",
            "audio_codec", "audio_layout", "audio_tracks", "audio_hint",
            "language", "hdr_format", "version",
        }

        for cand in self.resolved.values():
            if cand.type not in removable_types:
                continue
            start, end = cand.start, cand.end
            if start > 0 and end < len(text) and cand.bracket_type:
                if cand.bracket_type == "square" and text[start - 1] == "[" and text[end] == "]":
                    start -= 1
                    end   += 1
                elif cand.bracket_type == "round" and text[start - 1] == "(" and text[end] == ")":
                    start -= 1
                    end   += 1
            for i in range(start, end):
                if 0 <= i < len(mask):
                    mask[i] = True

        title = "".join(ch for i, ch in enumerate(text) if not mask[i])

        # Scrub surviving channel tags and watermark tails
        title = re.sub(r"(?i)(?<!\S)(?:@\w{3,})+", " ", title)
        title = re.sub(
            r"(?i)\b(?:Galaxy|Anime|Network|Web|Flio|Team|Rip|Encode)[A-Za-z0-9_]{2,}\b$",
            " ",
            title,
        )

        title = re.sub(r"(?i)\bEP_RANGE_\d+_\d+\b", " ", title)
        title = re.sub(r"(?i)\bBATCH_S\d{2}_S\d{2}\b", " ", title)
        title = re.sub(r"(?i)\b(HDR10\+?|HLG|DV|DolbyVision|Atmos)\b", " ", title)
        title = re.sub(r"(?i)\bCOMPLETE\b", " ", title)
        title = re.sub(r"(?i)\bv\d+\b", " ", title)
        title = re.sub(r"(?i)\b(Esub|MSubs|Sub|Dub)\b", " ", title)
        title = self._strip_title_language_noise(title)

        title = re.sub(r"[\[\(]\s*[\]\)]", " ", title)
        title = re.sub(r"[\(\[\]\)]+", " ", title)
        title = re.sub(r"[⌯•|]+", " ", title)
        title = re.sub(r"\s+[-:._]+\s+[-:._]+\s+", " ", title)
        title = re.sub(r"^\s*[-\u2013\u2014:._]+\s*", "", title)
        title = re.sub(r"\s*[-\u2013\u2014:._]+\s*$", "", title)
        title = re.sub(r"[-\u2013\u2014:._]{2,}", " ", title)
        title = re.sub(r"\s+", " ", title).strip(" .-_")

        # Phase 1b — strip trailing separator words and non-alphanumeric tail noise
        title = re.sub(r"(?i)\s+\b(by|from|via)\b\s*$", "", title).strip()
        title = re.sub(
            r"[\s\W]+$",
            lambda m: "" if not re.search(r"[A-Za-z0-9]", m.group()) else m.group(),
            title,
        )
        title = re.sub(r"\s+", " ", title).strip(" .-_")

        # Deduplicate consecutive identical words (case-insensitive)
        words: List[str] = title.split()
        deduped: List[str] = []
        for word in words:
            if deduped and deduped[-1].casefold() == word.casefold():
                continue
            deduped.append(word)

        return " ".join(deduped)

    # ------------------------------------------------------------------
    # Candidate factory
    # ------------------------------------------------------------------
    def _add_cand(
        self,
        stype: str,
        val: str,
        match: re.Match,
        conf: float,
        bracket_type: Optional[str] = None,
        source_type: int = SRC_INFERRED,
    ) -> None:
        sortable = None
        if stype == "chapter" and isinstance(val, str) and "." in val:
            try:
                sortable = f"{float(val):05.2f}"
            except Exception:
                pass

        self.candidates.append(
            Candidate(
                type=stype,
                value=val,
                raw=match.group(0),
                start=match.start(),
                end=match.end(),
                confidence=conf,
                priority=FIELD_PRIORITY.get(stype, 0),
                sortable=sortable,
                bracket_type=bracket_type,
                source_type=source_type,
            )
        )


# ---------------------------------------------------------------------------
# Phase 4c — LRU-cached parse with mutable-dict safety.
# The cache stores the raw result from parse(); parse_filename() returns a
# fresh shallow copy on every call so callers cannot corrupt the cache.
# ---------------------------------------------------------------------------
PARSER_VERSION = "2.3"   # bump on any parsing-logic change

@lru_cache(maxsize=512)
def _parse_filename_cached(filename: str, _cache_ver: str = PARSER_VERSION) -> Dict[str, Any]:
    return PeakTokenEngineV2_REXY(filename).parse()


def parse_filename(filename: str) -> Dict[str, Any]:
    """
    Parse a media filename and return a metadata dict.
    Always returns a fresh shallow copy — safe for callers who mutate the result.
    """
    return dict(_parse_filename_cached(filename, PARSER_VERSION))


# ---------------------------------------------------------------------------
# Convenience extractors
# ---------------------------------------------------------------------------
def extract_title(filename: str) -> str:
    return parse_filename(filename).get("title", "")


def extract_season_episode(filename: str) -> Tuple[Optional[str], Optional[str]]:
    res = parse_filename(filename)
    return res.get("season"), res.get("episode")


def extract_quality(filename: str) -> str:
    return parse_filename(filename).get("quality", "Unknown")


def get_display_quality(filename: str) -> str:
    return parse_filename(filename).get("quality", "Unknown")


def extract_audio(filename: str) -> str:
    return parse_filename(filename).get("audio", "AAC")


def extract_chapter(filename: str) -> Optional[str]:
    return parse_filename(filename).get("chapter")


def extract_volume(filename: str) -> Optional[str]:
    return parse_filename(filename).get("volume")


def extract_year(filename: str) -> Optional[str]:
    return parse_filename(filename).get("year")


def extract_group(filename: str) -> str:
    return parse_filename(filename).get("group", "Unknown")


def format_filename(
    filename: str,
    extension: Optional[str] = None,
    include_unknown_quality: bool = False,
    include_unknown_audio: bool = False,
) -> str:
    res     = parse_filename(filename)
    season  = res.get("season")
    episode = res.get("episode")
    title   = res.get("title") or "Unknown"
    quality = res.get("quality") or "Unknown"
    audio   = res.get("audio") or "AAC"

    ext = extension
    if ext is None:
        ext = os.path.splitext(os.path.basename(filename))[1] or ".mkv"
    if ext and not ext.startswith("."):
        ext = f".{ext}"

    parts: List[str] = []
    if season and episode:
        parts.append(f"[S{season}-E{episode}]")
    elif season:
        parts.append(f"[S{season}]")
    elif episode:
        parts.append(f"[E{episode}]")
    parts.append(title)
    if quality != "Unknown" or include_unknown_quality:
        parts.append(f"[{quality}]")
    if audio != "Unknown" or include_unknown_audio:
        parts.append(f"[{audio}]")

    return f"{' '.join(parts)}{ext}"


# ---------------------------------------------------------------------------
# Phase 5 — Debug helper
# ---------------------------------------------------------------------------
def confidence_report(filename: str) -> List[Dict[str, Any]]:
    """
    Return all candidates sorted by (type, confidence descending).
    Use this to diagnose why a specific candidate won conflict resolution.

    Example:
        import pprint
        pprint.pprint(confidence_report("Show.S01E07.720p.BluRay.mkv"))
    """
    eng = PeakTokenEngineV2_REXY(filename)
    eng.parse()
    return [
        {
            "type":        c.type,
            "value":       c.value,
            "confidence":  c.confidence,
            "source_type": c.source_type,
            "raw":         c.raw,
            "bracket_type": c.bracket_type,
        }
        for c in sorted(eng.candidates, key=lambda c: (c.type, -c.confidence))
    ]


def clear_cache() -> None:
    """Clear the LRU parse cache (useful when patching constants in tests)."""
    _parse_filename_cached.cache_clear()
