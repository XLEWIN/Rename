
#┌─ 𝗥𝗘𝗫𝗬 𝗪𝗢𝗥𝗞𝗦𝗛𝗢𝗣 ───────────────────────────────┐
#│  ʀᴇ-ᴄᴏᴅᴇ ᴏᴘᴛɪᴍɪᴢᴀᴛɪᴏɴ ᴅᴏɴᴇ                │      @NyxRexy
#│  (ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ)                   │      @ShadowedTomb
#└──────────────────────────────────────────┘

# ─────────────────────────────────────────────────────────────
#  | 𝗡𝗙𝗦𝗪 𝗖𝗢𝗡𝗧𝗘𝗡𝗧 𝗙𝗜𝗟𝗧𝗘𝗥 | - ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ᴠᴇʀsɪᴏɴ
# ─────────────────────────────────────────────────────────────

import re
from typing import Set, Dict, List
from functools import lru_cache

# ─────────────────────────────────────────────────────────────
#  | 𝗡𝗙𝗦𝗪 𝗞𝗘𝗬𝗪𝗢𝗥𝗗𝗦 𝗖𝗔𝗧𝗘𝗚𝗢𝗥𝗜𝗘𝗦 |
# ─────────────────────────────────────────────────────────────

NSFW_KEYWORDS: Dict[str, Set[str]] = {
    "general": {
        "porn", "sex", "nude", "naked", "boobs", "tits", "pussy", "dick", "cock", "ass",
        "fuck", "blowjob", "cum", "orgasm", "shemale", "erotic", "masturbate", "anal",
        "hardcore", "bdsm", "fetish", "lingerie", "xxx", "milf", "gay", "lesbian",
        "threesome", "squirting", "butt plug", "dildo", "vibrator", "escort", "handjob",
        "striptease", "kinky", "pornstar", "sex tape", "spank", "swinger", "taboo", "cumshot",
        "deepthroat", "domination", "submission", "handcuffs", "orgy", "roleplay", "sex toy",
        "voyeur", "cosplay", "adult", "culture", "pornhwa", "netorare", "netori", "netorase",
        "eromanga", "incest", "stepmom", "stepdad", "stepsister", "stepbrother", "stepson",
        "stepdaughter", "ntr", "gangbang", "facial", "golden shower", "pegging", "rimming",
        "rough sex", "dirty talk", "sex chat", "nude pic", "lewd", "titty", "twerk",
        "breasts", "penis", "vagina", "clitoris", "genitals", "sexual", "kamasutra",
        "pedo", "rape", "bondage", "cum inside", "creampie", "sex slave", "sex doll",
        "sex machine", "latex", "oral sex", "butt", "slut", "whore", "tramp", "skank",
        "cumdumpster", "cultured", "ecchi", "doujin", "hentai", "smut", "waifu",
        "futanari", "tentacle"
    },
    
    "hentai": {
        "hentai", "doujinshi", "ecchi", "yaoi", "shota", "loli", "tentacle", "futanari",
        "bishoujo", "bishounen", "mecha hentai", "hentai manga", "hentai anime", "smut",
        "eroge", "visual novel", "h-manga", "h-anime", "adult manga", "18+ anime",
        "18+ manga", "lewd anime", "lewd manga", "animated porn", "animated sex",
        "hentai game", "hentai art", "hentai drawing", "hentai doujin", "yaoi hentai",
        "hentai comic", "hentai picture", "hentai scene", "hentai story", "hentai video",
        "hentai movie", "hentai episode", "hentai series"
    },
    
    "abbreviations": {
        "pr0n", "s3x", "n00d", "fck", "bj", "hj", "l33t", "p0rn", "h3ntai", "h-ntai",
        "pnwh", "p0rnhwa", "l33tsp34k", "l3wd", "cultur3d", "s3xual"
    },
    
    "offensive_slang": {
        "slut", "whore", "tramp", "skank", "cumdumpster", "gangbang", "facial",
        "golden shower", "pegging", "rimming", "rough sex", "dirty talk", "sex chat",
        "nude pic", "lewd", "titty", "twerk", "breasts", "penis", "vagina", "clitoris",
        "genitals", "sexual", "kamasutra", "incest", "pedo", "rape", "sex slave",
        "bondage", "creampie", "cum inside", "sex doll", "sex machine", "latex",
        "oral sex", "cumshot", "deepthroat", "domination", "submission", "handcuffs",
        "orgy", "roleplay", "sex toy", "voyeur", "cosplay", "adult", "culture",
        "anal", "erotic", "masturbate", "hardcore", "bdsm", "fetish", "lingerie",
        "milf", "taboo"
    }
}

# Legitimate keywords that might trigger false positives
EXCEPTION_KEYWORDS: Set[str] = {
    "nxivm", "classroom", "assassination", "geass", "classic",
    "glass", "mass", "bass", "class", "passage", "compass",
    "assess", "harass", "embassy", "sussex", "cassette", "grassland"
}


# ─────────────────────────────────────────────────────────────
#  | 𝗘𝗡𝗚𝗜𝗡𝗘 𝗖𝗢𝗠𝗣𝗜𝗟𝗔𝗧𝗜𝗢𝗡 | - ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ᴠᴇʀsɪᴏɴ
# ─────────────────────────────────────────────────────────────

# 1. Compile Exceptions Pattern
# Matches any of the exception substrings efficiently
_EXCEPTIONS_PATTERN = re.compile(
    r'(?:' + '|'.join(map(re.escape, EXCEPTION_KEYWORDS)) + r')', 
    re.IGNORECASE
)

# 2. Compile NSFW Pattern 
# Separating single words (needs \b boundary) from multi-words (exact match)
_single_words = []
_multi_words = []
for category_kw in NSFW_KEYWORDS.values():
    for kw in category_kw:
        if ' ' in kw:
            _multi_words.append(re.escape(kw))
        else:
            _single_words.append(re.escape(kw))

_nsfw_regex_parts = []
if _single_words:
    _nsfw_regex_parts.append(r'\b(?:' + '|'.join(_single_words) + r')\b')
if _multi_words:
    _nsfw_regex_parts.append(r'(?:' + '|'.join(_multi_words) + r')')

_NSFW_PATTERN = re.compile('|'.join(_nsfw_regex_parts), re.IGNORECASE)


# ─────────────────────────────────────────────────────────────
#  | 𝗛𝗘𝗟𝗣𝗘𝗥 𝗙𝗨𝗡𝗖𝗧𝗜𝗢𝗡𝗦 |
# ─────────────────────────────────────────────────────────────

@lru_cache(maxsize=2048)
def _normalize_text(text: str) -> str:
    """Normalize text with increased LRU cache size for production throughput."""
    return text.lower().strip()


def _has_exception_keyword(text: str) -> bool:
    """O(1) check for exception substrings."""
    normalized = _normalize_text(text)
    return bool(_EXCEPTIONS_PATTERN.search(normalized))


def _contains_nsfw_keyword(text: str) -> bool:
    """O(1) check for all NSFW keywords using a unified C-compiled regex engine."""
    normalized = _normalize_text(text)
    return bool(_NSFW_PATTERN.search(normalized))


# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗔𝗜𝗡 𝗙𝗨𝗡𝗖𝗧𝗜𝗢𝗡𝗦 |
# ─────────────────────────────────────────────────────────────

async def check_anti_nsfw(new_name: str, message) -> bool:
    if not new_name:
        return False
    
    # Check for exception keywords first (optimization)
    if _has_exception_keyword(new_name):
        return False
    
    # Check for NSFW keywords
    if _contains_nsfw_keyword(new_name):
        await message.reply_text(
            "<blockquote>⚠️ <b>𝗡𝗦𝗙𝗪 - ᴄᴏɴᴛᴇɴᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ</b></blockquote>\n"
            "<blockquote>ʏᴏᴜ ᴄᴀɴ'ᴛ ʀᴇɴᴀᴍᴇ ғɪʟᴇs ᴡɪᴛʜ ɴsғᴡ ᴏʀ ɪɴᴀᴘᴘʀᴏᴘʀɪᴀᴛᴇ ᴄᴏɴᴛᴇɴᴛ.</blockquote>",
            parse_mode="html",
            quote=True
        )
        return True
    
    return False


def is_nsfw_content(text: str) -> bool:
    if not text:
        return False
    
    if _has_exception_keyword(text):
        return False
    
    return _contains_nsfw_keyword(text)


def get_matched_keywords(text: str) -> List[str]:
    if not text or _has_exception_keyword(text):
        return []
    
    normalized = _normalize_text(text)
    
    # .findall returns strings based on the non-capturing groups
    matches = _NSFW_PATTERN.findall(normalized)
    return list(set(m.lower() for m in matches if m))


def clear_cache():
    """Clear the LRU cache for text normalization."""
    _normalize_text.cache_clear()

