#┌─ 𝗥𝗘𝗫𝗬 𝗪𝗢𝗥𝗞𝗦𝗛𝗢𝗣 ───────────────────────────────┐
#│  ʀᴇ-ᴄᴏᴅᴇ ᴏᴘᴛɪᴍɪᴢᴀᴛɪᴏɴ ᴅᴏɴᴇ                │      @NyxRexy
#│  (ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ - V14 Pʀᴏ Sᴇʀᴠɪᴄᴇ)  │      @ShadowedTomb
#└──────────────────────────────────────────┘

from __future__ import annotations

import asyncio
import logging
import re
import time
import os
import difflib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TypedDict, Any
from functools import wraps

import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    MessageNotModified,
    QueryIdInvalid,
    ButtonDataInvalid,
    FloodWait,
    UserIsBlocked
)

from helper.database import DARKXSIDE78
from config import Config

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  | Tʏᴘᴇᴅ Rᴇsᴜʟᴛ Mᴏᴅᴇʟ |
# ═══════════════════════════════════════════════════════════

class SearchResult(TypedDict, total=False):
    success: bool
    source: str           # 'AniList' | 'TMDB' | 'FANART' | 'MAL' | 'AUTO'
    title: str
    poster_url: Optional[str]
    media_type: str       # 'anime' | 'movie' | 'tv'
    provider_id: Optional[int]
    metadata: Dict        # episodes, season, year, format, status
    error: Optional[str]
    retryable: bool       # True if caller may retry

def ok_result(
    source: str,
    title: str,
    poster_url: str,
    media_type: str = "anime",
    provider_id: Optional[int] = None,
    metadata: Optional[Dict] = None,
) -> SearchResult:
    return SearchResult(
        success=True,
        source=source,
        title=title,
        poster_url=poster_url,
        media_type=media_type,
        provider_id=provider_id,
        metadata=metadata or {},
        error=None,
        retryable=False,
    )

def err_result(error: str, retryable: bool = False) -> SearchResult:
    return SearchResult(
        success=False,
        source="",
        title="",
        poster_url=None,
        media_type="",
        provider_id=None,
        metadata={},
        error=error,
        retryable=retryable,
    )

# ═══════════════════════════════════════════════════════════
#  | Sʜᴀʀᴇᴅ HTTP Cʟɪᴇɴᴛ |
# ═══════════════════════════════════════════════════════════

_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=12, connect=5)
_HTTP_HEADERS = {"User-Agent": "REXY-V14-Pro/1.0", "Accept": "application/json"}

class HttpClient:
    """Single shared session with retry/backoff and rate-limit handling."""

    def __init__(self, metrics: Any = None, max_retries: int = 3, base_delay: float = 1.5):
        self._session: Optional[aiohttp.ClientSession] = None
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._metrics = metrics

    async def _session_(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=_HTTP_TIMEOUT, headers=_HTTP_HEADERS
            )
        return self._session

    async def get(self, url: str, engine: str = "unknown", **kwargs) -> Optional[Dict]:
        return await self._request("GET", url, engine, **kwargs)

    async def post(self, url: str, engine: str = "unknown", **kwargs) -> Optional[Dict]:
        return await self._request("POST", url, engine, **kwargs)

    async def _request(self, method: str, url: str, engine: str, **kwargs) -> Optional[Dict]:
        session = await self._session_()
        for attempt in range(self._max_retries + 1):
            try:
                async with session.request(method, url, **kwargs) as resp:
                    if resp.status == 429:
                        wait = float(resp.headers.get("Retry-After", self._base_delay * (2 ** attempt)))
                        logger.warning(f"[http] 429 rate-limit on {engine} → wait {wait:.1f}s (attempt {attempt+1})")
                        if self._metrics and engine != "unknown":
                            self._metrics.rate_limit(engine)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 500 and attempt < self._max_retries:
                        await asyncio.sleep(self._base_delay * (2 ** attempt))
                        continue
                    if resp.status == 200:
                        return await resp.json()
                    logger.warning(f"[http] {method} {url} → {resp.status}")
                    return None
            except asyncio.TimeoutError:
                if attempt < self._max_retries:
                    await asyncio.sleep(self._base_delay)
                    continue
                logger.error(f"[http] Timeout after {self._max_retries} retries: {url}")
                return None
            except aiohttp.ClientError as exc:
                logger.error(f"[http] ClientError: {exc}")
                return None
        return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# ═══════════════════════════════════════════════════════════
#  | TTL Cᴀᴄʜᴇ |
# ═══════════════════════════════════════════════════════════

@dataclass
class _CacheEntry:
    result: SearchResult
    ts: float = field(default_factory=time.monotonic)

class TTLCache:
    def __init__(self, ttl: float = 3600.0, max_size: int = 4096):
        self._store: Dict[str, _CacheEntry] = {}
        self._ttl = ttl
        self._max_size = max_size

    def _key(self, engine: str, title: str) -> str:
        return f"{engine}:{title.lower().strip()}"

    def get(self, engine: str, title: str) -> Optional[SearchResult]:
        e = self._store.get(self._key(engine, title))
        if e and (time.monotonic() - e.ts) < self._ttl:
            return e.result
        return None

    def set(self, engine: str, title: str, result: SearchResult):
        if len(self._store) >= self._max_size:
            # Evict oldest 25%
            sorted_keys = sorted(self._store, key=lambda k: self._store[k].ts)
            for k in sorted_keys[: self._max_size // 4]:
                del self._store[k]
        self._store[self._key(engine, title)] = _CacheEntry(result=result)

    def invalidate(self, engine: str, title: str):
        self._store.pop(self._key(engine, title), None)

    def clear(self):
        self._store.clear()

# ═══════════════════════════════════════════════════════════
#  | Mᴇᴛʀɪᴄs |
# ═══════════════════════════════════════════════════════════

@dataclass
class ProviderMetrics:
    requests: int = 0
    successes: int = 0
    failures: int = 0
    rate_limits: int = 0
    cache_hits: int = 0
    fallbacks: int = 0
    total_ms: float = 0.0

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.requests if self.requests else 0.0

    def record(self, success: bool, elapsed_ms: float):
        self.requests += 1
        self.total_ms += elapsed_ms
        if success:
            self.successes += 1
        else:
            self.failures += 1

class Metrics:
    def __init__(self):
        self._data: Dict[str, ProviderMetrics] = {}

    def _get(self, engine: str) -> ProviderMetrics:
        if engine not in self._data:
            self._data[engine] = ProviderMetrics()
        return self._data[engine]

    def record(self, engine: str, success: bool, elapsed_ms: float):
        self._get(engine).record(success, elapsed_ms)

    def cache_hit(self, engine: str):
        self._get(engine).cache_hits += 1

    def rate_limit(self, engine: str):
        self._get(engine).rate_limits += 1

    def fallback(self, engine: str):
        self._get(engine).fallbacks += 1

    def summary(self) -> Dict[str, Dict]:
        return {
            k: {
                "requests": v.requests, "successes": v.successes,
                "failures": v.failures, "cache_hits": v.cache_hits,
                "rate_limits": v.rate_limits, "fallbacks": v.fallbacks,
                "avg_ms": round(v.avg_ms, 1),
            }
            for k, v in self._data.items()
        }

# ═══════════════════════════════════════════════════════════
#  | Tɪᴛʟᴇ Pᴀʀsᴇʀ |
# ═══════════════════════════════════════════════════════════

# Compile once at module load
_RE_AT       = re.compile(r'@\w+')
_RE_SYMBOLS  = re.compile(r'[⌯•|]+')
_RE_EP_S     = re.compile(r'\bS\d{1,2}[- ]?E\d{1,3}\b', re.I)
_RE_EP_RANGE = re.compile(r'\b\d{1,3}[-~]\d{1,3}\b')
_RE_EP_WORD  = re.compile(r'\b(?:Season|Episode|Chapter|Volume|Ep)\s?\d+\b', re.I)
_RE_EP_ORD   = re.compile(r'\b\d{1,3}(?:st|nd|rd|th)?\s?episode\b', re.I)
_RE_BATCH    = re.compile(r'\bBatch\b', re.I)
_RE_CRC      = re.compile(r'\[[0-9A-Fa-f]{8}\]')
_RE_QUALITY  = re.compile(
    r'\b(2160p|1440p|1080p|720p|800p|480p|360p|240p|144p'
    r'|HDRip|WEB[- ]?DL|Blu[- ]?Ray|HDTV|DVDRip|CAMRip|WEBrip|TS'
    r'|DDP\d+|AAC\d+|AC3|EAC3|FLAC'
    r'|x264|x265|H\.264|H\.265|HEVC|AVC'
    r'|Dual[- ]?Audio|Multi[- ]?Audio|Multi[- ]?Subs'
    r'|Sub|Dub|Subbed|Dubbed|Esub|MSubs'
    r'|REPACK|PROPER|INTERNAL|LIMITED'
    r'|10bit|8bit|Hi10P)\b', re.I
)
_RE_BRACKETS = re.compile(r'\[.*?\]|\(.*?\)')
_RE_NONWORD  = re.compile(r'[^\w\s\-\u0900-\u097F\u3040-\u30FF\u4E00-\u9FFF]')
_RE_YEAR     = re.compile(r'\b(19|20)\d{2}\b')
_RE_WS       = re.compile(r'[\s_\-\.]{2,}')

@dataclass
class ParsedTitle:
    clean_title: str
    detected_hints: Dict  # season, episode, year, is_anime_likely, crc

def parse_filename(filename: str) -> ParsedTitle:
    """Parse and clean a filename into a search-ready title with metadata hints."""
    try:
        name = os.path.splitext(filename)[0]

        hints: Dict = {
            "season": None, "episode": None, "year": None,
            "is_anime_likely": False, "crc": None,
        }

        # Extract CRC hash
        crc_m = _RE_CRC.search(name)
        if crc_m:
            hints["crc"] = crc_m.group()
            name = _RE_CRC.sub("", name)

        # Extract year
        year_m = _RE_YEAR.search(name)
        if year_m:
            hints["year"] = int(year_m.group())

        # Extract S/E info
        se_m = _RE_EP_S.search(name)
        if se_m:
            raw = se_m.group().upper().replace(" ", "").replace("-", "")
            s_part = re.search(r'S(\d+)', raw)
            e_part = re.search(r'E(\d+)', raw)
            hints["season"] = int(s_part.group(1)) if s_part else None
            hints["episode"] = int(e_part.group(1)) if e_part else None

        # Anime-likely: japanese chars, or common patterns
        if re.search(r'[\u3040-\u30FF\u4E00-\u9FFF]', name):
            hints["is_anime_likely"] = True

        # Strip noise
        name = _RE_AT.sub("", name)
        name = _RE_SYMBOLS.sub(" ", name)
        name = _RE_EP_S.sub("", name)
        name = _RE_EP_RANGE.sub("", name)
        name = _RE_EP_WORD.sub("", name)
        name = _RE_EP_ORD.sub("", name)
        name = _RE_BATCH.sub("", name)
        name = _RE_QUALITY.sub("", name)
        name = _RE_BRACKETS.sub("", name)
        name = _RE_NONWORD.sub(" ", name)
        name = _RE_WS.sub(" ", name).strip(" .-_")

        if not name:
            name = filename

        return ParsedTitle(clean_title=name, detected_hints=hints)
    except Exception as exc:
        logger.error(f"[parser] Error: {exc}")
        return ParsedTitle(clean_title=filename, detected_hints={})

# ═══════════════════════════════════════════════════════════
#  | Pʀᴏᴠɪᴅᴇʀ Aʙsᴛʀᴀᴄᴛɪᴏɴ |
# ═══════════════════════════════════════════════════════════

class BaseProvider(ABC):
    name: str = "base"

    def __init__(self, http: HttpClient, sem: asyncio.Semaphore):
        self._http = http
        self._sem = sem

    @abstractmethod
    async def search(self, title: str, hints: Optional[Dict] = None) -> SearchResult: ...

    def is_available(self) -> bool:
        return True


class AniListProvider(BaseProvider):
    name = "anilist"
    _URL = "https://graphql.anilist.co"
    _QUERY = """
    query ($search: String) {
      Media(search: $search, type: ANIME) {
        id coverImage { extraLarge large medium }
        title { english romaji native }
        synonyms format status episodes season seasonYear
      }
    }"""
    _PAGE_QUERY = """
    query ($search: String) {
      Page(page: 1, perPage: 1) {
        media(search: $search, type: ANIME) {
          id coverImage { extraLarge large medium }
          title { english romaji native }
          synonyms format status episodes season seasonYear
        }
      }
    }"""

    async def search(self, title: str, hints: Optional[Dict] = None) -> SearchResult:
        async with self._sem:
            data = await self._http.post(
                self._URL,
                engine=self.name,
                json={"query": self._QUERY, "variables": {"search": title}},
            )
        if data and data.get("data", {}).get("Media"):
            return self._build(data["data"]["Media"])
        # fuzzy page fallback
        async with self._sem:
            data = await self._http.post(
                self._URL,
                engine=self.name,
                json={"query": self._PAGE_QUERY, "variables": {"search": title}},
            )
        if data:
            items = data.get("data", {}).get("Page", {}).get("media", [])
            if items:
                return self._build(items[0])
        return err_result("Anime not found on AniList", retryable=False)

    def _build(self, m: Dict) -> SearchResult:
        cov = m.get("coverImage", {})
        poster = cov.get("extraLarge") or cov.get("large") or cov.get("medium")
        if not poster:
            return err_result("No poster image available")
        t = m.get("title", {})
        title = t.get("english") or t.get("romaji") or t.get("native") or "Unknown"
        return ok_result(
            source="AniList", title=title, poster_url=poster,
            media_type="anime", provider_id=m.get("id"),
            metadata={
                "format": m.get("format"), "status": m.get("status"),
                "episodes": m.get("episodes"), "season": m.get("season"),
                "year": m.get("seasonYear"),
            },
        )


class TMDBProvider(BaseProvider):
    name = "tmdb"
    _SEARCH_URL = "https://api.themoviedb.org/3/search/multi"
    _IMG_URL    = "https://image.tmdb.org/t/p/w780"
    _EXT_URL    = "https://api.themoviedb.org/3/tv/{tv_id}/external_ids"

    def is_available(self) -> bool:
        return bool(Config.TMDB_API_KEY)

    async def search(self, title: str, hints: Optional[Dict] = None) -> SearchResult:
        params = {"api_key": Config.TMDB_API_KEY, "query": title, "language": "en-US"}
        async with self._sem:
            data = await self._http.get(self._SEARCH_URL, engine=self.name, params=params)
        if not data:
            return err_result("TMDB returned no data", retryable=True)
        best = next((r for r in data.get("results", []) if r.get("poster_path")), None)
        if not best:
            return err_result("No poster on TMDB")
        return ok_result(
            source="TMDB",
            title=best.get("title") or best.get("name") or "Unknown",
            poster_url=f"{self._IMG_URL}{best['poster_path']}",
            media_type=best.get("media_type", "movie"),
            provider_id=best.get("id"),
            metadata={
                "year": (best.get("release_date") or best.get("first_air_date", ""))[:4],
                "status": "Released",
            },
        )

    async def get_tvdb_id(self, tmdb_id: int) -> Optional[int]:
        url = self._EXT_URL.format(tv_id=tmdb_id)
        async with self._sem:
            data = await self._http.get(url, engine=self.name, params={"api_key": Config.TMDB_API_KEY})
        return data.get("tvdb_id") if data else None


class FanartProvider(BaseProvider):
    name = "fanart"
    _TV_URL    = "https://webservice.fanart.tv/v3/tv/{id}"
    _MOVIE_URL = "https://webservice.fanart.tv/v3/movies/{id}"

    def __init__(self, http: HttpClient, sem: asyncio.Semaphore, tmdb: TMDBProvider):
        super().__init__(http, sem)
        self._tmdb = tmdb

    def is_available(self) -> bool:
        return bool(Config.FANART_API_KEY and Config.TMDB_API_KEY)

    async def search(self, title: str, hints: Optional[Dict] = None) -> SearchResult:
        tmdb_res = await self._tmdb.search(title, hints)
        if not tmdb_res.get("success"):
            return tmdb_res

        tid       = tmdb_res.get("provider_id")
        mtype     = tmdb_res.get("media_type")
        params    = {"api_key": Config.FANART_API_KEY}

        if not tid:
            return tmdb_res

        if mtype == "tv":
            tvdb_id = await self._tmdb.get_tvdb_id(tid)
            if not tvdb_id:
                return tmdb_res
            url, img_key = self._TV_URL.format(id=tvdb_id), "tvposter"
        else:
            url, img_key = self._MOVIE_URL.format(id=tid), "movieposter"

        async with self._sem:
            data = await self._http.get(url, engine=self.name, params=params)

        if not data:
            return tmdb_res

        posters = data.get(img_key) or data.get("showbackground") or data.get("moviebackground") or []
        if not posters:
            return tmdb_res

        return ok_result(
            source="FANART",
            title=tmdb_res.get("title", ""),
            poster_url=posters[0]["url"],
            media_type=mtype,
            provider_id=tid,
            metadata=tmdb_res.get("metadata", {}),
        )

class MALProvider(BaseProvider):
    name = "mal"
    _SEARCH_URL = "https://api.jikan.moe/v4/anime"

    async def search(self, title: str, hints: Optional[Dict] = None) -> SearchResult:
        params = {"q": title, "limit": 3}
        async with self._sem:
            data = await self._http.get(self._SEARCH_URL, engine=self.name, params=params)
        
        if not data or not data.get("data"):
            return err_result("Anime not found on MyAnimeList", retryable=False)
            
        results = data["data"]
        best = results[0]
        best_score = -1
        
        target_year = hints.get("year") if hints else None
        
        for r in results:
            score = 0
            # Title similarity bonus
            eng = (r.get("title_english") or "").lower()
            rom = (r.get("title") or "").lower()
            q = title.lower()
            if q == eng or q == rom:
                score += 50
            elif q in eng or q in rom:
                score += 20
                
            # Year match bonus
            r_year = r.get("year")
            if target_year and r_year == target_year:
                score += 30
                
            # Popularity/Member bonus (small)
            members = r.get("members", 0)
            score += min(10, members // 100000)
            
            if score > best_score:
                best_score = score
                best = r
                
        images = best.get("images", {}).get("jpg", {})
        poster = images.get("large_image_url") or images.get("image_url")
        
        if not poster:
            return err_result("No poster image available on MAL")
            
        return ok_result(
            source="MAL",
            title=best.get("title_english") or best.get("title") or "Unknown",
            poster_url=poster,
            media_type="anime",
            provider_id=best.get("mal_id"),
            metadata={
                "format": best.get("type"),
                "status": best.get("status"),
                "episodes": best.get("episodes"),
                "year": best.get("year"),
            },
        )

# ═══════════════════════════════════════════════════════════
#  | Oʀᴄʜᴇsᴛʀᴀᴛɪᴏɴ Sᴇʀᴠɪᴄᴇ |
# ═══════════════════════════════════════════════════════════

class ThumbOrchestrator:
    """
    Single entry point for all provider interactions.
    Handles cache, metrics, fallback chains, and engine routing.
    """

    def __init__(self):
        self._metrics = Metrics()
        self._http    = HttpClient(metrics=self._metrics)
        self._sem     = asyncio.Semaphore(8)
        self._cache   = TTLCache(ttl=3600, max_size=4096)
        self._ready   = False

        # Provider instances
        self._tmdb    = TMDBProvider(self._http, self._sem)
        self._anilist = AniListProvider(self._http, self._sem)
        self._fanart  = FanartProvider(self._http, self._sem, self._tmdb)
        self._mal     = MALProvider(self._http, self._sem)

        self._providers: Dict[str, BaseProvider] = {
            "anilist": self._anilist,
            "tmdb":    self._tmdb,
            "fanart":  self._fanart,
            "mal":     self._mal,
        }

    # --- Lifecycle ---

    async def startup(self):
        """Call once at bot startup to validate engines and warm session."""
        await self._http._session_()
        self._ready = True
        disabled = [n for n, p in self._providers.items() if not p.is_available()]
        if disabled:
            logger.warning(f"[thumb] Disabled engines (missing keys): {disabled}")
        logger.info("[thumb] ThumbOrchestrator ready")

    async def shutdown(self):
        await self._http.close()
        logger.info("[thumb] Session closed")

    # --- Public API ---

    async def search(self, title: str, engine: str, hints: Optional[Dict] = None) -> SearchResult:
        if engine == "off":
            return err_result("Thumbnail engine is disabled")

        if hints is None:
            parsed = self.parse_title(title)
            hints = parsed.detected_hints
            clean_title = parsed.clean_title
        else:
            clean_title = title

        if engine == "auto":
            cached = self._cache.get("auto", clean_title)
            if cached:
                self._metrics.cache_hit("auto")
                return cached
                
            t0 = time.monotonic()
            result = await self._auto_resolve(clean_title, hints)
            elapsed = (time.monotonic() - t0) * 1000
            self._metrics.record("auto", bool(result.get("success")), elapsed)
            
            if result.get("success"):
                self._cache.set("auto", clean_title, result)
            return result

        provider = self._providers.get(engine)
        if not provider:
            return err_result(f"Unknown engine: {engine}")
        if not provider.is_available():
            return err_result(f"Engine '{engine}' is disabled (missing API key)")

        cached = self._cache.get(engine, clean_title)
        if cached:
            self._metrics.cache_hit(engine)
            return cached

        t0 = time.monotonic()
        result = await provider.search(clean_title, hints)
        elapsed = (time.monotonic() - t0) * 1000
        self._metrics.record(engine, bool(result.get("success")), elapsed)

        if result.get("success"):
            self._cache.set(engine, clean_title, result)
        return result

    async def _auto_resolve(self, title: str, hints: Dict) -> SearchResult:
        is_anime = hints.get("is_anime_likely", False)
        
        if is_anime:
            tasks = [
                self._providers["anilist"].search(title, hints),
                self._providers["mal"].search(title, hints)
            ]
        else:
            tasks = [
                self._providers["tmdb"].search(title, hints),
                self._providers["fanart"].search(title, hints)
            ]
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_results = [r for r in results if isinstance(r, dict) and r.get("success")]
        
        if not valid_results:
            # Cross-fallback
            fallback_tasks = [self._providers["tmdb"].search(title, hints)] if is_anime else [self._providers["anilist"].search(title, hints)]
            fallback_res = await asyncio.gather(*fallback_tasks, return_exceptions=True)
            valid_results = [r for r in fallback_res if isinstance(r, dict) and r.get("success")]
            
        if not valid_results:
            return err_result("Auto resolve failed across all queried providers", retryable=False)
            
        # Score results
        best_res = valid_results[0]
        best_score = -1
        
        target_year = hints.get("year")
        target_season = hints.get("season")
        
        for res in valid_results:
            score = 0
            res_title = res.get("title", "").lower()
            q = title.lower()
            
            # Fuzzy match score (0-50)
            ratio = difflib.SequenceMatcher(None, q, res_title).ratio()
            score += int(ratio * 50)
            
            # Exact match bonus
            if q == res_title:
                score += 30
            elif q in res_title or res_title in q:
                score += 15
                
            if res.get("poster_url"):
                score += 10
            
            meta = res.get("metadata", {})
            
            # Year match
            if target_year and meta.get("year") and str(meta["year"]) == str(target_year):
                score += 30
                
            # Season match
            if target_season and meta.get("season") and str(meta["season"]).lower() == str(target_season).lower():
                score += 20
                
            # Provider bias
            source = res.get("source", "").lower()
            if is_anime and source in ["anilist", "mal"]:
                score += 10
            elif not is_anime and source in ["tmdb", "fanart"]:
                score += 10
                
            if score > best_score:
                best_score = score
                best_res = res
                
        return best_res

    def parse_title(self, filename: str) -> ParsedTitle:
        return parse_filename(filename)

    def get_metrics(self) -> Dict:
        return self._metrics.summary()

# Singleton
orchestrator = ThumbOrchestrator()

# ═══════════════════════════════════════════════════════════
#  | Bᴀᴄᴋᴡᴀʀᴅ Cᴏᴍᴘᴀᴛɪʙɪʟɪᴛʏ Wʀᴀᴘᴘᴇʀs |
# ═══════════════════════════════════════════════════════════

def extract_anime_title(filename: str) -> str:
    return orchestrator.parse_title(filename).clean_title

async def search_anime_on_anilist(anime_title: str, retry_count: int = 0) -> Dict:
    res = await orchestrator.search(anime_title, "anilist")
    return _convert_to_old_dict(res)

async def search_on_tmdb(title: str) -> Dict:
    res = await orchestrator.search(title, "tmdb")
    return _convert_to_old_dict(res)

async def search_media_info(title: str, engine: str = "off") -> Dict:
    res = await orchestrator.search(title, engine)
    return _convert_to_old_dict(res)

def _convert_to_old_dict(res: SearchResult) -> Dict:
    if not res.get("success"):
        return {"success": False, "error": res.get("error", "Unknown error")}
    
    meta = res.get("metadata", {})
    return {
        "success": True,
        "poster_url": res.get("poster_url"),
        "title": res.get("title"),
        "id": res.get("provider_id"),
        "format": meta.get("format"),
        "status": meta.get("status"),
        "episodes": meta.get("episodes"),
        "season": meta.get("season"),
        "year": meta.get("year"),
        "source": res.get("source"),
        "media_type": res.get("media_type")
    }

# ═══════════════════════════════════════════════════════════
#  | Pʏʀᴏɢʀᴀᴍ Hᴀɴᴅʟᴇʀs & UI |
# ═══════════════════════════════════════════════════════════

class AniListError(Exception):
    """Custom exception for AniList-related errors"""
    pass

def check_ban_status(func):
    @wraps(func)
    async def wrapper(client: Client, message: Message, *args, **kwargs):
        try:
            user_id = message.from_user.id
            is_banned, ban_reason = await DARKXSIDE78.is_user_banned(user_id)
            
            if is_banned:
                ban_message = (
                    f"<blockquote><b>✗ Yᴏᴜ Aʀᴇ Bᴀɴɴᴇᴅ Fʀᴏᴍ Usɪɴɢ Tʜɪs Bᴏᴛ</b>\n\n"
                    f"<b>Rᴇᴀsᴏɴ:</b> <code>{ban_reason or 'No reason provided'}</code></blockquote>"
                )
                try:
                    await message.reply_text(
                        ban_message,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Failed to send ban message to {user_id}: {e}")
                return
            
            return await func(client, message, *args, **kwargs)
            
        except Exception as e:
            logger.error(f"Error in ban check decorator: {e}")
            return await func(client, message, *args, **kwargs)
    return wrapper

def create_anilist_keyboard(engine: str) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                f"AUTO (HYBRID) {'✓' if engine == 'auto' else ''}",
                callback_data='thumb_engine_auto'
            )
        ],
        [
            InlineKeyboardButton(
                f"ANILIST {'✓' if engine == 'anilist' else ''}",
                callback_data='thumb_engine_anilist'
            ),
            InlineKeyboardButton(
                f"MAL {'✓' if engine == 'mal' else ''}",
                callback_data='thumb_engine_mal'
            )
        ],
        [
            InlineKeyboardButton(
                f"TMDB {'✓' if engine == 'tmdb' else ''}",
                callback_data='thumb_engine_tmdb'
            ),
            InlineKeyboardButton(
                f"FANART {'✓' if engine == 'fanart' else ''}",
                callback_data='thumb_engine_fanart'
            )
        ],
        [
            InlineKeyboardButton(
                f"NORMAL {'✓' if engine == 'off' else ''}",
                callback_data='thumb_engine_off'
            ),
            InlineKeyboardButton(
                "CLOSE",
                callback_data='close_msg'
            )
        ]
    ]
    return InlineKeyboardMarkup(buttons)

def create_anilist_text(engine: str) -> str:
    status_text = engine.upper()
    return f"""
<blockquote><b>✦ 𝗔𝗨𝗧𝗢 𝗧𝗛𝗨𝗠𝗕𝗡𝗔𝗜𝗟 — {status_text}</b></blockquote>

◍ Sᴇᴀʀᴄʜᴇs ᴏɴ ʏᴏᴜʀ ᴄʜᴏsᴇɴ ᴅᴀᴛᴀʙᴀsᴇ (Aᴜᴛᴏ, Aɴɪʟɪsᴛ, MAL, TMDB, ᴏʀ Fᴀɴᴀʀᴛ)

◍ Aᴜᴛᴏ ᴍᴏᴅᴇ ᴅᴇᴛᴇᴄᴛs ᴄᴏɴᴛᴇɴᴛ ᴛʏᴘᴇ & ᴘɪᴄᴋs ᴛʜᴇ ʙᴇsᴛ-ᴍᴀᴛᴄʜ ᴇɴɢɪɴᴇ

<blockquote>➥ Sᴇʟᴇᴄᴛ ʏᴏᴜʀ ᴘʀᴇғᴇʀʀᴇᴅ ᴇɴɢɪɴᴇ ʙᴇʟᴏᴡ !</blockquote>
    """.strip()


@Client.on_message(
    filters.command([
        'autothumb',
        'auto_thumb'
    ]) & filters.private
)
@check_ban_status
async def anilist_thumb_command(client: Client, message: Message):
    try:
        user_id = message.from_user.id
        try:
            engine = await DARKXSIDE78.get_thumb_engine(user_id)
        except Exception as db_error:
            logger.error(f"Database error for user {user_id}: {db_error}")
            await message.reply_text(
                "<blockquote><b>✗ Dᴀᴛᴀʙᴀsᴇ Eʀʀᴏʀ.\n\n"
                "Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ.</b></blockquote>",
                parse_mode=ParseMode.HTML
            )
            return
        
        keyboard = create_anilist_keyboard(engine)
        text = create_anilist_text(engine)
        
        await message.reply_text(
            text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        
    except FloodWait as e:
        logger.warning(f"FloodWait: sleeping for {e.value} seconds")
        await asyncio.sleep(e.value)
        await anilist_thumb_command(client, message)
    except UserIsBlocked:
        logger.info(f"Bot is blocked by user {message.from_user.id}")
    except Exception as e:
        logger.error(f"Error in anilist_thumb_command: {e}", exc_info=True)
        try:
            await message.reply_text(
                "<blockquote><b>✗ Aɴ Eʀʀᴏʀ Oᴄᴄᴜʀʀᴇᴅ.\n\n"
                "Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ.</b></blockquote>",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass


@Client.on_callback_query(filters.regex(r"^thumb_engine_(auto|anilist|mal|tmdb|fanart|off)$"))
async def handle_anilist_thumb_toggle(client: Client, callback_query: CallbackQuery):
    try:
        user_id = callback_query.from_user.id
        engine = callback_query.data.split('_')[-1]
        
        try:
            await DARKXSIDE78.set_thumb_engine(user_id, engine)
        except Exception as db_error:
            logger.error(f"Database error updating engine for user {user_id}: {db_error}")
            await callback_query.answer(
                "✗ Dᴀᴛᴀʙᴀsᴇ ᴇʀʀᴏʀ. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.",
                show_alert=True
            )
            return
        
        keyboard = create_anilist_keyboard(engine)
        text = create_anilist_text(engine)
        
        try:
            await callback_query.message.edit_text(
                text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        except MessageNotModified:
            pass
        
        status_emoji = "✓" if engine != "off" else "✗"
        await callback_query.answer(
            f"{status_emoji} Tʜᴜᴍʙɴᴀɪʟ ᴇɴɢɪɴᴇ sᴇᴛ ᴛᴏ {engine.upper()}!",
            show_alert=False
        )
        
    except QueryIdInvalid:
        logger.warning("Query ID invalid - callback might be too old")
    except ButtonDataInvalid:
        logger.error("Invalid button data in callback")
    except FloodWait as e:
        logger.warning(f"FloodWait in callback: sleeping for {e.value} seconds")
        await asyncio.sleep(e.value)
        await handle_anilist_thumb_toggle(client, callback_query)
    except Exception as e:
        logger.error(f"Error in handle_anilist_thumb_toggle: {e}", exc_info=True)
        try:
            await callback_query.answer(
                "✗ Aɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ. Pʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.",
                show_alert=True
            )
        except Exception:
            pass

__all__ = [
    'extract_anime_title',
    'search_anime_on_anilist',
    'search_on_tmdb',
    'search_media_info',
    'anilist_thumb_command',
    'handle_anilist_thumb_toggle',
    'check_ban_status',
    'SearchResult', 'ParsedTitle', 'ThumbOrchestrator',
    'orchestrator', 'parse_filename', 'ok_result', 'err_result',
    'thumb_orchestrator', 'parse_filename_advanced'
]

thumb_orchestrator = orchestrator
parse_filename_advanced = parse_filename
