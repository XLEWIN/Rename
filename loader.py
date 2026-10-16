# © 𝟸𝟶𝟸𝟼 Rᴇxʏ (@NyxRexy)
# Licensed under the REXY V14-Pro Proprietary License.
# Unauthorized use, redistribution, or commercial deployment is prohibited.
# Reverse engineering or attempting to replicate the core logic is prohibited.

#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ────────────────────────────────────┐
#│  ʙᴀsᴇ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ                    │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy           │
#└──────────────────────────────────────────────────┘

import sys
import hashlib
import logging
import importlib
import importlib.util
import inspect
import types
import os
import asyncio
import time
from enum import Enum, auto
from typing import Any, Optional, Dict, List, Tuple, Set
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
from contextlib import suppress


# ══════════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗡𝗙𝗜𝗚𝗨𝗥𝗔𝗧𝗜𝗢𝗡 |
# ══════════════════════════════════════════════════════════════

@dataclass
class PluginConfig:
    timeout_seconds: float               = 30.0
    log_level: int                       = logging.INFO
    strict_mode: bool                    = False
    enable_dependency_resolution: bool   = True
    enable_watcher: bool                 = False
    watcher_interval_seconds: float      = 2.0
    show_startup_report: bool            = True
    metadata_scan_lines: int             = 30
    reload_drain_timeout_seconds: float  = 5.0
    excluded_dirs: Set[str]              = field(default_factory=set)


# ══════════════════════════════════════════════════════════════
#  | 𝗣𝗟𝗨𝗚𝗜𝗡 𝗦𝗧𝗔𝗧𝗘 |
# ══════════════════════════════════════════════════════════════

class PluginState(Enum):
    PENDING  = auto()
    LOADING  = auto()
    ACTIVE   = auto()
    DRAINING = auto()
    FAILED   = auto()
    UNLOADED = auto()


# ══════════════════════════════════════════════════════════════
#  | 𝗛𝗔𝗡𝗗𝗟𝗘𝗥 𝗧𝗥𝗔𝗖𝗞𝗜𝗡𝗚  (per-plugin Pyrogram handler tracking)
# ══════════════════════════════════════════════════════════════

@dataclass
class _HandlerEntry:
    """One registered Pyrogram handler for a plugin."""
    client: Any
    handler: Any
    group: int = 0


class HandlerRegistry:

    def __init__(self) -> None:
        self._entries: List[_HandlerEntry] = []

    def track(self, client: Any, handler: Any, group: int = 0) -> None:
        """Record a handler that was already added to `client`."""
        self._entries.append(_HandlerEntry(client, handler, group))

    def remove_all(self) -> None:
        """Un-register every tracked handler from its client."""
        for entry in self._entries:
            try:
                entry.client.remove_handler(entry.handler, entry.group)
            except (ValueError, KeyError):
                pass  # handler was already removed — fine
            except Exception:
                pass
        self._entries.clear()

    @property
    def count(self) -> int:
        return len(self._entries)


# ══════════════════════════════════════════════════════════════
#  | 𝗣𝗟𝗨𝗚𝗜𝗡 𝗠𝗘𝗧𝗔𝗗𝗔𝗧𝗔 |
# ══════════════════════════════════════════════════════════════

@dataclass
class PluginMetadata:
    name: str
    path: Path
    module_key: str
    folder: str
    dependencies: List[str]                = field(default_factory=list)
    priority: int                          = 100
    disabled: bool                         = False
    state: PluginState                     = PluginState.PENDING
    load_time_ms: int                      = 0
    loaded_at: Optional[str]              = None
    error: Optional[str]                  = None
    fingerprint: Optional[str]            = None
    reload_count: int                      = 0
    handler_registry: HandlerRegistry      = field(default_factory=HandlerRegistry)
    _drain_event: Optional[asyncio.Event]  = field(default=None, repr=False)

    def __hash__(self) -> int:
        return hash(self.module_key)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PluginMetadata) and self.module_key == other.module_key

    def reset_drain(self) -> asyncio.Event:
        self._drain_event = asyncio.Event()
        self._drain_event.set()
        return self._drain_event


# ══════════════════════════════════════════════════════════════
#  | 𝗟𝗢𝗚𝗚𝗘𝗥 |
# ══════════════════════════════════════════════════════════════

class _PluginLogger:
    def __init__(self, name: str = "loader", level: int = logging.INFO) -> None:
        self.logger = logging.getLogger(name)
        # Rely on root logger config but ensure level is set
        self.logger.setLevel(level)

    def info(self, msg: str, **ctx: Any) -> None:
        self.logger.info(f"{msg} {self._fmt(ctx)}")

    def warning(self, msg: str, **ctx: Any) -> None:
        self.logger.warning(f"{msg} {self._fmt(ctx)}")

    def debug(self, msg: str, **ctx: Any) -> None:
        self.logger.debug(f"{msg} {self._fmt(ctx)}")

    def error(self, msg: str, exc: Optional[Exception] = None, **ctx: Any) -> None:
        self.logger.error(f"{msg} {self._fmt(ctx)}", exc_info=exc)

    def critical(self, msg: str, exc: Optional[Exception] = None, **ctx: Any) -> None:
        self.logger.critical(f"{msg} {self._fmt(ctx)}", exc_info=exc)

    @staticmethod
    def _fmt(ctx: dict) -> str:
        return ("| " + " | ".join(f"{k}={v}" for k, v in ctx.items())) if ctx else ""


# ══════════════════════════════════════════════════════════════
#  | 𝗗𝗘𝗣𝗘𝗡𝗗𝗘𝗡𝗖𝗬 𝗥𝗘𝗦𝗢𝗟𝗩𝗘𝗥 |
# ══════════════════════════════════════════════════════════════

class _DependencyResolver:
    @staticmethod
    def resolve(plugins: List[PluginMetadata]) -> List[PluginMetadata]:
        plugin_map = {p.module_key: p for p in plugins}
        in_degree: Dict[str, int] = {p.module_key: 0 for p in plugins}

        for plugin in plugins:
            for dep in plugin.dependencies:
                if dep in plugin_map:
                    in_degree[plugin.module_key] += 1
                else:
                    logging.getLogger("loader").warning(
                        f"Plugin '{plugin.name}' requires missing dependency '{dep}'"
                    )

        queue = deque(p for p in plugins if in_degree[p.module_key] == 0)
        sorted_plugins: List[PluginMetadata] = []

        while queue:
            current = queue.popleft()
            sorted_plugins.append(current)
            for plugin in plugins:
                if current.module_key in plugin.dependencies:
                    in_degree[plugin.module_key] -= 1
                    if in_degree[plugin.module_key] == 0:
                        queue.append(plugin)

        if len(sorted_plugins) != len(plugins):
            remaining = [p.name for p in plugins if p not in sorted_plugins]
            raise RuntimeError(f"Circular dependency detected: {remaining}")

        return sorted(sorted_plugins, key=lambda p: p.priority)


# ══════════════════════════════════════════════════════════════
#  | 𝗣𝗟𝗨𝗚𝗜𝗡 𝗥𝗘𝗚𝗜𝗦𝗧𝗥𝗬 |
# ══════════════════════════════════════════════════════════════

class _PluginRegistry:
    def __init__(self) -> None:
        self._plugins: Dict[str, PluginMetadata] = {}
        self._load_order: List[str] = []

    def upsert(self, metadata: PluginMetadata) -> None:
        if metadata.module_key not in self._plugins:
            self._load_order.append(metadata.module_key)
        self._plugins[metadata.module_key] = metadata

    def get(self, module_key: str) -> Optional[PluginMetadata]:
        return self._plugins.get(module_key)

    def all(self) -> List[PluginMetadata]:
        return [self._plugins[k] for k in self._load_order if k in self._plugins]

    def by_state(self, *states: PluginState) -> List[PluginMetadata]:
        return [p for p in self._plugins.values() if p.state in states]

    def is_active(self, module_key: str) -> bool:
        p = self._plugins.get(module_key)
        return p is not None and p.state == PluginState.ACTIVE


# ══════════════════════════════════════════════════════════════
#  | 𝗙𝗜𝗟𝗘 𝗙𝗜𝗡𝗚𝗘𝗥𝗣𝗥𝗜𝗡𝗧 |
# ══════════════════════════════════════════════════════════════

_fp_cache: Dict[Path, Tuple[float, str]] = {}

def _fingerprint(path: Path) -> str:
    """Hash file content, using mtime to skip identical reads."""
    try:
        stat = os.stat(path)
        mtime = stat.st_mtime
        
        # Check cache
        if path in _fp_cache:
            cached_mtime, cached_hash = _fp_cache[path]
            if cached_mtime == mtime:
                return cached_hash
        
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
        
        res = h.hexdigest()
        _fp_cache[path] = (mtime, res)
        return res
    except (OSError, PermissionError):
        return ""


# ══════════════════════════════════════════════════════════════
#  | 𝗖𝗢𝗥𝗘 𝗟𝗢𝗔𝗗𝗘𝗥 |
# ══════════════════════════════════════════════════════════════

class PluginLoader:

    _SELF_NAMES: Set[str] = {"loader.py", "plugin_loader.py", "plugins_loader.py"}

    def __init__(self, config: Optional[PluginConfig] = None) -> None:
        self.config = config or PluginConfig()
        self.logger = _PluginLogger(level=self.config.log_level)
        self.registry = _PluginRegistry()

        self._watcher_task: Optional[asyncio.Task] = None
        self._watched_fps: Dict[Path, str] = {}
        self._client: Any = None
        self._root: Optional[Path] = None
        self._reload_lock = asyncio.Lock()

    # ──────────────────────────────────────────────────────────
    #  Discovery
    # ──────────────────────────────────────────────────────────

    def _setup_python_path(self, root: Path) -> None:
        """Add project root to sys.path so relative imports like ``from plugins.regex import ...`` work.
        Note: Recursively adding ALL subdirectories to sys.path is an anti-pattern that
        causes catastrophic namespace collisions (e.g. plugins/time.py shadowing the stdlib).
        """
        root_str = str(root)
        if root_str not in sys.path:
            sys.path.insert(0, root_str)

    def _should_skip_dir(self, name: str) -> bool:
        if name.startswith(".") or name.startswith("__"):
            return True
        if name.lower() in {n.lower() for n in self.config.excluded_dirs}:
            return True
        return False

    def _should_skip_file(self, path: Path) -> bool:
        name = path.name
        if name.startswith(".") or name.startswith("_"):
            return True
        if name in self._SELF_NAMES:
            return True
        if path.suffix != ".py":
            return True
        return False

    def _discover_all_plugins(self, root: Path) -> List[Path]:
        """Walk ``root/plugins/`` recursively and return all loadable .py files."""
        plugin_root = root / "plugins"
        if not plugin_root.exists() or not plugin_root.is_dir():
            return []

        results: List[Path] = []
        self._walk(plugin_root, results)
        return sorted(results)

    def _walk(self, directory: Path, results: List[Path]) -> None:
        if not directory.is_dir():
            return
        try:
            entries = sorted(directory.iterdir())
        except PermissionError as e:
            self.logger.warning("Discovery: permission denied", path=str(directory), error=str(e))
            return

        for entry in entries:
            try:
                if entry.is_dir():
                    if self._should_skip_dir(entry.name):
                        continue
                    self._walk(entry, results)
                elif entry.is_file():
                    if not self._should_skip_file(entry):
                        results.append(entry)
            except Exception as e:
                self.logger.debug("Discovery: error scanning entry",
                                  path=str(entry), error=str(e))

    def _make_module_key(self, path: Path, root: Path) -> str:
        rel = path.relative_to(root).with_suffix("")
        return ".".join(rel.parts)

    # ──────────────────────────────────────────────────────────
    #  Metadata extraction (# DEPENDS / # PRIORITY / # DISABLED)
    # ──────────────────────────────────────────────────────────

    def _extract_metadata(self, path: Path, root: Path) -> PluginMetadata:
        module_key = self._make_module_key(path, root)
        parts = path.relative_to(root).parts
        folder = parts[0] if len(parts) > 1 else "root"
        dependencies: List[str] = []
        priority = 100
        disabled = False

        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= self.config.metadata_scan_lines:
                        break
                    s = line.strip()
                    if s.startswith("# DEPENDS:"):
                        raw = s.split(":", 1)[1].strip()
                        dependencies = [d.strip() for d in raw.split(",") if d.strip()]
                    elif s.startswith("# PRIORITY:"):
                        try:
                            priority = int(s.split(":", 1)[1].strip())
                        except ValueError:
                            pass
                    elif s.startswith("# DISABLED"):
                        disabled = True
        except Exception as e:
            self.logger.debug("Metadata read failed", plugin=module_key, error=str(e))

        return PluginMetadata(
            name=path.stem,
            path=path,
            module_key=module_key,
            folder=folder,
            dependencies=dependencies,
            priority=priority,
            disabled=disabled,
        )

    # ──────────────────────────────────────────────────────────
    #  Import & handler registration
    # ──────────────────────────────────────────────────────────

    def _import_module(
        self,
        metadata: PluginMetadata,
        force_reload: bool,
    ) -> Any:
        module_key = metadata.module_key

        if force_reload and module_key in sys.modules:
            return importlib.reload(sys.modules[module_key])

        spec = importlib.util.spec_from_file_location(module_key, metadata.path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot build module spec for: {metadata.path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_key] = module

        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_key, None)
            raise

        return module

    def _register_handlers(
        self,
        module: Any,
        client: Any,
        registry: HandlerRegistry,
    ) -> int:
        count = 0
        for _name, obj in vars(module).items():
            # Fix: Ensure it's a function, not a class or other callable
            if not inspect.isfunction(obj):
                continue
            if getattr(obj, "__module__", None) != module.__name__:
                continue
            try:
                handler_list = getattr(obj, "handlers", None)
            except Exception:
                continue
                
            if not isinstance(handler_list, list) or not handler_list:
                continue
            for handler_inst, group in handler_list:
                try:
                    client.add_handler(handler_inst, group)
                    registry.track(client, handler_inst, group)
                    count += 1
                except Exception as exc:
                    self.logger.warning(
                        "Handler registration failed",
                        plugin=module.__name__,
                        handler=type(handler_inst).__name__,
                        error=str(exc),
                    )
        return count

    # ──────────────────────────────────────────────────────────
    #  Lifecycle hooks
    # ──────────────────────────────────────────────────────────

    async def _call_hook(
        self,
        module: Any,
        hook_name: str,
        *args: Any,
        timeout: Optional[float] = None,
    ) -> bool:
        """Call an optional module-level function like ``on_load(client)``."""
        fn = getattr(module, hook_name, None)
        if fn is None or not callable(fn):
            return True
        try:
            t = timeout or self.config.timeout_seconds
            res = fn(*args)
            if asyncio.iscoroutine(res):
                await asyncio.wait_for(res, timeout=t)
            return True
        except asyncio.TimeoutError:
            self.logger.warning("Hook timed out", hook=hook_name,
                                plugin=getattr(module, "__name__", "?"))
            return False
        except Exception as e:
            self.logger.error("Hook raised", hook=hook_name,
                              error=str(e), exc=e)
            return False

    async def _drain_plugin(self, metadata: PluginMetadata) -> None:
        """Wait for in-flight work to finish before tearing down."""
        old_state = metadata.state
        metadata.state = PluginState.DRAINING
        self.registry.upsert(metadata)

        event = metadata._drain_event
        if event is None or event.is_set():
            metadata.state = old_state
            return

        self.logger.info("Drain: waiting...", plugin=metadata.name)
        try:
            await asyncio.wait_for(
                asyncio.shield(event.wait()),
                timeout=self.config.reload_drain_timeout_seconds,
            )
        except asyncio.TimeoutError:
            self.logger.warning("Drain timed out", plugin=metadata.name)

        metadata.state = old_state

    async def _teardown_plugin(self, metadata: PluginMetadata) -> None:
        """Remove all registered handlers and call ``on_unload``."""
        metadata.handler_registry.remove_all()
        module = sys.modules.get(metadata.module_key)
        if module:
            await self._call_hook(module, "on_unload", timeout=10.0)
        sys.modules.pop(metadata.module_key, None)

    # ──────────────────────────────────────────────────────────
    #  Single-plugin load
    # ──────────────────────────────────────────────────────────

    async def _load_single_plugin(
        self,
        metadata: PluginMetadata,
        client: Any,
        is_reload: bool = False,
    ) -> Tuple[bool, Optional[str], int]:
        async with self._reload_lock:
            start_time = time.time()
            current_fp = _fingerprint(metadata.path)

            # Skip reload if file hasn't changed
            if is_reload and current_fp == metadata.fingerprint:
                metadata.state = PluginState.ACTIVE
                return True, None, 0

            metadata.state = PluginState.LOADING

            try:
                if is_reload:
                    await self._drain_plugin(metadata)
                    await self._teardown_plugin(metadata)

                metadata.handler_registry = HandlerRegistry()
                metadata.reset_drain()

                # Import in executor to avoid blocking the event loop
                loop = asyncio.get_running_loop()
                module = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        self._import_module,
                        metadata,
                        is_reload,
                    ),
                    timeout=self.config.timeout_seconds,
                )

                # Register @Client.on_* decorated handlers with the live client
                self._register_handlers(module, client, metadata.handler_registry)

                # Call lifecycle hooks
                if is_reload:
                    hook = "on_reload" if hasattr(module, "on_reload") else "on_load"
                    await self._call_hook(module, hook, client)
                else:
                    await self._call_hook(module, "init", client)
                    await self._call_hook(module, "on_load", client)

                load_time = int((time.time() - start_time) * 1000)
                metadata.state = PluginState.ACTIVE
                metadata.fingerprint = current_fp
                metadata.load_time_ms = load_time
                metadata.loaded_at = datetime.now().isoformat()
                metadata.error = None
                if is_reload:
                    metadata.reload_count += 1

                return True, None, load_time

            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                metadata.state = PluginState.FAILED
                metadata.error = err
                self.logger.error(f"✘ FAILED [{metadata.name}] {err}")
                return False, err, 0

    # ──────────────────────────────────────────────────────────
    #  Unload / Reload
    # ──────────────────────────────────────────────────────────

    async def unload_plugin(self, module_key: str) -> bool:
        metadata = self.registry.get(module_key)
        if metadata is None:
            return False
        await self._teardown_plugin(metadata)
        metadata.state = PluginState.UNLOADED
        self.registry.upsert(metadata)
        self.logger.info("✘ Unloaded", plugin=metadata.name)
        return True

    async def reload_plugin(self, module_key: str, client: Any) -> bool:
        metadata = self.registry.get(module_key)
        if metadata is None:
            return False
        success, _error, _ = await self._load_single_plugin(metadata, client, is_reload=True)
        self.registry.upsert(metadata)
        return success

    # ──────────────────────────────────────────────────────────
    #  Batch loading
    # ──────────────────────────────────────────────────────────

    async def _load_all(
        self, plugins: List[PluginMetadata], client: Any, is_reload: bool
    ) -> Tuple[int, int]:
        """Load all plugins sequentially (dependency-safe ordering)."""
        loaded = failed = 0
        for metadata in plugins:
            success, _error, _ = await self._load_single_plugin(metadata, client, is_reload)
            self.registry.upsert(metadata)
            if success:
                loaded += 1
            else:
                failed += 1
        return loaded, failed

    # ──────────────────────────────────────────────────────────
    #  Startup report
    # ──────────────────────────────────────────────────────────

    def _print_startup_report(self, plugins: List[PluginMetadata], duration_ms: int) -> None:
        active = [p for p in plugins if p.state == PluginState.ACTIVE]
        failed = [p for p in plugins if p.state == PluginState.FAILED]
        disabled = [p for p in plugins if p.disabled]

        W = 56
        div = "  ╠" + "═" * W + "╣"

        def row(text: str) -> str:
            return f"  ║  {text:<{W - 2}}║"

        lines = [
            "",
            "  ╔" + "═" * W + "╗",
            row("𝗥𝗘𝗫𝗬 𝗩𝟭𝟰 𝗟𝗜𝗧𝗘 · 𝗣𝗟𝗨𝗚𝗜𝗡 𝗟𝗢𝗔𝗗𝗘𝗥 𝗥𝗘𝗣𝗢𝗥𝗧"),
            div,
            row(f"✔  Active   : {len(active)}"),
            row(f"✘  Failed   : {len(failed)}"),
            row(f"⊘  Disabled : {len(disabled)}"),
            row(f"⏱  Duration : {duration_ms}ms"),
            div,
        ]

        by_folder: Dict[str, List[PluginMetadata]] = {}
        for p in active:
            by_folder.setdefault(p.folder, []).append(p)

        for folder, fps in sorted(by_folder.items()):
            lines.append(row(f"[{folder}]"))
            for p in sorted(fps, key=lambda x: x.name):
                detail = f"{p.load_time_ms}ms"
                name_col = f"    ✔ {p.name}"
                pad = W - 4 - len(name_col) - len(detail)
                lines.append(f"  ║{name_col}{'.' * max(pad, 1)}{detail}  ║")

        if failed:
            lines.append(div)
            lines.append(row("FAILED PLUGINS"))
            for p in failed:
                err = (p.error or "Unknown")[:46]
                lines.append(row(f"    ✘ {p.name}  —  {err}"))

        report = "\n".join(lines)
        for line in lines:
            if line.strip():
                self.logger.info(line)
        # Also print the full block for clean console output
        print(report)

    # ──────────────────────────────────────────────────────────
    #  File watcher (hot-reload, optional)
    # ──────────────────────────────────────────────────────────

    async def _watcher_loop(self, client: Any) -> None:
        self.logger.info("Watcher started")
        root = self._root or Path(".").resolve()
        for p in self.registry.by_state(PluginState.ACTIVE):
            self._watched_fps[p.path] = p.fingerprint or ""

        while True:
            try:
                await asyncio.sleep(self.config.watcher_interval_seconds)
                current_paths = self._discover_all_plugins(root)
                
                # 1. Detect file deletions
                for watched_path in list(self._watched_fps.keys()):
                    if watched_path not in current_paths:
                        module_key = self._make_module_key(watched_path, root)
                        await self.unload_plugin(module_key)
                        del self._watched_fps[watched_path]
                        # Evict from fingerprint cache
                        _fp_cache.pop(watched_path, None)

                # 2. Detect modifications and new files
                for path in current_paths:
                    current_fp = _fingerprint(path)
                    known_fp = self._watched_fps.get(path)

                    if known_fp is None:
                        metadata = self._extract_metadata(path, root)
                        if not metadata.disabled:
                            await self._load_single_plugin(metadata, client)
                            self.registry.upsert(metadata)
                            self._watched_fps[path] = current_fp
                    elif current_fp != known_fp:
                        module_key = self._make_module_key(path, root)
                        await self.reload_plugin(module_key, client)
                        self._watched_fps[path] = current_fp
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.logger.warning("Watcher encountered error", error=str(e))

    def start_watcher(self, client: Any) -> None:
        # Fix: Idempotent watcher start (cancel existing task first)
        if self._watcher_task and not self._watcher_task.done():
            self._watcher_task.cancel()
            
        self._watcher_task = asyncio.create_task(
            self._watcher_loop(client),
            name="plugin-watcher",
        )

    async def shutdown(self) -> None:
        """Gracefully stop the plugin loader and all associated tasks."""
        if self._watcher_task:
            self._watcher_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._watcher_task
            self.logger.info("Watcher shut down")
        
        # Clear entries from sys.modules if needed (optional)
        # importlib.invalidate_caches()

    # ──────────────────────────────────────────────────────────
    #  Main entry point
    # ──────────────────────────────────────────────────────────

    async def load_plugins(
        self, 
        client: Any, 
        is_reload: bool = False, 
        root: Optional[Path] = None
    ) -> Dict[str, Any]:
        start_time = time.time()
        self._client = client
        self._root = root or self._root or Path(".").resolve()

        if client is None:
            raise ValueError("client cannot be None")

        try:
            root = self._root
            self._setup_python_path(root)

            all_paths = self._discover_all_plugins(root)
            all_meta = [self._extract_metadata(p, root) for p in all_paths]
            plugins = [m for m in all_meta if not m.disabled]
            skipped = [m for m in all_meta if m.disabled]

            for m in skipped:
                m.state = PluginState.UNLOADED
                self.registry.upsert(m)

            # Resolve dependency order
            if self.config.enable_dependency_resolution:
                try:
                    plugins = _DependencyResolver.resolve(plugins)
                except RuntimeError as e:
                    self.logger.error("Dependency resolution failed", error=str(e))
                    # Fall back to priority-only ordering
                    plugins = sorted(plugins, key=lambda p: p.priority)

            loaded, failed = await self._load_all(plugins, client, is_reload)
            duration_ms = int((time.time() - start_time) * 1000)

            if self.config.show_startup_report:
                self._print_startup_report(self.registry.all(), duration_ms)

            if self.config.enable_watcher:
                self.start_watcher(client)

            return {"loaded": loaded, "failed": failed}

        except Exception as e:
            self.logger.critical("Loader failure", exc=e)
            return {"loaded": 0, "failed": 0}


# ══════════════════════════════════════════════════════════════
#  | 𝗦𝗜𝗡𝗚𝗟𝗘𝗧𝗢𝗡 |
# ══════════════════════════════════════════════════════════════

_loader_instance: Optional[PluginLoader] = None


def get_loader(config: Optional[PluginConfig] = None) -> PluginLoader:
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = PluginLoader(config)
    return _loader_instance


async def load_plugins_async(
    client: Any,
    config: Optional[PluginConfig] = None,
    reload: bool = False,
    root: Optional[Path] = None,
) -> Dict[str, Any]:
    """Top-level async entry point.  Called from ``bot.py``."""
    return await get_loader(config).load_plugins(client, is_reload=reload, root=root)


__all__ = [
    "load_plugins_async",
    "PluginConfig",
    "PluginState",
    "HandlerRegistry",
    "get_loader",
]

