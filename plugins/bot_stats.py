from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import socket
import time
from pathlib import Path
from typing import Any, Optional

from pyrogram import Client, filters
from pyrogram.enums import ChatAction, ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from config import Config
from plugins.admin import admin_svc, close_btn, safe_edit, _on_cooldown

logger = logging.getLogger(__name__)

try:
    import psutil  # type: ignore
except Exception:
    psutil = None


def humanbytes(size: Any) -> str:
    try:
        value = float(size or 0)
    except (TypeError, ValueError):
        value = 0.0
    if value <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    idx = 0
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    return f"{value:.2f} {units[idx]}" if idx else f"{int(value)} {units[idx]}"


def format_duration(seconds: Any) -> str:
    try:
        total = max(int(float(seconds or 0)), 0)
    except (TypeError, ValueError):
        total = 0
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts[:4])


def clamp_percent(value: Any) -> float:
    try:
        pct = float(value)
    except (TypeError, ValueError):
        pct = 0.0
    return max(0.0, min(100.0, pct))


def progress_bar(percent: Any, width: int = 10) -> str:
    pct = clamp_percent(percent)
    filled = int(round((pct / 100.0) * width))
    return "●" * filled + "○" * (width - filled) 


def format_metric(label: str, used: str, total: str, percent: Any) -> str:
    pct = clamp_percent(percent)
    return f"{label} <code>[{progress_bar(pct)}] {pct:.1f}%</code>\n<code>{used} / {total}</code>"


def _read_text(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _read_int(path: str) -> Optional[int]:
    text = _read_text(path)
    if not text or text == "max":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _detect_environment() -> dict[str, str]:
    env = os.environ
    if env.get("DYNO") or env.get("HEROKU_APP_NAME"):
        return {"name": "Heroku", "detail": env.get("DYNO") or env.get("HEROKU_APP_NAME", "")}
    if env.get("RENDER") or env.get("RENDER_SERVICE_ID"):
        return {"name": "Render", "detail": env.get("RENDER_INSTANCE_ID") or env.get("RENDER_SERVICE_NAME", "")}
    if env.get("KOYEB_SERVICE_NAME") or env.get("KOYEB_DEPLOYMENT_ID"):
        return {"name": "Koyeb", "detail": env.get("KOYEB_SERVICE_NAME", "")}
    if env.get("RAILWAY_PROJECT_ID"):
        return {"name": "Railway", "detail": env.get("RAILWAY_SERVICE_NAME", "")}
    if env.get("FLY_APP_NAME"):
        return {"name": "Fly.io", "detail": env.get("FLY_APP_NAME", "")}
    if env.get("VERCEL"):
        return {"name": "Vercel", "detail": env.get("VERCEL_ENV", "")}
    if env.get("container") or Path("/.dockerenv").exists():
        return {"name": "Container", "detail": env.get("HOSTNAME", "")}
    return {"name": "VPS / Bare Metal", "detail": socket.gethostname()}


def _detect_cgroup_limits() -> dict[str, Optional[float]]:
    memory_limit: Optional[float] = None
    cpu_limit_cores: Optional[float] = None

    mem_v2 = _read_int("/sys/fs/cgroup/memory.max")
    mem_v1 = _read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    for candidate in (mem_v2, mem_v1):
        if candidate and 0 < candidate < (1 << 60):
            memory_limit = float(candidate)
            break

    cpu_v2 = _read_text("/sys/fs/cgroup/cpu.max")
    if cpu_v2 and cpu_v2 != "max":
        parts = cpu_v2.split()
        if len(parts) == 2:
            try:
                quota = float(parts[0])
                period = float(parts[1])
                if quota > 0 and period > 0:
                    cpu_limit_cores = quota / period
            except ValueError:
                pass

    if cpu_limit_cores is None:
        quota = _read_int("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period = _read_int("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        if quota and period and quota > 0 and period > 0:
            cpu_limit_cores = float(quota) / float(period)

    return {"memory_limit": memory_limit, "cpu_limit_cores": cpu_limit_cores}


def _meminfo_total_bytes() -> Optional[int]:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except Exception:
        return None
    return None


def _proc_uptime_seconds() -> Optional[float]:
    try:
        raw = _read_text("/proc/uptime")
        if raw:
            return float(raw.split()[0])
    except Exception:
        return None
    return None


def _collect_system_snapshot() -> dict[str, Any]:
    env_info = _detect_environment()
    cgroup = _detect_cgroup_limits()
    disk_root = os.environ.get("DYNO") and "/app" or os.getcwd()
    disk = shutil.disk_usage(disk_root)

    snapshot: dict[str, Any] = {
        "env_name": env_info["name"],
        "env_detail": env_info["detail"],
        "hostname": socket.gethostname(),
        "os_name": platform.system(),
        "os_release": platform.release(),
        "arch": platform.machine(),
        "python": platform.python_version(),
        "disk_total": disk.total,
        "disk_used": disk.used,
        "disk_free": disk.free,
        "disk_percent": (disk.used / disk.total * 100.0) if disk.total else 0.0,
        "cpu_percent": 0.0,
        "cpu_count_logical": os.cpu_count() or 0,
        "cpu_count_physical": None,
        "cpu_freq_mhz": None,
        "load_avg": None,
        "mem_total": None,
        "mem_used": None,
        "mem_percent": 0.0,
        "swap_total": None,
        "swap_used": None,
        "swap_percent": 0.0,
        "proc_rss": 0,
        "proc_cpu": 0.0,
        "proc_threads": 0,
        "proc_uptime": max(time.time() - getattr(Config, "BOT_UPTIME", time.time()), 0.0),
        "sys_uptime": None,
        "fds": None,
        "cgroup_memory_limit": cgroup["memory_limit"],
        "cgroup_cpu_limit": cgroup["cpu_limit_cores"],
    }

    if hasattr(os, "getloadavg"):
        try:
            snapshot["load_avg"] = os.getloadavg()
        except OSError:
            pass

    if psutil is not None:
        cpu_percent = psutil.cpu_percent(interval=0.35)
        vm = psutil.virtual_memory()
        swap = psutil.swap_memory()
        proc = psutil.Process()
        try:
            proc.cpu_percent(None)
            time.sleep(0.12)
            proc_cpu = proc.cpu_percent(None)
        except Exception:
            proc_cpu = 0.0

        snapshot.update(
            {
                "cpu_percent": cpu_percent,
                "cpu_count_logical": psutil.cpu_count(logical=True) or snapshot["cpu_count_logical"],
                "cpu_count_physical": psutil.cpu_count(logical=False),
                "cpu_freq_mhz": getattr(psutil.cpu_freq(), "current", None) if psutil.cpu_freq() else None,
                "mem_total": vm.total,
                "mem_used": vm.used,
                "mem_percent": vm.percent,
                "swap_total": swap.total,
                "swap_used": swap.used,
                "swap_percent": swap.percent,
                "proc_rss": proc.memory_info().rss,
                "proc_cpu": proc_cpu,
                "proc_threads": proc.num_threads(),
                "sys_uptime": max(time.time() - psutil.boot_time(), 0.0),
                "fds": proc.num_fds() if hasattr(proc, "num_fds") else None,
            }
        )
    else:
        mem_total = cgroup["memory_limit"] or _meminfo_total_bytes()
        snapshot["mem_total"] = int(mem_total) if mem_total else None
        snapshot["sys_uptime"] = _proc_uptime_seconds()

    if snapshot["mem_total"] and snapshot["proc_rss"] and snapshot["mem_used"] is None:
        snapshot["mem_used"] = min(snapshot["proc_rss"], snapshot["mem_total"])
        snapshot["mem_percent"] = (snapshot["mem_used"] / snapshot["mem_total"] * 100.0)

    return snapshot


def _render_system_text(stats: dict[str, Any]) -> str:
    cpu_limit = stats.get("cgroup_cpu_limit")
    mem_limit = stats.get("cgroup_memory_limit")
    cpu_total = (
        f"{cpu_limit:.2f} cores"
        if isinstance(cpu_limit, float) and cpu_limit > 0
        else str(stats.get("cpu_count_logical") or "N/A")
    )
    mem_total = int(mem_limit) if mem_limit else stats.get("mem_total") or 0
    mem_used = stats.get("mem_used") or 0
    disk_total = stats.get("disk_total") or 0
    disk_used = stats.get("disk_used") or 0
    swap_total = stats.get("swap_total") or 0
    swap_used = stats.get("swap_used") or 0
    load_avg = stats.get("load_avg")
    load_text = (
        f"{load_avg[0]:.2f} / {load_avg[1]:.2f} / {load_avg[2]:.2f}"
        if load_avg else "N/A"
    )
    freq = stats.get("cpu_freq_mhz")
    freq_text = f"{float(freq):.0f} MHz" if freq else "N/A"
    env_detail = f" ({stats['env_detail']})" if stats.get("env_detail") else ""
    cpu_pct = clamp_percent(stats.get("cpu_percent", 0.0))
    ram_pct = clamp_percent(stats.get("mem_percent", 0.0))
    swap_pct = clamp_percent(stats.get("swap_percent", 0.0))
    disk_pct = clamp_percent(stats.get("disk_percent", 0.0))
    proc_rss_pct = clamp_percent((stats.get("proc_rss", 0) / mem_total * 100.0) if mem_total else 0.0)

    lines = [
        "<blockquote expandable>✦ 𝗦𝗬𝗦𝗧𝗘𝗠 𝗜𝗡𝗙𝗢</blockquote>",
        "",
        "<b><blockquote>≡ | 𝗥𝗨𝗡𝗧𝗜𝗠𝗘 |</blockquote></b>",
        "<blockquote>"
        f"◍ Environment : <code>{stats['env_name']}{env_detail}</code>\n"
        f"◍ Hostname : <code>{stats['hostname']}</code>\n"
        f"◍ OS : <code>{stats['os_name']} {stats['os_release']}</code>\n"
        f"◍ Arch : <code>{stats['arch']}</code>\n"
        f"◍ Python : <code>{stats['python']}</code>"
        "</blockquote>",
        "",
        "<b><blockquote>≡ | 𝗖𝗣𝗨 |</blockquote></b>",
        "<blockquote>"
        f"{format_metric('CPU', f'{cpu_pct:.1f}%', cpu_total, cpu_pct)}\n"
        f"◍ Load Avg : <code>{load_text}</code>\n"
        f"◍ Freq : <code>{freq_text}</code>\n"
        f"◍ Physical Cores : <code>{stats.get('cpu_count_physical') or 'N/A'}</code>\n"
        f"◍ Logical Cores : <code>{stats.get('cpu_count_logical') or 'N/A'}</code>"
        "</blockquote>",
        "",
        "<b><blockquote>≡ | 𝗠𝗘𝗠𝗢𝗥𝗬 |</blockquote></b>",
        "<blockquote>"
        f"◍ {format_metric('RAM', humanbytes(mem_used), humanbytes(mem_total), ram_pct)}"
    ]

    if mem_limit:
        lines[-1] += f"\n◍ Limit Source : <code>cgroup</code>"
    if swap_total:
        lines[-1] += (
            f"\n◍ Swap <code>[{progress_bar(swap_pct)}] {swap_pct:.1f}%</code>\n"
            f"<code>{humanbytes(swap_used)} / {humanbytes(swap_total)}</code>"
        )
    lines[-1] += "</blockquote>"

    lines.extend([
        "",
        "<b><blockquote>≡ | 𝗦𝗧𝗢𝗥𝗔𝗚𝗘 |</blockquote></b>",
        "<blockquote>"
        f"◍ {format_metric('Disk', humanbytes(disk_used), humanbytes(disk_total), disk_pct)}\n"
        f"◍ Free : <code>{humanbytes(stats.get('disk_free') or 0)}</code>"
        "</blockquote>",
        "",
        "<b><blockquote>≡ | 𝗕𝗢𝗧 𝗣𝗥𝗢𝗖𝗘𝗦𝗦 |</blockquote></b>",
        "<blockquote>"
        f"◍ RSS <code>[{progress_bar(proc_rss_pct)}] {proc_rss_pct:.1f}%</code>\n"
        f"<code>{humanbytes(stats.get('proc_rss', 0))} / {humanbytes(mem_total)}</code>\n"
        f"◍ CPU : <code>{clamp_percent(stats.get('proc_cpu', 0.0)):.1f}%</code>\n"
        f"◍ Threads : <code>{stats.get('proc_threads') or 0}</code>\n"
        f"◍ FDs : <code>{stats.get('fds') if stats.get('fds') is not None else 'N/A'}</code>"
        "</blockquote>",
        "",
        "<b><blockquote>≡ | 𝗨𝗣𝗧𝗜𝗠𝗘 |</blockquote></b>",
        "<blockquote>"
        f"◍ Bot : <code>{format_duration(stats.get('proc_uptime', 0.0))}</code>\n"
        f"◍ System : <code>{format_duration(stats.get('sys_uptime', 0.0)) if stats.get('sys_uptime') is not None else 'N/A'}</code>"
        "</blockquote>",
    ])

    return "\n".join(lines)


async def _progress_edit(msg: Message, stage: str, percent: int) -> None:
    text = (
        "<blockquote expandable>⧗ 𝗔𝗡𝗔𝗟𝗬𝗦𝗜𝗡𝗚</blockquote>\n"
        f"<blockquote><code>[{progress_bar(percent, 12)}] {percent}%</code></blockquote>\n"
        f"<blockquote>{stage}</blockquote>"
    )
    await safe_edit(msg, text, parse_mode=ParseMode.HTML, reply_markup=close_btn())


async def _is_allowed(message: Message) -> bool:
    user = message.from_user
    if not user:
        return False
    if user.id == getattr(Config, "OWNER_ID", 0):
        return True
    return await admin_svc.is_admin(user.id)


@Client.on_message(filters.command(["system", "systerm"]))
async def system_info(client: Client, message: Message) -> None:
    if not await _is_allowed(message):
        return

    user = message.from_user
    if user and _on_cooldown(user.id, "system"):
        return

    try:
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    except FloodWait as exc:
        await asyncio.sleep(exc.value)
    except Exception:
        pass

    status = await message.reply_text(
        "<blockquote expandable>⧗ 𝗔𝗡𝗔𝗟𝗬𝗦𝗜𝗡𝗚</blockquote>\n"
        "<blockquote><code>[░░░░░░░░░░░░] 0%</code></blockquote>\n"
        "<blockquote>sᴛᴀʀᴛɪɴɢ ᴅɪᴀɢɴᴏsᴛɪᴄs...</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=close_btn(),
        quote=True,
    )

    try:
        await _progress_edit(status, "ᴄᴏʟʟᴇᴄᴛɪɴɢ ʀᴜɴᴛɪᴍᴇ ᴅᴇᴛᴀɪʟs...", 20)
        await asyncio.sleep(0)
        await _progress_edit(status, "ʀᴇᴀᴅɪɴɢ ᴄᴘᴜ / ᴍᴇᴍᴏʀʏ / ᴅɪsᴋ...", 55)
        snapshot = await asyncio.to_thread(_collect_system_snapshot)
        await _progress_edit(status, "ʀᴇɴᴅᴇʀɪɴɢ ғɪɴᴀʟ ʀᴇᴘᴏʀᴛ...", 85)
        response = _render_system_text(snapshot)
        await safe_edit(
            status,
            response,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=close_btn(),
        )
    except Exception as exc:
        logger.error("[system] diagnostics failed: %s", exc, exc_info=True)
        err_text = str(exc).strip() or exc.__class__.__name__
        await safe_edit(
            status,
            f"<blockquote expandable>✗ sʏsᴛᴇᴍ ᴅɪᴀɢɴᴏsᴛɪᴄs ғᴀɪʟᴇᴅ</blockquote>\n\n<code>{err_text[:300]}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=close_btn(),
        )


__all__ = ["humanbytes", "format_duration"]
