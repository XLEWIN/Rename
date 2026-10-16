
#┌─ 𝗥𝗘𝗫𝗬 𝗜𝗡𝗧𝗘𝗥𝗡𝗔𝗟 ──────────────────────────────┐
#│  ʀᴇsɪʟɪᴇɴᴛ ᴀʀᴄʜɪᴛᴇᴄᴛᴜʀᴇ • sᴄᴀʟᴇ ʀᴇᴀᴅʏ              │
#│  ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ ᴄᴏᴍᴘʟᴇᴛᴇ ʙʏ @NyxRexy     │
#└──────────────────────────────────────────┘

# ─────────────────────────────────────────────────────────────
#  | 𝗖𝗢𝗥𝗘 𝗜𝗠𝗣𝗢𝗥𝗧𝗦 |
# ─────────────────────────────────────────────────────────────

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, Set, Callable
from collections import defaultdict, deque

from aiohttp import web
from aiohttp.web import middleware

# ─────────────────────────────────────────────────────────────
#  | 𝗟𝗢𝗚𝗚𝗘𝗥 𝗦𝗘𝗧𝗨𝗣 - sᴛʀᴜᴄᴛᴜʀᴇᴅ ʟᴏɢɢɪɴɢ |
# ─────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

try:
    from rich.logging import RichHandler
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%Y-%m-%d %H:%M:%S]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)]
    )
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗘𝗧𝗥𝗜𝗖𝗦 & 𝗠𝗢𝗡𝗜𝗧𝗢𝗥𝗜𝗡𝗚 | 
# ─────────────────────────────────────────────────────────────

class ServerMetrics:
    
    def __init__(self):
        self.request_count: int = 0
        self.error_count: int = 0
        self.total_response_time: float = 0.0
        self.response_times: deque = deque(maxlen=1000)
        self.status_codes: Dict[int, int] = defaultdict(int)
        self.endpoint_stats: Dict[str, Dict] = defaultdict(
            lambda: {"count": 0, "avg_time": 0.0, "errors": 0}
        )
        self.start_time: float = time.time()
    
    def record_request(
        self,
        endpoint: str,
        status_code: int,
        response_time: float,
        is_error: bool = False
    ):
        """Record request metrics"""
        self.request_count += 1
        self.status_codes[status_code] += 1
        self.response_times.append(response_time)
        self.total_response_time += response_time
        
        if is_error:
            self.error_count += 1
            self.endpoint_stats[endpoint]["errors"] += 1
        
        # Update endpoint stats
        stats = self.endpoint_stats[endpoint]
        stats["count"] += 1
        stats["avg_time"] = (
            (stats["avg_time"] * (stats["count"] - 1) + response_time)
            / stats["count"]
        )
    
    def get_stats(self) -> Dict:
        """Get current server statistics"""
        uptime = time.time() - self.start_time
        avg_response_time = (
            self.total_response_time / self.request_count
            if self.request_count > 0
            else 0
        )
        
        # Calculate percentiles
        sorted_times = sorted(self.response_times)
        p50 = sorted_times[len(sorted_times) // 2] if sorted_times else 0
        p95 = sorted_times[int(len(sorted_times) * 0.95)] if sorted_times else 0
        p99 = sorted_times[int(len(sorted_times) * 0.99)] if sorted_times else 0
        
        return {
            "uptime_seconds": round(uptime, 2),
            "total_requests": self.request_count,
            "total_errors": self.error_count,
            "error_rate": (
                round((self.error_count / self.request_count) * 100, 2)
                if self.request_count > 0
                else 0
            ),
            "avg_response_time_ms": round(avg_response_time * 1000, 2),
            "p50_response_time_ms": round(p50 * 1000, 2),
            "p95_response_time_ms": round(p95 * 1000, 2),
            "p99_response_time_ms": round(p99 * 1000, 2),
            "requests_per_second": round(
                self.request_count / uptime if uptime > 0 else 0,
                2
            ),
            "status_codes": dict(self.status_codes),
            "top_endpoints": dict(
                sorted(
                    self.endpoint_stats.items(),
                    key=lambda x: x[1]["count"],
                    reverse=True
                )[:10]
            )
        }

# Global metrics instance
metrics = ServerMetrics()

# ─────────────────────────────────────────────────────────────
#  | 𝗥𝗔𝗧𝗘 𝗟𝗜𝗠𝗜𝗧𝗜𝗡𝗚 |
# ─────────────────────────────────────────────────────────────

class RateLimiter:
    
    def __init__(self, requests_per_second: int = 10, burst: int = 20):
        self.rate = requests_per_second
        self.burst = burst
        self.clients: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
    
    async def is_allowed(self, client_id: str) -> bool:
        """Check if request is allowed under rate limit"""
        async with self._lock:
            now = time.time()
            
            if client_id not in self.clients:
                self.clients[client_id] = {
                    "tokens": self.burst,
                    "last_update": now
                }
            
            client = self.clients[client_id]
            
            # Add tokens based on elapsed time
            elapsed = now - client["last_update"]
            client["tokens"] = min(
                self.burst,
                client["tokens"] + (elapsed * self.rate)
            )
            client["last_update"] = now
            
            # Check if we have a token available
            if client["tokens"] >= 1:
                client["tokens"] -= 1
                return True
            
            return False
    
    async def cleanup_old_clients(self, max_age: int = 3600):
        """Remove clients that haven't made requests in max_age seconds"""
        async with self._lock:
            now = time.time()
            to_remove = [
                client_id
                for client_id, data in self.clients.items()
                if now - data["last_update"] > max_age
            ]
            for client_id in to_remove:
                del self.clients[client_id]
            
            if to_remove:
                logger.debug(f"Cleaned up {len(to_remove)} stale rate limit entries")

# Global rate limiter - adjust as needed
rate_limiter = RateLimiter(requests_per_second=10, burst=20)

# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗜𝗗𝗗𝗟𝗘𝗪𝗔𝗥𝗘 𝗦𝗧𝗔𝗖𝗞 |
# ─────────────────────────────────────────────────────────────

@middleware
async def request_id_middleware(request: web.Request, handler):
    """Add unique request ID for tracing"""
    request_id = str(uuid.uuid4())
    request["request_id"] = request_id
    
    response = await handler(request)
    response.headers["X-Request-ID"] = request_id
    
    return response


@middleware
async def security_headers_middleware(request: web.Request, handler):
    """Add security headers to all responses"""
    response = await handler(request)
    
    # Security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    
    # CORS headers for API access
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    
    return response


@middleware
async def rate_limit_middleware(request: web.Request, handler):
    """Rate limiting middleware"""
    # Skip rate limiting for health checks
    if request.path in ["/health", "/ping"]:
        return await handler(request)
    
    client_ip = request.remote or "unknown"
    
    if not await rate_limiter.is_allowed(client_ip):
        logger.warning(f"Rate limit exceeded for {client_ip}")
        return web.json_response(
            {
                "error": "Rate limit exceeded",
                "message": "Too many requests. Please try again later."
            },
            status=429,
            headers={"Retry-After": "60"}
        )
    
    return await handler(request)


@middleware
async def timing_middleware(request: web.Request, handler):
    """Track request timing and metrics"""
    start_time = time.time()
    request["start_time"] = start_time
    
    try:
        response = await handler(request)
        response_time = time.time() - start_time
        
        # Record metrics
        metrics.record_request(
            endpoint=request.path,
            status_code=response.status,
            response_time=response_time,
            is_error=response.status >= 400
        )
        
        # Add timing header
        response.headers["X-Response-Time"] = f"{response_time * 1000:.2f}ms"
        
        return response
        
    except Exception as e:
        response_time = time.time() - start_time
        
        # Record error
        metrics.record_request(
            endpoint=request.path,
            status_code=500,
            response_time=response_time,
            is_error=True
        )
        
        raise


@middleware
async def logging_middleware(request: web.Request, handler):
    """Comprehensive request/response logging"""
    request_id = request.get("request_id", "unknown")
    
    logger.info(
        f"[{request_id[:8]}] → {request.method} {request.path} "
        f"from {request.remote}"
    )
    
    try:
        response = await handler(request)
        
        response_time = time.time() - request.get("start_time", time.time())
        
        # Color-coded log level based on status
        if response.status >= 500:
            log_func = logger.error
        elif response.status >= 400:
            log_func = logger.warning
        else:
            log_func = logger.info
        
        log_func(
            f"[{request_id[:8]}] ← {request.method} {request.path} "
            f"{response.status} ({response_time * 1000:.2f}ms)"
        )
        
        return response
        
    except Exception as e:
        logger.error(
            f"[{request_id[:8]}] ✗ {request.method} {request.path} "
            f"failed: {e}",
            exc_info=True
        )
        raise


@middleware
async def error_middleware(request: web.Request, handler):
    """Global error handling with detailed logging"""
    try:
        return await handler(request)
        
    except web.HTTPException:
        # Let HTTP exceptions pass through
        raise
        
    except asyncio.CancelledError:
        # Handle graceful shutdown
        logger.info("Request cancelled (server shutting down)")
        raise
        
    except Exception as e:
        request_id = request.get("request_id", "unknown")
        
        logger.error(
            f"[{request_id[:8]}] Unhandled error in {request.path}: {e}",
            exc_info=True
        )
        
        return web.json_response(
            {
                "error": "Internal server error",
                "request_id": request_id,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            status=500
        )

# ─────────────────────────────────────────────────────────────
#  | 𝗥𝗢𝗨𝗧𝗘𝗦 |
# ─────────────────────────────────────────────────────────────

routes = web.RouteTableDef()

@routes.get("/")
async def root_route_handler(request: web.Request):
    return web.json_response(
        {
            "status": "ok",
            "message": "Kaito- The Greatest",
            "server": "Production-Grade Aiohttp Server",
            "version": "2.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": round(time.time() - metrics.start_time, 2),
            "endpoints": {
                "health": "/health - Detailed health check",
                "metrics": "/metrics - Server metrics",
                "ping": "/ping - Simple connectivity test",
                "stats": "/stats - Detailed statistics"
            }
        },
        headers={"Cache-Control": "no-cache"}
    )


@routes.get("/health")
async def health_check_handler(request: web.Request):
    """
    Comprehensive health check endpoint.
    Used by load balancers and monitoring systems.
    """
    # 1. Server-side health
    uptime = time.time() - metrics.start_time
    error_rate = (
        (metrics.error_count / metrics.request_count * 100)
        if metrics.request_count > 0
        else 0
    )
    is_server_healthy = error_rate < 5.0  # Less than 5% error rate

    # 2. Bot‑side check (if probe is provided)
    bot_status = True
    if request.app.get('health_probe'):
        try:
            # The probe can be async or sync
            probe = request.app['health_probe']
            if asyncio.iscoroutinefunction(probe):
                bot_status = await probe()
            else:
                bot_status = probe()
        except Exception as e:
            logger.error(f"Health probe failed: {e}")
            bot_status = False
    
    is_healthy = is_server_healthy and bot_status
    status_code = 200 if is_healthy else 503
    
    return web.json_response(
        {
            "status": "healthy" if is_healthy else "degraded",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": round(uptime, 2),
            "checks": {
                "server": "ok" if is_server_healthy else "error",
                "bot": "ok" if bot_status else "error",
                "error_rate": f"{error_rate:.2f}%",
                "total_requests": metrics.request_count,
                "total_errors": metrics.error_count
            }
        },
        status=status_code,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"}
    )


@routes.get("/ping")
async def ping_handler(request: web.Request):
    """
    Simple ping endpoint for basic connectivity tests.
    Lightweight - no metrics or logging overhead.
    """
    return web.Response(
        text="pong",
        content_type="text/plain",
        headers={"Cache-Control": "no-cache"}
    )


@routes.get("/metrics")
async def metrics_handler(request: web.Request):
    """
    Prometheus-style metrics endpoint.
    Returns detailed server metrics in JSON format.
    """
    stats = metrics.get_stats()
    
    return web.json_response(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metrics": stats
        },
        headers={"Cache-Control": "no-cache"}
    )


@routes.get("/stats")
async def stats_handler(request: web.Request):
    """
    Detailed statistics endpoint with human-readable format.
    """
    stats = metrics.get_stats()
    
    return web.json_response(
        {
            "server_info": {
                "name": "Kaito Production Server",
                "version": "2.0.0",
                "uptime": f"{stats['uptime_seconds']:.2f}s"
            },
            "performance": {
                "total_requests": stats["total_requests"],
                "requests_per_second": stats["requests_per_second"],
                "avg_response_time": f"{stats['avg_response_time_ms']:.2f}ms",
                "p50_response_time": f"{stats['p50_response_time_ms']:.2f}ms",
                "p95_response_time": f"{stats['p95_response_time_ms']:.2f}ms",
                "p99_response_time": f"{stats['p99_response_time_ms']:.2f}ms"
            },
            "reliability": {
                "total_errors": stats["total_errors"],
                "error_rate": f"{stats['error_rate']:.2f}%",
                "status_code_distribution": stats["status_codes"]
            },
            "top_endpoints": stats["top_endpoints"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        },
        headers={"Cache-Control": "no-cache"}
    )


@routes.options("/{tail:.*}")
async def options_handler(request: web.Request):
    """Handle CORS preflight requests"""
    return web.Response(
        status=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "86400"
        }
    )

# ─────────────────────────────────────────────────────────────
#  | 𝗔𝗣𝗣𝗟𝗜𝗖𝗔𝗧𝗜𝗢𝗡 𝗙𝗔𝗖𝗧𝗢𝗥𝗬 |
# ─────────────────────────────────────────────────────────────

async def web_server(health_probe: Optional[Callable] = None):
    web_app = web.Application(
        client_max_size=30 * 1024 * 1024,  # 30MB explicit
        middlewares=[
            error_middleware,         
            request_id_middleware,    
            security_headers_middleware,
            rate_limit_middleware,
            timing_middleware,
            logging_middleware,
        ]
    )
    
    # Store health probe for the health check handler
    web_app['health_probe'] = health_probe
    
    web_app.add_routes(routes)
    
    # ─────────────────────────────────────────────────────────
    #  | 𝗦𝗧𝗔𝗥𝗧-𝗨𝗣 𝗛𝗔𝗡𝗗𝗟𝗘𝗥 |
    # ─────────────────────────────────────────────────────────
    
    async def on_startup(app: web.Application):
        logger.info("=" * 60)
        logger.info("🚀 Production Web Server Starting")
        logger.info("=" * 60)
        logger.info(f"• Version: 2.0.0")
        logger.info(f"• Max Request Size: 30MB")
        logger.info(f"• Rate Limit: 10 req/s (burst: 20)")
        logger.info(f"• Middleware Stack: {len(web_app.middlewares)} layers")
        logger.info(f"• Routes Registered: {len(list(web_app.router.routes()))}")
        logger.info("=" * 60)
        
        # Start background cleanup task
        async def cleanup_task():
            """Periodic cleanup of stale data"""
            while True:
                try:
                    await asyncio.sleep(3600)  # Every hour
                    await rate_limiter.cleanup_old_clients()
                    logger.debug("Completed periodic cleanup")
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Cleanup task error: {e}")
        
        app["cleanup_task"] = asyncio.create_task(cleanup_task())
    
    # ─────────────────────────────────────────────────────────
    #  | 𝗦𝗛𝗨𝗧𝗗𝗢𝗪𝗡 𝗛𝗔𝗡𝗗𝗟𝗘𝗥 |
    # ─────────────────────────────────────────────────────────
    
    async def on_shutdown(app: web.Application):
        logger.info("=" * 60)
        logger.info("🛑 Production Web Server Shutting Down")
        logger.info("=" * 60)
        
        # Cancel cleanup task
        if "cleanup_task" in app:
            app["cleanup_task"].cancel()
            try:
                await app["cleanup_task"]
            except asyncio.CancelledError:
                pass
        
        # Log final metrics
        final_stats = metrics.get_stats()
        logger.info(f"• Total Requests Served: {final_stats['total_requests']}")
        logger.info(f"• Total Errors: {final_stats['total_errors']}")
        logger.info(f"• Error Rate: {final_stats['error_rate']:.2f}%")
        logger.info(f"• Avg Response Time: {final_stats['avg_response_time_ms']:.2f}ms")
        logger.info(f"• Uptime: {final_stats['uptime_seconds']:.2f}s")
        logger.info("=" * 60)
        logger.info("✅ Graceful Shutdown Complete")
    
    # ─────────────────────────────────────────────────────────
    #  | 𝗖𝗟𝗘𝗔𝗡-𝗨𝗣 𝗛𝗔𝗡𝗗𝗟𝗘𝗥 |
    # ─────────────────────────────────────────────────────────
    
    async def on_cleanup(app: web.Application):
        logger.info("🧹 Performing final cleanup...")
        
        logger.info("✅ Cleanup complete")
    
    # Register lifecycle handlers
    web_app.on_startup.append(on_startup)
    web_app.on_shutdown.append(on_shutdown)
    web_app.on_cleanup.append(on_cleanup)
    
    return web_app

# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗢𝗗𝗨𝗟𝗘 𝗘𝗫𝗣𝗢𝗥𝗧𝗦 |
# ─────────────────────────────────────────────────────────────

__all__ = [
    "web_server",
    "routes",
    "metrics",
    "rate_limiter"
]

# ─────────────────────────────────────────────────────────────
#  | 𝗗𝗘𝗣𝗟𝗢𝗬𝗠𝗘𝗡𝗧 𝗘𝗡𝗧𝗥𝗬 𝗣𝗢𝗜𝗡𝗧 |
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    async def main():
        app = await web_server()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8023)
        
        logger.info("Starting development server on http://0.0.0.0:8023")
        await site.start()
        
        # Keep running
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            await runner.cleanup()
    
    asyncio.run(main())
