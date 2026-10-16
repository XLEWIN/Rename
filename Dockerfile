#┌─ 𝗥𝗘𝗫𝗬 𝗪𝗢𝗥𝗞𝗦𝗛𝗢𝗣 ───────────────────────────────┐
#│  ʀᴇ-ᴄᴏᴅᴇ ᴏᴘᴛɪᴍɪᴢᴀ𝗧𝗜𝗢𝗡 𝗗𝗢𝗡𝗘                │      @NyxRexy
#│  (ᴘʀᴏᴅᴜᴄᴛɪᴏɴ ʀᴇғᴀᴄᴛᴏʀ)                   │      @ShadowedTomb
#└──────────────────────────────────────────┘

# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗨𝗟𝗧𝗜 𝗦𝗧𝗔𝗚𝗘 𝗕𝗨𝗜𝗟𝗗 | - ʙᴜɪʟᴅᴇʀ sᴛᴀɢᴇ
# ─────────────────────────────────────────────────────────────

FROM python:3.12-slim-bookworm AS builder

# Set environment variables for optimized pip builds
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# Install build dependencies for C-extensions
# gcc/g++/make: for psutil, uvloop, and other C-based libraries
# libffi-dev/libssl-dev: for TgCrypto and cryptographic security
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .

# ─────────────────────────────────────────────────────────────
#  | 𝗔𝗨𝗧𝗢𝗠𝗔𝗧𝗜𝗖 𝗣𝗜𝗣 𝗜𝗡𝗦𝗧𝗔𝗟𝗟
#  Every package in requirements.txt is installed at BUILD time, before the
#  service ever starts on Railway.  Nothing has to be installed by hand.
# ─────────────────────────────────────────────────────────────
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ─────────────────────────────────────────────────────────────
#  | 𝗠𝗨𝗟𝗧𝗜 𝗦𝗧𝗔𝗚𝗘 𝗕𝗨𝗜𝗟𝗗 | - ʀᴜɴᴛɪᴍᴇ sᴛᴀɢᴇ
# ─────────────────────────────────────────────────────────────

FROM python:3.12-slim-bookworm

# Set environment variables
# PYTHONPATH: Ensures the loader find plugins relative to the root
# PORT: Fallback listen port for local docker runs.  Railway/Render inject
#       their own PORT at runtime, which overrides this image default, and
#       config.py reads WEBHOOK_PORT -> PORT -> 8023 in that order.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/venv/bin:$PATH" \
    PYTHONPATH="/app" \
    PORT=8023

WORKDIR /app

# Install runtime dependencies
# ffmpeg: Critical for video processing and thumbnails
# curl: Required for internal health-checks
# ca-certificates: Essential for secure HTTPS API calls
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create non-root user for high-security isolation
RUN useradd -m -u 1000 botuser && \
    mkdir -p /app && \
    chown -R botuser:botuser /app

# Copy virtual environment and code with correct ownership
COPY --from=builder --chown=botuser:botuser /app/venv /app/venv
COPY --chown=botuser:botuser . .

# Switch to isolated non-root user
USER botuser

# Expose ports for external traffic (8023 is what Railway port-detects;
# the process itself always binds 0.0.0.0:$PORT / $WEBHOOK_PORT)
EXPOSE 8023

# Production Healthcheck
# Monitors the internal unified /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://127.0.0.1:${PORT}/health || exit 1

# Start the bot as the main process
# Using JSON list form for proper signal propagation (SIGTERM/SIGINT)
CMD ["python", "bot.py"]
