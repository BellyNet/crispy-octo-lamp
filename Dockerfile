# ── Build stage: install production node_modules ─────────────────────────────
FROM node:20-slim AS deps

# sharp ships prebuilt linux-x64 binaries; the toolchain is only a fallback
# in case npm ever has to build it from source. Builder-only, so it never
# reaches the runtime image.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# puppeteer's postinstall downloads Chrome; the dashboard never launches it.
ENV PUPPETEER_SKIP_DOWNLOAD=true

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM node:20-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY dashboard/ ./dashboard/
# The scrapers ship in the same image so the dashboard's NAS worker can run
# scrapes against the same dataset it serves.
COPY scrapyard/ ./scrapyard/
COPY hoghaul/ ./hoghaul/
COPY milkmaid/ ./milkmaid/
COPY stuffinglogger/ ./stuffinglogger/
COPY audit/ ./audit/
COPY banners.js model_aliases.json ./

# Scraper scratch space (partial downloads, run reports) lives in the NAS
# state folder mounted at /data/state, so it is writable by the share user
# and survives container restarts.
RUN ln -s /data/state/tmp /app/tmp && ln -s /data/state/incomplete /app/incomplete

ENV DATASET_DIR=/data/dataset
# The dataset is the NAS dataset: tells scrapyard/datasetLocation.js there is
# no separate local copy to sync or evict.
ENV NAS_DATASET_DIR=/data/dataset
ENV SLOPVAULT_ROOT=/data/state
# No Chrome in this image: browser-only sources (StufferDB, Tumblr, Reddit)
# refuse to start here and run on the PC instead.
ENV SCRAPER_NO_BROWSER=1
# Scrape queue shared with the PC worker, and this container's role in it.
ENV SCRAPE_QUEUE_DIR=/data/state/scrapes
ENV SCRAPE_WORKER=nas
# The queue folder is local disk here; the PC goes through the dashboard API.
ENV SCRAPE_QUEUE_MODE=local
ENV THUMB_DIR=/data/thumbs
ENV DASHBOARD_PORT=3420
ENV NODE_ENV=production

EXPOSE 3420
# Group-writable files (umask 0002) so the PC, which writes to the same share
# as the same group, can modify what the dashboard creates.
CMD ["sh", "-c", "umask 0002 && exec node dashboard/server.js"]
