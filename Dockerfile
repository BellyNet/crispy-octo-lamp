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
# The dashboard reuses scrapyard/ helpers (mediaDates, registry, transcoders).
COPY scrapyard/ ./scrapyard/
COPY audit/audit-exact-media-duplicates.js audit/export-exact-media-review.js ./audit/
COPY model_aliases.json ./

ENV DATASET_DIR=/data/dataset
ENV THUMB_DIR=/data/thumbs
ENV DASHBOARD_PORT=3420
ENV NODE_ENV=production

EXPOSE 3420
CMD ["node", "dashboard/server.js"]
