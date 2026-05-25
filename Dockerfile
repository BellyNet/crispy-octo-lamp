# ── Build stage: compile native modules (canvas, sharp) ──────────────────────
FROM node:20-slim AS deps

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3 \
    libcairo2-dev \
    libpango1.0-dev \
    libjpeg-dev \
    libgif-dev \
    librsvg2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM node:20-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libcairo2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libjpeg62-turbo \
    libgif7 \
    librsvg2-2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY dashboard/ ./dashboard/
COPY milkmaid/media-dates.js ./milkmaid/
# milkmaid/media-dates.js is now a thin re-export of scrapyard/mediaDates,
# which in turn has its own transitive helpers under scrapyard/. Copy the
# whole dir so the dashboard always boots even as the refactor continues.
COPY scrapyard/ ./scrapyard/
COPY model_aliases.json ./

ENV DATASET_DIR=/data/dataset
ENV THUMB_DIR=/data/thumbs
ENV DASHBOARD_PORT=3420
ENV NODE_ENV=production

EXPOSE 3420
CMD ["node", "dashboard/server.js"]
