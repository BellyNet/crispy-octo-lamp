'use strict'

// Builds the payload for GET /api/stats. Pure arithmetic over the per-model
// aggregates computeStatsFromResponse() keeps in modelStatsCache plus
// visits.json — O(models) per request, never touches the dataset.

const DAY_MS = 24 * 60 * 60 * 1000
const WEEKS_SHOWN = 26
const TRENDING_WINDOW_DAYS = 14
const RANKING_SIZE = 10
const TOP_FILE_COUNT = 10

// Display names for the `post.site` values the scrapers record. Files with
// no sidecar metadata land under 'unknown'.
const SITE_LABELS = {
  reddit: 'Reddit',
  stufferdb: 'StufferDB',
  coomerfans: 'CoomerFans',
  onlyhaven: 'OnlyHaven',
  kemono: 'Pawchive',
  coomer: 'Coomer',
  tumblr: 'Tumblr',
  unknown: 'No source info',
}

// Per-model aggregates, computed whenever a model is scanned or its response
// cache is loaded. Walks the media list once; buildStatsPayload() only sums these.
function computeStatsFromResponse(allMedia) {
  let earliestMs = Infinity,
    latestMs = 0,
    latestAddedMs = 0,
    totalBytes = 0,
    bytesImages = 0,
    bytesGifs = 0,
    bytesVideos = 0
  const yearCounts = {}
  const typeCounts = { image: 0, gif: 0, video: 0 }
  const siteCounts = {}
  const addedByDay = {}
  const addedCutoffMs = Date.now() - STATS_ADDED_HISTORY_DAYS * DAY_MS
  let videoSeconds = 0
  let flaggedCount = 0
  let captionCount = 0
  const largestFiles = []
  const longestVideos = []
  for (const m of allMedia) {
    if (m.type in typeCounts) typeCounts[m.type] += 1
    const site = m.post?.site || 'unknown'
    siteCounts[site] = (siteCounts[site] || 0) + 1
    if (m.addedMs >= addedCutoffMs) {
      const day = new Date(m.addedMs).toISOString().slice(0, 10)
      addedByDay[day] = (addedByDay[day] || 0) + 1
    }
    if (m.type === 'video' && m.duration > 0) {
      videoSeconds += m.duration
      keepTop(longestVideos, statFileRef(m), 'duration')
    }
    if (m.flagged) flaggedCount += 1
    if (m.hasCaption) captionCount += 1
    keepTop(largestFiles, statFileRef(m), 'size')
    if (m.addedMs > latestAddedMs) latestAddedMs = m.addedMs
    if (m.mediaDateMs) {
      if (m.mediaDateMs < earliestMs) earliestMs = m.mediaDateMs
      if (m.mediaDateMs > latestMs) latestMs = m.mediaDateMs
    }
    const dateMs = m.mediaDateMs || m.addedMs
    if (dateMs > 0) {
      const yr = new Date(dateMs).getFullYear()
      if (yr >= 1990 && yr <= 2035) yearCounts[yr] = (yearCounts[yr] || 0) + 1
    }
    const sz = m.size || 0
    totalBytes += sz
    if (m.type === 'image') bytesImages += sz
    else if (m.type === 'gif') bytesGifs += sz
    else if (m.type === 'video') bytesVideos += sz
  }
  return {
    earliestMs: earliestMs === Infinity ? 0 : earliestMs,
    latestMs,
    latestAddedMs,
    fileCount: allMedia.length,
    yearCounts,
    totalBytes,
    bytesImages,
    bytesGifs,
    bytesVideos,
    typeCounts,
    siteCounts,
    addedByDay,
    videoSeconds,
    flaggedCount,
    captionCount,
    largestFiles,
    longestVideos,
  }
}

// Enough per-day history for the stats page's 26-week chart.
const STATS_ADDED_HISTORY_DAYS = 26 * 7 + 7
const STATS_TOP_FILES_PER_MODEL = 5

function statFileRef(m) {
  return {
    folder: m.folder,
    filename: m.filename,
    type: m.type,
    size: m.size || 0,
    duration: m.duration || 0,
    thumbUrl: m.thumbUrl || null,
  }
}

// Keep the STATS_TOP_FILES_PER_MODEL largest entries by `key`, sorted desc.
function keepTop(list, entry, key) {
  if (!(entry[key] > 0)) return
  if (
    list.length >= STATS_TOP_FILES_PER_MODEL &&
    entry[key] <= list[list.length - 1][key]
  ) {
    return
  }
  list.push(entry)
  list.sort((a, b) => b[key] - a[key])
  if (list.length > STATS_TOP_FILES_PER_MODEL) list.pop()
}

function dayKey(ms) {
  return new Date(ms).toISOString().slice(0, 10)
}

// Monday (UTC) of the week containing `ms`, as a day key.
function weekStartKey(ms) {
  const d = new Date(ms)
  const offset = (d.getUTCDay() + 6) % 7
  return dayKey(
    Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) -
      offset * DAY_MS
  )
}

function sumDaysSince(addedByDay, sinceKey) {
  let total = 0
  for (const [day, count] of Object.entries(addedByDay || {})) {
    if (day > sinceKey) total += count
  }
  return total
}

// Every year from the first to the last with data, so empty years show as
// gaps on the chart instead of being skipped.
function fillYears(yearCounts) {
  const years = Object.keys(yearCounts).map(Number)
  if (!years.length) return []
  const out = []
  for (let year = Math.min(...years); year <= Math.max(...years); year += 1) {
    out.push({ year, count: yearCounts[year] || 0 })
  }
  return out
}

function topBy(rows, value, size = RANKING_SIZE) {
  return rows
    .filter((row) => value(row) > 0)
    .sort((a, b) => value(b) - value(a) || a.name.localeCompare(b.name))
    .slice(0, size)
}

function buildStatsPayload({
  modelNames,
  statsByName,
  visitsData,
  scan = {},
  now = Date.now(),
}) {
  const models = modelNames.map((name) => ({
    name,
    stats: statsByName[name] || null,
    visits: visitsData[name] || null,
  }))
  const scanned = models.filter((m) => m.stats)

  const totals = {
    models: modelNames.length,
    files: 0,
    bytes: 0,
    typeCounts: { image: 0, gif: 0, video: 0 },
    typeBytes: { image: 0, gif: 0, video: 0 },
    videoSeconds: 0,
    flagged: 0,
    captioned: 0,
    addedLast7d: 0,
    addedLast30d: 0,
    totalViews: 0,
    modelsViewed: 0,
  }
  const sevenKey = dayKey(now - 7 * DAY_MS)
  const thirtyKey = dayKey(now - 30 * DAY_MS)
  const trendingCutoff = now - TRENDING_WINDOW_DAYS * DAY_MS

  const weekly = new Map()
  const firstWeek =
    Date.parse(weekStartKey(now)) - (WEEKS_SHOWN - 1) * 7 * DAY_MS
  for (let i = 0; i < WEEKS_SHOWN; i += 1) {
    weekly.set(dayKey(firstWeek + i * 7 * DAY_MS), 0)
  }
  const yearCounts = {}
  const siteCounts = {}
  const largestFiles = []
  const longestVideos = []

  for (const m of models) {
    const s = m.stats
    if (m.visits?.totalCount) {
      totals.totalViews += m.visits.totalCount
      totals.modelsViewed += 1
    }
    if (!s) continue

    totals.files += s.fileCount || 0
    totals.bytes += s.totalBytes || 0
    totals.typeBytes.image += s.bytesImages || 0
    totals.typeBytes.gif += s.bytesGifs || 0
    totals.typeBytes.video += s.bytesVideos || 0
    for (const type of Object.keys(totals.typeCounts)) {
      totals.typeCounts[type] += s.typeCounts?.[type] || 0
    }
    totals.videoSeconds += s.videoSeconds || 0
    totals.flagged += s.flaggedCount || 0
    totals.captioned += s.captionCount || 0

    m.added7d = sumDaysSince(s.addedByDay, sevenKey)
    m.added30d = sumDaysSince(s.addedByDay, thirtyKey)
    totals.addedLast7d += m.added7d
    totals.addedLast30d += m.added30d

    for (const [day, count] of Object.entries(s.addedByDay || {})) {
      const week = weekStartKey(Date.parse(day))
      if (weekly.has(week)) weekly.set(week, weekly.get(week) + count)
    }
    for (const [year, count] of Object.entries(s.yearCounts || {})) {
      yearCounts[year] = (yearCounts[year] || 0) + count
    }
    for (const [site, count] of Object.entries(s.siteCounts || {})) {
      siteCounts[site] = (siteCounts[site] || 0) + count
    }
    for (const file of s.largestFiles || [])
      largestFiles.push({ model: m.name, ...file })
    for (const file of s.longestVideos || [])
      longestVideos.push({ model: m.name, ...file })

    if (m.visits?.lastVisitedAt) {
      const lastVisitMs = Date.parse(m.visits.lastVisitedAt)
      m.newSinceVisit =
        (s.latestAddedMs || 0) > lastVisitMs
          ? sumDaysSince(s.addedByDay, dayKey(lastVisitMs))
          : 0
    }
    m.recentViews = (m.visits?.recent || []).filter(
      (ts) => Date.parse(ts) >= trendingCutoff
    ).length
  }

  const row = (m, value, extra = {}) => ({ name: m.name, value, ...extra })
  const withFiles = scanned.filter((m) => (m.stats.fileCount || 0) > 0)

  const rankings = {
    mostViewed: topBy(models, (m) => m.visits?.totalCount || 0).map((m) =>
      row(m, m.visits.totalCount, { lastVisitedAt: m.visits.lastVisitedAt })
    ),
    trending: topBy(models, (m) => m.recentViews || 0).map((m) =>
      row(m, m.recentViews)
    ),
    mostActive: topBy(scanned, (m) => m.added30d || 0).map((m) =>
      row(m, m.added30d, { added7d: m.added7d })
    ),
    newSinceLastVisit: topBy(scanned, (m) => m.newSinceVisit || 0).map((m) =>
      row(m, m.newSinceVisit, { lastVisitedAt: m.visits.lastVisitedAt })
    ),
    mostFiles: topBy(scanned, (m) => m.stats.fileCount || 0).map((m) =>
      row(m, m.stats.fileCount)
    ),
    largestSize: topBy(scanned, (m) => m.stats.totalBytes || 0).map((m) =>
      row(m, m.stats.totalBytes)
    ),
    quietest: withFiles
      .filter((m) => m.stats.latestAddedMs > 0)
      .sort((a, b) => a.stats.latestAddedMs - b.stats.latestAddedMs)
      .slice(0, RANKING_SIZE)
      .map((m) => row(m, m.stats.latestAddedMs)),
    neverOpened: withFiles
      .filter((m) => !m.visits?.totalCount)
      .sort(
        (a, b) =>
          b.stats.fileCount - a.stats.fileCount || a.name.localeCompare(b.name)
      )
      .map((m) => row(m, m.stats.fileCount)),
  }

  return {
    generatedAt: new Date(now).toISOString(),
    scan: {
      ...scan,
      modelsWithStats: scanned.length,
    },
    totals,
    weeklyAdded: [...weekly.entries()].map(([weekStart, count]) => ({
      weekStart,
      count,
    })),
    postsPerYear: fillYears(yearCounts),
    sources: Object.entries(siteCounts)
      .map(([site, count]) => ({
        site,
        label: SITE_LABELS[site] || site,
        count,
      }))
      .sort((a, b) => b.count - a.count),
    rankings,
    largestFiles: largestFiles
      .sort((a, b) => b.size - a.size)
      .slice(0, TOP_FILE_COUNT),
    longestVideos: longestVideos
      .sort((a, b) => b.duration - a.duration)
      .slice(0, TOP_FILE_COUNT),
  }
}

module.exports = { buildStatsPayload, computeStatsFromResponse }
