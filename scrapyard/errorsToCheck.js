'use strict'

const fs = require('fs')
const path = require('path')
const os = require('os')

const slopvaultRoot = path.resolve(
  process.env.SLOPVAULT_ROOT ||
    path.join(
      process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
      '.slopvault'
    )
)

const statePath = path.join(slopvaultRoot, 'errors-to-check-state.json')
const latestJsonPath = path.join(slopvaultRoot, 'errors-to-check-latest.json')
const latestMdPath = path.join(slopvaultRoot, 'errors-to-check-latest.md')

function upsertErrorsSource(sourceKey, section) {
  ensureDir(slopvaultRoot)
  const state = loadState()
  state.sources[sourceKey] = {
    source: sourceKey,
    generatedAt: new Date().toISOString(),
    ...(section || {}),
  }
  writeState(state)
  writeLatestOutputs(state)
}

function loadState() {
  if (!fs.existsSync(statePath)) {
    return { version: 1, updatedAt: null, sources: {} }
  }

  try {
    const raw = fs.readFileSync(statePath, 'utf8').trim()
    const parsed = raw ? JSON.parse(raw) : {}
    return {
      version: 1,
      updatedAt: parsed?.updatedAt || null,
      sources:
        parsed?.sources && typeof parsed.sources === 'object'
          ? parsed.sources
          : {},
    }
  } catch {
    return { version: 1, updatedAt: null, sources: {} }
  }
}

function writeState(state) {
  state.updatedAt = new Date().toISOString()
  fs.writeFileSync(statePath, JSON.stringify(state, null, 2))
}

function writeLatestOutputs(state) {
  const sections = Object.values(state.sources || {}).sort((a, b) =>
    String(a.source || '').localeCompare(String(b.source || ''))
  )

  const payload = {
    version: 1,
    updatedAt: state.updatedAt,
    slopvaultRoot,
    sections,
  }

  fs.writeFileSync(latestJsonPath, JSON.stringify(payload, null, 2))
  fs.writeFileSync(latestMdPath, renderMarkdown(payload))
}

function renderMarkdown(payload) {
  const lines = [
    '# Errors To Check',
    '',
    `Updated: ${payload.updatedAt || 'unknown'}`,
    '',
  ]

  const sections = Array.isArray(payload.sections) ? payload.sections : []
  if (!sections.length) {
    lines.push('No current error action items.', '')
    return `${lines.join('\n')}\n`
  }

  for (const section of sections) {
    lines.push(`## ${section.title || section.source || 'Unknown Source'}`, '')
    if (section.summary) {
      lines.push(section.summary, '')
    }
    if (section.commandHint) {
      lines.push(`Command hint: \`${section.commandHint}\``, '')
    }

    const items = Array.isArray(section.items) ? section.items : []
    if (!items.length) {
      lines.push('No action items.', '')
      continue
    }

    for (const item of items) {
      const parts = [
        item.model ? `model=\`${item.model}\`` : null,
        item.status ? `status=\`${item.status}\`` : null,
        item.count != null ? `count=\`${item.count}\`` : null,
        item.details ? item.details : null,
      ].filter(Boolean)
      lines.push(`- ${parts.join(' :: ')}`)
    }
    lines.push('')
  }

  return `${lines.join('\n')}\n`
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

module.exports = {
  upsertErrorsSource,
  latestJsonPath,
  latestMdPath,
  statePath,
  slopvaultRoot,
}
