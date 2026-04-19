const fs = require('fs')
const path = require('path')
const https = require('https')
const readline = require('readline')
const { exec, execFile } = require('child_process')
const puppeteer = require('puppeteer')

const registryPath = path.join(__dirname, '..', 'model_aliases.json')

function sanitize(name) {
  return String(name || '')
    .replace(/[^a-z0-9_\-]/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase()
}

function normalizeCategoryUrl(url) {
  if (!url) return null

  const trimmed = String(url).trim()

  const match = trimmed.match(/category\/?(\d+)/i)
  if (!match) return null

  const categoryId = match[1]
  return `https://stufferdb.com/index?/category/${categoryId}`
}

function loadRegistry() {
  if (!fs.existsSync(registryPath)) {
    throw new Error(`Could not find registry at: ${registryPath}`)
  }

  const raw = fs.readFileSync(registryPath, 'utf8').trim()
  if (!raw) return {}

  const parsed = JSON.parse(raw)
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('model_aliases.json is not a valid grouped registry object')
  }

  return parsed
}

function saveRegistry(registry) {
  fs.writeFileSync(registryPath, JSON.stringify(registry, null, 2))
}

function ensureModelEntryShape(entry, canonicalName) {
  const aliasList = Array.isArray(entry?.aliases)
    ? entry.aliases.filter(Boolean)
    : []
  const ordered = []

  for (const alias of aliasList) {
    if (!ordered.includes(alias)) ordered.push(alias)
  }

  if (canonicalName && !ordered.includes(canonicalName)) {
    ordered.push(canonicalName)
  }

  return {
    aliases: ordered,
    sources: {
      stufferdb: Array.isArray(entry?.sources?.stufferdb)
        ? entry.sources.stufferdb
        : [],
    },
  }
}

function hasStufferSource(entry) {
  return (
    Array.isArray(entry?.sources?.stufferdb) &&
    entry.sources.stufferdb.length > 0
  )
}

function getAliasSearchOrder(entry, canonicalName) {
  const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []
  const ordered = []

  for (const alias of aliases) {
    const trimmed = String(alias || '').trim()
    if (trimmed && !ordered.includes(trimmed)) {
      ordered.push(trimmed)
    }
  }

  if (canonicalName && !ordered.includes(canonicalName)) {
    ordered.push(canonicalName)
  }

  return ordered
}

async function findFirstCandidateForAlias(alias) {
  const searchVariants = Array.from(
    new Set([alias, alias.replace(/_/g, ' '), alias.replace(/-/g, ' ')])
  )

  for (const variant of searchVariants) {
    const queries = [
      `site:stufferdb.com/index "${variant}" category`,
      `site:stufferdb.com "${variant}" category`,
      `site:stufferdb.com "${variant}"`,
    ]

    for (const query of queries) {
      try {
        const resultUrl = await findFirstDuckDuckGoResultUrl(query)
        const normalized = normalizeCategoryUrl(resultUrl)

        if (normalized) {
          return {
            url: normalized,
            matchedBy: variant,
            alias,
            queryUsed: query,
          }
        }
      } catch (err) {
        console.log(`   ⚠️ Search failed for "${query}": ${err.message}`)
      }
    }
  }

  return null
}

function addStufferSource(registry, canonicalName, sourceUrl, discoveredAs) {
  const normalizedUrl = normalizeCategoryUrl(sourceUrl)
  if (!normalizedUrl) {
    throw new Error(`Invalid StufferDB category URL: ${sourceUrl}`)
  }

  const categoryId = normalizedUrl.match(/category\/(\d+)/i)?.[1] || null

  registry[canonicalName] = ensureModelEntryShape(
    registry[canonicalName],
    canonicalName
  )

  const entry = registry[canonicalName]
  if (!Array.isArray(entry.sources.stufferdb)) {
    entry.sources.stufferdb = []
  }

  const existingIndex = entry.sources.stufferdb.findIndex(
    (src) =>
      src?.url === normalizedUrl ||
      (categoryId && src?.categoryId === categoryId)
  )

  if (existingIndex === -1) {
    entry.sources.stufferdb.push({
      url: normalizedUrl,
      categoryId,
      discoveredAs: discoveredAs || canonicalName,
    })
  } else {
    const existing = entry.sources.stufferdb[existingIndex]

    entry.sources.stufferdb[existingIndex] = {
      ...existing,
      url: normalizedUrl,
      categoryId,
      discoveredAs: discoveredAs || existing.discoveredAs || canonicalName,
    }

    // Intentionally do NOT update lastCheckedAt during backfill.
    // That field should only change after a real scrape/update run.
  }

  saveRegistry(registry)
}

function httpGet(url, headers = {}, redirectCount = 0) {
  return new Promise((resolve, reject) => {
    https
      .get(
        url,
        {
          headers: {
            'User-Agent':
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            Accept:
              'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            ...headers,
          },
        },
        (res) => {
          const statusCode = res.statusCode || 0
          const location = res.headers.location

          // Follow redirects manually
          if ([301, 302, 303, 307, 308].includes(statusCode) && location) {
            res.resume()

            if (redirectCount >= 5) {
              return reject(new Error(`Too many redirects for ${url}`))
            }

            const nextUrl = location.startsWith('http')
              ? location
              : new URL(location, url).toString()

            return resolve(httpGet(nextUrl, headers, redirectCount + 1))
          }

          const chunks = []

          res.on('data', (chunk) => chunks.push(chunk))
          res.on('end', () => {
            const body = Buffer.concat(chunks).toString('utf8')
            resolve({
              statusCode,
              headers: res.headers,
              body,
              finalUrl: url,
            })
          })
        }
      )
      .on('error', reject)
  })
}

function decodeHtmlEntities(str) {
  return String(str || '')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
}

function extractCategoryCandidatesFromHtml(html) {
  const found = new Map()

  const hrefRegex =
    /https?:\/\/(?:www\.)?stufferdb\.com\/index(?:\.php)?\?\/category\/\d+(?:[^\s"'<>)]*)?/gi

  for (const match of html.matchAll(hrefRegex)) {
    const rawUrl = decodeHtmlEntities(match[0])
    const normalized = normalizeCategoryUrl(rawUrl)
    if (!normalized) continue

    if (!found.has(normalized)) {
      found.set(normalized, {
        url: normalized,
        source: 'search-result',
      })
    }
  }

  return Array.from(found.values())
}

async function searchDuckDuckGo(query) {
  const searchUrls = [
    `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}`,
    `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`,
  ]

  for (const searchUrl of searchUrls) {
    try {
      const res = await httpGet(searchUrl, {
        Referer: 'https://duckduckgo.com/',
      })

      if (res.statusCode >= 200 && res.statusCode < 300) {
        const candidates = extractCategoryCandidatesFromHtml(res.body)
        if (candidates.length) return candidates
      }
    } catch (err) {
      // try next endpoint
    }
  }

  return []
}

async function findFirstDuckDuckGoResultUrl(query) {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  })

  try {
    const page = await browser.newPage()
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    )

    const searchUrls = [
      `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`,
      `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}`,
    ]

    for (const searchUrl of searchUrls) {
      try {
        await page.goto(searchUrl, {
          waitUntil: 'domcontentloaded',
          timeout: 30000,
        })

        await page
          .waitForSelector('a[href]', { timeout: 10000 })
          .catch(() => {})

        const firstMatch = await page.evaluate(() => {
          function extractRealUrl(rawHref) {
            if (!rawHref) return null

            try {
              const absolute = new URL(rawHref, window.location.href)

              // Direct result
              if (
                /stufferdb\.com/i.test(absolute.href) &&
                /category\/\d+/i.test(absolute.href)
              ) {
                return absolute.href
              }

              // DuckDuckGo redirect result: /l/?uddg=<encoded real url>
              const uddg = absolute.searchParams.get('uddg')
              if (uddg) {
                const decoded = decodeURIComponent(uddg)
                if (
                  /stufferdb\.com/i.test(decoded) &&
                  /category\/\d+/i.test(decoded)
                ) {
                  return decoded
                }
              }
            } catch (err) {
              return null
            }

            return null
          }

          const anchors = [...document.querySelectorAll('a[href]')]

          for (const a of anchors) {
            const rawHref = a.getAttribute('href') || a.href || ''
            const realUrl = extractRealUrl(rawHref)
            if (realUrl) return realUrl
          }

          return null
        })

        if (firstMatch) {
          return firstMatch
        }
      } catch (err) {
        // try next endpoint
      }
    }

    return null
  } finally {
    await browser.close()
  }
}

function getAliasSearchVariants(entry, canonicalName) {
  const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []

  return Array.from(
    new Set(
      [canonicalName, ...aliases]
        .map((name) => String(name || '').trim())
        .filter(Boolean)
    )
  )
}

function hasSavedSourceForAlias(entry, alias) {
  const normalizedAlias = sanitize(alias)
  const sources = Array.isArray(entry?.sources?.stufferdb)
    ? entry.sources.stufferdb
    : []

  return sources.some((src) => sanitize(src?.discoveredAs) === normalizedAlias)
}

async function buildCandidatesForModel(canonicalName, entry) {
  const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []
  const searchTerms = Array.from(
    new Set(
      [canonicalName, ...aliases]
        .map((name) => String(name || '').trim())
        .filter(Boolean)
    )
  )

  const allCandidates = new Map()

  for (const term of searchTerms) {
    const searchVariants = Array.from(
      new Set([term, term.replace(/_/g, ' '), term.replace(/-/g, ' ')])
    )

    for (const variant of searchVariants) {
      const query = `site:stufferdb.com/index "${variant}" category`
      try {
        const results = await searchDuckDuckGo(query)

        for (const candidate of results) {
          if (!allCandidates.has(candidate.url)) {
            allCandidates.set(candidate.url, {
              ...candidate,
              matchedBy: variant,
            })
          }
        }
      } catch (err) {
        console.log(`   ⚠️ Search failed for "${variant}": ${err.message}`)
      }
    }
  }

  return Array.from(allCandidates.values())
}

function expandWindowsEnvVars(input) {
  return input.replace(/%([^%]+)%/g, (_, key) => process.env[key] || `%${key}%`)
}

const YANDEX_BROWSER_CANDIDATES = [
  process.env.YANDEX_BROWSER_PATH,
  'C:\\Users\\jagsr\\AppData\\Local\\Yandex\\YandexBrowser\\Application\\browser.exe',
  'C:\\Program Files\\Yandex\\YandexBrowser\\Application\\browser.exe',
].filter(Boolean)

function getYandexBrowserPath() {
  for (const candidate of YANDEX_BROWSER_CANDIDATES) {
    const expanded = expandWindowsEnvVars(candidate)
    if (fs.existsSync(expanded)) {
      return expanded
    }
  }

  // Fallback to first candidate even if it doesn't exist,
  // so the error output still shows something useful.
  return expandWindowsEnvVars(YANDEX_BROWSER_CANDIDATES[0])
}

function openInYandex(url) {
  const browserPath = getYandexBrowserPath()

  execFile(browserPath, [url], (err) => {
    if (err) {
      console.log(`⚠️ Could not open Yandex Browser:`)
      console.log(`   ${browserPath}`)
      console.log(`   Error: ${err.message}`)
      console.log(`   URL: ${url}`)
    }
  })
}

function createPrompt() {
  return readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  })
}

function ask(rl, question) {
  return new Promise((resolve) => rl.question(question, resolve))
}

async function run() {
  const registry = loadRegistry()

  const modelsToProcess = Object.entries(registry)
    .map(([canonicalName, entry]) => [
      canonicalName,
      ensureModelEntryShape(entry, canonicalName),
    ])
    .filter(([, entry]) => !hasStufferSource(entry))

  console.log(
    `\nFound ${modelsToProcess.length} model(s) with no StufferDB source.\n`
  )

  if (!modelsToProcess.length) {
    console.log('Nothing to do.')
    return
  }

  const rl = createPrompt()

  try {
    for (
      let modelIndex = 0;
      modelIndex < modelsToProcess.length;
      modelIndex++
    ) {
      const [canonicalName, entry] = modelsToProcess[modelIndex]

      console.log('='.repeat(80))
      console.log(
        `[${modelIndex + 1}/${modelsToProcess.length}] ${canonicalName}`
      )
      console.log(`Aliases: ${entry.aliases.join(', ')}`)

      const aliasTerms = getAliasSearchVariants(entry, canonicalName)

      for (const alias of aliasTerms) {
        const freshEntry = ensureModelEntryShape(
          registry[canonicalName],
          canonicalName
        )

        if (hasSavedSourceForAlias(freshEntry, alias)) {
          console.log(`⏭️ Alias already has a saved source: ${alias}`)
          continue
        }

        console.log(`\n🔎 Searching alias: ${alias}`)

        let current = null

        try {
          current = await findFirstCandidateForAlias(alias)
        } catch (err) {
          console.log(
            `⚠️ Could not find first candidate for ${alias}: ${err.message}`
          )
        }

        if (current) {
          console.log(`🌐 Opening first candidate in Yandex...`)
          console.log(`   ${current.url}`)
          console.log(`   matched by: ${current.matchedBy}`)
          console.log(`   alias: ${alias}`)
          openInYandex(current.url)
        } else {
          console.log(
            `No candidate links found automatically for alias: ${alias}`
          )
          openInYandex(
            `https://duckduckgo.com/?q=${encodeURIComponent(`site:stufferdb.com "${alias}" `)}`
          )
        }

        while (true) {
          console.log('\nCommands:')
          console.log('  y = accept opened URL')
          console.log('  o = reopen current URL in Yandex')
          console.log('  c = paste correct StufferDB category URL manually')
          console.log('  s = skip this alias')
          console.log('  q = save and quit\n')

          console.log(`Current alias: ${alias}`)

          if (current) {
            console.log(`Matched URL: ${current.url}`)
            console.log(`Matched by: ${current.matchedBy}`)
          }

          const answer = (await ask(rl, '> ')).trim().toLowerCase()
          if (!answer) continue

          if (answer === 'y') {
            if (!current) {
              console.log('No current candidate to accept.')
              continue
            }

            addStufferSource(registry, canonicalName, current.url, alias)
            console.log(
              `✅ Saved source for ${canonicalName} via alias ${alias}: ${current.url}`
            )
            break
          }

          if (answer === 'o') {
            if (!current) {
              console.log('No current candidate to open.')
              continue
            }

            openInYandex(current.url)
            continue
          }

          if (answer === 'c') {
            const customUrl = (
              await ask(rl, 'Paste the correct StufferDB category URL: ')
            ).trim()

            const normalized = normalizeCategoryUrl(customUrl)
            if (!normalized) {
              console.log(
                'That does not look like a valid StufferDB category URL.'
              )
              continue
            }

            current = {
              url: normalized,
              matchedBy: alias,
              alias,
            }

            openInYandex(current.url)

            const confirm = (await ask(rl, 'Save this URL now? (y/n): '))
              .trim()
              .toLowerCase()

            if (confirm === 'y') {
              addStufferSource(registry, canonicalName, current.url, alias)
              console.log(
                `✅ Saved manual source for ${canonicalName} via alias ${alias}: ${current.url}`
              )
              break
            }

            continue
          }

          if (answer === 's') {
            console.log(`⏭️ Skipped alias ${alias}`)
            break
          }

          if (answer === 'q') {
            console.log('💾 Quitting. Progress already saved as you went.')
            rl.close()
            return
          }

          console.log('Unknown command.')
        }
      }
    }

    console.log('\n🎉 Done. All unsourced models have been reviewed.')
  } finally {
    rl.close()
  }
}

run().catch((err) => {
  console.error(`\n❌ Fatal error: ${err.message}`)
  process.exit(1)
})
