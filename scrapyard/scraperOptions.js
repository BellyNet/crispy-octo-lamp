'use strict'

const minimist = require('minimist')

const { sanitize } = require('./modelRegistry')

const MILKMAID_STRING_OPTIONS = ['model']
const MILKMAID_BOOLEAN_OPTIONS = [
  'review-errors',
  'skip-nas-sync',
  'keep-history',
]

const HOGHAUL_STRING_OPTIONS = [
  'pages',
  'model',
  'max-posts',
  'max-files',
  'cookie',
  'cookie-file',
  'browser-executable',
  'browser-profile',
  'browser-connect',
  'browser-validate-ms',
  'post-concurrency',
  'image-concurrency',
  'video-concurrency',
]

const HOGHAUL_BOOLEAN_OPTIONS = [
  'dry-run',
  'preflight',
  'skip-nas-sync',
  'track-source',
  'keep-history',
  'download-oversized',
  'browser-media',
  'browser-headless',
  'headless',
  'download-oversized',
]

const RUNNER_STRING_OPTIONS = [
  'only-models',
  'models',
  'start-from',
  'limit',
  'delay-ms',
  'source',
  'host-contains',
  'registry',
  'log-dir',
  'dataset-root',
  'nas-dataset-root',
  'cleanup-mp4',
  'cleanup-gif-mp4',
]

const RUNNER_BOOLEAN_OPTIONS = [
  'no-model-infer',
  'stop-on-error',
  'with-repair',
  'scrape',
  'push',
  'pull',
  'help',
]

const STRING_OPTIONS = Array.from(
  new Set([
    ...MILKMAID_STRING_OPTIONS,
    ...HOGHAUL_STRING_OPTIONS,
    ...RUNNER_STRING_OPTIONS,
  ])
)

const BOOLEAN_OPTIONS = Array.from(
  new Set([
    ...MILKMAID_BOOLEAN_OPTIONS,
    ...HOGHAUL_BOOLEAN_OPTIONS,
    ...RUNNER_BOOLEAN_OPTIONS,
  ])
)

function isTruthy(value) {
  if (value === true) return true
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
  return ['1', 'true', 'yes'].includes(normalized)
}

function parseRunnerArgs(argvInput = process.argv.slice(2)) {
  if (Array.isArray(argvInput)) {
    return minimist(argvInput, {
      string: STRING_OPTIONS,
      boolean: BOOLEAN_OPTIONS,
      alias: {
        m: 'model',
        h: 'help',
      },
    })
  }

  return {
    _: [],
    ...(argvInput || {}),
  }
}

function parseMilkmaidArgs(argvInput = process.argv.slice(2)) {
  const args = minimist(argvInput, {
    string: MILKMAID_STRING_OPTIONS,
    boolean: MILKMAID_BOOLEAN_OPTIONS,
    alias: {
      m: 'model',
    },
  })

  return normalizeMilkmaidRunOptions(args)
}

function parseHoghaulArgs(argvInput = process.argv.slice(2)) {
  return minimist(argvInput, {
    string: HOGHAUL_STRING_OPTIONS,
    boolean: HOGHAUL_BOOLEAN_OPTIONS,
    alias: {
      model: 'm',
    },
    default: {
      'browser-media': true,
    },
  })
}

function getOption(argv, name) {
  if (argv?.[name] !== undefined) return argv[name]
  const envName = `npm_config_${String(name).replace(/-/g, '_')}`
  return process.env[envName]
}

function getRunOption(argv, camelName, dashName, envName) {
  if (argv?.[camelName] !== undefined) return argv[camelName]
  if (dashName && argv?.[dashName] !== undefined) return argv[dashName]
  if (envName) return process.env[envName]
  return undefined
}

function normalizeMilkmaidRunOptions(input = process.argv.slice(2)) {
  const argv = Array.isArray(input)
    ? parseRunnerArgs(input)
    : {
        _: [],
        ...(input || {}),
      }

  return {
    inputUrl: argv.inputUrl || argv.url || argv._?.[0] || '',
    modelOverride: sanitize(argv.modelOverride || argv.model || ''),
    reviewErrors: Boolean(argv.reviewErrors || argv['review-errors']),
    skipNasSync: Boolean(argv.skipNasSync || argv['skip-nas-sync']),
    keepHistory: Boolean(argv.keepHistory || argv['keep-history']),
  }
}

function getRequestTimeoutMs(fallback = 30000) {
  return (
    Number.parseInt(process.env.HOGHAUL_REQUEST_TIMEOUT_MS || '', 10) ||
    fallback
  )
}

function normalizeHoghaulRunOptions(input = process.argv.slice(2), opts = {}) {
  const argv = Array.isArray(input)
    ? parseHoghaulArgs(input)
    : {
        _: [],
        ...(input || {}),
      }
  const existingBrowserOptions = argv.browserOptions || {}
  const inputUrl =
    argv._.find((arg) => /^https?:\/\//i.test(arg)) ||
    argv.inputUrl ||
    argv.url ||
    ''
  const existingUseBrowserMedia = getRunOption(argv, 'useBrowserMedia')
  const browserMedia = getRunOption(argv, 'browserMedia', 'browser-media')
  const browserHeadless =
    Boolean(argv.headless) ||
    Boolean(getRunOption(argv, 'browserHeadless', 'browser-headless')) ||
    isTruthy(process.env.npm_config_headless) ||
    isTruthy(process.env.HOGHAUL_BROWSER_HEADLESS) ||
    Boolean(existingBrowserOptions.headless)

  return {
    inputUrl,
    model: getRunOption(argv, 'model', 'model', 'npm_config_model'),
    dryRun:
      Boolean(getRunOption(argv, 'dryRun', 'dry-run')) ||
      isTruthy(process.env.npm_config_dry_run),
    preflight:
      Boolean(argv.preflight) || isTruthy(process.env.npm_config_preflight),
    skipNasSync:
      Boolean(getRunOption(argv, 'skipNasSync', 'skip-nas-sync')) ||
      isTruthy(process.env.npm_config_skip_nas_sync),
    trackSource:
      Boolean(getRunOption(argv, 'trackSource', 'track-source')) ||
      isTruthy(process.env.npm_config_track_source),
    keepHistory:
      Boolean(getRunOption(argv, 'keepHistory', 'keep-history')) ||
      isTruthy(process.env.npm_config_keep_history),
    downloadOversized:
      Boolean(getRunOption(argv, 'downloadOversized', 'download-oversized')) ||
      isTruthy(process.env.npm_config_download_oversized) ||
      isTruthy(process.env.HOGHAUL_DOWNLOAD_OVERSIZED),
    useBrowserMedia:
      existingUseBrowserMedia !== undefined
        ? Boolean(existingUseBrowserMedia)
        : browserMedia !== false &&
          !isTruthy(process.env.npm_config_no_browser_media),
    browserOptions: {
      browserExecutable:
        getRunOption(argv, 'browserExecutable', 'browser-executable') ||
        process.env.npm_config_browser_executable ||
        process.env.HOGHAUL_BROWSER_EXECUTABLE ||
        existingBrowserOptions.browserExecutable,
      browserProfile:
        getRunOption(argv, 'browserProfile', 'browser-profile') ||
        process.env.npm_config_browser_profile ||
        process.env.HOGHAUL_BROWSER_PROFILE ||
        existingBrowserOptions.browserProfile,
      browserConnect:
        getRunOption(argv, 'browserConnect', 'browser-connect') ||
        process.env.npm_config_browser_connect ||
        process.env.HOGHAUL_BROWSER_CONNECT ||
        existingBrowserOptions.browserConnect,
      cookieHeader:
        getRunOption(argv, 'cookie', 'cookie') ||
        process.env.npm_config_cookie ||
        process.env.HOGHAUL_COOKIE ||
        existingBrowserOptions.cookieHeader,
      cookieFile:
        getRunOption(argv, 'cookieFile', 'cookie-file') ||
        process.env.npm_config_cookie_file ||
        process.env.HOGHAUL_COOKIE_FILE ||
        existingBrowserOptions.cookieFile,
      headless: browserHeadless,
      timeoutMs:
        opts.requestTimeoutMs ||
        existingBrowserOptions.timeoutMs ||
        getRequestTimeoutMs(),
      validateMs:
        Number.parseInt(
          getRunOption(argv, 'browserValidateMs', 'browser-validate-ms') ||
            process.env.npm_config_browser_validate_ms ||
            process.env.HOGHAUL_BROWSER_VALIDATE_MS ||
            existingBrowserOptions.validateMs ||
            '0',
          10
        ) || 0,
    },
    pages: getRunOption(argv, 'pages', 'pages', 'npm_config_pages'),
    maxPosts: getRunOption(
      argv,
      'maxPosts',
      'max-posts',
      'npm_config_max_posts'
    ),
    maxFiles: getRunOption(
      argv,
      'maxFiles',
      'max-files',
      'npm_config_max_files'
    ),
    postConcurrency: getRunOption(
      argv,
      'postConcurrency',
      'post-concurrency',
      'npm_config_post_concurrency'
    ),
    imageConcurrency: getRunOption(
      argv,
      'imageConcurrency',
      'image-concurrency',
      'npm_config_image_concurrency'
    ),
    videoConcurrency: getRunOption(
      argv,
      'videoConcurrency',
      'video-concurrency',
      'npm_config_video_concurrency'
    ),
  }
}

module.exports = {
  BOOLEAN_OPTIONS,
  HOGHAUL_BOOLEAN_OPTIONS,
  HOGHAUL_STRING_OPTIONS,
  MILKMAID_BOOLEAN_OPTIONS,
  MILKMAID_STRING_OPTIONS,
  RUNNER_BOOLEAN_OPTIONS,
  RUNNER_STRING_OPTIONS,
  STRING_OPTIONS,
  getOption,
  getRunOption,
  isTruthy,
  normalizeHoghaulRunOptions,
  normalizeMilkmaidRunOptions,
  parseHoghaulArgs,
  parseMilkmaidArgs,
  parseRunnerArgs,
}
