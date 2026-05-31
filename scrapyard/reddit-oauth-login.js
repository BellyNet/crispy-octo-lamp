const { spawn } = require('child_process')
const readline = require('readline')

const redditOAuth = require('./redditOAuth')

function parseArgs(argv = process.argv.slice(2)) {
  const out = {}
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    if (!arg.startsWith('--')) continue
    const [rawKey, inlineValue] = arg.slice(2).split('=')
    const key = rawKey.trim()
    if (inlineValue !== undefined) {
      out[key] = inlineValue
    } else if (argv[index + 1] && !argv[index + 1].startsWith('--')) {
      out[key] = argv[index + 1]
      index += 1
    } else {
      out[key] = true
    }
  }
  return out
}

function ask(question) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  })
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close()
      resolve(answer.trim())
    })
  })
}

function openBrowser(url) {
  if (process.platform === 'win32') {
    spawn('cmd', ['/c', 'start', '', url], {
      detached: true,
      stdio: 'ignore',
    }).unref()
    return
  }
  const opener = process.platform === 'darwin' ? 'open' : 'xdg-open'
  spawn(opener, [url], { detached: true, stdio: 'ignore' }).unref()
}

async function main() {
  const args = parseArgs()
  const clientId =
    args['client-id'] ||
    process.env.REDDIT_CLIENT_ID ||
    (await ask('Reddit app client ID: '))
  const clientSecret =
    args['client-secret'] || process.env.REDDIT_CLIENT_SECRET || ''
  const redirectUri =
    args['redirect-uri'] ||
    process.env.REDDIT_REDIRECT_URI ||
    redditOAuth.DEFAULT_REDIRECT_URI
  const userAgent =
    args['user-agent'] ||
    process.env.REDDIT_USER_AGENT ||
    redditOAuth.DEFAULT_USER_AGENT
  const scopes = String(args.scope || args.scopes || '')
    .split(/[,\s]+/)
    .map((scope) => scope.trim())
    .filter(Boolean)
  const requestedScopes = scopes.length ? scopes : redditOAuth.DEFAULT_SCOPES

  if (!clientId) {
    throw new Error('A Reddit app client ID is required.')
  }

  const { url, state } = redditOAuth.buildAuthorizationUrl({
    clientId,
    redirectUri,
    scopes: requestedScopes,
  })
  const codePromise = redditOAuth.waitForAuthorizationCode({
    redirectUri,
    state,
  })

  console.log(`Listening for Reddit OAuth callback at ${redirectUri}`)
  console.log('Opening Reddit authorization page...')
  console.log(url)
  openBrowser(url)

  const code = await codePromise
  const token = await redditOAuth.exchangeAuthorizationCode({
    clientId,
    clientSecret,
    code,
    redirectUri,
    userAgent,
  })

  if (!token.refresh_token) {
    throw new Error(
      'Reddit did not return a refresh token. Make sure the authorization URL used duration=permanent.'
    )
  }

  const configPath = redditOAuth.saveRedditOAuthConfig({
    clientId,
    clientSecret,
    refreshToken: token.refresh_token,
    userAgent,
    redirectUri,
    scopes: requestedScopes,
    createdAt: new Date().toISOString(),
  })
  console.log(`Saved Reddit OAuth refresh token to ${configPath}`)
}

main().catch((err) => {
  console.error(err.stack || err.message)
  process.exit(1)
})
