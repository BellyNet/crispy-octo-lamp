const { exec } = require('child_process')

const baseLocal = `$env:APPDATA\\.slopvault\\dataset`
const baseNAS = `Z:\\dataset`

const isPush = process.env.npm_config_push
const isPull = process.env.npm_config_pull
const modelName = process.env.npm_config_model

let cmd = ''

if (isPush) {
  cmd = `robocopy "${baseLocal}" "${baseNAS}" /E /XC /XN /XO`
} else if (isPull) {
  cmd = `robocopy "${baseNAS}" "${baseLocal}" /MIR`
} else if (modelName) {
  cmd = `robocopy "${baseLocal}\\${modelName}" "${baseNAS}\\${modelName}" /E /XC /XN /XO`
} else {
  console.error(
    '❌ Missing flag.\nUsage:\n  npm run sync --all\n  npm run sync --pull\n  npm run sync --model=<name>'
  )
  process.exit(1)
}

console.log(`🚀 Running: ${cmd}`)
exec(`powershell -Command "${cmd}"`, (err, stdout, stderr) => {
  console.log(stdout)
  if (err && err.code > 3) {
    console.error('❌ Sync failed:', stderr || err.message)
    process.exit(1)
  } else {
    console.log('✅ Sync complete!')
  }
})
