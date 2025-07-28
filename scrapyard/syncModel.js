const { exec } = require('child_process');

const modelName = process.argv[2];
if (!modelName) {
  console.error('❌ Please provide a model name.\nUsage: npm run sync:model fattyprincess');
  process.exit(1);
}

const source = `$env:APPDATA\\.slopvault\\dataset\\${modelName}`;
const dest = `Z:\\dataset\\${modelName}`;
const cmd = `robocopy "${source}" "${dest}" /E /XC /XN /XO`;

console.log(`🚀 Syncing model: ${modelName}`);
exec(`powershell -Command "${cmd}"`, (err, stdout, stderr) => {
  console.log(stdout);
  if (err) {
    console.error('❌ Sync failed:', stderr || err.message);
  } else {
    console.log('✅ Sync complete!');
  }
});
