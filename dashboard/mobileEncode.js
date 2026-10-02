'use strict'

// Helpers for the mobile-variant encodes in server.js.
//
// runFfmpeg: execFile buffers all of stderr and kills ffmpeg once it passes
// maxBuffer (1 MB). A damaged video makes ffmpeg log a decode error for
// every bad frame, so those encodes were killed partway with "stderr
// maxBuffer length exceeded" even though ffmpeg would have finished. This
// keeps only the tail of stderr, for the error message.
//
// Failure notes: a file ffmpeg can't encode used to be retried on every
// prewarm pass, forever. A failed encode leaves <dst>.failed recording the
// source's size and mtime; the file is skipped until the source changes.

const fs = require('fs')
const { spawn } = require('child_process')

const STDERR_TAIL_BYTES = 8 * 1024

function runFfmpeg(ffmpegPath, args, { timeoutMs } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(ffmpegPath, args, {
      stdio: ['ignore', 'ignore', 'pipe'],
    })
    let tail = ''
    let timedOut = false
    child.stderr.setEncoding('utf8')
    child.stderr.on('data', (chunk) => {
      tail = (tail + chunk).slice(-STDERR_TAIL_BYTES)
    })
    const timer = timeoutMs
      ? setTimeout(() => {
          timedOut = true
          child.kill('SIGKILL')
        }, timeoutMs)
      : null
    child.on('error', (err) => {
      if (timer) clearTimeout(timer)
      reject(err)
    })
    child.on('close', (code, signal) => {
      if (timer) clearTimeout(timer)
      if (code === 0) return resolve()
      const lastLine = tail.trim().split(/\r?\n/).pop() || ''
      const err = new Error(
        timedOut
          ? `timed out after ${Math.round(timeoutMs / 1000)}s`
          : `ffmpeg exited ${signal || code}${lastLine ? `: ${lastLine}` : ''}`
      )
      err.timedOut = timedOut
      reject(err)
    })
  })
}

const failurePath = (dstPath) => `${dstPath}.failed`

function sourceSignature(srcPath) {
  try {
    const stat = fs.statSync(srcPath)
    return { size: stat.size, mtimeMs: Math.round(stat.mtimeMs) }
  } catch {
    return null
  }
}

// True when an earlier encode of this exact source failed.
function failedBefore(srcPath, dstPath) {
  let note
  try {
    note = JSON.parse(fs.readFileSync(failurePath(dstPath), 'utf8'))
  } catch {
    return false
  }
  const current = sourceSignature(srcPath)
  return Boolean(
    current && note.size === current.size && note.mtimeMs === current.mtimeMs
  )
}

function recordFailure(srcPath, dstPath, message) {
  const signature = sourceSignature(srcPath)
  if (!signature) return
  try {
    fs.writeFileSync(
      failurePath(dstPath),
      JSON.stringify({
        ...signature,
        error: message,
        at: new Date().toISOString(),
      })
    )
  } catch {}
}

function clearFailure(dstPath) {
  fs.promises.unlink(failurePath(dstPath)).catch(() => {})
}

module.exports = { runFfmpeg, failedBefore, recordFailure, clearFailure }
