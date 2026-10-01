'use strict'

// Packed in-memory index for "which stored hex hashes are within N bits of
// this one" queries. Replaces a per-entry loop that parsed every hash string
// on every lookup (~1.4 s per image against ~120k stored image hashes);
// scanning packed Uint32 words is ~1000x faster with identical results.
//
// Distances match visualHasher.getVisualHashDistance(): case-insensitive,
// only plain hex hashes of equal length are comparable, and multi-frame
// video hashes (containing '|') never match.

const HEX_PATTERN = /^[0-9a-f]+$/
const INITIAL_CAPACITY = 1024

function normalizeHash(hash) {
  const value = String(hash || '')
    .trim()
    .toLowerCase()
  return value && HEX_PATTERN.test(value) ? value : null
}

function popcount32(value) {
  let v = value - ((value >>> 1) & 0x55555555)
  v = (v & 0x33333333) + ((v >>> 2) & 0x33333333)
  return (((v + (v >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24
}

// Splits a hex string into 32-bit words. A shorter final chunk is parsed as
// is; hashes are only ever compared against hashes of the same length, so
// both sides split identically.
function writeWords(target, offset, hex, wordCount) {
  for (let word = 0; word < wordCount; word += 1) {
    target[offset + word] =
      Number.parseInt(hex.slice(word * 8, word * 8 + 8), 16) >>> 0
  }
}

function createGroup(hexLength) {
  return {
    wordCount: Math.ceil(hexLength / 8),
    hashes: [],
    words: new Uint32Array(Math.ceil(hexLength / 8) * INITIAL_CAPACITY),
  }
}

function createHammingIndex() {
  // One packed table per hex length, since only equal lengths compare.
  const groups = new Map()

  function add(hash) {
    const normalized = normalizeHash(hash)
    if (!normalized) return false

    let group = groups.get(normalized.length)
    if (!group) {
      group = createGroup(normalized.length)
      groups.set(normalized.length, group)
    }

    const index = group.hashes.length
    const needed = (index + 1) * group.wordCount
    if (needed > group.words.length) {
      const grown = new Uint32Array(group.words.length * 2)
      grown.set(group.words)
      group.words = grown
    }
    writeWords(
      group.words,
      index * group.wordCount,
      normalized,
      group.wordCount
    )
    group.hashes.push(String(hash))
    return true
  }

  // Returns [{ hash, distance }] for every indexed hash within maxDistance
  // bits, with `hash` exactly as it was added.
  function findWithin(hash, maxDistance) {
    const normalized = normalizeHash(hash)
    if (!normalized || !Number.isFinite(maxDistance) || maxDistance < 0) {
      return []
    }
    const group = groups.get(normalized.length)
    if (!group) return []

    const { wordCount, words, hashes } = group
    const query = new Uint32Array(wordCount)
    writeWords(query, 0, normalized, wordCount)

    const matches = []
    for (let index = 0; index < hashes.length; index += 1) {
      const base = index * wordCount
      let distance = 0
      for (
        let word = 0;
        word < wordCount && distance <= maxDistance;
        word += 1
      ) {
        distance += popcount32(words[base + word] ^ query[word])
      }
      if (distance <= maxDistance) {
        matches.push({ hash: hashes[index], distance })
      }
    }
    return matches
  }

  function size() {
    let total = 0
    for (const group of groups.values()) total += group.hashes.length
    return total
  }

  return { add, findWithin, size }
}

module.exports = { createHammingIndex }
