function extractMediaUrls(site = '', pageContext = document) {
  const urls = []

  if (site === 'stufferdb') {
    // Grab video directly
    const video = pageContext.querySelector('video.vjs-tech[src]')
    if (video?.src) urls.push(video.src)

    // Get the *true server filename* from download anchor
    const anchor = pageContext.querySelector('a[href*="/upload/"]')
    if (anchor?.href) {
      const url = anchor.href.startsWith('http')
        ? anchor.href
        : `https:${anchor.href}`
      urls.push(url)
    }

    // Fallback to preview image only if nothing else was found
    if (urls.length === 0) {
      const img = pageContext.querySelector('#theMainImage')
      if (img?.src) urls.push(img.src)
    }
  } else if (site === 'coomer') {
    const elements = pageContext.querySelectorAll(
      'a.fileThumb.image-link, video source, a.post__attachment-link[href]'
    )

    elements.forEach((el) => {
      const raw =
        el.href || el.src || el.getAttribute('src') || el.getAttribute('href')
      if (!raw) return

      const url = raw.startsWith('http') ? raw : `https:${raw}`

      if (
        url.includes('/data/') || // full-res images
        url.endsWith('.mp4') ||
        url.endsWith('.m4v') ||
        (!url.includes('/thumbnail/') &&
          !url.includes('/icons/') &&
          !url.includes('/static/') &&
          !url.includes('/user/') &&
          !url.endsWith('.svg'))
      ) {
        urls.push(url)
      }
    })
  }

  return urls
}

module.exports = { extractMediaUrls }
