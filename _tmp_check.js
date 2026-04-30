
      'use strict'

      // ─── STATE ────────────────────────────────────────────────────────────
      let allMedia = [] // full list for current user
      let filtered = [] // filtered + sorted list
      let activeType = 'all'
      let activeSort = 'asc'
      let lbIndex = 0 // lightbox current index into filtered[]
      let homeVisible = false

      // ─── DOM REFS ─────────────────────────────────────────────────────────
      const userListEl = document.getElementById('user-list')
      const userSearchEl = document.getElementById('user-search')
      const userCountEl = document.getElementById('user-count')
      const currentNameEl = document.getElementById('current-user-name')
      const mediaStatsEl = document.getElementById('media-stats')
      const filterBtns = document.querySelectorAll('.filter-btn')
      const filterSelect = document.getElementById('filter-select')
      const sortBtns = document.querySelectorAll('.sort-btn')
      const sortBar = document.getElementById('sort-bar')
      const dateRangeEl = document.getElementById('date-range')
      const gridEl = document.getElementById('media-grid')
      const emptyState = document.getElementById('empty-state')
      const loadingEl = document.getElementById('loading-indicator')
      const noResultsEl = document.getElementById('no-results')
      const homeViewEl = document.getElementById('home-view')
      const homeGridEl = document.getElementById('home-grid')
      const smpNameEl = document.getElementById('smp-name')
      const smpStatsEl = document.getElementById('smp-stats')
      const smpModelPanel = document.getElementById('sidebar-model-panel')
      const smpFilterBtns = document.querySelectorAll('.smp-filter-btn')
      const homeSizeBtns = document.querySelectorAll('.home-size-btn')
      const lightbox = document.getElementById('lightbox')
      const lbMediaWrap = document.getElementById('lb-media-wrap')
      const lbDate = document.getElementById('lb-date')
      const lbFilename = document.getElementById('lb-filename')
      const lbAddedDate = document.getElementById('lb-added-date')
      const lbCounter = document.getElementById('lb-counter')
      const lbTypeBadge = document.getElementById('lb-type-badge')

      // ─── HELPERS ──────────────────────────────────────────────────────────
      function formatDate(iso) {
        if (!iso) return 'Unknown date'
        const d = new Date(iso)
        return d.toLocaleString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: 'numeric',
          minute: '2-digit',
          hour12: true,
        })
      }

      function formatDateShort(iso) {
        if (!iso) return '—'
        const d = new Date(iso)
        return d.toLocaleDateString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
        })
      }

      // ─── USERS ────────────────────────────────────────────────────────────
      let usersData = [] // [{ name, sources }]

      async function loadUsers() {
        const res = await fetch('/api/users')
        usersData = await res.json()
        userCountEl.textContent = usersData.length
        renderUserList(usersData)
        showHomeView() // start on the home grid
      }

      // ─── HOME VIEW ────────────────────────────────────────────────────────
      function showHomeView() {
        homeVisible = true
        document.getElementById('home-btn').classList.add('active')
        homeViewEl.classList.add('visible')
        emptyState.style.display = 'none'
        loadingEl.style.display = 'none'
        noResultsEl.style.display = 'none'
        gridEl.style.display = 'none'
        sortBar.style.display = 'none'
        currentNameEl.textContent = 'All Models'
        smpModelPanel.classList.remove('visible')
        filterSelect.style.display = 'none'
        renderHomeGrid()
      }

      function hideHomeView() {
        homeVisible = false
        document.getElementById('home-btn').classList.remove('active')
        homeViewEl.classList.remove('visible')
        gridEl.style.display = ''
      }

      function renderHomeGrid() {
        homeGridEl.innerHTML = ''
        for (const u of usersData) {
          const card = document.createElement('div')
          card.className = 'home-card'
          card.addEventListener('click', () => selectUser(u))

          // Placeholder while loading
          const mediaEl = document.createElement('div')
          mediaEl.className = 'home-card-thumb'
          mediaEl.style.background = 'var(--card-bg)'
          card.appendChild(mediaEl)

          // Fetch random cover async, replace placeholder when ready
          fetch(`/api/users/${encodeURIComponent(u.name)}/cover`)
            .then((r) => r.ok ? r.json() : null)
            .then((cover) => {
              if (!cover) return
              let el
              if (cover.type === 'video') {
                el = document.createElement('video')
                el.src = cover.url
                el.autoplay = true
                el.muted = true
                el.loop = true
                el.playsInline = true
              } else {
                el = document.createElement('img')
                el.src = cover.url
                el.loading = 'lazy'
                el.alt = u.name
              }
              el.className = 'home-card-thumb'
              card.replaceChild(el, mediaEl)
            })
            .catch(() => {})

          const body = document.createElement('div')
          body.className = 'home-card-body'

          const name = document.createElement('div')
          name.className = 'home-card-name'
          name.textContent = u.name

          const srcs = document.createElement('div')
          srcs.className = 'home-card-sources'
          for (const [key, label] of [['coomer','OF'],['kemono','K'],['stufferdb','S']]) {
            const dot = document.createElement('span')
            dot.className = 'home-src-dot' + ((u.sources?.[key]?.length) ? ' has-source' : '')
            dot.textContent = label
            srcs.appendChild(dot)
          }

          body.appendChild(name)
          body.appendChild(srcs)
          card.appendChild(body)
          homeGridEl.appendChild(card)
        }
      }

      function renderUserList(users) {
        userListEl.innerHTML = ''
        for (const u of users) {
          const li = document.createElement('li')
          li.className = 'user-item'
          li.textContent = u.name
          li.dataset.username = u.name
          li.addEventListener('click', () => selectUser(u, li))
          userListEl.appendChild(li)
        }
      }

      userSearchEl.addEventListener('input', () => {
        const q = userSearchEl.value.toLowerCase()
        for (const li of userListEl.querySelectorAll('.user-item')) {
          li.style.display = li.dataset.username.toLowerCase().includes(q)
            ? ''
            : 'none'
        }
      })

      // ─── SOURCE LINKS ─────────────────────────────────────────────────────────────
      const sourceLinksEl = document.getElementById('source-links')

      const LINK_ICON = `<svg width="9" height="9" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 3H3a1 1 0 0 0-1 1v5a1 1 0 0 0 1 1h5a1 1 0 0 0 1-1V7"/><polyline points="8 1 11 1 11 4"/><line x1="5" y1="7" x2="11" y2="1"/></svg>`

      const SERVICE_LABELS = {
        onlyfans: 'OnlyFans',
        fansly: 'Fansly',
        patreon: 'Patreon',
        fanbox: 'Fanbox',
        candfans: 'C&F',
        subscribestar: 'SubStar',
        gumroad: 'Gumroad',
        afdian: 'Afdian',
        boosty: 'Boosty',
        discord: 'Discord',
        fantia: 'Fantia',
        dlsite: 'DLsite',
      }

      // platform → short display prefix shown before the service name
      const PLATFORM_PREFIX = {
        coomer: '',      // coomer URLs already carry the service name
        kemono: 'K·',
        stufferdb: '',
      }

      function renderSourceLinks(sources) {
        sourceLinksEl.innerHTML = ''
        if (!sources) return

        for (const url of sources.stufferdb || []) {
          const a = document.createElement('a')
          a.className = 'src-link'
          a.href = url
          a.target = '_blank'
          a.rel = 'noopener noreferrer'
          a.innerHTML = `${LINK_ICON} StufferDB`
          sourceLinksEl.appendChild(a)
        }

        for (const [platform, prefix] of [['coomer', ''], ['kemono', 'K·']]) {
          for (const url of sources[platform] || []) {
            const service = url.split('/')[3] || platform
            const label = SERVICE_LABELS[service] || service
            const a = document.createElement('a')
            a.className = 'src-link'
            a.href = url
            a.target = '_blank'
            a.rel = 'noopener noreferrer'
            a.innerHTML = `${LINK_ICON} ${prefix}${label}`
            sourceLinksEl.appendChild(a)
          }
        }
      }

      async function selectUser(userObj, liEl) {
        const username = typeof userObj === 'string' ? userObj : userObj.name
        const sources = typeof userObj === 'object' ? userObj.sources : null
        closeSidebar()
        hideHomeView()

        userListEl
          .querySelectorAll('.user-item')
          .forEach((el) => el.classList.remove('active'))
        // find the li if not passed
        const li = liEl || userListEl.querySelector(`[data-username="${CSS.escape(username)}"]`)
        if (li) li.classList.add('active')

        activeSort = 'asc'
        sortBtns.forEach((b) =>
          b.classList.toggle('active', b.dataset.sort === 'asc')
        )

        currentNameEl.textContent = username
        smpNameEl.textContent = username
        renderSourceLinks(sources)
        smpStatsEl.textContent = ''
        smpModelPanel.classList.add('visible')
        setActiveType('all')
        filterSelect.style.display = isMobile() ? 'block' : 'none'
        gridEl.innerHTML = ''
        emptyState.style.display = 'none'
        noResultsEl.style.display = 'none'
        sortBar.style.display = 'none'
        loadingEl.style.display = 'flex'

        const mediaRes = await fetch(
          `/api/users/${encodeURIComponent(username)}/media`
        ).catch(() => null)
        allMedia = mediaRes?.ok ? await mediaRes.json() : []

        loadingEl.style.display = 'none'
        sortBar.style.display = allMedia.length ? 'flex' : 'none'
        applyFilter()
        updateStats()
      }

      // ─── FILTER + SORT ────────────────────────────────────────────────────
      function applyFilter() {
        filtered =
          activeType === 'all'
            ? [...allMedia]
            : allMedia.filter((m) => m.type === activeType)

        if (activeSort === 'desc') filtered.reverse()
        else if (activeSort === 'added-asc') filtered.sort((a, b) => a.addedMs - b.addedMs)
        else if (activeSort === 'added-desc') filtered.sort((a, b) => b.addedMs - a.addedMs)

        renderGrid()
        updateDateRange()
      }

      // Keep pills + select in sync
      function setActiveType(type) {
        activeType = type
        filterBtns.forEach((b) => b.classList.toggle('active', b.dataset.type === type))
        smpFilterBtns.forEach((b) => b.classList.toggle('active', b.dataset.type === type))
        filterSelect.value = type
      }

      filterBtns.forEach((btn) => {
        btn.addEventListener('click', () => {
          setActiveType(btn.dataset.type)
          applyFilter()
        })
      })

      filterSelect.addEventListener('change', () => {
        setActiveType(filterSelect.value)
        applyFilter()
      })

      smpFilterBtns.forEach((btn) => {
        btn.addEventListener('click', () => {
          setActiveType(btn.dataset.type)
          applyFilter()
        })
      })

      sortBtns.forEach((btn) => {
        btn.addEventListener('click', () => {
          activeSort = btn.dataset.sort
          sortBtns.forEach((b) => b.classList.toggle('active', b === btn))
          applyFilter()
        })
      })

      function updateStats() {
        if (!allMedia.length) {
          if (mediaStatsEl) mediaStatsEl.textContent = 'No media found'
          smpStatsEl.textContent = 'No media found'
          return
        }
        const imgs = allMedia.filter((m) => m.type === 'image').length
        const gifs = allMedia.filter((m) => m.type === 'gif').length
        const vids = allMedia.filter((m) => m.type === 'video').length
        const parts = []
        if (imgs) parts.push(`${imgs} image${imgs !== 1 ? 's' : ''}`)
        if (gifs) parts.push(`${gifs} GIF${gifs !== 1 ? 's' : ''}`)
        if (vids) parts.push(`${vids} video${vids !== 1 ? 's' : ''}`)
        if (mediaStatsEl) mediaStatsEl.textContent = parts.join(' · ')
        smpStatsEl.textContent = parts.join(' · ')

        // Update filter button + select labels with counts
        const counts = { all: allMedia.length, image: imgs, gif: gifs, video: vids }
        const labels = { all: `All (${allMedia.length})`, image: imgs ? `Images (${imgs})` : 'Images', gif: gifs ? `GIFs (${gifs})` : 'GIFs', video: vids ? `Videos (${vids})` : 'Videos' }
        for (const [type, label] of Object.entries(labels)) {
          const btn = document.querySelector(`.filter-btn[data-type="${type}"]`)
          if (btn) btn.textContent = label
          const smpBtn = document.querySelector(`.smp-filter-btn[data-type="${type}"]`)
          if (smpBtn) smpBtn.textContent = label
          const opt = filterSelect.querySelector(`option[value="${type}"]`)
          if (opt) opt.textContent = label
        }
      }

      function updateDateRange() {
        if (!filtered.length) {
          dateRangeEl.textContent = ''
          return
        }

        if (activeSort === 'added-asc' || activeSort === 'added-desc') {
          const withAdded = filtered.filter((m) => m.addedMs)
          if (!withAdded.length) { dateRangeEl.textContent = ''; return }
          const first = withAdded[activeSort === 'added-asc' ? 0 : withAdded.length - 1]
          const last  = withAdded[activeSort === 'added-asc' ? withAdded.length - 1 : 0]
          const toISO = (ms) => new Date(ms).toISOString()
          dateRangeEl.textContent = `${formatDateShort(toISO(first.addedMs))} – ${formatDateShort(toISO(last.addedMs))}`
          return
        }

        const withDates = filtered.filter((m) => m.date)
        if (!withDates.length) {
          dateRangeEl.textContent = ''
          return
        }
        const earliest =
          withDates[activeSort === 'asc' ? 0 : withDates.length - 1]
        const latest =
          withDates[activeSort === 'asc' ? withDates.length - 1 : 0]
        dateRangeEl.textContent = `${formatDateShort(earliest.date)} – ${formatDateShort(latest.date)}`
      }

      // ─── GRID RENDER ──────────────────────────────────────────────────────
      function renderGrid() {
        gridEl.innerHTML = ''

        if (!filtered.length) {
          noResultsEl.style.display = 'block'
          return
        }
        noResultsEl.style.display = 'none'

        const frag = document.createDocumentFragment()

        filtered.forEach((item, i) => {
          const card = document.createElement('div')
          card.className = 'media-card'
          card.dataset.index = i

          const thumb = document.createElement('div')
          thumb.className = 'card-thumb'

          if (item.type === 'video') {
            // Video: real thumbnail with play icon overlay
            const img = document.createElement('img')
            img.loading = 'lazy'
            img.style.cssText =
              'width:100%;height:100%;object-fit:cover;display:block;'
            if (item.thumbnailUrl) {
              img.src = item.thumbnailUrl
              img.onerror = () => {
                // Fallback to plain play icon if thumbnail generation failed
                img.remove()
                const ph = document.createElement('div')
                ph.style.cssText =
                  'width:100%;height:100%;background:#0a0a10;display:flex;align-items:center;justify-content:center;'
                thumb.insertBefore(ph, thumb.firstChild)
              }
            }
            thumb.appendChild(img)
            const icon = document.createElement('div')
            icon.className = 'play-icon'
            icon.innerHTML =
              '<svg viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>'
            thumb.appendChild(icon)
          } else {
            // Image / GIF: lazy load
            const img = document.createElement('img')
            img.loading = 'lazy'
            img.src = item.url
            img.alt = item.filename
            img.onerror = () => {
              img.style.opacity = '0.3'
              img.alt = '⚠ load error'
            }
            thumb.appendChild(img)
          }

          const overlay = document.createElement('div')
          overlay.className = 'card-overlay'
          thumb.appendChild(overlay)

          const footer = document.createElement('div')
          footer.className = 'card-footer'

          // Left: type badge + optional source badge + media date
          const left = document.createElement('div')
          left.style.cssText = 'display:flex;align-items:center;gap:3px;min-width:0;overflow:hidden;'

          const badge = document.createElement('span')
          badge.className = `type-badge ${item.type}`
          badge.textContent = item.type === 'image' ? 'img' : item.type
          left.appendChild(badge)

          if (item.source && item.source !== 'filename') {
            const srcBadge = document.createElement('span')
            srcBadge.className = `source-badge ${item.source}`
            const labels = {
              exif: 'exif',
              mp4: 'mp4',
              uploaded: 'upload',
              filesystem: 'local*',
            }
            srcBadge.textContent = labels[item.source] || item.source
            const tips = {
              exif: 'Date from embedded EXIF metadata',
              mp4: 'Date from video container metadata (ffprobe)',
              uploaded: 'Platform upload date (set by milkmaid)',
              filesystem: '* Local download date — least reliable',
            }
            srcBadge.title = tips[item.source] || item.source
            left.appendChild(srcBadge)
          }

          const dateEl = document.createElement('span')
          dateEl.className = 'card-date'
          dateEl.textContent = item.date
            ? new Date(item.date).toLocaleDateString('en-US', {
                year: '2-digit',
                month: 'numeric',
                day: 'numeric',
              })
            : '—'
          dateEl.title =
            formatDate(item.date) + (item.source ? ` [${item.source}]` : '')
          left.appendChild(dateEl)

          // Right: date added to disk
          const addedEl = document.createElement('span')
          addedEl.className = 'card-added'
          addedEl.textContent = item.addedMs
            ? new Date(item.addedMs).toLocaleDateString('en-US', {
                year: '2-digit',
                month: 'numeric',
                day: 'numeric',
              })
            : '—'
          addedEl.title = item.addedMs
            ? `Added: ${formatDate(new Date(item.addedMs).toISOString())}`
            : 'Added date unknown'

          footer.appendChild(left)
          footer.appendChild(addedEl)
          card.appendChild(thumb)
          card.appendChild(footer)

          card.addEventListener('click', () => openLightbox(i))
          frag.appendChild(card)
        })

        gridEl.appendChild(frag)
      }

      // ─── LIGHTBOX ─────────────────────────────────────────────────────────
      function openLightbox(index) {
        lbIndex = index
        renderLightboxItem()
        lightbox.classList.remove('hidden')
        document.body.style.overflow = 'hidden'
      }

      function closeLightbox() {
        resetZoom()
        lightbox.classList.add('hidden')
        document.body.style.overflow = ''
        lbMediaWrap.innerHTML = ''
      }

      function renderLightboxItem() {
        const item = filtered[lbIndex]
        if (!item) return

        lbMediaWrap.innerHTML = ''

        if (item.type === 'video') {
          const video = document.createElement('video')
          video.src = item.url
          video.controls = true
          video.autoplay = true
          video.loop = true
          lbMediaWrap.appendChild(video)
        } else {
          const img = document.createElement('img')
          img.src = item.url
          img.alt = item.filename
          lbMediaWrap.appendChild(img)
        }

        const sourceTips = {
          filename: 'filename timestamp',
          exif: 'EXIF metadata',
          mp4: 'MP4 container (ffprobe)',
          uploaded: 'platform upload date',
          filesystem: 'local download date*',
        }
        const srcLabel = item.source
          ? ` [${sourceTips[item.source] || item.source}]`
          : ''
        lbDate.textContent = formatDate(item.date) + srcLabel
        lbFilename.textContent = item.filename
        lbAddedDate.textContent = item.addedMs
          ? `Added ${formatDate(new Date(item.addedMs).toISOString())}`
          : ''
        lbCounter.textContent = `${lbIndex + 1} / ${filtered.length}`

        lbTypeBadge.className = `type-badge ${item.type}`
        lbTypeBadge.textContent = item.type === 'image' ? 'img' : item.type
      }

      // ─── LIGHTBOX ZOOM ────────────────────────────────────────────────────
      let zoomActive = false
      const ZOOM_SCALE = 2.5

      function getLbImage() {
        return lbMediaWrap.querySelector('img')
      }

      function setZoom(on) {
        const img = getLbImage()
        if (!img) return
        zoomActive = on
        img.style.transform = on ? `scale(${ZOOM_SCALE})` : 'scale(1)'
      }

      function resetZoom() {
        setZoom(false)
      }

      function lbPrev() {
        if (!filtered.length) return
        resetZoom()
        lbIndex = (lbIndex - 1 + filtered.length) % filtered.length
        renderLightboxItem()
      }

      function lbNext() {
        if (!filtered.length) return
        resetZoom()
        lbIndex = (lbIndex + 1) % filtered.length
        renderLightboxItem()
      }

      document
        .getElementById('lb-close')
        .addEventListener('click', closeLightbox)
      document.getElementById('lb-prev').addEventListener('click', lbPrev)
      document.getElementById('lb-next').addEventListener('click', lbNext)

      // Desktop: tap backdrop to close
      lightbox.addEventListener('click', (e) => {
        if (Date.now() - pinchEndMs < 400) return // suppress click after pinch
        if (window.matchMedia('(pointer: coarse)').matches) return // mobile: use swipe-down
        if (e.target === lightbox) closeLightbox()
      })

      document.addEventListener('keydown', (e) => {
        if (lightbox.classList.contains('hidden')) return
        if (e.key === 'Escape') closeLightbox()
        else if (e.key === 'ArrowLeft') lbPrev()
        else if (e.key === 'ArrowRight') lbNext()
      })

      // ─── CARD SIZE ────────────────────────────────────────────────────────
      const sizeBtns = document.querySelectorAll('.size-btn')
      const CARD_SIZES   = { small: '160px', medium: '300px', large: '500px' }
      const MOBILE_COLS  = { small: 4, medium: 2, large: 1 }
      const isMobile = () => window.matchMedia('(max-width: 640px)').matches

      sizeBtns.forEach((btn) => {
        btn.addEventListener('click', () => {
          sizeBtns.forEach((b) => b.classList.toggle('active', b === btn))
          if (isMobile()) {
            document.documentElement.style.setProperty(
              '--mobile-cols', MOBILE_COLS[btn.dataset.size]
            )
          } else {
            document.documentElement.style.setProperty(
              '--card-min', CARD_SIZES[btn.dataset.size]
            )
          }
        })
      })

      // ─── HOME GRID SIZE ───────────────────────────────────────────────────
      const HOME_CARD_SIZES = { small: '140px', medium: '200px', large: '280px' }
      const HOME_MOBILE_COLS = { small: 3, medium: 2, large: 1 }

      homeSizeBtns.forEach((btn) => {
        btn.addEventListener('click', () => {
          homeSizeBtns.forEach((b) => b.classList.toggle('active', b === btn))
          if (isMobile()) {
            document.documentElement.style.setProperty('--mobile-cols', HOME_MOBILE_COLS[btn.dataset.homeSize])
          } else {
            document.documentElement.style.setProperty('--home-card-min', HOME_CARD_SIZES[btn.dataset.homeSize])
          }
        })
      })

      // ─── HOME BUTTON ──────────────────────────────────────────────────────
      document.getElementById('home-btn').addEventListener('click', () => {
        if (homeVisible) return
        showHomeView()
        userListEl.querySelectorAll('.user-item').forEach((el) => el.classList.remove('active'))
      })

      // ─── MOBILE SIDEBAR ───────────────────────────────────────────────────
      const hamburger = document.getElementById('hamburger')
      const sidebarEl = document.getElementById('sidebar')
      const sidebarOverlay = document.getElementById('sidebar-overlay')

      function openSidebar() {
        sidebarEl.classList.add('open')
        sidebarOverlay.classList.add('visible')
        document.body.style.overflow = 'hidden'
      }

      function closeSidebar() {
        sidebarEl.classList.remove('open')
        sidebarOverlay.classList.remove('visible')
        document.body.style.overflow = ''
      }

      hamburger.addEventListener('click', () => {
        if (isMobile()) {
          sidebarEl.classList.contains('open') ? closeSidebar() : openSidebar()
        } else {
          document.body.classList.toggle('sidebar-collapsed')
        }
      })
      sidebarOverlay.addEventListener('click', closeSidebar)

      // ─── TOUCH GESTURES (LIGHTBOX) ────────────────────────────────────────
      // Handles: swipe left/right (navigate), swipe down (close),
      //          double-tap (zoom toggle), pinch detection (suppress close).
      let touchStartX = 0
      let touchStartY = 0
      let isPinching  = false
      let pinchEndMs  = 0
      let lastTapMs   = 0

      lightbox.addEventListener('touchstart', (e) => {
        if (e.touches.length >= 2) {
          isPinching = true
          return
        }
        // Only record start for single-touch (ignore mid-pinch single releases)
        if (!isPinching) {
          touchStartX = e.touches[0].clientX
          touchStartY = e.touches[0].clientY
        }
      }, { passive: true })

      lightbox.addEventListener('touchend', (e) => {
        // All fingers lifted after a pinch — mark cooldown and bail
        if (isPinching) {
          if (e.touches.length === 0) {
            isPinching = false
            pinchEndMs = Date.now()
          }
          return
        }

        const touch = e.changedTouches[0]
        const dx = touch.clientX - touchStartX
        const dy = touch.clientY - touchStartY
        const absDx = Math.abs(dx)
        const absDy = Math.abs(dy)

        // Swipe down → close (prioritise over horizontal nav)
        if (absDy > absDx && dy > 72) {
          closeLightbox()
          return
        }

        // Swipe left/right → navigate (only when not zoomed in)
        if (!zoomActive && absDx > absDy && absDx > 40) {
          if (dx < 0) lbNext()
          else lbPrev()
          return
        }

        // Small movement = tap — check for double-tap zoom (images only)
        if (absDx < 12 && absDy < 12 && getLbImage()) {
          const now = Date.now()
          if (now - lastTapMs < 300) {
            setZoom(!zoomActive)
            lastTapMs = 0
          } else {
            lastTapMs = now
          }
        }
      }, { passive: true })

      // ─── INIT ─────────────────────────────────────────────────────────────
      loadUsers()

      // Version / last-updated footer
      fetch('/api/info').then(r => r.json()).then(info => {
        const el = document.getElementById('sidebar-footer')
        const fmtShort = (iso) => iso ? new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true }) : '—'
        el.innerHTML = `<span>Registry: ${fmtShort(info.registryUpdatedAt)}</span><span>Server up: ${fmtShort(info.startedAt)}</span>`
      }).catch(() => {})
    