/* ============================================================
   RediSearch — Premium Frontend
   Animated search, autocomplete, crawl pipeline, dashboard
   ============================================================ */

const API = '';

// ---- State ----
let currentQuery = '';
let currentSubreddit = '';
let currentCursor = null;
let acDebounce = null;
let acIndex = -1;
let acItems = [];
let searchDebounce = null;

// ---- Placeholder cycling ----
const placeholders = [
    'Search r/python for decorators...',
    'Find machine learning posts...',
    'Try "async await" in r/learnpython...',
    'Search "web scraping" tutorials...',
    'Look up "FastAPI" discussions...',
    'Explore "neural networks" research...',
];
let placeholderIdx = 0;
let placeholderInterval = null;

function cyclePlaceholder() {
    const input = document.getElementById('search-input');
    if (document.activeElement === input || input.value) return;
    placeholderIdx = (placeholderIdx + 1) % placeholders.length;
    input.style.transition = 'opacity 0.2s';
    input.style.opacity = '0';
    setTimeout(() => {
        input.placeholder = placeholders[placeholderIdx];
        input.style.opacity = '1';
    }, 200);
}

// ---- Health check ----
async function checkHealth() {
    const el = document.getElementById('health-indicator');
    try {
        const res = await fetch(`${API}/health`);
        if (res.ok) {
            el.className = 'health-indicator online';
            el.querySelector('.health-label').textContent = 'Online';
        } else {
            el.className = 'health-indicator offline';
            el.querySelector('.health-label').textContent = 'Error';
        }
    } catch {
        el.className = 'health-indicator offline';
        el.querySelector('.health-label').textContent = 'Offline';
    }
}

// ---- Page navigation ----
function showPage(name) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    document.getElementById(`page-${name}`).classList.add('active');
    document.querySelector(`[data-page="${name}"]`).classList.add('active');

    if (name === 'stats') loadStats();
    if (name === 'search') loadSubredditOptions();
}

// ---- Navbar scroll shadow ----
window.addEventListener('scroll', () => {
    document.querySelector('.navbar').classList.toggle('scrolled', window.scrollY > 8);
}, { passive: true });

// ============================================================
// SEARCH
// ============================================================

const searchInput = document.getElementById('search-input');
const searchBox = document.getElementById('search-box');
const acDropdown = document.getElementById('autocomplete-dropdown');
const searchHero = document.getElementById('search-hero');
const searchSubmitBtn = document.getElementById('search-submit');

searchInput.addEventListener('input', (e) => {
    const q = e.target.value.trim();
    if (q.length >= 1) {
        fetchAutocomplete(q);
        clearTimeout(searchDebounce);
        searchDebounce = setTimeout(() => {
            if (q.length >= 2) doSearch(false);
        }, 400);
    } else {
        hideAutocomplete();
    }
});

searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') {
        e.preventDefault();
        navigateAc(1);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        navigateAc(-1);
    } else if (e.key === 'Enter') {
        e.preventDefault();
        if (acIndex >= 0 && acItems[acIndex]) {
            searchInput.value = acItems[acIndex];
            hideAutocomplete();
        }
        doSearch();
    } else if (e.key === 'Escape') {
        hideAutocomplete();
    }
});

document.addEventListener('click', (e) => {
    if (!searchBox.contains(e.target)) hideAutocomplete();
});

function fetchAutocomplete(q) {
    clearTimeout(acDebounce);
    acDebounce = setTimeout(async () => {
        try {
            const sub = document.getElementById('subreddit-filter').value;
            let url = `${API}/autocomplete?q=${encodeURIComponent(q)}&top_k=8`;
            if (sub) url += `&subreddit=${encodeURIComponent(sub)}`;
            const res = await fetch(url);
            if (!res.ok) return;
            const data = await res.json();
            showAutocomplete(data.suggestions);
        } catch {
            // silently ignore
        }
    }, 150);
}

function showAutocomplete(suggestions) {
    if (!suggestions || suggestions.length === 0) {
        hideAutocomplete();
        return;
    }
    acItems = suggestions.map(s => s.term);
    acIndex = -1;
    acDropdown.innerHTML = suggestions.map((s, i) => `
        <div class="ac-item" data-index="${i}" onclick="selectAc(${i})">
            <svg class="ac-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="11" cy="11" r="8"></circle>
                <path d="m21 21-4.35-4.35"></path>
            </svg>
            <span class="ac-text">${escapeHtml(s.term)}</span>
            <span class="ac-score">${s.score.toFixed(0)}</span>
        </div>
    `).join('');
    acDropdown.classList.add('visible');
}

function hideAutocomplete() {
    acDropdown.classList.remove('visible');
    acIndex = -1;
    acItems = [];
}

function navigateAc(dir) {
    const items = acDropdown.querySelectorAll('.ac-item');
    if (items.length === 0) return;
    items.forEach(i => i.classList.remove('selected'));
    acIndex = Math.max(-1, Math.min(items.length - 1, acIndex + dir));
    if (acIndex >= 0) {
        items[acIndex].classList.add('selected');
        searchInput.value = acItems[acIndex];
    }
}

function selectAc(index) {
    searchInput.value = acItems[index];
    hideAutocomplete();
    doSearch();
}

async function doSearch(resetCursor = true) {
    const q = searchInput.value.trim();
    if (!q) return;

    if (resetCursor) currentCursor = null;
    currentQuery = q;
    currentSubreddit = document.getElementById('subreddit-filter').value;
    hideAutocomplete();
    searchHero.classList.add('collapsed');

    // Button press animation
    searchSubmitBtn.classList.add('searching');
    setTimeout(() => searchSubmitBtn.classList.remove('searching'), 400);

    const resultsList = document.getElementById('results-list');
    const resultsHeader = document.getElementById('results-header');
    const pagination = document.getElementById('results-pagination');

    if (resetCursor) {
        resultsList.innerHTML = `
            <div class="loading-state">
                <div class="loading-text">Indexing the chaos<span class="loading-dots"><span></span><span></span><span></span></span></div>
            </div>
        `;
        resultsHeader.innerHTML = '';
        pagination.innerHTML = '';
    }

    try {
        let url = `${API}/search?q=${encodeURIComponent(q)}&page_size=15`;
        if (currentSubreddit) url += `&subreddit=${encodeURIComponent(currentSubreddit)}`;
        if (currentCursor) url += `&cursor=${encodeURIComponent(currentCursor)}`;

        const res = await fetch(url);
        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: 'Search failed' }));
            resultsHeader.innerHTML = `<span style="color:var(--red)">Error: ${escapeHtml(err.detail)}</span>`;
            if (resetCursor) resultsList.innerHTML = '';
            return;
        }

        const data = await res.json();
        if (resetCursor) resultsList.innerHTML = '';

        if (data.hits.length === 0 && resetCursor) {
            const crawlSub = currentSubreddit || '';
            // Show mascot shrug
            const mascot = document.getElementById('mascot-container');
            if (mascot) mascot.classList.add('shrug');

            resultsList.innerHTML = `
                <div class="no-results">
                    <h3>No results found</h3>
                    <p>Try a different query, or crawl more posts to expand the index.</p>
                    <div class="crawl-inline" style="margin-top:20px;">
                        <div class="crawl-inline-row">
                            <span class="crawl-inline-label">Crawl</span>
                            <div class="crawl-inline-input-wrap">
                                <span class="crawl-inline-prefix">r/</span>
                                <input type="text" id="inline-crawl-sub" value="${escapeHtml(crawlSub)}" placeholder="subreddit" class="crawl-inline-input">
                            </div>
                            <select id="inline-crawl-pages" class="crawl-inline-select">
                                <option value="1">1 page</option>
                                <option value="2" selected>2 pages</option>
                                <option value="5">5 pages</option>
                                <option value="10">10 pages</option>
                            </select>
                            <button class="btn-primary" onclick="crawlAndSearch()" id="inline-crawl-btn">
                                Crawl &amp; Search
                            </button>
                        </div>
                        <div id="inline-crawl-status" class="crawl-inline-status"></div>
                    </div>
                </div>
            `;
            resultsHeader.innerHTML = `No results for "<strong>${escapeHtml(q)}</strong>"`;
        } else {
            // Remove shrug
            const mascot = document.getElementById('mascot-container');
            if (mascot) mascot.classList.remove('shrug');

            // Build header with latency
            let headerHtml = `About <strong>${data.total_hits}</strong> results for "<strong>${escapeHtml(q)}</strong>"`;
            if (data.query_time_ms != null) {
                headerHtml += `
                    <span class="latency-badge">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
                            <circle cx="12" cy="12" r="10"></circle>
                            <polyline points="12 6 12 12 16 14"></polyline>
                        </svg>
                        ${data.query_time_ms}ms
                    </span>
                `;
            }
            resultsHeader.innerHTML = headerHtml;

            data.hits.forEach((hit, i) => {
                const el = document.createElement('div');
                el.innerHTML = renderResult(hit);
                const card = el.firstElementChild;
                card.style.animationDelay = `${i * 40}ms`;
                card.classList.add('result-enter');
                resultsList.appendChild(card);
            });

            if (data.next_cursor) {
                currentCursor = data.next_cursor;
                pagination.innerHTML = `<button class="btn-load-more" onclick="doSearch(false)">Load more results</button>`;
            } else {
                pagination.innerHTML = '';
            }
        }
    } catch {
        resultsHeader.innerHTML = `<span style="color:var(--red)">Network error &mdash; is the server running?</span>`;
        if (resetCursor) resultsList.innerHTML = '';
    }
}

function renderResult(hit) {
    const permalink = hit.permalink.startsWith('http')
        ? hit.permalink
        : `https://old.reddit.com${hit.permalink}`;
    const scoreWidth = Math.min(100, Math.round(hit.score * 100));

    return `
        <div class="result-card" onclick="window.open('${escapeHtml(permalink)}', '_blank')">
            <div class="result-subreddit">
                <span>r/${escapeHtml(hit.subreddit)}</span>
            </div>
            <div class="result-title">${escapeHtml(hit.title)}</div>
            <div class="result-meta">
                <span class="score-badge">
                    BM25 ${hit.score.toFixed(3)}
                    <span class="score-bar"><span class="score-bar-fill" style="width:${scoreWidth}%"></span></span>
                </span>
                <span>&#128100; ${escapeHtml(hit.author || 'unknown')}</span>
                <span>&#11014; ${hit.post_score}</span>
                <span>&#128172; ${hit.comment_count}</span>
            </div>
        </div>
    `;
}

// Add enter animation CSS dynamically
(function() {
    const style = document.createElement('style');
    style.textContent = `
        .result-enter {
            opacity: 0;
            transform: translateY(8px);
            animation: resultSlideIn 0.35s cubic-bezier(0.22,1,0.36,1) forwards;
        }
        @keyframes resultSlideIn {
            to { opacity: 1; transform: translateY(0); }
        }
    `;
    document.head.appendChild(style);
})();

// ============================================================
// CRAWL & SEARCH (inline from search page)
// ============================================================

let inlinePollInterval = null;

async function crawlAndSearch() {
    const subInput = document.getElementById('inline-crawl-sub');
    const sub = subInput.value.trim().toLowerCase();
    const pages = parseInt(document.getElementById('inline-crawl-pages').value) || 2;
    const statusEl = document.getElementById('inline-crawl-status');
    const btn = document.getElementById('inline-crawl-btn');

    if (!sub) {
        statusEl.innerHTML = '<span style="color:var(--red)">Please enter a subreddit name</span>';
        return;
    }

    btn.disabled = true;
    btn.textContent = 'Crawling...';
    statusEl.innerHTML = '<span class="spinner-small"></span> Crawling r/' + escapeHtml(sub) + '...';

    try {
        const res = await fetch(`${API}/api/pipeline/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ subreddit: sub, max_pages: pages }),
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: 'Failed to start' }));
            statusEl.innerHTML = `<span style="color:var(--red)">Error: ${escapeHtml(err.detail)}</span>`;
            btn.disabled = false;
            btn.textContent = 'Crawl & Search';
            return;
        }

        inlinePollInterval = setInterval(async () => {
            try {
                const sr = await fetch(`${API}/api/pipeline/status/${encodeURIComponent(sub)}`);
                const sd = await sr.json();
                const stage = sd.stage || 'crawl';
                const stageNames = { crawl: 'Crawling', preprocess: 'Preprocessing', index: 'Building index', autocomplete: 'Building autocomplete', done: 'Done' };

                if (sd.status === 'running') {
                    const posts = sd.crawl_result ? ` (${sd.crawl_result.posts_inserted || 0} posts)` : '';
                    statusEl.innerHTML = `<span class="spinner-small"></span> ${stageNames[stage] || stage}${posts}...`;
                } else if (sd.status === 'completed') {
                    clearInterval(inlinePollInterval);
                    inlinePollInterval = null;
                    const inserted = sd.crawl_result ? sd.crawl_result.posts_inserted : 0;
                    statusEl.innerHTML = `<span style="color:var(--green);">\u2713 Crawled ${inserted} posts in ${sd.elapsed_seconds}s — searching now...</span>`;
                    btn.disabled = false;
                    btn.textContent = 'Crawl & Search';
                    await loadSubredditOptions();
                    document.getElementById('subreddit-filter').value = sub;
                    currentSubreddit = sub;
                    setTimeout(() => doSearch(), 300);
                } else if (sd.status === 'failed') {
                    clearInterval(inlinePollInterval);
                    inlinePollInterval = null;
                    statusEl.innerHTML = `<span style="color:var(--red);">\u2717 Failed: ${escapeHtml(sd.detail || 'Unknown error')}</span>`;
                    btn.disabled = false;
                    btn.textContent = 'Crawl & Search';
                }
            } catch { /* keep polling */ }
        }, 800);

    } catch {
        statusEl.innerHTML = '<span style="color:var(--red)">Network error</span>';
        btn.disabled = false;
        btn.textContent = 'Crawl & Search';
    }
}

// ============================================================
// CRAWL PIPELINE
// ============================================================

let pollInterval = null;

async function startCrawl() {
    const sub = document.getElementById('crawl-subreddit').value.trim().toLowerCase();
    const pages = parseInt(document.getElementById('crawl-pages').value) || 2;

    if (!sub) {
        alert('Please enter a subreddit name');
        return;
    }

    const btn = document.getElementById('crawl-btn');
    btn.disabled = true;
    btn.textContent = 'Starting...';

    const progress = document.getElementById('pipeline-progress');
    progress.style.display = 'block';
    resetPipelineUI();

    try {
        const res = await fetch(`${API}/api/pipeline/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ subreddit: sub, max_pages: pages }),
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: 'Failed to start' }));
            showPipelineError(err.detail);
            btn.disabled = false;
            btn.textContent = 'Start Pipeline';
            return;
        }

        pollPipelineStatus(sub);
    } catch {
        showPipelineError('Network error — is the server running?');
        btn.disabled = false;
        btn.textContent = 'Start Pipeline';
    }
}

function pollPipelineStatus(subreddit) {
    if (pollInterval) clearInterval(pollInterval);
    pollInterval = setInterval(async () => {
        try {
            const res = await fetch(`${API}/api/pipeline/status/${encodeURIComponent(subreddit)}`);
            const data = await res.json();
            updatePipelineUI(data);

            if (data.status === 'completed' || data.status === 'failed') {
                clearInterval(pollInterval);
                pollInterval = null;
                const btn = document.getElementById('crawl-btn');
                btn.disabled = false;
                btn.innerHTML = `
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="btn-icon" width="18" height="18">
                        <path d="M5 12h14"></path><path d="m12 5 7 7-7 7"></path>
                    </svg>
                    Start Pipeline
                `;
                loadSubredditOptions();
            }
        } catch {
            // keep polling
        }
    }, 800);
}

function resetPipelineUI() {
    const stages = ['crawl', 'preprocess', 'index', 'autocomplete'];
    stages.forEach(s => {
        document.getElementById(`stage-${s}`).className = 'stage';
        document.getElementById(`stage-${s}-detail`).textContent = 'Waiting...';
    });
    const result = document.getElementById('pipeline-result');
    result.className = 'progress-result';
    result.textContent = '';
}

function updatePipelineUI(data) {
    const stageOrder = ['crawl', 'preprocess', 'index', 'autocomplete'];
    const currentStage = data.stage || 'crawl';
    const currentIdx = stageOrder.indexOf(currentStage);

    stageOrder.forEach((s, i) => {
        const el = document.getElementById(`stage-${s}`);
        if (data.status === 'failed' && s === currentStage) {
            el.className = 'stage error';
        } else if (i < currentIdx || (data.status === 'completed' && currentStage === 'done')) {
            el.className = 'stage done';
        } else if (i === currentIdx && data.status === 'running') {
            el.className = 'stage active';
        } else {
            el.className = 'stage';
        }
    });

    if (data.crawl_result) {
        document.getElementById('stage-crawl-detail').textContent =
            `${data.crawl_result.pages_crawled || 0} pages, ${data.crawl_result.posts_inserted || 0} posts inserted`;
    }
    if (data.preprocess_result) {
        document.getElementById('stage-preprocess-detail').textContent =
            `${data.preprocess_result.processed || 0} posts processed`;
    }
    if (data.index_result) {
        document.getElementById('stage-index-detail').textContent =
            `${data.index_result.doc_count || 0} docs indexed (v${data.index_result.version || '?'})`;
    }

    if (data.status === 'completed') {
        document.getElementById('stage-autocomplete-detail').textContent = 'Complete';
        const result = document.getElementById('pipeline-result');
        result.className = 'progress-result visible';
        result.textContent = `\u2713 Pipeline completed in ${data.elapsed_seconds || '?'}s — you can now search r/${data.subreddit}`;
    } else if (data.status === 'failed') {
        const result = document.getElementById('pipeline-result');
        result.className = 'progress-result visible error';
        result.textContent = `\u2717 Pipeline failed: ${data.detail || 'Unknown error'}`;
    }
}

function showPipelineError(msg) {
    const result = document.getElementById('pipeline-result');
    result.className = 'progress-result visible error';
    result.textContent = `\u2717 ${msg}`;
    document.getElementById('pipeline-progress').style.display = 'block';
}

// ============================================================
// DASHBOARD — animated counters
// ============================================================

function animateCounter(el, target) {
    const start = parseInt(el.textContent) || 0;
    if (start === target) return;
    const duration = 800;
    const startTime = performance.now();

    function tick(now) {
        const elapsed = now - startTime;
        const progress = Math.min(elapsed / duration, 1);
        // ease-out cubic
        const eased = 1 - Math.pow(1 - progress, 3);
        el.textContent = Math.round(start + (target - start) * eased);
        if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
}

async function loadStats() {
    try {
        const res = await fetch(`${API}/stats`);
        const data = await res.json();

        animateCounter(document.getElementById('stat-posts'), data.raw_post_count);
        animateCounter(document.getElementById('stat-processed'), data.processed_post_count);
        animateCounter(document.getElementById('stat-indexes'), data.active_indexes);
        animateCounter(document.getElementById('stat-subreddits'), data.subreddits.length);

        const chips = document.getElementById('subreddit-chips');
        if (data.subreddits.length === 0) {
            chips.innerHTML = '<span style="color:var(--text-muted)">No subreddits crawled yet. Go to the Crawl page to add some!</span>';
        } else {
            chips.innerHTML = data.subreddits.map(s => `
                <div class="subreddit-chip" onclick="searchSubreddit('${escapeHtml(s)}')">
                    <span class="chip-prefix">r/</span>${escapeHtml(s)}
                </div>
            `).join('');
        }
    } catch {
        document.getElementById('stat-posts').textContent = '?';
    }
}

function searchSubreddit(sub) {
    showPage('search');
    document.getElementById('subreddit-filter').value = sub;
    searchInput.value = '';
    searchInput.focus();
}

// ============================================================
// SUBREDDIT OPTIONS
// ============================================================

async function loadSubredditOptions() {
    try {
        const res = await fetch(`${API}/stats`);
        const data = await res.json();
        const select = document.getElementById('subreddit-filter');
        const current = select.value;
        select.innerHTML = '<option value="">All subreddits</option>';
        data.subreddits.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s;
            opt.textContent = `r/${s}`;
            select.appendChild(opt);
        });
        select.value = current;
    } catch {
        // ignore
    }
}

// ============================================================
// UTILS
// ============================================================

function escapeHtml(text) {
    if (!text) return '';
    const d = document.createElement('div');
    d.textContent = text;
    return d.innerHTML;
}

// ---- Init ----
loadSubredditOptions();
checkHealth();
setInterval(checkHealth, 15000);
placeholderInterval = setInterval(cyclePlaceholder, 4000);
