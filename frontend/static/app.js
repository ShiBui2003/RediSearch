/* ============================================================
   RediSearch — Frontend JavaScript
   Live search with autocomplete, crawl pipeline, dashboard
   ============================================================ */

const API = '';  // same origin

// ---- State ----
let currentQuery = '';
let currentSubreddit = '';
let currentCursor = null;
let acDebounce = null;
let acIndex = -1;
let acItems = [];
let searchDebounce = null;

// ---- Page navigation ----
function showPage(name) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    document.getElementById(`page-${name}`).classList.add('active');
    document.querySelector(`[data-page="${name}"]`).classList.add('active');

    if (name === 'stats') loadStats();
    if (name === 'search') loadSubredditOptions();
}

// ============================================================
// SEARCH
// ============================================================

const searchInput = document.getElementById('search-input');
const searchBox = document.getElementById('search-box');
const acDropdown = document.getElementById('autocomplete-dropdown');
const searchHero = document.getElementById('search-hero');

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
        } catch (e) {
            // silently ignore autocomplete errors
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

    const resultsList = document.getElementById('results-list');
    const resultsHeader = document.getElementById('results-header');
    const pagination = document.getElementById('results-pagination');

    if (resetCursor) {
        resultsList.innerHTML = '<div class="loading-spinner"><div class="spinner"></div></div>';
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

        if (resetCursor) {
            resultsList.innerHTML = '';
        }

        if (data.hits.length === 0 && resetCursor) {
            resultsList.innerHTML = `
                <div class="no-results">
                    <div class="no-results-icon">&#128269;</div>
                    <h3>No results found</h3>
                    <p>Try a different query or crawl a subreddit first</p>
                </div>
            `;
            resultsHeader.innerHTML = `No results for "<strong>${escapeHtml(q)}</strong>"`;
        } else {
            resultsHeader.innerHTML = `About <strong>${data.total_hits}</strong> results for "<strong>${escapeHtml(q)}</strong>"`;
            data.hits.forEach(hit => {
                resultsList.innerHTML += renderResult(hit);
            });

            if (data.next_cursor) {
                currentCursor = data.next_cursor;
                pagination.innerHTML = `<button class="btn-load-more" onclick="doSearch(false)">Load more results</button>`;
            } else {
                pagination.innerHTML = '';
            }
        }
    } catch (e) {
        resultsHeader.innerHTML = `<span style="color:var(--red)">Network error &mdash; is the server running?</span>`;
        if (resetCursor) resultsList.innerHTML = '';
    }
}

function renderResult(hit) {
    const permalink = hit.permalink.startsWith('http')
        ? hit.permalink
        : `https://old.reddit.com${hit.permalink}`;
    return `
        <div class="result-card" onclick="window.open('${escapeHtml(permalink)}', '_blank')">
            <div class="result-subreddit">
                <span>r/${escapeHtml(hit.subreddit)}</span>
            </div>
            <div class="result-title">${escapeHtml(hit.title)}</div>
            <div class="result-meta">
                <span class="result-score-badge">BM25 ${hit.score.toFixed(2)}</span>
                <span>&#128100; ${escapeHtml(hit.author || 'unknown')}</span>
                <span>&#11014; ${hit.post_score}</span>
                <span>&#128172; ${hit.comment_count}</span>
            </div>
        </div>
    `;
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
    } catch (e) {
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
                document.getElementById('crawl-btn').disabled = false;
                document.getElementById('crawl-btn').innerHTML = `
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="btn-icon">
                        <path d="M5 12h14"></path>
                        <path d="m12 5 7 7-7 7"></path>
                    </svg>
                    Start Pipeline
                `;
                loadSubredditOptions();
            }
        } catch (e) {
            // keep polling
        }
    }, 800);
}

function resetPipelineUI() {
    const stages = ['crawl', 'preprocess', 'index', 'autocomplete'];
    stages.forEach(s => {
        const el = document.getElementById(`stage-${s}`);
        el.className = 'stage';
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
        const cr = data.crawl_result;
        document.getElementById('stage-crawl-detail').textContent =
            `${cr.pages_crawled || 0} pages, ${cr.posts_inserted || 0} posts inserted`;
    }
    if (data.preprocess_result) {
        const pr = data.preprocess_result;
        document.getElementById('stage-preprocess-detail').textContent =
            `${pr.processed || 0} posts processed`;
    }
    if (data.index_result) {
        const ir = data.index_result;
        document.getElementById('stage-index-detail').textContent =
            `${ir.doc_count || 0} docs indexed (v${ir.version || '?'})`;
    }

    if (data.status === 'completed') {
        document.getElementById('stage-autocomplete-detail').textContent = 'Complete';
        const result = document.getElementById('pipeline-result');
        result.className = 'progress-result visible';
        result.textContent = `\u2713 Pipeline completed in ${data.elapsed_seconds || '?'}s \u2014 you can now search r/${data.subreddit}`;
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
// DASHBOARD
// ============================================================

async function loadStats() {
    try {
        const res = await fetch(`${API}/stats`);
        const data = await res.json();
        document.getElementById('stat-posts').textContent = data.raw_post_count;
        document.getElementById('stat-processed').textContent = data.processed_post_count;
        document.getElementById('stat-indexes').textContent = data.active_indexes;
        document.getElementById('stat-subreddits').textContent = data.subreddits.length;

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
    } catch (e) {
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
    } catch (e) {
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
