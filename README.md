# RediSearch

A BM25 and WebCrawler based Search Engine for Reddit (will soon extend support to more Sites too).

---

## System Design Plan

### High-Level System Architecture

The system has five major components, connected by data — never by shared state or direct calls across concerns.

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Crawler    │────▶│  Raw Store   │────▶│ Preprocessor │
│  (old.reddit)│     │  (SQLite)    │     │  (pipeline)  │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                 │
                                                 ▼
                                          ┌──────────────┐
                                          │  Processed   │
                                          │   Store      │
                                          └──────┬───────┘
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                       ┌────────────┐     ┌────────────┐     ┌────────────┐
                       │ BM25 Index │     │ TF-IDF Idx │     │ Vector Idx │
                       │ (inverted) │     │  (sparse)  │     │  (FAISS)   │
                       └─────┬──────┘     └─────┬──────┘     └─────┬──────┘
                             │                  │                  │
                             └──────────────────┼──────────────────┘
                                                ▼
                                         ┌────────────┐
                                         │Query Engine│
                                         │ (hybrid)   │
                                         └──────┬─────┘
                                                │
                                  ┌─────────────┼─────────────┐
                                  ▼             ▼             ▼
                            ┌──────────┐ ┌──────────┐ ┌────────────┐
                            │  Search  │ │Autocmplt │ │ Pagination │
                            │ Endpoint │ │ Endpoint │ │  (cursor)  │
                            └──────────┘ └──────────┘ └────────────┘
```

**Data flow, end-to-end:**

1. **Crawler** fetches HTML pages from `old.reddit.com`, follows pagination, extracts structured post data.
2. **Raw Store** persists every crawled post with its original HTML. This is the system's source of truth.
3. **Preprocessor** reads raw posts, applies a deterministic pipeline (normalize → tokenize → stem), writes results to processed store. Tagged with a pipeline version number.
4. **Index Builder** reads processed data and builds three index types — BM25 inverted index, TF-IDF sparse matrix, and FAISS vector index — partitioned by shard (subreddit).
5. **Query Engine** receives a search query, preprocesses it with the same pipeline (minus HTML cleaning), fans out to relevant shard indexes, executes BM25 + vector search in parallel, fuses scores, applies pagination, and returns results.
6. **Background Job Manager** orchestrates all of the above via a SQLite-backed queue and worker threads.

The critical invariant: **raw data is never modified after insertion**. All downstream state is derived and disposable.

---

### Technology Choices

| Decision | Choice | Why |
|----------|--------|-----|
| Storage | SQLite (WAL mode) | Zero-ops, sufficient for 500K docs, single file you can inspect |
| Embedding model | all-MiniLM-L6-v2 | 2x faster, half the memory, quality sufficient for learning |
| Task queue | SQLite + threads | No external deps, full visibility into job state |
| Shard key | Subreddit | Natural partition, enables query pruning |
| Pagination | Cursor-based | Stable results, O(1) resume, no shifting |
| Rate limiting | Token bucket (in-memory) | Single process, ephemeral state acceptable |
| Hybrid fusion | Weighted linear combination | Transparent scoring, tunable α parameter |
| BM25 | Custom implementation | Shard-aware IDF, full control, educational value |

---

### Incremental Build Plan

- [x] **Phase 1 — Foundation**: project scaffolding, config, SQLite schema, storage CRUD, tests
- [ ] **Phase 2 — Crawler**: HTTP client, robots.txt, listing/post parsing, dedup, CLI
- [ ] **Phase 3 — Preprocessing**: 9-step pipeline, profiles (DOCUMENT/QUERY/AUTOCOMPLETE), CLI
- [ ] **Phase 4 — BM25 Search**: inverted index, BM25 builder/searcher, search CLI
- [ ] **Phase 5 — API**: FastAPI endpoints, rate limiting, cursor pagination
- [ ] **Phase 6 — Sharding**: shard manager, router, cross-shard merge
- [ ] **Phase 7 — TF-IDF + Vector + Hybrid**: embeddings, FAISS, score fusion
- [ ] **Phase 8 — Autocomplete**: trie, builder, prefix suggester
- [ ] **Phase 9 — Background Jobs**: job queue workers, scheduler, index versioning
