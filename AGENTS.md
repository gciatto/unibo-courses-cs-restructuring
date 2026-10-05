# Project map

This repository builds a syllabus-grounded view of UniBo courses and uses it to
explore curriculum restructuring. The main path is:

```text
UniBo catalogues + people directory
  -> scraping/download_programmes.py      -> data/programmes/<year>/<dept>/programme-*.yml
  -> scraping/download_contacts.py        -> data/contacts.csv
  -> scraping/download_course_headers.py  -> data/course_headers.csv
  -> scraping/download_teachings.py       -> data/courses/<teacher>/<year>/teaching-*.yml
  -> scraping/merge_teachings.py           -> data/courses/.files/<year>/course-*.yml
  -> clustering/                           -> data/clusters/runs/.../cluster_courses.yml
  -> restructuring/                        -> data/restructuring/attempt-*/
```

`clustering/` is the bridge between the two areas described below:
`restructuring` consumes its full `cluster_courses.yml`, whose course entries
point at canonical merged course YAML files.

## Domain intent

The root background decks frame this as work for the DISI service-teaching
working group. The primary scope is first- and second-cycle teaching delivered
outside DISI degree programmes; internal DISI teaching and third-cycle teaching
are context, not the main target.

The update deck's operational definition counts a service course when DISI
members deliver INF/01 or ING-INF/05 credits and none of the associated degree
programmes belongs to DISI. The scrapers collect broader source data; this cohort
selection belongs to downstream analysis, not page parsing.

The intended result is evidence for a sustainable portfolio of courses and
reusable modules, potentially including shared courses, partial mutualization,
and MOOCs. Generated clusters and restructuring diagrams are discussion aids,
not final decisions. Human review must still account for prerequisites, depth,
laboratories, assessment, language, semester, campus, enrolment, regulation,
and organizational feasibility.

The September 2026 update reports the research baseline behind the repository:
632 teaching instances merged into 293 courses, then compared from multilingual
title/outcomes/contents/bibliography embeddings and grouped into 30 spectral
clusters. Four clusters (7, 1, 26, and 5) were also decomposed manually into
macro-topic modules with CFU allocations. That manual CFU decomposition is
background methodology; the current `restructuring/` workflow extracts topic
memberships and diagrams, but does not allocate CFU or prove feasibility.

The same update describes a later `pd25`/`pd26` coverage-enrichment flow
(`merge_pd25_pd26.py` and `apply_pd_coverage.py`). Those scripts are not present
in this checkout, so do not assume `teaching-*.yml` contains that coverage block.
When slides and code differ, treat the checked-in code and tests as executable
truth and the decks as domain/rationale documentation.

## Runtime and conventions

- This is an un-packaged Python repository. Run commands from the repository
  root with `.venv/bin/python -m ...` so absolute package imports resolve.
- Dependencies are pinned loosely in `requirements.txt`; the local environment
  is `.venv` when present.
- Use Conventional Commits for commit messages, with a meaningful scope when
  applicable (for example, `feat(restructuring): ...`). Mark breaking changes
  with `!` and a `BREAKING CHANGE:` footer.
- Node is used only to render Mermaid diagrams: after `npm install`,
  `npm run mmd -- path/to/a.mmd [...]` writes `a.svg` next to each input,
  using `mermaid-config.json` (raised edge/text limits for global proposals).
- Tests use `unittest`, not pytest:
  `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'`.
- Treat course IDs, teaching IDs, and programme codes as opaque strings. Leading
  zeroes and suffixes such as `00819-B` are meaningful.
- `resources/departments.yml` and `resources/roles.yml` are the normalization
  dictionaries used by the scrapers. Update them when UniBo introduces an
  unrecognized department or role instead of scattering aliases in parsers.
- Most files under `data/` are generated or research-run artifacts, but many are
  tracked. Do not bulk-regenerate, rewrite, or delete them unless the task asks
  for it.
- Scraper commands make live UniBo requests. Restructuring makes OpenAI
  requests. Prefer parser/unit tests for routine verification.

## `scraping/`

### Shared support

- `_utils.py`: HTTP download/retry/backoff, URL-keyed HTML caching in
  `data/.cache`, logging setup, and lookup of programme YAML by year/name/code.
  Cache keys are only URL hashes; the programme/header crawlers expose refresh
  switches, while `download_teachings.py` currently does not.
- `html_to_md.py`: HTML-to-Markdown adapter used by syllabus parsing; it is also
  a standalone stdin/file CLI.

### Acquisition pipeline

- `download_programmes.py`: crawls Italian and English first/single-cycle and
  second-cycle catalogues, reconciles bilingual cards, classifies departments,
  validates with Pydantic, and writes one programme YAML per code. Run this
  before teaching extraction when programme links must resolve.
- `download_contacts.py`: walks the UniBo people directory by surname initial,
  parses contact details, and streams `data/contacts.csv`.
- `download_course_headers.py`: reads contacts, visits each teacher's English
  teaching list for an academic year, and streams the course-discovery CSV.
  `--limit` limits input contacts; `--skip` skips contact rows.
- `download_teachings.py`: the main normalization stage. It discovers IT/EN
  course pages, converts them to Markdown, parses syllabus sections and course
  facts, enriches teacher metadata, resolves mentioned programmes, filters with
  whitelist/blacklist terms, and writes
  `data/courses/<email-handle>/<year>/teaching-<teaching-id>.yml`.
  Its `--limit` counts successful downloads, not input rows.
- `merge_teachings.py`: reads the per-teacher teaching YAML tree and creates the
  canonical course corpus in `data/courses/.files/<year>/`. It also creates
  per-teacher `course-*.yml` symlinks back to those canonical files.

### Merge semantics

Records are considered only within the same `(year, course_title.id)`. They join
when they share a teaching ID, the same programme-code set, the same syllabus
excluding learning outcomes, or sufficiently similar learning outcomes. Grouping
is transitive. Distinct components get deterministic `-A`, `-B`, ... suffixes.
The default similarity backend is a Sentence Transformers embedding model and
may download model data; use `--learning-outcomes-similarity-backend text` or
`token` for an explicitly offline/lightweight run.

Relevant tests are `tests/test_download_course_headers.py`,
`tests/test_download_teachings_headers.py`, `tests/test_course_titles.py`,
`tests/test_programmes_from_teachings.py`, `tests/test_pd25_teachings.py`, and
`tests/test_merge_teachings.py`.

## `restructuring/`

- `__main__.py` / `cli.py`:
  `.venv/bin/python -m restructuring <cluster_courses.yml>`;
  selects clusters, model/retry settings, syllabus sections, cache refresh, and
  stateless/full topic conversation mode.
- `models.py`: strict Pydantic response contracts for topics, topic diffs,
  memberships, and restructuring proposals, plus immutable workflow
  configuration/input types.
- `io.py`: loads cluster manifests and referenced course YAML, prefers English
  then Italian for requested syllabus sections, selects clusters, owns the LLM
  cache format, validates restructuring proposals, and atomically writes YAML
  and Mermaid artifacts.
- `workflow.py`: processes courses in stable course-ID order, incrementally
  evolves the cluster topic ontology, validates every model diff, rewrites any
  affected course-topic files, then separately asks for a structured
  restructuring proposal and renders Mermaid locally. API retries use
  exponential backoff with jitter.
- `prompts/*.txt`: system, per-course topic extraction, and restructuring
  proposal prompts, plus `design_system`, `modules`, and `assembly` prompts for
  the all-courses proposal. Prompts are loaded at module import time.

### Restructuring contracts

- Input is the full clustering report, not `cluster_courses.short.yml`. Relative
  course paths are resolved against the current directory, repository root, and
  cluster-manifest directory.
- Default syllabus evidence is title + learning outcomes + course contents.
  Titles and IDs are metadata only; topic evidence must come from syllabus text.
- Topic keys are lowercase snake case. Model replies are diffs against the
  current ontology; removals/renames must also replace affected earlier-course
  memberships. `apply_topic_response` is the authoritative invariant checker.
- `stateless` is the default: each course request receives the system prompt,
  current ontology, and current syllabus. `full` additionally retains all prior
  turns.
- Conversation caches are YAML files in `data/.cache`. Cache metadata includes
  `PROMPT_VERSION`, model settings, selected sections, conversation mode, and
  cluster identity; cached user messages are also matched exactly. Increment
  `PROMPT_VERSION` in `restructuring/io.py` when prompt or response semantics
  change and old conversations should be invalidated deliberately.
- Every run creates `data/restructuring/attempt-YYYY-MM-DD-HH-MM/`; a second run
  in the same minute collides rather than reusing the directory.
- `--reuse-topics-from ATTEMPT_DIR` may be repeated to reuse complete topic YAML
  for matching clusters from earlier attempts. Reused artifacts are validated
  and copied into the new attempt; only proposal generation is rerun.
- `topics-of-cluster-*.yml` and `topics-of-course-*.yml` are written
  incrementally and remain definitive if proposal generation fails. Successful
  proposal generation writes a validated `restructure-proposal-*.yml` plus a
  deterministic `.mmd` Mermaid view; proposal failures are logged and do not
  fail the overall run.
- The model never lists source-course mappings. It must only ensure every
  assigned source topic appears in some proposed topic's provenance;
  `derive_source_course_mappings` then maps each source course by greedy set
  cover, and the proposal YAML reports per-course `overshoot` and per-new-course
  `proposed_course_reuse`.
- `--all-courses` proposals are decomposed: `topic_batches` splits assigned
  topics into co-teaching communities (Louvain, at most 60 per batch), one
  `TopicPartition` request per batch groups them into modules
  (`modules-global.yml`), and one `CourseAssembly` request builds new courses
  from module keys. Each request is cached independently by its exact messages.
- A rejected structured reply is sent back with the validation error on retry.
- Live runs require `OPENAI_API_KEY`; endpoint/model may come from
  `OPENAI_BASE_URL` and `OPENAI_MODEL`.

The principal test is `tests/test_restructuring.py`; it uses fake clients to
cover input loading, cache replay/truncation, topic-state updates, retries,
incremental artifacts, proposal validation, Mermaid output, and non-fatal
proposal failures.

## Safe change routing

- HTML selector or page-format changes belong in the matching scraper parser and
  its focused tests; keep network access outside parsing helpers.
- Changes to teaching YAML shape must be checked in both
  `download_teachings.py` and `merge_teachings.py`, then against clustering and
  restructuring consumers.
- Changes to topic/proposal response schemas must stay aligned across
  `models.py`, prompt files, workflow validation, cache versioning, and
  `tests/test_restructuring.py`.
- Preserve atomic writes for caches and restructuring artifacts: partial results
  are intentionally useful after long model-driven runs.
