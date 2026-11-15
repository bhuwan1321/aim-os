# AIM OS

Artificial Intelligence Memory Operating System: a persistent, JARVIS-style personal AI that watches
your context, remembers it (vector + relational memory), researches automatically, and talks to you
in a Baymax-style voice. Fourteen containers talking over a Redis event bus.

## Run it (Windows, macOS, Linux)

Requirements: **Docker** (Desktop or Engine) and **Python 3.8+**. Nothing else.

| OS | Start | Stop |
|----|-------|------|
| macOS | double-click `start.command` or `./start.sh` | `./stop.sh` |
| Linux | `./start.sh` | `./stop.sh` |
| Windows | double-click `start.bat` | `stop.bat` |

or on any OS: `python aim.py start` / `stop` / `status` / `logs [service]` / `test` / `clean`.

The first start builds images and downloads the local AI models (about 6 GB, in the background).
The JARVIS dashboard opens at <http://localhost:8888/index.html>.

## Everything stays in this folder

| What | Where |
|------|-------|
| PostgreSQL, Qdrant, Redis data | `./data/postgres`, `./data/qdrant`, `./data/redis` |
| AI models (gemma3:4b, qwen3:4b, nomic-embed-text) | `./data/ollama` |
| Baymax voice + speech-recognition models | `./data/voice` |
| Host-bridge Python environment | `./runtime` |

Docker itself keeps the built *images* in its own storage; `python aim.py clean` removes them.
Delete this folder (after `clean`) and nothing is left behind.

## Services

| Port | Service | Notes |
|------|---------|-------|
| 8888 | Dashboard | JARVIS UI, timer (`/timer.html`), Research Center (`/research.html`) |
| 8001 | LLM Router | Groq / Gemini / local Ollama; falls back to local when offline or no key |
| 8002 | Timeline | activity log |
| 8003 | Memory Engine | 4 tiers, semantic search, decay |
| 8004 | Core Brain | intent + event processing |
| 8005 | Research Agent | Gemini grounded search + dedupe |
| 8006 | Notification | cooldown + history |
| 8007 | Browser Agent | headless Chromium: `POST /browse {"url": ...}` |
| 8008 | Voice Agent | Baymax voice: `/speak`, `/transcribe`, `/converse` |
| 11434 | Ollama | local models |
| 9999 | Host Bridge | runs on your OS: CPU / RAM / disk readout only |

API keys are optional: put `GROQ_API_KEY` / `GOOGLE_API_KEY` in `.env`. Without them everything runs on
the local models.

## Baymax voice

Click **MIC** in the bottom-right widget of the dashboard, speak, click again. Or type and press GO.
The voice is a warm male neural voice ([Piper](https://github.com/rhasspy/piper)) slowed, lowered and
softened to a Baymax style. It is an original voice, not a recording of the character. Swap the base
voice with `VOICE_MODEL` in `.env` (any Piper voice name, e.g. `en_US-lessac-medium`).

## Host bridge and privacy

The bridge is the only thing running outside Docker. It listens on localhost only and reports CPU, RAM and disk
usage to the dashboard. AIM does **not** take screenshots, read window titles or list your processes.

## Research Mode (http://localhost:8888/research.html, or the RESEARCH MODE button on the dashboard)

A live map of how a project really works: files, services, links, health and completion.

- **Open project / paste a git link** (public repos): it is cloned into `workspace/`, scanned and explained, and you get a plan to finish it.
- **New project**: *Think with me* (finds your motive), then a comparison of three approaches with charts, a novelty check
  (what already exists, where you could stand out), and a planned map. Nothing is built before you pick an approach.
- **Edit**: rename, edit tasks (some tick themselves from live signals) and edit files. Only projects inside `workspace/`
  are editable; AIM OS's own source is read-only there, and `.env` / key files are never served.
- **Test**: the full test cycle below, with a verdict, per-layer results and coverage.
- Live web search uses **Tavily** (free tier): add `TAVILY_API_KEY=...` to `.env`. Without it, Gemini's own search is the fallback.

## Tests

`python aim.py test` runs static, smoke, API contract (every route, from each service's OpenAPI), data round trips,
event bus, end-to-end flows, security, performance and link proof. `--chaos` also stops services on purpose and checks
the map reacts and recovers; `--no-ai` skips tests that use LLMs/web/git. The UI layer (every button on the dashboard
pages) runs from the page: Research Mode > AIM OS > Test > Run UI tests.
Uploaded/cloned projects get a generated plan you approve first (read-only calls by default; nothing is ever executed).

## Security model

All ports listen on localhost only. Browsers on other websites cannot change data (cross-site writes are refused and CORS is
dashboard-only). The browser agent blocks private/internal addresses (set `AIM_BROWSE_ALLOW=host1,host2` to allow some).
There is no login: this is a local, single-user tool.

## Version control

The folder is a git repository (`.env`, `data/`, `runtime/` and `workspace/` are ignored). Commit before big changes.

## Project scanner

`python tools/aim_scanner.py --path <project> --out result.json` builds the dependency graph from the command line.

## GPU

On Windows/Linux with an NVIDIA GPU and the NVIDIA Container Toolkit, `aim.py` automatically enables
GPU for Ollama. macOS runs models on CPU inside Docker.
