<h1 align="center">AIM OS</h1>

<p align="center"><b>A personal AI that remembers, researches and talks back.</b><br>
Persistent vector and relational memory, automatic research, a JARVIS-style dashboard and a Baymax-style voice.<br>
Windows, macOS and Linux. Runs locally in Docker. API keys are optional.</p>

<p align="center">
  <a href="#quick-start"><b>Quick start</b></a> ·
  <a href="#what-you-can-do"><b>What you can do</b></a> ·
  <a href="#services"><b>Services</b></a> ·
  <a href="#research-mode"><b>Research Mode</b></a>
</p>

<p align="center"><sub>Built with</sub><br>
<img src="https://skillicons.dev/icons?i=py,fastapi,docker,redis,postgres,html,css,js" alt="Python, FastAPI, Docker, Redis, PostgreSQL, HTML, CSS, JavaScript" height="40"></p>

AIM OS (Artificial Intelligence Memory Operating System) is fourteen containers talking over a Redis event bus.
It keeps your context in a four-tier memory, researches topics on its own, notifies you when something matters
and answers in a warm, slowed-down voice. Everything it stores stays inside this folder.

## What you can do

- **Talk to it.** Click **MIC** on the dashboard, speak, click again. Or type and press GO. Answers are spoken in a Baymax-style voice.
- **Give it memory.** A memory engine with four tiers, semantic search and decay keeps what matters and lets the rest fade.
- **Let it research.** The research agent runs grounded searches, removes duplicates and files the results.
- **Map a project.** Paste a public git link and Research Mode clones it, scans it, explains how it works and drafts a plan to finish it.
- **Browse with an agent.** A headless Chromium agent opens pages on request.
- **Work offline.** With no API keys it runs entirely on local models through Ollama.

## Quick start

You need **Docker** (Desktop or Engine) and **Python 3.8 or newer**. Nothing else.

### 1. Start it

| OS | Start | Stop |
|---|---|---|
| Windows | double-click `start.bat` | `stop.bat` |
| macOS | double-click `start.command`, or `./start.sh` | `./stop.sh` |
| Linux | `./start.sh` | `./stop.sh` |

Or on any OS:

```bash
python aim.py start
```

The first start builds the images and downloads the local AI models, about 6 GB, in the background. It
copies `.env.example` to `.env` and fills in random passwords for you.

### 2. Open the dashboard

<http://localhost:8888/index.html> opens on its own when the stack is up. Check that every service is healthy with:

```bash
python aim.py status
```

### 3. Add API keys (optional)

Put free-tier keys in `.env` to use hosted models. Without them AIM falls back to the local models.

```ini
GROQ_API_KEY=...
GOOGLE_API_KEY=...
TAVILY_API_KEY=...   # live web search in Research Mode
```

### Everyday commands

```bash
python aim.py start        # start everything and open the dashboard
python aim.py stop         # stop everything
python aim.py status       # health of every service
python aim.py logs [name]  # follow logs, optionally for one service
python aim.py test         # run the full test cycle
python aim.py clean        # stop and remove containers and images (keeps ./data)
```

## Services

Every port listens on `localhost` only.

| Port | Service | Notes |
|---|---|---|
| 8888 | Dashboard | JARVIS UI, timer (`/timer.html`), Research Center (`/research.html`) |
| 8001 | LLM Router | Groq, Gemini or local Ollama; falls back to local when offline or without a key |
| 8002 | Timeline | Activity log |
| 8003 | Memory Engine | Four tiers, semantic search, decay |
| 8004 | Core Brain | Intent and event processing |
| 8005 | Research Agent | Gemini grounded search and deduplication |
| 8006 | Notification | Cooldown and history |
| 8007 | Browser Agent | Headless Chromium: `POST /browse {"url": "..."}` |
| 8008 | Voice Agent | Baymax voice: `/speak`, `/transcribe`, `/converse` |
| 11435 | Ollama | Local models (`gemma3:4b`, `qwen3:4b`, `nomic-embed-text`) |
| 6333 | Qdrant | Vector memory |
| 9999 | Host Bridge | Runs on your computer, not in Docker: CPU, RAM and disk readout only |

## Research Mode

Open <http://localhost:8888/research.html>, or press **RESEARCH MODE** on the dashboard. It draws a live map
of how a project really works: files, services, links, health and completion.

- **Open project or paste a git link.** Public repos are cloned into `workspace/`, scanned and explained, and you get a plan to finish them.
- **New project.** *Think with me* finds your motive, then compares three approaches with charts and runs a novelty check on what already exists. Nothing is built before you pick one.
- **Edit.** Rename, edit tasks and edit files. Only projects inside `workspace/` are editable. AIM OS's own source is read-only there, and `.env` and key files are never served.
- **Test.** Runs the full test cycle with a verdict, per-layer results and coverage.

Cloned projects get a generated plan that you approve first. Calls are read-only by default and nothing in a cloned project is ever executed.

## Baymax voice

Click **MIC** in the bottom-right widget of the dashboard. The voice is a warm male neural voice
([Piper](https://github.com/rhasspy/piper)), slowed, lowered and softened to a Baymax style. It is an original
voice, not a recording of the character. Change the base voice with `VOICE_MODEL` in `.env`, for example
`en_US-lessac-medium`.

## Where your data lives

Everything stays in this folder.

| What | Where |
|---|---|
| PostgreSQL, Qdrant and Redis data | `./data/postgres`, `./data/qdrant`, `./data/redis` |
| AI models | `./data/ollama` |
| Baymax voice and speech-recognition models | `./data/voice` |
| Host-bridge Python environment | `./runtime` |

Docker keeps the built images in its own storage; `python aim.py clean` removes them. Delete this folder after
`clean` and nothing is left behind.

## Security and privacy

- All ports listen on `localhost` only. Websites open in your browser cannot change data: cross-site writes are refused and CORS allows the dashboard only.
- The browser agent blocks private and internal addresses. Allow specific hosts with `AIM_BROWSE_ALLOW=host1,host2`.
- The host bridge is the only part outside Docker. It listens on localhost and reports CPU, RAM and disk usage. AIM does **not** take screenshots, read window titles or list your processes.
- There is no login. This is a local, single-user tool: do not expose it to a network.
- `.env`, `data/`, `runtime/` and `workspace/` are git-ignored, so keys and personal data never reach the repository.

## Tests

```bash
python aim.py test             # static, smoke, API contract, data, event bus, flows, security, performance, links
python aim.py test --no-ai     # skip tests that use LLMs, web or git (uses no quota)
python aim.py test --chaos     # also stop services on purpose and check they recover (needs Docker CLI)
```

The UI layer (every button on the dashboard pages) runs from the page: Research Mode, then AIM OS, then Test, then
**Run UI tests**.

## Project layout

```
aim.py            Launcher: start, stop, status, logs, test, clean
core/             Brain, LLM router, memory engine
agents/           Research, browser and voice agents
services/         Timeline and notification
apps/             Dashboard, UI pages and timer
tools/            Host bridge and project scanner
tests/            Full test cycle
docker-compose.yml        The stack (docker-compose.gpu.yml adds GPU for Ollama)
```

Scan a project from the command line with `python tools/aim_scanner.py --path <project> --out result.json`.

## GPU

On Windows or Linux with an NVIDIA GPU and the NVIDIA Container Toolkit, `aim.py` turns on GPU for Ollama
automatically. macOS runs models on CPU inside Docker.

## License

MIT. See [LICENSE](LICENSE).
