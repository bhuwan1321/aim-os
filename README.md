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

## Project scanner

`python tools/aim_scanner.py --path <project> --out result.json` builds a dependency graph.

## API tests

`python aim.py test` runs `tests/aim-os.postman_collection.json` with the Postman CLI.

## GPU

On Windows/Linux with an NVIDIA GPU and the NVIDIA Container Toolkit, `aim.py` automatically enables
GPU for Ollama. macOS runs models on CPU inside Docker.
