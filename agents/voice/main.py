"""
AIM Voice Agent (port 8008) - speaks with a Baymax-style voice and listens.

  POST /speak       {"text": "..."}              -> audio/wav (Baymax-style)
  POST /transcribe  multipart file=<audio>       -> {"text": "..."}
  POST /converse    multipart file=<audio> | JSON {"text": "..."}
                                                 -> {"transcript","reply","audio_b64"}
  GET  /health

"Baymax-style" = a warm, soft male neural voice (Piper) slowed and
lowered slightly, with gentle filtering and a faint synthetic shimmer.
It is an original voice in that style, not a recording of the character.
Models are cached under /data (= ./data/voice on the host).
"""
import asyncio
import base64
import io
import logging
import os
import re
import threading
import urllib.request
import wave
from typing import Optional

import httpx
import numpy as np
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
log = logging.getLogger("aim.voice")

DATA = os.getenv("VOICE_DATA", "/data")
VOICE_MODEL = os.getenv("VOICE_MODEL", "en_US-ryan-medium")
WHISPER_MODEL = os.getenv("WHISPER_MODEL", "base.en")
ROUTER = os.getenv("LLM_ROUTER_URL", "http://llm-router:8001")
MEMORY = os.getenv("MEMORY_URL", "http://memory:8003")
HF = "https://huggingface.co/rhasspy/piper-voices/resolve/main"

PERSONA = (
    "You are Baymax, a gentle, caring personal healthcare-style companion built into AIM OS, "
    "the user's personal AI operating system. Speak calmly and kindly in 1-3 short sentences. "
    "Be warm and reassuring, plain and simple. Never use markdown, lists or emojis. "
    "If the user seems stressed or tired, gently suggest a short break. "
    "If you are not sure of something, say so honestly."
)

app = FastAPI(title="AIM Voice Agent (Baymax)", version="0.1.0")

# -- AIM live-map trace: reports real HTTP traffic to the Research Agent (stdlib only, fire-and-forget) --
import threading as _aim_th, time as _aim_t, json as _aim_j, urllib.request as _aim_ur


def _aim_trace(ev):
    try:
        _aim_ur.urlopen(_aim_ur.Request("http://research-agent:8005/trace", _aim_j.dumps(ev).encode(),
                                        {"Content-Type": "application/json"}), timeout=1).read()
    except Exception:
        pass


@app.middleware("http")
async def _aim_trace_mw(request, call_next):
    p = request.url.path
    if request.method == "OPTIONS" or p == "/health" or p.startswith(("/trace", "/docs", "/openapi")):
        return await call_next(request)
    t0, status = _aim_t.time(), 500
    try:
        resp = await call_next(request)
        status = resp.status_code
        return resp
    finally:
        _aim_th.Thread(target=_aim_trace, daemon=True, args=({
            "dst": "voice-agent", "method": request.method, "path": p, "status": status,
            "ms": int((_aim_t.time() - t0) * 1000), "ip": request.client.host if request.client else "",
            "size": int(request.headers.get("content-length") or 0)},)).start()
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:8888", "http://127.0.0.1:8888"], allow_methods=["*"], allow_headers=["*"])

_state = {"voice": None, "whisper": None, "tts_error": None, "stt_error": None}


# ── Model loading (cached in /data) ───────────────────────────────────
def _download(url: str, dest: str):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    if os.path.exists(dest):
        return
    log.info("Downloading %s", url)
    urllib.request.urlretrieve(url, dest + ".part")
    os.replace(dest + ".part", dest)


def _load_tts():
    try:
        lang, name, quality = VOICE_MODEL.split("-")
        base = f"{HF}/{lang.split('_')[0]}/{lang}/{name}/{quality}/{VOICE_MODEL}"
        onnx = f"{DATA}/voices/{VOICE_MODEL}.onnx"
        _download(base + ".onnx", onnx)
        _download(base + ".onnx.json", onnx + ".json")
        from piper.voice import PiperVoice
        _state["voice"] = PiperVoice.load(onnx, config_path=onnx + ".json")
        log.info("Baymax voice ready (%s)", VOICE_MODEL)
    except Exception as e:
        _state["tts_error"] = str(e)
        log.exception("TTS load failed")


def _load_stt():
    try:
        from faster_whisper import WhisperModel
        _state["whisper"] = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8",
                                         download_root=f"{DATA}/whisper")
        log.info("Whisper ready (%s)", WHISPER_MODEL)
    except Exception as e:
        _state["stt_error"] = str(e)
        log.exception("STT load failed")


@app.on_event("startup")
async def startup():
    threading.Thread(target=_load_tts, daemon=True).start()
    threading.Thread(target=_load_stt, daemon=True).start()


# ── Baymax voice DSP ──────────────────────────────────────────────────
def baymax_fx(samples: np.ndarray, sr: int) -> np.ndarray:
    x = samples.astype(np.float32) / 32768.0
    # slower + a little lower (resample then play at the original rate)
    factor = 0.93
    idx = np.arange(0, len(x) - 1, factor)
    x = np.interp(idx, np.arange(len(x)), x)
    # soft low-pass at ~5.5 kHz with a gentle roll-off
    spec = np.fft.rfft(x)
    freqs = np.fft.rfftfreq(len(x), 1 / sr)
    spec *= 1 / (1 + (freqs / 5500.0) ** 4)
    x = np.fft.irfft(spec, len(x))
    # faint synthetic shimmer (short comb) for the "robot" warmth
    d = int(sr * 0.007)
    y = x.copy()
    y[d:] += 0.18 * x[:-d]
    # soft limit and normalise to about -3 dB
    y = np.tanh(1.4 * y)
    y *= 0.7 / max(np.max(np.abs(y)), 1e-6)
    return (y * 32767).astype(np.int16)


def synth_wav(text: str, fx: bool = True) -> bytes:
    voice = _state["voice"]
    if voice is None:
        raise HTTPException(503, f"Voice not ready: {_state['tts_error'] or 'still downloading the voice model'}")
    raw = io.BytesIO()
    with wave.open(raw, "wb") as w:
        voice.synthesize(text, w, length_scale=1.12, sentence_silence=0.25)
    raw.seek(0)
    with wave.open(raw, "rb") as r:
        sr, frames = r.getframerate(), r.readframes(r.getnframes())
    pcm = np.frombuffer(frames, dtype=np.int16)
    if fx:
        pcm = baymax_fx(pcm, sr)
    out = io.BytesIO()
    with wave.open(out, "wb") as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(sr); w.writeframes(pcm.tobytes())
    return out.getvalue()


def clean_for_speech(t: str) -> str:
    t = re.sub(r"<think>.*?</think>", "", t, flags=re.S)
    t = re.sub(r"[`*_#>\[\]]", "", t)
    t = re.sub(r"https?://\S+", "a link", t)
    return re.sub(r"\s+", " ", t).strip()[:700]


# ── Brain ─────────────────────────────────────────────────────────────
async def recall(query: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.post(f"{MEMORY}/memories/search", json={"query": query, "limit": 3})
            r.raise_for_status()
            data = r.json()
        items = data.get("results", data) if isinstance(data, dict) else data
        return "\n".join(f"- {(i.get('content') or '')[:200]}" for i in items if isinstance(i, dict))
    except Exception:
        return ""


async def think(text: str) -> str:
    memories = await recall(text)
    ctx = PERSONA + (f"\nThings you remember about the user:\n{memories}" if memories else "")
    async with httpx.AsyncClient(timeout=180) as c:
        r = await c.post(f"{ROUTER}/route", json={"task": "fast", "prompt": text, "context": ctx})
        r.raise_for_status()
        return clean_for_speech(r.json()["result"])


async def transcribe_bytes(data: bytes) -> str:
    model = _state["whisper"]
    if model is None:
        raise HTTPException(503, f"Speech recognition not ready: {_state['stt_error'] or 'still loading'}")

    def run():
        segs, _ = model.transcribe(io.BytesIO(data), language="en", vad_filter=True, beam_size=1)
        return " ".join(s.text.strip() for s in segs).strip()

    return await asyncio.get_running_loop().run_in_executor(None, run)


# ── API ───────────────────────────────────────────────────────────────
class SpeakReq(BaseModel):
    text: str
    fx: bool = True


class ConverseReq(BaseModel):
    text: str


@app.get("/health")
async def health():
    return {"status": "ok", "service": "voice-agent", "persona": "baymax",
            "tts_ready": _state["voice"] is not None, "stt_ready": _state["whisper"] is not None,
            "tts_error": _state["tts_error"], "stt_error": _state["stt_error"]}


@app.post("/speak")
async def speak(req: SpeakReq):
    wav = await asyncio.get_running_loop().run_in_executor(None, synth_wav, clean_for_speech(req.text), req.fx)
    return Response(wav, media_type="audio/wav")


@app.post("/transcribe")
async def transcribe(file: UploadFile = File(...)):
    return {"text": await transcribe_bytes(await file.read())}


async def _converse(text: str) -> dict:
    if not text:
        text = "I did not hear anything."
        reply = "I did not hear anything. Please try again."
    else:
        try:
            reply = await think(text)
        except Exception as e:
            log.warning("think failed: %s", e)
            reply = "I am having trouble reaching my thinking models. Please check that AIM is fully started."
    wav = await asyncio.get_running_loop().run_in_executor(None, synth_wav, reply, True)
    return {"transcript": text, "reply": reply, "audio_b64": base64.b64encode(wav).decode()}


@app.post("/converse")
async def converse_audio(file: UploadFile = File(...)):
    return await _converse(await transcribe_bytes(await file.read()))


@app.post("/converse/text")
async def converse_text(req: ConverseReq):
    return await _converse(req.text.strip())


@app.get("/greeting")
async def greeting():
    wav = await asyncio.get_running_loop().run_in_executor(
        None, synth_wav, "Hello. I am Baymax, your personal AIM companion.", True)
    return Response(wav, media_type="audio/wav")
