"""
AIM Browser Agent (port 8007) - opens a page in headless Chromium, extracts the
readable text, optionally summarises it with the LLM router and stores the
summary in long-term memory.

  POST /browse {"url": "https://...", "summarize": true, "store": true}
  GET  /health
"""
import logging
import os
from typing import Optional
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.async_api import async_playwright

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
log = logging.getLogger("aim.browser-agent")

ROUTER = os.getenv("LLM_ROUTER_URL", "http://llm-router:8001")
MEMORY = os.getenv("MEMORY_URL", "http://memory:8003")

app = FastAPI(title="AIM Browser Agent", version="0.2.0")

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
            "dst": "browser-agent", "method": request.method, "path": p, "status": status,
            "ms": int((_aim_t.time() - t0) * 1000), "ip": request.client.host if request.client else "",
            "size": int(request.headers.get("content-length") or 0)},)).start()
_pw = {"p": None, "browser": None}


class BrowseReq(BaseModel):
    url: str
    summarize: bool = True
    store: bool = True
    max_chars: int = 8000


@app.on_event("startup")
async def startup():
    _pw["p"] = await async_playwright().start()
    _pw["browser"] = await _pw["p"].chromium.launch(args=["--no-sandbox"])
    log.info("Browser Agent started (Chromium ready)")


@app.on_event("shutdown")
async def shutdown():
    if _pw["browser"]:
        await _pw["browser"].close()
    if _pw["p"]:
        await _pw["p"].stop()


async def _announce_tab(out):
    """Tell the rest of AIM what was browsed (the brain logs it to the timeline)."""
    try:
        import json as _j, os as _o
        import redis.asyncio as _r
        c = _r.Redis(host=_o.getenv("REDIS_HOST", "redis"), port=int(_o.getenv("REDIS_PORT", 6379)), password=_o.getenv("REDIS_PASSWORD"), decode_responses=True)
        try:
            await c.publish("aim.browser.tab", _j.dumps({"url": out["url"], "title": out["title"], "app": "browser", "summary": (out["summary"] or out["text"])[:300]}))
        finally:
            await c.aclose()
    except Exception as e:
        log.warning("could not announce tab: %s", e)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "browser-agent", "browser": _pw["browser"] is not None}


@app.post("/browse")
async def browse(req: BrowseReq):
    parsed = urlparse(req.url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(400, "Only http(s) URLs are allowed")

    ctx = await _pw["browser"].new_context()
    try:
        page = await ctx.new_page()
        await page.goto(req.url, wait_until="domcontentloaded", timeout=30000)
        title = await page.title()
        text = await page.evaluate("document.body ? document.body.innerText : ''")
    except Exception as e:
        raise HTTPException(502, f"Could not load page: {e}")
    finally:
        await ctx.close()

    text = " ".join(text.split())[: req.max_chars]
    out = {"url": req.url, "title": title, "text": text, "summary": None, "memory_id": None}

    if req.summarize and text:
        try:
            async with httpx.AsyncClient(timeout=180) as c:
                r = await c.post(f"{ROUTER}/route", json={
                    "task": "fast",
                    "prompt": f"Summarise this web page in 3-5 sentences.\nTitle: {title}\n\n{text[:6000]}"})
                r.raise_for_status()
                out["summary"] = r.json()["result"]
        except Exception as e:
            log.warning("summary failed: %s", e)

    if req.store and (out["summary"] or text):
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post(f"{MEMORY}/memories", json={
                    "content": f"Web page: {title} ({req.url})\n{out['summary'] or text[:600]}",
                    "source": "browser", "importance": 0.6, "confidence": 0.8,
                    "tags": ["web", "browser-agent"], "tier": "short"})
                r.raise_for_status()
                out["memory_id"] = r.json().get("id")
        except Exception as e:
            log.warning("store failed: %s", e)
    if req.store and (out["summary"] or text):
        await _announce_tab(out)
    return out
