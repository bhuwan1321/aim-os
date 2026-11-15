import os
import asyncio
import logging
import json
import base64
import httpx
from datetime import datetime
from typing import Optional
import redis.asyncio as redis

logging.basicConfig(level=os.getenv("AIM_LOG_LEVEL", "INFO"))
logger = logging.getLogger("aim.screen-agent")

# ── Config ────────────────────────────────────────

REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
LLM_ROUTER     = os.getenv("LLM_ROUTER_URL", "http://llm-router:8001")
INTERVAL       = int(os.getenv("AIM_SCREEN_INTERVAL", 30))
HOST_BRIDGE    = os.getenv("HOST_BRIDGE_URL", "http://host.docker.internal:9999")

redis_client = None

# ── Screen capture via host bridge ────────────────
# A tiny script runs on Windows host and serves
# screenshots at localhost:9999/screenshot
# This keeps Docker isolated but still gets screen data

async def capture_screen() -> Optional[str]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{HOST_BRIDGE}/screenshot", headers={"X-AIM-Token": os.getenv("AIM_BRIDGE_TOKEN", "")})
            resp.raise_for_status()
            data = resp.json()
            return data.get("image_b64")
    except Exception as e:
        logger.warning(f"Screen capture failed: {e}")
        return None

async def get_active_window() -> dict:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{HOST_BRIDGE}/active-window")
            resp.raise_for_status()
            return resp.json()
    except Exception:
        return {"app": "unknown", "title": "unknown"}

# ── Vision analysis ───────────────────────────────
async def analyze_screen(image_b64: str, window_info: dict) -> dict:
    # Skip if looking at AIM itself
    skip_apps = ["jarvis", "aim os", "localhost", "127.0.0.1"]
    title_lower = window_info.get("title", "").lower()
    if any(s in title_lower for s in skip_apps):
        return {
            "app": "jarvis-ui",
            "task": "Using AIM OS interface",
            "project": "AIM-OS",
            "has_error": False,
            "error_text": None,
            "context": "User is interacting with AIM"
        }
    prompt = f"""
Active window: {window_info.get('app', 'unknown')}
Title: {window_info.get('title', 'unknown')}
Time: {datetime.utcnow().strftime('%H:%M')}

Look at this screenshot and tell me:
1. What app/tool is open
2. What the user is working on
3. What task or project this belongs to

Respond in JSON:
{{
  "app": "app name",
  "task": "what user is doing",
  "project": "project name or null",
  "context": "brief summary"
}}
"""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{LLM_ROUTER}/route",
                json={
                    "task": "vision",
                    "prompt": prompt,
                    "image_b64": image_b64
                }
            )
            resp.raise_for_status()
            result = resp.json()["result"]
            # Clean JSON
            result = result.strip()
            if "```" in result:
                result = result.split("```")[1]
                if result.startswith("json"):
                    result = result[4:]
            return json.loads(result.strip())
    except Exception as e:
        logger.error(f"Vision analysis failed: {e}")
        return {
                "app":     window_info.get("app", "unknown"),
                "task":    "unknown",
                "project": None,
                "context": "Analysis failed"
            }

# ── Main loop ─────────────────────────────────────

async def screen_loop():
    # Skip these apps entirely — AIM should not watch itself
    SKIP_TITLES = [
    "aim os", "jarvis", "localhost", "127.0.0.1",
    "devtools", "developer tools", "chrome devtools",
    "cmd", "powershell", "terminal", "windows terminal",
    "host bridge", "aim host bridge", "aim ui server",
    "python", "start.bat", "command prompt"
    ]
    global redis_client
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )

    logger.info(f"Screen Agent started — interval: {INTERVAL}s ✅")

    while True:
        try:
           # Get active window info
            window_info = await get_active_window()

            # Skip if watching AIM itself or devtools
            title_lower = window_info.get("title", "").lower()
            if any(s in title_lower for s in SKIP_TITLES):
                logger.info("Skipping AIM/browser window")
                await asyncio.sleep(INTERVAL)
                continue

            # Capture screenshot
            image_b64 = await capture_screen()

            if image_b64:
                # Analyze with vision model
                analysis = await analyze_screen(image_b64, window_info)

                # Publish to event bus
                event = {
                    "app": analysis.get("app", window_info.get("app")),
                    "window_title": window_info.get("title", ""),
                    "task": analysis.get("task"),
                    "project": analysis.get("project"),
                    "has_error": analysis.get("has_error", False),
                    "error_text": analysis.get("error_text"),
                    "context": analysis.get("context"),
                    "image_b64": image_b64,
                    "timestamp": datetime.utcnow().isoformat()
                }

                await redis_client.publish(
                    "aim.screen.context",
                    json.dumps(event)
                )


                logger.info(f"Screen: {analysis.get('app')} — {analysis.get('task', '')[:60]}")

            else:
                # No screenshot — just publish window info
                await redis_client.publish(
                    "aim.screen.context",
                    json.dumps({
                        "app": window_info.get("app", "unknown"),
                        "window_title": window_info.get("title", ""),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                )

        except Exception as e:
            logger.error(f"Screen loop error: {e}")

        await asyncio.sleep(INTERVAL)

if __name__ == "__main__":
    asyncio.run(screen_loop())