#!/usr/bin/env python3
"""
AIM OS full test cycle, from your computer.

    python tests/run_all.py            run every server-side layer (static, smoke, contract, data, bus, flows, security, perf, links)
    python tests/run_all.py --no-ai    skip tests that use LLMs / web / git (no quota used)
    python tests/run_all.py --chaos    also stop services on purpose and check the map reacts and recovers (needs Docker CLI)
    (UI layer: open the Research Center, select AIM OS, press 'Run UI tests')

Exit code 1 when anything FAILS. Warnings never fail the run but are always listed.
"""
import argparse, json, subprocess, sys, time, urllib.error, urllib.request

R = "http://localhost:8005"
ICON = {"pass": "PASS", "fail": "FAIL", "warn": "WARN", "skip": "skip"}


def call(method, path, body=None, timeout=60):
    req = urllib.request.Request(R + path, json.dumps(body).encode() if body is not None else None, {"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code}: {e.read()[:200].decode(errors='replace')}")
    except Exception as e:
        raise SystemExit(f"Research agent not reachable at {R} ({e}). Start AIM first: python aim.py start")


def project_id():
    for p in call("GET", "/projects"):
        if p["name"] == "AIM OS":
            return p["id"]
    return call("POST", "/projects/open", {"path": "/projects/aim-os", "name": "AIM OS"}, 120)["id"]


def run_server_layers(pid, ai):
    call("POST", f"/projects/{pid}/test", {"ai": ai, "destructive": False})
    last = ""
    while True:
        time.sleep(2)
        r = call("GET", f"/projects/{pid}/test")
        if r.get("status") == "running":
            line = f"  running {r.get('done', 0)}/{r.get('total', '?')}  {str(r.get('current', ''))[:70]}"
            if line != last:
                print(line, flush=True); last = line
            continue
        return r


# ───────────── resilience: stop a service on purpose ─────────────
def docker(*a):
    return subprocess.run(["docker", *a], capture_output=True, text=True, timeout=120)


def live(pid):
    return call("GET", f"/projects/{pid}/live")


def wait(fn, timeout, step=1.5):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return None


def live_state(pid, node):
    try:
        return live(pid)["nodes"].get(node, {}).get("state")
    except BaseException:
        return None


def drill(pid, svc, cont, node, add):
    t0 = time.time()
    docker("stop", cont)
    down = wait(lambda: live_state(pid, node) == "fail", 25)
    add(f"{svc} stopped -> the map marks it FAILING within 25 s", "pass" if down else "fail", f"after {time.time() - t0:.1f}s" if down else "node never turned red")
    lv = live(pid)
    graph = call("GET", f"/projects/{pid}")["graph"]
    red = [e for e in graph["edges"] if e["kind"] in ("http", "db", "redis") and lv["edges"].get(e["id"], {}).get("state") == "fail" and svc in (e.get("fromSvc"), e.get("toSvc"))]
    add(f"{svc} stopped -> links touching it turn RED with a reason", "pass" if red else "fail", f"{len(red)} red link(s)" if red else "no link turned red")
    ports = {"llm-router": 8001, "notification": 8006, "voice-agent": 8008, "browser-agent": 8007, "timeline": 8002}
    bad = []
    for o in ("llm-router", "notification", "voice-agent", "browser-agent", "timeline"):
        if o == svc:
            continue
        try:
            ok = urllib.request.urlopen(f"http://localhost:{ports[o]}/health", timeout=5).status == 200
        except Exception:
            ok = False
        if not ok:
            bad.append(o)
    add(f"{svc} stopped -> the other services keep answering", "pass" if not bad else "fail", ", ".join(bad))
    if svc == "memory":
        try:
            req = urllib.request.Request("http://localhost:8008/converse/text", json.dumps({"text": "aimtest are you still there"}).encode(), {"Content-Type": "application/json"})
            d = json.load(urllib.request.urlopen(req, timeout=90))
            add("memory stopped -> voice still answers without memories (graceful degradation)", "pass" if d.get("reply") else "fail", (d.get("reply") or "")[:60])
        except Exception as e:
            add("memory stopped -> voice still answers without memories (graceful degradation)", "fail", f"voice agent failed: {e}")
    t1 = time.time()
    docker("start", cont)
    up = wait(lambda: live_state(pid, node) in ("ok", "slow"), 90)
    add(f"{svc} restarted -> the map shows it HEALTHY again within 90 s", "pass" if up else "fail", f"after {time.time() - t1:.1f}s" if up else "never recovered", int((time.time() - t1) * 1000))
    def calm():
        g = call("GET", f"/projects/{pid}")["graph"]; l = live(pid)
        return not [e for e in g["edges"] if e["kind"] == "http" and svc in (e.get("fromSvc"), e.get("toSvc")) and l["edges"].get(e["id"], {}).get("state") == "fail"]
    add(f"{svc} restarted -> its links return to non-failing", "pass" if wait(calm, 40) else "fail")


def chaos(pid):
    tests = []

    def add(name, status, detail="", ms=0):
        tests.append({"name": name, "status": status, "detail": detail, "ms": ms})
        print(f"  [{ICON[status]}] {name}  {detail}", flush=True)

    if docker("version").returncode != 0:
        add("docker CLI available", "skip", "Docker CLI not found - cannot run drills")
        return tests
    drills = [("timeline", "aim-timeline", "svc:timeline"), ("memory", "aim-memory", "svc:memory")]
    for svc, cont, node in drills:
        try:
            drill(pid, svc, cont, node, add)
        except Exception as e:
            add(f"{svc} drill", "fail", f"drill crashed: {type(e).__name__}: {str(e)[:120]}")
        finally:
            docker("start", cont)            # never leave a service stopped, whatever happened
            wait(lambda: live_state(pid, node) in ("ok", "slow"), 90)
    return tests


def report(r, verbose):
    sm = r["summary"]
    print("\n" + "=" * 72)
    print(f" VERDICT: {sm['verdict']}    pass {sm['pass']}   warn {sm['warn']}   fail {sm['fail']}   skip {sm['skip']}   ({sm['ran']} run, {sm['ms'] / 1000:.0f}s of test time)")
    cov = r.get("coverage") or {}
    if cov.get("routes"):
        print(f" API routes exercised: {cov['routes']['tested']}/{cov['routes']['total']} ({cov['routes']['pct']}%)")
    if cov.get("links"):
        print(f" Map links proven by real traffic: {cov['links']['proven']}/{cov['links']['total']} ({cov['links']['pct']}%)")
    print("=" * 72)
    for l in r["layers"]:
        c = {k: sum(1 for t in l["tests"] if t["status"] == k) for k in ICON}
        print(f"\n {l['name'].upper():<18} pass {c['pass']:<3} warn {c['warn']:<3} fail {c['fail']:<3} skip {c['skip']:<3}  - {l['desc']}")
        for t in l["tests"]:
            if t["status"] in ("fail", "warn") or (verbose and t["status"] != "skip"):
                print(f"   [{ICON[t['status']]}] {t['name']}")
                if t.get("detail"):
                    print(f"          {t['detail']}")
                if t["status"] == "fail" and t.get("expect") is not None:
                    print(f"          expected: {str(t['expect'])[:120]}   actual: {str(t['actual'])[:160]}")
    slow = sorted([t for l in r["layers"] for t in l["tests"] if t.get("ms")], key=lambda t: -t["ms"])[:5]
    print("\n Slowest tests:")
    for t in slow:
        print(f"   {t['ms'] / 1000:6.1f}s  {t['name'][:80]}")


def main():
    ap = argparse.ArgumentParser(description="AIM OS full test cycle")
    ap.add_argument("--no-ai", action="store_true", help="skip tests that use LLMs, web or git")
    ap.add_argument("--chaos", action="store_true", help="also stop services on purpose (needs Docker CLI)")
    ap.add_argument("-v", "--verbose", action="store_true", help="list every passing test too")
    a = ap.parse_args()
    pid = project_id()
    print("AIM OS test cycle - running server-side layers...")
    r = run_server_layers(pid, not a.no_ai)
    if r.get("status") == "error":
        raise SystemExit(f"test run crashed: {r.get('error')}")
    if a.chaos:
        print("\nResilience drills (services are stopped and restarted on purpose)...")
        tests = chaos(pid)
        call("POST", f"/projects/{pid}/test/layer/resilience", {"tests": tests, "coverage": {"drills": len(tests)}})
        r = call("GET", f"/projects/{pid}/test")
    report(r, a.verbose)
    sys.exit(1 if r["summary"]["fail"] else 0)


if __name__ == "__main__":
    main()
