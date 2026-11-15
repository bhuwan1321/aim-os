/* Baymax voice widget for the JARVIS dashboard.
   Talks to the Voice Agent on :8008. Click the mic, speak, click again to send. */
(function () {
  const VOICE = 'http://localhost:8008';
  const css = `
  #bx{position:fixed;right:18px;bottom:18px;z-index:9999;font-family:'Share Tech Mono',monospace;width:290px;
      background:rgba(2,8,16,.92);border:1px solid #00d4ff55;border-radius:10px;padding:10px;color:#9fefff;
      box-shadow:0 0 18px #00d4ff22}
  #bx h4{margin:0 0 6px;font:600 11px Orbitron,sans-serif;letter-spacing:2px;color:#00d4ff;display:flex;justify-content:space-between}
  #bxlog{font-size:12px;line-height:1.4;max-height:130px;overflow:auto;margin-bottom:8px;min-height:34px}
  #bxlog .u{color:#7aa}#bxlog .b{color:#e8fbff}
  #bx .row{display:flex;gap:6px}
  #bx input{flex:1;background:#031422;border:1px solid #00d4ff44;color:#cfffff;padding:6px;border-radius:5px;font:inherit;font-size:12px}
  #bx button{background:#00d4ff22;border:1px solid #00d4ff77;color:#00d4ff;border-radius:5px;cursor:pointer;padding:0 10px;font:inherit}
  #bx button.rec{background:#ff334422;border-color:#ff3344;color:#ff6677;animation:bxp 1s infinite}
  @keyframes bxp{50%{opacity:.5}}
  #bxs{font-size:10px;color:#4aa}`;
  const st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);
  const box = document.createElement('div'); box.id = 'bx';
  box.innerHTML = `<h4><span>BAYMAX</span><span id="bxs">...</span></h4><div id="bxlog"></div>
    <div class="row"><button id="bxm" title="Talk">MIC</button><input id="bxi" placeholder="Say something to Baymax"><button id="bxg">GO</button></div>`;
  document.body.appendChild(box);
  const $ = id => document.getElementById(id);
  const log = (cls, t) => { const d = document.createElement('div'); d.className = cls; d.textContent = (cls === 'u' ? 'You: ' : 'Baymax: ') + t; $('bxlog').appendChild(d); $('bxlog').scrollTop = 1e9; };
  const state = s => { try { setState(s); } catch (e) {} };

  async function health() {
    try { const h = await (await fetch(VOICE + '/health')).json();
      $('bxs').textContent = h.tts_ready && h.stt_ready ? 'ready' : 'loading models...';
    } catch (e) { $('bxs').textContent = 'offline'; }
  }
  health(); setInterval(health, 8000);

  function play(b64) {
    const a = new Audio('data:audio/wav;base64,' + b64);
    state('speaking'); a.onended = () => state('idle'); a.play().catch(() => state('idle'));
  }
  async function finish(r) {
    if (!r.ok) { log('b', 'I am not ready yet. Please try again in a moment.'); state('idle'); return; }
    const d = await r.json(); log('u', d.transcript); log('b', d.reply); play(d.audio_b64);
  }
  async function sendText() {
    const t = $('bxi').value.trim(); if (!t) return; $('bxi').value = ''; state('thinking');
    finish(await fetch(VOICE + '/converse/text', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text: t }) }).catch(() => ({ ok: false })));
  }
  $('bxg').onclick = sendText; $('bxi').onkeydown = e => { if (e.key === 'Enter') sendText(); };

  let rec = null, chunks = [];
  $('bxm').onclick = async () => {
    if (rec) { rec.stop(); return; }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      rec = new MediaRecorder(stream); chunks = [];
      rec.ondataavailable = e => chunks.push(e.data);
      rec.onstop = async () => {
        stream.getTracks().forEach(t => t.stop()); rec = null; $('bxm').classList.remove('rec'); $('bxm').textContent = 'MIC'; state('thinking');
        const fd = new FormData(); fd.append('file', new Blob(chunks, { type: 'audio/webm' }), 'speech.webm');
        finish(await fetch(VOICE + '/converse', { method: 'POST', body: fd }).catch(() => ({ ok: false })));
      };
      rec.start(); $('bxm').classList.add('rec'); $('bxm').textContent = 'STOP';
    } catch (e) { log('b', 'I cannot reach the microphone. Please allow access.'); }
  };
})();
