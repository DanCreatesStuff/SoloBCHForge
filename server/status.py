#!/usr/bin/env python3
"""
SoloBCH Forge - status + config service (stdlib async HTTP).

Runs on the asyncio loop (reads live state safely, no threads). Routes:
    GET  /            live dashboard
    GET  /status      JSON snapshot (server + node + miners + blocks)
    GET  /metrics     Prometheus text exposition (for Grafana/Prometheus)
    GET  /history     persisted hashrate/share time-series (dashboard chart)
    GET  /config      settings page
    GET  /config.json current settings (password redacted, env-locked keys listed)
    POST /config      save settings -> config.json (validated), applied live
    POST /test-rpc    probe the node with the saved credentials
    POST /test-webhook  send a test alert to the saved webhook
    POST /reset-stats   zero lifetime counters (found blocks are kept)
    GET/POST /ui-prefs  dashboard card layout
    GET  /diag          progress/result of the "Is this working?" checks
    POST /diag/start, /diag/stop   run / abort those checks

NEVER returns the RPC password. In the Umbrel app this is behind app_proxy auth.
Every state-changing route is POST-only and requires Content-Type:
application/json, which a cross-site HTML form cannot send and a cross-origin
fetch cannot send without a CORS preflight we never grant, so a web page the
operator happens to visit cannot drive this API. CORS is opened only for the
read-only /status, /metrics and /history endpoints.
"""

import asyncio
import json
import re
import time

import config

REQUEST_TIMEOUT = 10.0        # seconds to receive a full request
MAX_HEADERS = 100
MAX_BODY = 65536
MAX_HTTP_CONNECTIONS = 32
CORS_OPEN = ("/status", "/metrics", "/history")   # read-only, safe to share
_CARD_ID = re.compile(r"[a-z0-9_]{1,40}")


def human_hashrate(hps: float) -> str:
    if not hps:
        return "0 H/s"
    for unit in ("H", "KH", "MH", "GH", "TH", "PH"):
        if hps < 1000:
            return f"{hps:.2f} {unit}/s"
        hps /= 1000
    return f"{hps:.2f} EH/s"


def build_snapshot(server, jobs) -> dict:
    now = time.time()
    # The diagnostics self-test client is internal plumbing, not a miner.
    clients = [c for c in server.clients if not getattr(c, "internal", False)]
    miners = [c.stats_snapshot(now) for c in clients]

    def sum_hr(w):
        return sum(c.window_hashrate(w) for c in clients)

    # Lifetime achievements come from the persistent store so they stay visible
    # across reboots and with no miner connected; live values are per-session.
    st = jobs.stats.snapshot()
    live_best = max((c.best_diff for c in clients), default=0.0)
    pool = {
        "hashrate_1m": sum_hr(60),
        "hashrate_5m": sum_hr(300),
        "hashrate_1h": sum_hr(3600),
        "best_diff": max(st["best_diff"], live_best),   # all-time, persisted
        "best_diff_at": st["best_diff_at"],
        "best_diff_worker": st["best_diff_worker"],
        "accepted": st["lifetime_accepted"],            # lifetime, persisted
        "rejected": st["lifetime_rejected"],            # lifetime, persisted
        "accepted_session": sum(c.accepted for c in clients),
        "rejected_session": sum(c.rejected for c in clients),
        "shares_per_sec": sum(c.shares_in(60) for c in clients) / 60.0,
        "workers": len(clients),
        "mining_since": st["first_started"],
        "mining_seconds": st["mining_seconds"],
        "total_hashes": st["lifetime_work"] * (2 ** 32),
        "lifetime_avg_hps": (st["lifetime_work"] * (2 ** 32) / st["mining_seconds"]
                             if st["mining_seconds"] > 0 else 0.0),
    }
    return {
        "server": {
            "name": "SoloBCH Forge",
            "status": "running",
            "stratum_port": server.stratum_port,
            "uptime_s": round(now - server.start_time_wall, 1),
            "connections": len(server.clients),
        },
        "pool": pool,
        "node": jobs.status_snapshot(),
        "miners": miners,
        "generated": now,
    }


METRICS_CT = "text/plain; version=0.0.4; charset=utf-8"


def _mlabel(v) -> str:
    """Escape a Prometheus label value (backslash, quote, newline)."""
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _mnum(x) -> str:
    """Render a value as a Prometheus sample number (None -> NaN)."""
    if x is None:
        return "NaN"
    if isinstance(x, bool):
        return "1" if x else "0"
    if isinstance(x, int):
        return str(x)
    try:
        return repr(float(x))
    except (TypeError, ValueError):
        return "NaN"


def build_metrics(server, jobs) -> str:
    """Render the current snapshot in Prometheus text exposition format.

    Reuses build_snapshot so the numbers match the dashboard exactly. Safe to
    scrape frequently: it only reads already-collected in-memory state.
    """
    s = build_snapshot(server, jobs)
    p, n, srv = s["pool"], s["node"], s["server"]
    lines = []

    def metric(name, typ, help_, samples):
        lines.append(f"# HELP {name} {help_}")
        lines.append(f"# TYPE {name} {typ}")
        for labels, val in samples:
            lbl = "{" + labels + "}" if labels else ""
            lines.append(f"{name}{lbl} {_mnum(val)}")

    metric("solobch_up", "gauge", "1 if the exporter is responding.", [("", 1)])
    metric("solobch_uptime_seconds", "gauge", "Server process uptime.",
           [("", srv["uptime_s"])])
    metric("solobch_stratum_connections", "gauge",
           "Current open Stratum connections.", [("", srv["connections"])])
    metric("solobch_workers", "gauge", "Connected workers.", [("", p["workers"])])
    metric("solobch_hashrate_hps", "gauge",
           "Pool hashrate (hashes/sec) by averaging window.",
           [('window="1m"', p["hashrate_1m"]),
            ('window="5m"', p["hashrate_5m"]),
            ('window="1h"', p["hashrate_1h"])])
    metric("solobch_shares_per_second", "gauge",
           "Accepted shares per second (1m average).", [("", p["shares_per_sec"])])
    metric("solobch_best_difficulty", "gauge",
           "All-time best share difficulty.", [("", p["best_diff"])])
    metric("solobch_shares_accepted_total", "counter",
           "Lifetime accepted shares (persisted).", [("", p["accepted"])])
    metric("solobch_shares_rejected_total", "counter",
           "Lifetime rejected shares (persisted).", [("", p["rejected"])])
    metric("solobch_session_shares_accepted", "gauge",
           "Accepted shares since this process started.",
           [("", p["accepted_session"])])
    metric("solobch_session_shares_rejected", "gauge",
           "Rejected shares since this process started.",
           [("", p["rejected_session"])])
    metric("solobch_total_hashes", "gauge",
           "Estimated lifetime hashes computed.", [("", p["total_hashes"])])
    metric("solobch_lifetime_avg_hps", "gauge",
           "Lifetime average hashrate (hashes/sec).", [("", p["lifetime_avg_hps"])])
    metric("solobch_mining_seconds", "gauge",
           "Accumulated mining time (seconds).", [("", p["mining_seconds"])])
    metric("solobch_blocks_found_total", "counter",
           "Blocks found (all-time).", [("", n.get("blocks_found"))])
    metric("solobch_node_reachable", "gauge",
           "1 if the BCHN node RPC is reachable.", [("", n.get("node_reachable"))])
    metric("solobch_node_synced", "gauge",
           "1 if the node is synced (not in IBD).", [("", n.get("node_ready"))])
    metric("solobch_node_block_height", "gauge",
           "Node best block height.", [("", n.get("blocks"))])
    metric("solobch_network_difficulty", "gauge",
           "Current BCH network difficulty.", [("", n.get("difficulty"))])
    metric("solobch_block_reward_sats", "gauge",
           "Current block reward incl. fees (satoshis).",
           [("", n.get("coinbase_value"))])
    metric("solobch_mining_real", "gauge",
           "1 when serving real templates, 0 when mock (IBD).",
           [("", 1 if n.get("mode") == "real" else 0)])
    metric("solobch_share_difficulty", "gauge",
           "Configured share (vardiff anchor) difficulty.",
           [("", n.get("share_difficulty"))])

    miners = s["miners"]
    if miners:
        def per(key):
            out = []
            for m in miners:
                lbl = (f'worker="{_mlabel(m.get("worker") or "?")}",'
                       f'ip="{_mlabel(m.get("ip") or "?")}"')
                out.append((lbl, m.get(key)))
            return out
        metric("solobch_miner_hashrate_hps", "gauge",
               "Per-miner hashrate (5m average).", per("hashrate_hps"))
        metric("solobch_miner_difficulty", "gauge",
               "Per-miner current share difficulty.", per("difficulty"))
        metric("solobch_miner_best_difficulty", "gauge",
               "Per-miner best share difficulty (session).", per("best_diff"))
        metric("solobch_miner_shares_accepted", "gauge",
               "Per-miner accepted shares (session).", per("accepted"))
        metric("solobch_miner_shares_rejected", "gauge",
               "Per-miner rejected shares (session).", per("rejected"))
        metric("solobch_miner_blocks", "gauge",
               "Per-miner blocks found (all-time).", per("blocks"))
        metric("solobch_miner_last_share_seconds", "gauge",
               "Seconds since this miner's last accepted share.",
               per("last_share_ago"))

    return "\n".join(lines) + "\n"


DASHBOARD_HTML = """<!doctype html><html><head><meta charset="utf-8">
<title>SoloBCH Forge</title><meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='7' fill='%230d1117'/><text x='16' y='24' font-size='21' text-anchor='middle' fill='%233fb950' font-family='sans-serif' font-weight='700'>B</text></svg>">
<style>
:root{color-scheme:dark}
body{margin:0;background:#0d1117;color:#e6edf3;font:14px/1.5 system-ui,sans-serif}
header{padding:16px 20px;background:#161b22;border-bottom:1px solid #30363d;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:30;box-shadow:0 2px 8px rgba(0,0,0,.3)}
h1{margin:0;font-size:18px}h1 span{color:#3fb950}
.btn{color:#58a6ff;text-decoration:none;font-size:13px;border:1px solid #30363d;padding:6px 10px;border-radius:6px;flex:none;background:none;font-family:inherit;line-height:1.5;cursor:pointer}
.hbtns{display:flex;gap:8px;align-items:center;flex:none}
.btn.coffee{color:#e3b341;border-color:#3a2f1a;font-size:15px;padding:5px 10px}
.btn.coffee:hover{border-color:#e3b341}
.editbar{display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:16px}
.editbtn{flex:none;background:#21262d;border:1px solid #30363d;color:#e6edf3;font-size:12px;padding:5px 12px;border-radius:6px;cursor:pointer}
.editbtn.active{background:#238636;border-color:#238636;color:#fff}
.removed{display:flex;flex-wrap:wrap;align-items:center;gap:6px}
.rlabel{color:#8b949e;font-size:11px}
.chip{background:#21262d;border:1px dashed #444c56;color:#adbac7;font-size:12px;padding:4px 10px;border-radius:12px;cursor:pointer;white-space:nowrap}
.chip:hover{color:#e6edf3;border-color:#58a6ff}
.wrap{padding:16px;max-width:1000px;margin:0 auto}
@media (min-width:640px){.wrap{padding:20px}}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:20px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 14px;position:relative}
.card .k{color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:.04em}
.card .v{font-size:18px;font-weight:600;margin-top:4px;word-break:break-all}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(115px,1fr));gap:12px;margin-bottom:16px}
.stat{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;position:relative}
.minus{position:absolute;top:5px;right:5px;width:20px;height:20px;line-height:1;border-radius:50%;background:#f85149;border:0;color:#fff;font-size:15px;cursor:pointer;display:flex;align-items:center;justify-content:center;padding:0;box-shadow:0 1px 3px rgba(0,0,0,.4)}
.minus:hover{background:#da3633}
.editing .stat,.editing .card{outline:1px dashed #30363d;outline-offset:2px}
.stat .k{color:#8b949e;font-size:11px;text-transform:uppercase;letter-spacing:.05em}
.stat .v{font-size:22px;font-weight:700;margin-top:6px;color:#e6edf3}
.stat .v small{font-size:13px;color:#8b949e;font-weight:600}
.stat.hl .v{color:#3fb950}
.groups{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px}
.group{display:grid;grid-template-columns:1fr 1fr;gap:12px;flex:1 1 300px}
/* Tables are mobile-first: each row is a stacked card on phones (label on the
   left via data-label, value on the right — no left/right scrolling), and only
   become a real multi-column table at >=640px. */
table{width:100%;border-collapse:collapse;background:transparent}
thead{display:none}
tr{display:block;background:#161b22;border:1px solid #30363d;border-radius:8px;margin-bottom:10px;overflow:hidden}
td{display:flex;justify-content:space-between;align-items:baseline;gap:16px;padding:7px 12px;border-bottom:1px solid #21262d;font-variant-numeric:tabular-nums;text-align:right;word-break:break-word}
td::before{content:attr(data-label);color:#8b949e;font-size:11px;text-transform:uppercase;letter-spacing:.04em;font-weight:600;text-align:left;flex:none}
tr td:last-child{border-bottom:none}
td.empty{display:block;text-align:center;padding:20px}
td.empty::before{content:none}
@media (min-width:640px){
 table{border:1px solid #30363d;border-radius:8px;overflow:hidden}
 thead{display:table-header-group}
 tr{display:table-row;background:none;border:0;border-radius:0;margin:0}
 th,td{display:table-cell;text-align:left;padding:8px 10px;border-bottom:1px solid #21262d;font-variant-numeric:tabular-nums;word-break:normal;gap:0}
 th{color:#8b949e;font-size:12px;text-transform:uppercase;letter-spacing:.04em}
 td::before{content:none}
 tr:last-child td{border-bottom:none}
}
.ok{color:#3fb950}.warn{color:#d29922}.bad{color:#f85149}
.dim{color:#8b949e}.empty{padding:20px;text-align:center;color:#8b949e}
.panel{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 14px;margin-bottom:16px}
.panel .k{color:#8b949e;font-size:11px;text-transform:uppercase;letter-spacing:.05em;display:flex;justify-content:space-between}
.panel svg{display:block;width:100%;height:48px;margin-top:8px}
.bar{height:8px;background:#21262d;border-radius:4px;overflow:hidden;margin-top:8px}
.bar>i{display:block;height:100%;background:#3fb950;border-radius:4px;transition:width .4s}
.copy{cursor:pointer;border-bottom:1px dotted #8b949e}.copy:hover{color:#e6edf3}
.lnk{color:#58a6ff;text-decoration:none}.lnk:hover{text-decoration:underline}
#toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:#238636;color:#fff;padding:8px 14px;border-radius:6px;font-size:13px;box-shadow:0 2px 8px rgba(0,0,0,.4);z-index:60}
.modal{position:fixed;inset:0;background:rgba(1,4,9,.72);z-index:50;display:flex;align-items:center;justify-content:center;padding:16px}
.modal[hidden]{display:none}
.dialog{background:#161b22;border:1px solid #30363d;border-radius:12px;max-width:440px;width:100%;padding:16px 18px 20px;box-shadow:0 8px 32px rgba(0,0,0,.5);max-height:90vh;overflow:auto}
.dhead{display:flex;align-items:center;justify-content:space-between;gap:12px}
.dhead h3{margin:0;font-size:16px}
.x{background:none;border:0;color:#8b949e;font-size:24px;line-height:1;cursor:pointer;padding:0 4px}
.x:hover{color:#e6edf3}
.ddesc{color:#8b949e;font-size:13px;margin:10px 0 4px}
.addr{display:flex;align-items:center;gap:10px;background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px 12px;margin-top:10px;cursor:pointer}
.addr:hover{border-color:#58a6ff}
.addr .chain{flex:none;width:38px;font-weight:700;font-size:13px;color:#3fb950}
.addr .a{flex:1;min-width:0;font-size:12px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#adbac7;word-break:break-all}
.addr .ci{flex:none;color:#8b949e;font-size:16px}
</style></head><body>
<header><h1>SoloBCH <span>Forge</span></h1>
<div class="hbtns"><button class="btn coffee" title="Buy me a coffee" onclick="openDonate()">&#9749;</button>
<a class="btn" href="/config">&#9881; Settings</a></div></header>
<div class="wrap">
<div class="editbar"><button id="editBtn" class="editbtn" onclick="toggleEdit()">Edit layout</button>
<div id="removed" class="removed" hidden></div></div>
<div class="groups"><div class="group" id="grpA"></div><div class="group" id="grpB"></div></div>
<div class="stats" id="odds"></div>
<div class="stats" id="lifetime"></div>
<div class="panel" id="sparkPanel" hidden><div class="k"><span>Hashrate <span class="dim">(live, this session)</span></span><span id="sparkNow"></span></div><svg id="spark" viewBox="0 0 300 48" preserveAspectRatio="none"></svg></div>
<div class="panel" id="histPanel" hidden><div class="k"><span>Hashrate history <span class="dim" id="histSpan"></span></span><span id="histMax"></span></div><svg id="histChart" viewBox="0 0 600 120" preserveAspectRatio="none" style="height:120px"></svg></div>
<div class="cards" id="cards"></div>
<table><thead><tr><th>Miner</th><th>IP</th><th>Model</th><th>Payout (BCH)</th><th>Hashrate</th>
<th>Diff</th><th>Best diff</th><th>Blocks</th><th>Accepted</th><th>Rejected</th><th>Last share</th></tr></thead>
<tbody id="miners"></tbody></table>
<div id="blocks"></div>
<p class="dim" id="foot"></p>
</div>
<div id="donate" class="modal" hidden onclick="if(event.target===this)closeDonate()">
 <div class="dialog">
  <div class="dhead"><h3>&#9749; Buy me a coffee</h3><button class="x" title="Close" onclick="closeDonate()">&times;</button></div>
  <p class="ddesc">If SoloBCH Forge is useful to you, a tip is hugely appreciated and helps keep it maintained. Tap an address to copy it.</p>
  <div class="addr" onclick="cp('bitcoincash:qpp5dt6yh7u2dtm9ted23dmdnfpf99yrmcuzs6fhns')"><div class="chain">BCH</div><div class="a">bitcoincash:qpp5dt6yh7u2dtm9ted23dmdnfpf99yrmcuzs6fhns</div><span class="ci">&#10697;</span></div>
  <div class="addr" onclick="cp('bc1q5z24l8kuz3ht4zytk9la2mxj7tlhgavw6j9k00')"><div class="chain">BTC</div><div class="a">bc1q5z24l8kuz3ht4zytk9la2mxj7tlhgavw6j9k00</div><span class="ci">&#10697;</span></div>
  <div class="addr" onclick="cp('0xeaA00aD2bd100126a0E591b25A564d8dE4b05c0C')"><div class="chain">ETH</div><div class="a">0xeaA00aD2bd100126a0E591b25A564d8dE4b05c0C</div><span class="ci">&#10697;</span></div>
 </div>
</div>
<div id="toast" hidden></div>
<script>
function ago(s){if(s==null)return '—';s=Math.round(s);if(s<60)return s+'s';
if(s<3600)return Math.floor(s/60)+'m '+(s%60)+'s';return Math.floor(s/3600)+'h '+Math.floor(s%3600/60)+'m';}
let editing=false;let hidden=new Set();
const CARD_LABELS={best_diff:'Best difficulty',hr_1m:'Hashrate 1m',hr_5m:'Hashrate 5m',hr_1h:'Hashrate 1h',accepted:'Accepted',rejected:'Rejected',shares_per_sec:'Shares/s',workers:'Workers',net_diff:'Network difficulty',reward:'Block reward',expected_block:'Expected block',best_vs_block:'Best share vs block',total_hashes:'Total hashes',lifetime_avg:'Lifetime avg',mining_time:'Mining time',node:'Node',mining:'Mining',stratum_port:'Stratum port',block_height:'Block height',difficulty:'Difficulty',uptime:'Uptime'};
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function minusBtn(id){return editing?'<button class="minus" title="Hide card" onclick="hideCard(\\''+id+'\\')">&minus;</button>':'';}
function statX(id,k,inner,hl){if(hidden.has(id))return '';return '<div class="stat'+(hl?' hl':'')+'" data-card="'+id+'">'+minusBtn(id)+'<div class="k">'+k+'</div>'+inner+'</div>';}
function stat(id,k,v,hl){return statX(id,k,'<div class="v">'+v+'</div>',hl);}
function card(id,k,v,cls){if(hidden.has(id))return '';return '<div class="card" data-card="'+id+'">'+minusBtn(id)+'<div class="k">'+k+'</div><div class="v '+(cls||'')+'">'+v+'</div></div>';}
async function loadPrefs(){try{const r=await (await fetch('/ui-prefs')).json();hidden=new Set(r.hidden_cards||[]);}catch(e){}}
async function savePrefs(){try{await fetch('/ui-prefs',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({hidden_cards:[...hidden]})});}catch(e){}}
function renderRemoved(){
 const el=document.getElementById('removed');
 if(!editing){el.hidden=true;el.innerHTML='';return;}
 el.hidden=false;const ids=[...hidden];
 el.innerHTML='<span class="rlabel">Hidden'+(ids.length?' — tap to restore:':': none')+'</span>'+
  ids.map(id=>'<button class="chip" data-id="'+esc(id)+'" onclick="showCard(this.dataset.id)">+ '+esc(CARD_LABELS[id]||id)+'</button>').join('');
}
function toggleEdit(){editing=!editing;const b=document.getElementById('editBtn');b.textContent=editing?'Done':'Edit layout';b.classList.toggle('active',editing);document.body.classList.toggle('editing',editing);renderRemoved();tick();}
function hideCard(id){hidden.add(id);savePrefs();renderRemoved();tick();}
function showCard(id){hidden.delete(id);savePrefs();renderRemoved();tick();}
function humanHR(h){if(!h)return '0 <small>H/s</small>';const u=['H','KH','MH','GH','TH','PH'];let i=0;while(h>=1000&&i<u.length-1){h/=1000;i++;}return h.toFixed(2)+' <small>'+u[i]+'/s</small>';}
function humanNum(n){if(!n)return '0';const u=['','K','M','G','T','P'];let i=0;while(n>=1000&&i<u.length-1){n/=1000;i++;}return n.toFixed(i?2:0)+(u[i]?' <small>'+u[i]+'</small>':'');}
function humanHashes(h){if(!h)return '0 <small>H</small>';const u=['H','KH','MH','GH','TH','PH','EH','ZH'];let i=0;while(h>=1000&&i<u.length-1){h/=1000;i++;}return h.toFixed(2)+' <small>'+u[i]+'</small>';}
function shortaddr(a){if(!a)return '<span class="bad">— none</span>';a=a.replace(/^bitcoincash:/,'');return a.length>22?a.slice(0,12)+'…'+a.slice(-6):a;}
function copyAddr(a){if(!a)return '<span class="bad">— none</span>';return '<span class="copy" title="Click to copy '+esc(a)+'" data-a="'+esc(a)+'" onclick="cp(this.dataset.a)">'+esc(shortaddr(a))+'</span>';}
let _toastT;function toast(m){const el=document.getElementById('toast');if(!el)return;el.textContent=m;el.hidden=false;clearTimeout(_toastT);_toastT=setTimeout(()=>{el.hidden=true;},1500);}
function cp(t){
 let ok=false;
 try{if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(t);ok=true;}}catch(e){}
 if(!ok){try{const ta=document.createElement('textarea');ta.value=t;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();ok=document.execCommand('copy');document.body.removeChild(ta);}catch(e){}}
 toast(ok?'Copied address':'Copy failed — long-press to select');
}
function humanTime(s){if(!isFinite(s)||s<=0)return '—';const y=s/31557600;if(y>=1)return y>=1000?(y/1000).toFixed(1)+' <small>k yr</small>':y.toFixed(y>=10?0:1)+' <small>yr</small>';const d=s/86400;if(d>=1)return d.toFixed(d>=10?0:1)+' <small>d</small>';const h=s/3600;if(h>=1)return h.toFixed(1)+' <small>h</small>';return Math.max(1,Math.round(s/60))+' <small>min</small>';}
function humanSpan(s){s=Math.round(s);const d=Math.floor(s/86400);const h=Math.floor(s%86400/3600);const m=Math.floor(s%3600/60);if(d)return d+'d '+h+'h';if(h)return h+'h '+m+'m';return m+'m';}
async function drawHistory(){
 try{
  const r=await (await fetch('/history')).json();
  const pts=(r.points||[]).filter(p=>Array.isArray(p)&&p.length>=2);
  const panel=document.getElementById('histPanel');
  if(pts.length<2){panel.hidden=true;return;}
  panel.hidden=false;
  const hrs=pts.map(p=>p[1]);
  const mx=Math.max(...hrs)||1,W=600,H=120,pad=6;
  const X=i=>i/(pts.length-1)*W;
  const Y=h=>H-pad-(h/mx)*(H-2*pad);
  const line=pts.map((p,i)=>X(i).toFixed(1)+','+Y(p[1]).toFixed(1)).join(' ');
  document.getElementById('histChart').innerHTML=
    '<polygon fill="rgba(63,185,80,.12)" points="0,'+H+' '+line+' '+W+','+H+'"/>'+
    '<polyline fill="none" stroke="#3fb950" stroke-width="2" points="'+line+'"/>';
  document.getElementById('histMax').innerHTML='peak '+humanHR(mx);
  document.getElementById('histSpan').textContent=humanSpan(pts[pts.length-1][0]-pts[0][0]);
 }catch(e){}
}
const hrHist=[];
function drawSpark(v){
 hrHist.push(v);if(hrHist.length>150)hrHist.shift();
 const panel=document.getElementById('sparkPanel');
 if(hrHist.length<2){panel.hidden=true;return;}
 panel.hidden=false;
 const mx=Math.max(...hrHist)||1,W=300,H=48,pad=3;
 const pts=hrHist.map((h,i)=>{const x=i/(hrHist.length-1)*W;const y=H-pad-(h/mx)*(H-2*pad);return x.toFixed(1)+','+y.toFixed(1);}).join(' ');
 document.getElementById('spark').innerHTML='<polyline fill="none" stroke="#3fb950" stroke-width="2" points="'+pts+'"/>';
 document.getElementById('sparkNow').innerHTML=humanHR(hrHist[hrHist.length-1]);
}
async function tick(){
 try{
  const s=await (await fetch('/status')).json();
  const n=s.node, nodeCls=n.node_reachable?(n.node_ready?'ok':'warn'):'bad';
  const nodeTxt=n.node_reachable?(n.node_ready?'Synced':'Syncing '+((n.progress||0)*100).toFixed(2)+'%'):'Unreachable';
  const p=s.pool||{};
  const allTime='<span class="dim" style="text-transform:none;letter-spacing:0;font-size:10px"> · all-time</span>';
  document.getElementById('grpA').innerHTML=
    stat('best_diff','Best difficulty'+allTime,humanNum(p.best_diff),true)
   +stat('hr_1m','Hashrate 1m',humanHR(p.hashrate_1m))
   +stat('hr_5m','Hashrate 5m',humanHR(p.hashrate_5m),true)
   +stat('hr_1h','Hashrate 1h',humanHR(p.hashrate_1h));
  document.getElementById('grpB').innerHTML=
    stat('accepted','Accepted'+allTime,humanNum(p.accepted))
   +stat('rejected','Rejected'+allTime,humanNum(p.rejected))
   +stat('shares_per_sec','Shares/s',(p.shares_per_sec||0).toFixed(2))
   +stat('workers','Workers',p.workers||0);
  const reward = n.coinbase_value!=null ? (n.coinbase_value/1e8) : null;
  const hrEff = p.hashrate_1h||p.hashrate_5m||p.hashrate_1m||0;
  const ttb = (n.difficulty && hrEff>0) ? n.difficulty*Math.pow(2,32)/hrEff : NaN;
  const bestPct = (n.difficulty && p.best_diff>0) ? (p.best_diff/n.difficulty*100) : null;
  const bestRatio = (n.difficulty && p.best_diff>0) ? (n.difficulty/p.best_diff) : null;
  const bestTxt = bestRatio==null ? '—'
                : bestRatio<=1 ? '<span class="ok">block!</span>'
                : '1 in '+humanNum(bestRatio);
  document.getElementById('odds').innerHTML=
    stat('net_diff','Network difficulty', n.difficulty!=null?humanNum(n.difficulty):'—')
   +stat('reward','Block reward', reward!=null?reward.toFixed(3)+' <small>BCH</small>':'—')
   +stat('expected_block','Expected block', humanTime(ttb), true)
   +statX('best_vs_block','Best share vs block','<div class="v">'+bestTxt+'</div>'
     +(bestPct!=null?'<div class="bar"><i style="width:'+Math.min(100,Math.max(0.6,bestPct)).toFixed(2)+'%"></i></div>':''));
  document.getElementById('lifetime').innerHTML=
    stat('total_hashes','Total hashes'+allTime,humanHashes(p.total_hashes||0),true)
   +stat('lifetime_avg','Lifetime avg'+allTime,humanHR(p.lifetime_avg_hps||0))
   +stat('mining_time','Mining time'+allTime,humanTime(p.mining_seconds||0));
  drawSpark(p.hashrate_1m||0);
  let c='';
  c+=card('node','Node', nodeTxt, nodeCls);
  c+=card('mining','Mining', n.mode==='real'?'● Live':'○ Mock', n.mode==='real'?'ok':'warn');
  c+=card('stratum_port','Stratum port',s.server.stratum_port);
  c+=card('block_height','Block height',n.blocks!=null?n.blocks.toLocaleString():'—');
  c+=card('difficulty','Difficulty',(n.vardiff&&n.vardiff.enabled)?('Auto · '+n.vardiff.target_spm+'/min'):('Fixed '+n.share_difficulty));
  c+=card('uptime','Uptime',ago(s.server.uptime_s));
  document.getElementById('cards').innerHTML=c;
  const mb=document.getElementById('miners');
  if(!s.miners.length){mb.innerHTML='<tr><td colspan="11" class="empty">No miners connected</td></tr>';}
  else{mb.innerHTML=s.miners.map(m=>'<tr>'+
   '<td data-label="Miner">'+esc(m.worker||'—')+(m.authorized?'':' <span class="dim">(connecting)</span>')+'</td>'+
   '<td data-label="IP">'+esc(m.ip)+'</td>'+
   '<td data-label="Model" class="dim">'+esc(m.model||'—')+'</td>'+
   '<td data-label="Payout" class="dim">'+copyAddr(m.payout)+'</td>'+
   '<td data-label="Hashrate" class="ok">'+esc(m.hashrate)+'</td>'+
   '<td data-label="Diff" class="dim">'+(m.difficulty!=null?humanNum(m.difficulty):'—')+'</td>'+
   '<td data-label="Best diff" class="dim">'+humanNum(m.best_diff)+'</td>'+
   '<td data-label="Blocks"'+(m.blocks?' class="ok"':'')+'>'+(m.blocks||0)+'</td>'+
   '<td data-label="Accepted">'+m.accepted+'</td>'+
   '<td data-label="Rejected"'+(m.rejected?' class="warn"':'')+'>'+m.rejected+'</td>'+
   '<td data-label="Last share">'+ago(m.last_share_ago)+'</td>'+
   '</tr>').join('');}
  const bl=n.recent_blocks||[];
  document.getElementById('blocks').innerHTML = bl.length ?
   ('<h3 style="margin:24px 0 8px">Blocks</h3><table><thead><tr><th>Miner</th><th>Height</th><th>Hash</th>'
    +'<th>Status</th><th>Conf</th><th>Payout</th><th>When</th></tr></thead><tbody>'
    +bl.map(b=>{
      const acc=String(b.status).startsWith('accepted');
      const conf=!acc?null:(b.confirmations!=null?b.confirmations:((n.blocks!=null&&b.height!=null)?Math.max(0,n.blocks-b.height+1):null));
      const ctxt=conf==null?'—':(b.mature?conf+' · spendable':conf+' / 100');
      const hash=b.hash||'';
      const hcell=hash?'<a class="lnk" href="https://blockchair.com/bitcoin-cash/block/'+encodeURIComponent(hash)+'" target="_blank" rel="noopener">'+esc(hash.slice(0,20))+'…</a>':'—';
      return '<tr>'+
       '<td data-label="Miner">'+esc(b.worker||'—')+'</td>'+
       '<td data-label="Height">'+esc(b.height)+'</td>'+
       '<td data-label="Hash" class="dim">'+hcell+'</td>'+
       '<td data-label="Status" class="'+(acc?'ok':'bad')+'">'+esc(b.status)+'</td>'+
       '<td data-label="Conf" class="dim">'+esc(ctxt)+'</td>'+
       '<td data-label="Payout" class="dim">'+copyAddr(b.payout)+'</td>'+
       '<td data-label="When" class="dim">'+ago((Date.now()/1000)-b.time)+' ago</td>'+
       '</tr>';}).join('')
    +'</tbody></table>') : '';
  let foot='Updated '+new Date().toLocaleTimeString();
  if(p.mining_since){foot='Mining since '+new Date(p.mining_since*1000).toLocaleDateString()+' · '+foot;}
  document.getElementById('foot').textContent=foot;
 }catch(e){document.getElementById('foot').textContent='status fetch failed: '+e;}
}
function openDonate(){document.getElementById('donate').hidden=false;}
function closeDonate(){document.getElementById('donate').hidden=true;}
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeDonate();});
loadPrefs().then(()=>{tick();drawHistory();setInterval(tick,2000);setInterval(drawHistory,60000);});
</script></body></html>"""


CONFIG_HTML = """<!doctype html><html><head><meta charset="utf-8">
<title>SoloBCH Forge · Settings</title><meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='7' fill='%230d1117'/><text x='16' y='24' font-size='21' text-anchor='middle' fill='%233fb950' font-family='sans-serif' font-weight='700'>B</text></svg>">
<style>
:root{color-scheme:dark}
body{margin:0;background:#0d1117;color:#e6edf3;font:14px/1.5 system-ui,sans-serif}
header{padding:16px 20px;background:#161b22;border-bottom:1px solid #30363d;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:30;box-shadow:0 2px 8px rgba(0,0,0,.3)}
h1{margin:0;font-size:18px}h1 span{color:#3fb950}
a.btn{color:#58a6ff;text-decoration:none;font-size:13px;border:1px solid #30363d;padding:6px 10px;border-radius:6px}
.wrap{padding:20px;max-width:900px;margin:0 auto}
.grid{display:flex;flex-direction:column;gap:16px}
.col{display:flex;flex-direction:column;gap:16px}
@media (min-width:700px){.grid{flex-direction:row;align-items:flex-start}.col{flex:1;min-width:0}}
.sec{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:2px 16px 16px}
label{display:block;margin:14px 0 4px;color:#8b949e;font-size:13px}
input{width:100%;box-sizing:border-box;background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:9px 10px;border-radius:6px;font:14px system-ui}
.row{display:flex;gap:12px}.row>div{flex:1}
button{margin-top:20px;background:#238636;border:0;color:#fff;padding:10px 16px;border-radius:6px;font-size:14px;cursor:pointer}
button.alt{margin-top:0;background:#21262d;border:1px solid #30363d;color:#e6edf3}
select{width:100%;box-sizing:border-box;background:#0d1117;border:1px solid #30363d;color:#e6edf3;padding:9px 10px;border-radius:6px;font:14px system-ui}
h2{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:#8b949e;margin:16px 0 0}
.sec>h2:first-child{margin-top:14px}
.inline{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px}
.note{color:#8b949e;font-size:12px;margin-top:4px}
.toggles{margin-top:12px}
.toggle{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:11px 0;border-bottom:1px solid #21262d}
.toggle:last-child{border-bottom:none}
.toggle .tlabel{color:#e6edf3;font-size:14px}
.toggle .txt{min-width:0}
.toggle .note{margin-top:2px}
.switch{position:relative;display:inline-block;width:42px;height:24px;flex:none}
.switch input{opacity:0;width:0;height:0}
.slider{position:absolute;cursor:pointer;inset:0;background:#30363d;border-radius:24px;transition:.2s}
.slider:before{content:"";position:absolute;height:18px;width:18px;left:3px;top:3px;background:#e6edf3;border-radius:50%;transition:.2s}
.switch input:checked+.slider{background:#238636}
.switch input:checked+.slider:before{transform:translateX(18px)}
.switch input:focus-visible+.slider{outline:2px solid #58a6ff;outline-offset:2px}
#save{width:100%;margin-top:16px}
#msg{margin-top:14px}
.diag{list-style:none;padding:0;margin:12px 0 0}
.diag li{display:flex;gap:10px;align-items:flex-start;padding:7px 0;border-bottom:1px solid #21262d;font-size:13px}
.diag li:last-child{border-bottom:none}
.diag .ic{flex:none;width:20px;text-align:center;font-weight:700}
.diag .nm{flex:none;width:230px;max-width:45%;color:#e6edf3}
.diag .dt{color:#8b949e;min-width:0;word-break:break-word}
.diag li.pass .ic{color:#3fb950}.diag li.fail .ic{color:#f85149}.diag li.warn .ic{color:#d29922}
.diag li.running .ic{color:#58a6ff}.diag li.skip .ic,.diag li.pending .ic{color:#484f58}
.diag li.skip .nm,.diag li.pending .nm{color:#8b949e}
.diagbox{margin-top:12px;padding:12px 14px;border-radius:8px;border:1px solid;font-size:13px;line-height:1.5;color:#e6edf3}
.diagbox.bad{border-color:#f85149;background:rgba(248,81,73,.08)}
.diagbox.warn{border-color:#d29922;background:rgba(210,153,34,.08)}
.diagbox b{display:block;margin-bottom:2px}
.diagbox p{margin:6px 0 0}
</style></head><body>
<header><h1>SoloBCH <span>Forge</span> · Settings</h1><a class="btn" href="/">&#8592; Dashboard</a></header>
<div class="wrap">
<div class="sec">
<h2>Is this working?</h2>
<div class="note">Checks the whole chain step by step: settings, the node, block building, the Stratum port, your miner and its shares. It stops at the first problem and shows a potential fix. Takes a few seconds, or up to about five minutes while it waits for your miner and its shares.</div>
<div class="inline"><button id="diagrun" class="alt">Run checks</button><button id="diagstop" class="alt" hidden>Stop</button><span id="diagmsg" class="note"></span></div>
<ol id="diaglist" class="diag" hidden></ol>
<div id="diagerr" class="diagbox bad" hidden></div>
<div id="diagwarn" class="diagbox warn" hidden></div>
</div>

<div class="grid">
<div class="col">
<div class="sec">
<h2>Node RPC</h2>
<label>BCHN RPC host</label><input id="bchn_rpc_host">
<div class="row"><div><label>RPC port</label><input id="bchn_rpc_port"></div>
<div><label>RPC user</label><input id="bchn_rpc_user"></div></div>
<label>RPC password</label><input id="bchn_rpc_password" type="password" placeholder="leave blank to keep current">
<div class="note" id="envnote" hidden>Greyed-out fields are provided automatically by Umbrel from the Bitcoin Cash Node app and cannot be changed here.</div>
<div class="note">Elsewhere, copy these from the Bitcoin Cash Node app's "Node RPC" panel. Stored in config.json, never shown again.</div>
<div class="inline"><button id="testrpc" class="alt">Test connection</button><span id="rpcmsg" class="note"></span></div>
</div>

<div class="sec">
<h2>Notifications</h2>
<label>Webhook URL <span class="note">(blank = disabled)</span></label><input id="webhook_url" placeholder="https://…">
<div class="note">A JSON POST is sent to this URL for each alert you switch on below (Discord webhook or any custom endpoint).</div>
<div class="toggles">
<div class="toggle"><div class="txt"><div class="tlabel">Block found</div><div class="note">The moment your hardware finds a BCH block.</div></div><label class="switch"><input type="checkbox" id="notify_block"><span class="slider"></span></label></div>
<div class="toggle"><div class="txt"><div class="tlabel">New best share</div><div class="note">When a new all-time best share difficulty is set (rare).</div></div><label class="switch"><input type="checkbox" id="notify_best_share"><span class="slider"></span></label></div>
<div class="toggle"><div class="txt"><div class="tlabel">Miner offline</div><div class="note">When a connected miner stops sharing for over 2 minutes.</div></div><label class="switch"><input type="checkbox" id="notify_miner_offline"><span class="slider"></span></label></div>
</div>
<div class="inline"><button id="testhook" class="alt">Send test webhook</button><span id="hookmsg" class="note"></span></div>
</div>
</div>

<div class="col">
<div class="sec">
<h2>Difficulty</h2>
<label>Share difficulty <span class="note">(fixed value, and the vardiff starting point)</span></label><input id="share_difficulty">
<label>Variable difficulty</label>
<select id="vardiff_enabled"><option value="true">On — auto-tune each worker</option><option value="false">Off — use fixed difficulty</option></select>
<div class="row"><div><label>Target shares/min</label><input id="vardiff_target_spm"></div>
<div><label>Min diff</label><input id="vardiff_min"></div>
<div><label>Max diff</label><input id="vardiff_max"></div></div>
<div class="note">With vardiff on, each miner is retargeted toward the share rate above (bounded by min/max). Off pins everyone to the fixed share difficulty.</div>
</div>

<div class="sec">
<h2>Network</h2>
<div class="row"><div><label>Stratum port <span class="note">(restart)</span></label><input id="stratum_port"></div>
<div><label>Status port <span class="note">(restart)</span></label><input id="status_port"></div></div>
</div>

<div class="sec">
<h2>Reset stats</h2>
<div class="note">Clears all-time best difficulty, lifetime accepted/rejected shares, total hashes and mining time. Found blocks are kept. This cannot be undone.</div>
<div class="inline"><button id="resetstats" class="alt" style="color:#f85149;border-color:#f85149">Reset lifetime stats</button><span id="resetmsg" class="note"></span></div>
</div>

<div class="sec">
<h2>Backup</h2>
<div class="note">Download these settings as a JSON file, or restore them from one. The RPC password is never included in the export — after importing, re-enter it (or leave it, if you're restoring onto the same node).</div>
<div class="inline"><button id="exportcfg" class="alt">Export settings</button><button id="importcfg" class="alt">Import settings</button><input id="importfile" type="file" accept="application/json,.json" hidden><span id="backupmsg" class="note"></span></div>
</div>
</div>
</div>

<button id="save">Save settings</button>
<div id="msg"></div>
</div>
<script>
const FIELDS=['bchn_rpc_host','bchn_rpc_port','bchn_rpc_user','stratum_port','status_port','share_difficulty','vardiff_enabled','vardiff_target_spm','vardiff_min','vardiff_max','webhook_url'];
const BOOL_FIELDS=['vardiff_enabled'];
const CHECK_FIELDS=['notify_block','notify_best_share','notify_miner_offline'];
const JSON_HDR={'content-type':'application/json'};
async function postJSON(url,body){return (await fetch(url,{method:'POST',headers:JSON_HDR,body:JSON.stringify(body||{})})).json();}
async function load(){
 const c=await (await fetch('/config.json')).json();
 FIELDS.forEach(f=>{const el=document.getElementById(f);if(!el)return;el.value=BOOL_FIELDS.includes(f)?String(!!c[f]):(c[f]==null?'':c[f]);});
 CHECK_FIELDS.forEach(f=>{const el=document.getElementById(f);if(el)el.checked=!!c[f];});
 // Keys pinned by the environment (Umbrel injects the node's RPC details) are
 // shown but locked, instead of silently ignoring what the user types.
 const locked=new Set(c.env_locked||[]);
 locked.forEach(f=>{const el=document.getElementById(f);if(!el)return;el.disabled=true;el.title='Set by the environment; edits here are ignored.';if(f==='bchn_rpc_password')el.placeholder='provided automatically';});
 if(locked.size)document.getElementById('envnote').hidden=false;
}
function collect(){
 const body={};FIELDS.forEach(f=>{const el=document.getElementById(f);if(!el.disabled)body[f]=el.value;});
 CHECK_FIELDS.forEach(f=>{body[f]=document.getElementById(f).checked;});
 const pwEl=document.getElementById('bchn_rpc_password');
 if(pwEl.value&&!pwEl.disabled)body.bchn_rpc_password=pwEl.value;
 return body;
}
function num(id){return parseFloat(document.getElementById(id).value);}
function validate(){
 const port=v=>Number.isInteger(v)&&v>=1&&v<=65535;
 const sp=num('stratum_port'),st=num('status_port'),sd=num('share_difficulty'),
   tg=num('vardiff_target_spm'),mn=num('vardiff_min'),mx=num('vardiff_max');
 if(!port(sp)||!port(st))return 'Ports must be whole numbers 1–65535';
 if(sp===st)return 'Stratum and status ports must differ';
 if(!(sd>=1))return 'Share difficulty must be ≥ 1';
 if(!(tg>=1))return 'Target shares/min must be ≥ 1';
 if(!(mn>=1))return 'Min diff must be ≥ 1';
 if(!(mx>=mn))return 'Max diff must be ≥ min diff';
 const wh=document.getElementById('webhook_url').value.trim().toLowerCase();
 if(wh&&!(wh.startsWith('http://')||wh.startsWith('https://')))return 'Webhook URL must start with http:// or https://';
 return '';
}
async function save(){
 const msg=document.getElementById('msg');
 const err=validate();
 if(err){msg.innerHTML='<span style="color:#f85149">'+err+'</span>';return false;}
 msg.innerHTML='Saving…';
 try{
  const r=await postJSON('/config',collect());
  msg.innerHTML = r.ok ? '<span style="color:#3fb950">Saved. RPC, difficulty, vardiff &amp; webhook applied live; port changes need an app restart.</span>'
                       : '<span style="color:#f85149">Error: '+(r.error||'unknown')+'</span>';
  return !!r.ok;
 }catch(e){msg.innerHTML='<span style="color:#f85149">Request failed: '+e+'</span>';return false;}
}
document.getElementById('save').onclick=save;
document.getElementById('testrpc').onclick=async()=>{
 const m=document.getElementById('rpcmsg');m.textContent='saving + testing…';
 if(!(await save())){m.textContent='';return;}
 try{const r=await postJSON('/test-rpc');
  m.innerHTML = r.ok ? '<span style="color:#3fb950">OK — chain '+r.chain+', height '+r.blocks+(r.ibd?' (syncing)':' (synced)')+'</span>'
                     : '<span style="color:#f85149">Failed: '+(r.error||'?')+'</span>';
 }catch(e){m.innerHTML='<span style="color:#f85149">'+e+'</span>';}
};
document.getElementById('testhook').onclick=async()=>{
 const m=document.getElementById('hookmsg');m.textContent='saving + sending…';
 if(!(await save())){m.textContent='';return;}
 try{const r=await postJSON('/test-webhook');
  m.innerHTML = r.ok ? '<span style="color:#3fb950">Sent — check your endpoint</span>'
                     : '<span style="color:#f85149">Failed: '+(r.error||'?')+'</span>';
 }catch(e){m.innerHTML='<span style="color:#f85149">'+e+'</span>';}
};
document.getElementById('resetstats').onclick=async()=>{
 if(!confirm('Reset all-time best difficulty, lifetime share counts, total hashes and mining time?\\n\\nFound blocks are kept. This cannot be undone.'))return;
 const m=document.getElementById('resetmsg');m.textContent='resetting…';
 try{const r=await postJSON('/reset-stats');
  m.innerHTML = r.ok ? '<span style="color:#3fb950">Lifetime stats reset.</span>'
                     : '<span style="color:#f85149">Failed: '+(r.error||'?')+'</span>';
 }catch(e){m.innerHTML='<span style="color:#f85149">'+e+'</span>';}
};
document.getElementById('exportcfg').onclick=async()=>{
 const m=document.getElementById('backupmsg');
 try{
  const c=await (await fetch('/config.json')).json();
  delete c.bchn_rpc_password;                       // never export the secret
  delete c.env_locked;                              // runtime info, not a setting
  const blob=new Blob([JSON.stringify(c,null,2)],{type:'application/json'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download='solobch-forge-config.json';document.body.appendChild(a);a.click();
  a.remove();URL.revokeObjectURL(a.href);
  m.innerHTML='<span style="color:#3fb950">Downloaded settings JSON.</span>';
 }catch(e){m.innerHTML='<span style="color:#f85149">Export failed: '+e+'</span>';}
};
document.getElementById('importcfg').onclick=()=>document.getElementById('importfile').click();
document.getElementById('importfile').onchange=async(ev)=>{
 const m=document.getElementById('backupmsg');const f=ev.target.files[0];
 if(!f){return;}
 try{
  const data=JSON.parse(await f.text());
  if(!data||typeof data!=='object')throw new Error('not a settings object');
  // Load known fields into the form, then Save (reuses validation + live apply).
  FIELDS.forEach(fld=>{if(data[fld]!=null){const el=document.getElementById(fld);if(el)el.value=BOOL_FIELDS.includes(fld)?String(!!data[fld]):data[fld];}});
  CHECK_FIELDS.forEach(fld=>{if(data[fld]!=null){const el=document.getElementById(fld);if(el)el.checked=!!data[fld];}});
  m.innerHTML='<span style="color:#3fb950">Imported — applying…</span>';
  await save();
 }catch(e){m.innerHTML='<span style="color:#f85149">Import failed: '+e+'</span>';}
 ev.target.value='';                                // allow re-importing the same file
};
// ---- "Is this working?" diagnostics ------------------------------------
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
const DIAG_ICON={pending:'&#9675;',running:'&#8987;',pass:'&#10003;',warn:'!',fail:'&#10007;',skip:'&ndash;'};
let diagTimer=null;
function renderDiag(d){
 const list=document.getElementById('diaglist');list.hidden=false;
 const checks=d.checks||[];
 list.innerHTML=checks.map(c=>'<li class="'+esc(c.status)+'"><span class="ic">'+(DIAG_ICON[c.status]||'?')+'</span><span class="nm">'+esc(c.name)+'</span><span class="dt">'+esc(c.detail||'')+'</span></li>').join('');
 const msg=document.getElementById('diagmsg'),el=Math.round(d.elapsed||0);
 const warns=checks.filter(c=>c.status==='warn');
 const eb=document.getElementById('diagerr'),wb=document.getElementById('diagwarn');
 if(d.running){msg.textContent='running… '+el+'s';}
 else if(d.result==='pass'){msg.innerHTML='<span style="color:#3fb950">Everything is working'+(warns.length?' ('+warns.length+' warning'+(warns.length>1?'s':'')+' below)':'')+' &middot; '+el+'s</span>';}
 else if(d.result==='fail'){msg.innerHTML='<span style="color:#f85149">Stopped at: '+esc(d.error&&d.error.check||'?')+'</span>';}
 else if(d.result==='stopped'){msg.textContent='stopped';}
 else msg.textContent='';
 if(d.error&&!d.running){eb.hidden=false;eb.innerHTML='<b>Problem &mdash; '+esc(d.error.check)+'</b>'+esc(d.error.detail)+'<p><b>Potential fix</b>'+esc(d.error.fix)+'</p>';}
 else eb.hidden=true;
 if(warns.length&&!d.running){wb.hidden=false;wb.innerHTML=warns.map(c=>'<b>Warning &mdash; '+esc(c.name)+'</b>'+esc(c.detail)+(c.fix?'<p>'+esc(c.fix)+'</p>':'')).join('<br>');}
 else wb.hidden=true;
 document.getElementById('diagrun').disabled=!!d.running;
 document.getElementById('diagstop').hidden=!d.running;
}
async function pollDiag(){
 try{const d=await (await fetch('/diag')).json();renderDiag(d);
  if(!d.running&&diagTimer){clearInterval(diagTimer);diagTimer=null;}
 }catch(e){}
}
document.getElementById('diagrun').onclick=async()=>{
 const m=document.getElementById('diagmsg');m.textContent='starting…';
 try{const r=await postJSON('/diag/start');
  if(!r.ok){m.textContent='could not start: '+(r.error||'?');return;}
  if(!diagTimer)diagTimer=setInterval(pollDiag,1000);
  pollDiag();
 }catch(e){m.textContent='request failed: '+e;}
};
document.getElementById('diagstop').onclick=async()=>{try{await postJSON('/diag/stop');pollDiag();}catch(e){}};
(async()=>{try{const d=await (await fetch('/diag')).json();
 if(d.started){renderDiag(d);if(d.running&&!diagTimer)diagTimer=setInterval(pollDiag,1000);}
}catch(e){}})();
load();
</script></body></html>"""


def _json(status, obj):
    return status, "application/json", json.dumps(obj).encode()


def _to_int(v):
    if isinstance(v, bool):
        raise ValueError("boolean")
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, str):
        return int(v.strip())
    raise ValueError("not a number")


def validate_updates(updates, current):
    """Server-side sanity checks for a settings POST (the browser validates too,
    but anything can POST). Normalizes numeric strings to ints in place and
    returns an error message, or None when the updates are acceptable. A bad
    value that got saved could otherwise crash-loop the app on its next start
    (share difficulty 0 divides by zero, a port above 65535 fails to bind)."""
    ints = {}
    for k in ("bchn_rpc_port", "stratum_port", "status_port", "share_difficulty",
              "vardiff_target_spm", "vardiff_min", "vardiff_max"):
        if k in updates:
            try:
                ints[k] = _to_int(updates[k])
            except (TypeError, ValueError):
                return f"{k} must be a whole number"
    for k in ("bchn_rpc_port", "stratum_port", "status_port"):
        if k in ints and not 1 <= ints[k] <= 65535:
            return f"{k} must be between 1 and 65535"
    for k in ("share_difficulty", "vardiff_target_spm", "vardiff_min", "vardiff_max"):
        if k in ints and ints[k] < 1:
            return f"{k} must be at least 1"
    if ints.get("stratum_port", current["stratum_port"]) == \
            ints.get("status_port", current["status_port"]):
        return "Stratum and status ports must differ"
    if ints.get("vardiff_max", current["vardiff_max"]) < \
            ints.get("vardiff_min", current["vardiff_min"]):
        return "vardiff_max must be >= vardiff_min"
    updates.update(ints)

    for k in ("bchn_rpc_host", "bchn_rpc_user", "bchn_rpc_password", "webhook_url",
              "zmq_block_endpoint"):
        if k in updates and not isinstance(updates[k], str):
            return f"{k} must be a string"
    if "bchn_rpc_host" in updates:
        host = updates["bchn_rpc_host"].strip()
        if not host or len(host) > 253 or any(c in host for c in "/\\ ?#@\r\n"):
            return "bchn_rpc_host must be a hostname or IP address"
        updates["bchn_rpc_host"] = host
    if "webhook_url" in updates:
        url = updates["webhook_url"].strip()
        if url and not url.lower().startswith(("http://", "https://")):
            return "Webhook URL must start with http:// or https://"
        if len(url) > 2048 or any(c in url for c in " \r\n"):
            return "Webhook URL is not valid"
        updates["webhook_url"] = url
    for k in ("vardiff_enabled", "notify_block", "notify_best_share",
              "notify_miner_offline", "zmq_enabled"):
        v = updates.get(k)
        if v is None or isinstance(v, bool):
            continue
        if not (isinstance(v, str)
                and v.strip().lower() in ("1", "0", "true", "false", "yes", "no",
                                          "on", "off")):
            return f"{k} must be true or false"
    return None


class StatusServer:
    def __init__(self, server, jobs, host, port, history=None, diag=None):
        self.server = server
        self.jobs = jobs
        self.host = host
        self.port = port
        self.history = history
        self.diag = diag
        self._active = 0

    async def start(self):
        return await asyncio.start_server(self._handle, self.host, self.port)

    async def _read_request(self, reader):
        request_line = await reader.readline()
        parts = request_line.decode(errors="replace").split()
        method = parts[0].upper() if parts else "GET"
        path = parts[1] if len(parts) > 1 else "/"
        headers = {}
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            if len(headers) >= MAX_HEADERS:
                raise ValueError("too many headers")
            k, _, v = line.decode(errors="replace").partition(":")
            headers[k.strip().lower()] = v.strip()
        body = b""
        if method == "POST":
            n = int(headers.get("content-length", "0") or 0)
            if n > MAX_BODY:
                raise ValueError("body too large")
            if n > 0:
                body = await reader.readexactly(n)
        return method, path, headers, body

    async def _handle(self, reader, writer):
        if self._active >= MAX_HTTP_CONNECTIONS:
            writer.close()
            return
        self._active += 1
        try:
            try:
                method, path, headers, body = await asyncio.wait_for(
                    self._read_request(reader), REQUEST_TIMEOUT)
            except (asyncio.TimeoutError, ValueError, asyncio.IncompleteReadError):
                return
            status, ctype, out = await self._route(method, path, headers, body)
            head = (f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
                    f"Content-Length: {len(out)}\r\n"
                    "X-Content-Type-Options: nosniff\r\n"
                    "Referrer-Policy: no-referrer\r\n"
                    "Cache-Control: no-store\r\n")
            if path.startswith(CORS_OPEN):
                head += "Access-Control-Allow-Origin: *\r\n"
            head += "Connection: close\r\n\r\n"
            writer.write(head.encode() + out)
            await writer.drain()
        except Exception:
            pass
        finally:
            self._active -= 1
            try:
                writer.close()
            except Exception:
                pass

    @staticmethod
    def _is_json(headers):
        ct = headers.get("content-type", "").split(";", 1)[0].strip().lower()
        return ct == "application/json"

    async def _route(self, method, path, headers, body):
        if method == "OPTIONS":
            # No CORS grant for preflights: cross-origin JSON POSTs are refused.
            return "204 No Content", "text/plain", b""
        if path.startswith("/status"):
            out = json.dumps(build_snapshot(self.server, self.jobs), default=str)
            return "200 OK", "application/json", out.encode()
        if path.startswith("/metrics"):
            try:
                out = build_metrics(self.server, self.jobs)
                return "200 OK", METRICS_CT, out.encode()
            except Exception as e:
                return ("500 Internal Server Error",
                        "text/plain; charset=utf-8", f"# error: {e}\n".encode())
        if path.startswith("/history"):
            pts = self.history.snapshot() if self.history else []
            return _json("200 OK", {"points": pts})
        if path.startswith("/config.json"):
            out = config.redacted(config.load())
            out["env_locked"] = config.env_locked()
            return _json("200 OK", out)
        if path.startswith("/ui-prefs") and method != "POST":
            return self._get_ui_prefs()
        if path.startswith("/diag") and method != "POST":
            if self.diag is None:
                return _json("503 Service Unavailable",
                             {"ok": False, "error": "diagnostics unavailable"})
            return _json("200 OK", self.diag.snapshot())
        if path.startswith("/config") and not path.startswith("/config.json") \
                and method != "POST":
            return "200 OK", "text/html; charset=utf-8", CONFIG_HTML.encode()

        # --- state-changing routes: POST + JSON content type only ----------- #
        mutating = ("/test-rpc", "/test-webhook", "/reset-stats", "/ui-prefs", "/config",
                    "/diag/start", "/diag/stop")
        if path.startswith(mutating):
            if method != "POST":
                return _json("405 Method Not Allowed",
                             {"ok": False, "error": "POST only"})
            if not self._is_json(headers):
                return _json("415 Unsupported Media Type",
                             {"ok": False,
                              "error": "Content-Type must be application/json"})
            loop = asyncio.get_running_loop()
            if path.startswith("/test-rpc"):
                # Network calls run in the executor: a 30 s RPC timeout must
                # never stall share processing for every connected miner.
                return await loop.run_in_executor(None, self._test_rpc)
            if path.startswith("/test-webhook"):
                return await loop.run_in_executor(None, self._test_webhook)
            if path.startswith("/reset-stats"):
                return self._reset_stats()
            if path.startswith("/ui-prefs"):
                return self._save_ui_prefs(body)
            if path.startswith("/diag/"):
                if self.diag is None:
                    return _json("503 Service Unavailable",
                                 {"ok": False, "error": "diagnostics unavailable"})
                if path.startswith("/diag/start"):
                    if self.diag.start():
                        return _json("200 OK", {"ok": True})
                    return _json("409 Conflict",
                                 {"ok": False, "error": "checks are already running"})
                self.diag.stop()
                return _json("200 OK", {"ok": True})
            return self._save_config(body)
        return "200 OK", "text/html; charset=utf-8", DASHBOARD_HTML.encode()

    def _test_rpc(self):
        try:
            info = self.jobs.rpc.getblockchaininfo()
            out = {"ok": True, "chain": info.get("chain"),
                   "blocks": info.get("blocks"),
                   "ibd": bool(info.get("initialblockdownload"))}
        except Exception as e:
            out = {"ok": False, "error": str(e)}
        return "200 OK", "application/json", json.dumps(out).encode()

    def _get_ui_prefs(self):
        cfg = config.load()
        hc = cfg.get("hidden_cards")
        if not isinstance(hc, list):
            hc = []
        return ("200 OK", "application/json",
                json.dumps({"hidden_cards": hc}).encode())

    def _save_ui_prefs(self, body):
        # Dashboard view preference only — saved to config.json but NOT run through
        # apply_config, so hiding a card never reconnects RPC or disturbs mining.
        try:
            data = json.loads(body or b"{}")
            if not isinstance(data, dict):
                raise ValueError("expected object")
            hc = data.get("hidden_cards", [])
            if not isinstance(hc, list):
                raise ValueError("hidden_cards must be a list")
            # Card ids are our own [a-z0-9_] identifiers; anything else is
            # dropped so nothing unexpected is ever stored or rendered.
            hc = list(dict.fromkeys(x for x in hc
                                    if isinstance(x, str) and _CARD_ID.fullmatch(x)))[:64]
            config.save({"hidden_cards": hc})
        except Exception as e:
            return ("400 Bad Request", "application/json",
                    json.dumps({"ok": False, "error": str(e)}).encode())
        return "200 OK", "application/json", json.dumps({"ok": True}).encode()

    def _reset_stats(self):
        try:
            self.jobs.stats.reset()
            # Clear live per-connection best so the pool best reflects the reset
            # until miners reconnect (accepted/rejected shown are lifetime already).
            for c in list(self.server.clients):
                c.best_diff = 0.0
            out = {"ok": True}
        except Exception as e:
            out = {"ok": False, "error": str(e)}
        return "200 OK", "application/json", json.dumps(out).encode()

    def _test_webhook(self):
        url = self.jobs.webhook_url
        if not url:
            out = {"ok": False, "error": "no webhook URL saved — Save settings first"}
        else:
            try:
                self.jobs._post_webhook(url, {
                    "event": "test", "server": "SoloBCH Forge",
                    "message": "SoloBCH Forge test webhook"})
                out = {"ok": True}
            except Exception as e:
                out = {"ok": False, "error": str(e)}
        return "200 OK", "application/json", json.dumps(out).encode()

    def _save_config(self, body):
        try:
            data = json.loads(body or b"{}")
            if not isinstance(data, dict):
                raise ValueError("expected object")
        except Exception as e:
            return ("400 Bad Request", "application/json",
                    json.dumps({"ok": False, "error": f"bad json: {e}"}).encode())
        updates = {}
        for k in config.DEFAULTS:
            if k not in data or k == "hidden_cards":     # layout has its own route
                continue
            # blank means "leave unchanged" for every field except webhook_url,
            # where an empty string is the valid way to disable notifications.
            if data[k] == "" and k != "webhook_url":
                continue
            updates[k] = data[k]
        err = validate_updates(updates, config.load())
        if err:
            return _json("400 Bad Request", {"ok": False, "error": err})
        try:
            config.save(updates)
            cfg = config.load()
            self.jobs.apply_config(cfg)
        except Exception as e:
            return ("500 Internal Server Error", "application/json",
                    json.dumps({"ok": False, "error": str(e)}).encode())
        return ("200 OK", "application/json",
                json.dumps({"ok": True, "config": config.redacted(cfg)}).encode())
