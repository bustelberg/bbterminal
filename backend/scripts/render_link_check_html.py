"""Render a GuruFocus link-check CSV as a self-contained, filterable HTML page.

No server, no build — embeds the data inline so the output file opens straight
in a browser (file://). Re-run after re-running check_gurufocus_links.py to
refresh.

Usage (from backend/):
    uv run python scripts/render_link_check_html.py
    uv run python scripts/render_link_check_html.py gurufocus_link_check.csv out.html
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

_CSV = sys.argv[1] if len(sys.argv) > 1 else "gurufocus_link_check.csv"
_OUT = sys.argv[2] if len(sys.argv) > 2 else "gurufocus_link_check.html"


def main() -> int:
    src = Path(_CSV)
    if not src.exists():
        print(f"CSV not found: {src.resolve()}")
        return 1
    with src.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    counts: dict[str, int] = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    data_json = json.dumps(rows, ensure_ascii=False)
    counts_json = json.dumps(counts, ensure_ascii=False)
    html = _TEMPLATE.replace("__DATA__", data_json).replace("__COUNTS__", counts_json).replace("__TOTAL__", str(len(rows)))

    out = Path(_OUT)
    out.write_text(html, encoding="utf-8")
    print(f"Wrote {out.resolve()} ({len(rows)} rows, {out.stat().st_size // 1024} KB)")
    print("Open it in a browser: file://" + str(out.resolve()).replace("\\", "/"))
    return 0


_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GuruFocus link check</title>
<style>
  :root { --bg:#f5f7fa; --card:#fff; --ink:#1e293b; --muted:#64748b; --line:#e2e8f0;
          --ok:#059669; --okbg:#ecfdf5; --bad:#dc2626; --badbg:#fef2f2;
          --warn:#d97706; --warnbg:#fffbeb; --grey:#475569; --greybg:#f1f5f9; --accent:#2563eb; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--ink); }
  header { padding:20px 24px; background:var(--card); border-bottom:1px solid var(--line); position:sticky; top:0; z-index:5; }
  h1 { margin:0 0 4px; font-size:18px; }
  .sub { color:var(--muted); font-size:12px; }
  .controls { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-top:12px; }
  .pill { cursor:pointer; border:1px solid var(--line); background:var(--card); color:var(--ink);
          padding:5px 11px; border-radius:999px; font-size:12px; font-weight:600; display:inline-flex; gap:6px; align-items:center; }
  .pill.active { border-color:var(--accent); background:#eff6ff; color:var(--accent); }
  .pill .n { font-variant-numeric:tabular-nums; opacity:.7; }
  input[type=search] { flex:1; min-width:180px; padding:7px 11px; border:1px solid var(--line);
          border-radius:8px; font-size:13px; background:var(--card); color:var(--ink); }
  .wrap { padding:16px 24px; }
  table { width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:10px; overflow:hidden; }
  th,td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); font-size:13px; vertical-align:top; }
  th { background:#f8fafc; color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.03em; cursor:pointer; user-select:none; position:sticky; top:0; }
  th:hover { color:var(--ink); }
  tr:hover td { background:#fafcff; }
  td.mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }
  a { color:var(--accent); text-decoration:none; } a:hover { text-decoration:underline; }
  .badge { display:inline-block; padding:2px 7px; border-radius:6px; font-size:11px; font-weight:700; white-space:nowrap; }
  .s-ok { color:var(--ok); background:var(--okbg); }
  .s-not_found, .s-error { color:var(--bad); background:var(--badbg); }
  .s-mismatch { color:var(--warn); background:var(--warnbg); }
  .s-restricted, .s-no_symbol { color:var(--grey); background:var(--greybg); }
  .empty { text-align:center; color:var(--muted); padding:40px; }
  .count { color:var(--muted); font-size:12px; margin:0 0 8px; }
</style>
</head>
<body>
<header>
  <h1>GuruFocus link check</h1>
  <div class="sub">__TOTAL__ companies · status from <code>stock/&lt;symbol&gt;/summary</code> · click a status to filter, a header to sort</div>
  <div class="controls" id="pills"></div>
  <div class="controls"><input id="q" type="search" placeholder="Filter by id / ticker / exchange / name / detail…"></div>
</header>
<div class="wrap">
  <p class="count" id="count"></p>
  <table>
    <thead><tr>
      <th data-k="company_id">ID</th><th data-k="status">Status</th><th data-k="exchange">Exch</th>
      <th data-k="ticker">Ticker</th><th data-k="stored_name">Stored name</th>
      <th data-k="gurufocus_name">GuruFocus name</th><th data-k="detail">Detail</th><th>Link</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
</div>
<script>
const DATA = __DATA__;
const COUNTS = __COUNTS__;
const ORDER = ["NOT_FOUND","MISMATCH","RESTRICTED","ERROR","NO_SYMBOL","OK"];
let active = "ALL", q = "", sortK = null, sortDir = 1;

const pills = document.getElementById("pills");
function mkPill(label, key, n) {
  const el = document.createElement("button");
  el.className = "pill" + (active === key ? " active" : "");
  el.innerHTML = label + ' <span class="n">' + n + '</span>';
  el.onclick = () => { active = key; render(); };
  return el;
}
function buildPills() {
  pills.innerHTML = "";
  pills.appendChild(mkPill("All", "ALL", DATA.length));
  const keys = Object.keys(COUNTS).sort((a,b)=>{
    const ia=ORDER.indexOf(a.toUpperCase()), ib=ORDER.indexOf(b.toUpperCase());
    return (ia<0?9:ia)-(ib<0?9:ib);
  });
  for (const k of keys) pills.appendChild(mkPill(k, k, COUNTS[k]));
}
document.getElementById("q").addEventListener("input", e => { q = e.target.value.toLowerCase().trim(); render(); });

function rowsFor() {
  let rs = DATA;
  if (active !== "ALL") rs = rs.filter(r => r.status === active);
  if (q) rs = rs.filter(r => [r.company_id,r.exchange,r.ticker,r.symbol,r.stored_name,r.gurufocus_name,r.detail]
                             .some(v => (v||"").toLowerCase().includes(q)));
  if (sortK) rs = [...rs].sort((a,b)=>{
    let x=a[sortK]||"", y=b[sortK]||"";
    if (sortK==="company_id"){ x=+x; y=+y; }
    return (x<y?-1:x>y?1:0)*sortDir;
  });
  return rs;
}
function esc(s){ return (s||"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c])); }
function render() {
  buildPills();
  const rs = rowsFor();
  document.getElementById("count").textContent = rs.length + " of " + DATA.length + " shown";
  const tb = document.getElementById("rows");
  if (!rs.length) { tb.innerHTML = '<tr><td colspan="8" class="empty">No matches</td></tr>'; return; }
  tb.innerHTML = rs.map(r => {
    const cls = "s-" + (r.status||"").toLowerCase();
    const link = r.gurufocus_url ? '<a href="'+esc(r.gurufocus_url)+'" target="_blank" rel="noopener">view ↗</a>' : '';
    return '<tr>'
      + '<td class="mono">'+esc(r.company_id)+'</td>'
      + '<td><span class="badge '+cls+'">'+esc(r.status)+'</span></td>'
      + '<td class="mono">'+esc(r.exchange)+'</td>'
      + '<td class="mono">'+esc(r.ticker)+'</td>'
      + '<td>'+esc(r.stored_name)+'</td>'
      + '<td>'+esc(r.gurufocus_name)+'</td>'
      + '<td style="color:var(--muted)">'+esc(r.detail)+'</td>'
      + '<td>'+link+'</td></tr>';
  }).join("");
}
document.querySelectorAll("th[data-k]").forEach(th => th.onclick = () => {
  const k = th.dataset.k;
  if (sortK === k) sortDir = -sortDir; else { sortK = k; sortDir = 1; }
  render();
});
// Default to the actionable view: anything that isn't OK.
active = "NOT_FOUND";
render();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(main())
