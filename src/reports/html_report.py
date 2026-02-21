"""
Generador de reporte HTML interactivo con tabla filtrable y graficos Plotly.
Produce un archivo HTML standalone (sin servidor) que se puede abrir en cualquier browser.
"""

import json
import logging
from pathlib import Path
from datetime import datetime

from src.models.database import DatabaseManager

logger = logging.getLogger(__name__)

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mining Equipment - Technical Specs Report</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"
        integrity="sha384-Hl48Kq2HifOWdXEjMsKo6qxqvRLTYqIGbvlENBmkHAxZKIGCXv43H6W1jA671RzC"
        crossorigin="anonymous"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #f0f2f5; color: #333; }
  .header { background: linear-gradient(135deg, #1a1a2e, #16213e); color: white;
             padding: 24px 32px; }
  .header h1 { font-size: 1.6rem; margin-bottom: 4px; }
  .header .subtitle { opacity: 0.7; font-size: 0.85rem; }
  .cards { display: flex; gap: 16px; padding: 20px 32px; flex-wrap: wrap; }
  .card { background: white; border-radius: 10px; padding: 18px 24px;
          flex: 1; min-width: 160px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .card .label { font-size: 0.75rem; text-transform: uppercase; color: #888;
                 letter-spacing: 0.5px; }
  .card .value { font-size: 1.8rem; font-weight: 700; color: #1a1a2e; margin-top: 4px; }
  .section { padding: 0 32px 24px; }
  .section h2 { font-size: 1.2rem; margin-bottom: 12px; color: #1a1a2e; }
  .filters { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; align-items: end; }
  .filters label { font-size: 0.75rem; text-transform: uppercase; color: #666; display: block;
                   margin-bottom: 2px; }
  .filters select, .filters input { padding: 6px 10px; border: 1px solid #ccc;
    border-radius: 6px; font-size: 0.85rem; }
  .filters input { width: 180px; }
  table { width: 100%; border-collapse: collapse; background: white;
          border-radius: 10px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  th { background: #1a1a2e; color: white; padding: 10px 12px; text-align: left;
       font-size: 0.8rem; cursor: pointer; user-select: none; white-space: nowrap; }
  th:hover { background: #2a2a4e; }
  th .arrow { margin-left: 4px; font-size: 0.65rem; }
  td { padding: 8px 12px; border-bottom: 1px solid #eee; font-size: 0.82rem; }
  tr:hover td { background: #f7f9fc; }
  a { color: #2563eb; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .conf-high { background: #dcfce7; color: #166534; }
  .conf-mid { background: #fef9c3; color: #854d0e; }
  .conf-low { background: #fee2e2; color: #991b1b; }
  .conf-badge { padding: 2px 8px; border-radius: 10px; font-size: 0.75rem; font-weight: 600; }
  .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .chart-box { background: white; border-radius: 10px; padding: 16px;
               box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  @media (max-width: 900px) { .charts { grid-template-columns: 1fr; } }
  .table-wrapper { max-height: 600px; overflow: auto; border-radius: 10px; }
  .table-wrapper thead th { position: sticky; top: 0; z-index: 1; }
  .row-count { font-size: 0.8rem; color: #888; margin-bottom: 8px; }
</style>
</head>
<body>

<div class="header">
  <h1>Mining Equipment - Technical Specs Report</h1>
  <div class="subtitle">Generated: {{generated_at}}</div>
</div>

<div class="cards">
  <div class="card"><div class="label">Brands</div><div class="value">{{total_brands}}</div></div>
  <div class="card"><div class="label">Models</div><div class="value">{{total_models}}</div></div>
  <div class="card"><div class="label">Specs</div><div class="value">{{total_specs}}</div></div>
  <div class="card"><div class="label">Avg Confidence</div><div class="value">{{avg_confidence}}</div></div>
</div>

<div class="section">
  <h2>Specifications Table</h2>
  <div class="filters">
    <div><label>Brand</label><select id="fBrand" onchange="applyFilters()">
      <option value="">All</option>
    </select></div>
    <div><label>Parameter</label><select id="fParam" onchange="applyFilters()">
      <option value="">All</option>
    </select></div>
    <div><label>Min Confidence</label><select id="fConf" onchange="applyFilters()">
      <option value="0">All</option>
      <option value="0.5">≥ 0.5</option>
      <option value="0.7">≥ 0.7</option>
      <option value="0.9">≥ 0.9</option>
    </select></div>
    <div><label>Model (text)</label><input id="fModel" type="text" placeholder="Filter model..."
      oninput="applyFilters()"></div>
  </div>
  <div class="row-count" id="rowCount"></div>
  <div class="table-wrapper">
  <table>
    <thead><tr>
      <th onclick="sortTable(0)">Brand <span class="arrow"></span></th>
      <th onclick="sortTable(1)">Model <span class="arrow"></span></th>
      <th onclick="sortTable(2)">Parameter <span class="arrow"></span></th>
      <th onclick="sortTable(3)">Value <span class="arrow"></span></th>
      <th onclick="sortTable(4)">Unit <span class="arrow"></span></th>
      <th onclick="sortTable(5)">Confidence <span class="arrow"></span></th>
      <th onclick="sortTable(6)">Source <span class="arrow"></span></th>
    </tr></thead>
    <tbody id="specBody"></tbody>
  </table>
  </div>
</div>

<div class="section">
  <h2>Charts</h2>
  <div class="charts">
    <div class="chart-box" id="chartWeight"></div>
    <div class="chart-box" id="chartPower"></div>
    <div class="chart-box" id="chartScatter"></div>
    <div class="chart-box" id="chartHeatmap"></div>
    <div class="chart-box" id="chartRimpull" style="grid-column: 1 / -1;"></div>
    <div class="chart-box" id="chartRimpullSpeed" style="grid-column: 1 / -1;"></div>
  </div>
</div>

<script>
const DATA = {{data_json}};
const RIMPULL_DATA = {{rimpull_json}};

// Populate table and filters
const brands = [...new Set(DATA.map(d => d.brand))].sort();
const params = [...new Set(DATA.map(d => d.parameter))].sort();
const fBrand = document.getElementById('fBrand');
const fParam = document.getElementById('fParam');
brands.forEach(b => { const o = document.createElement('option'); o.value = b; o.textContent = b; fBrand.appendChild(o); });
params.forEach(p => { const o = document.createElement('option'); o.value = p; o.textContent = p; fParam.appendChild(o); });

function esc(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = String(s);
  return d.innerHTML;
}

function confBadge(c) {
  const v = parseFloat(c);
  if (v >= 0.7) return `<span class="conf-badge conf-high">${v.toFixed(2)}</span>`;
  if (v >= 0.5) return `<span class="conf-badge conf-mid">${v.toFixed(2)}</span>`;
  return `<span class="conf-badge conf-low">${v.toFixed(2)}</span>`;
}

function safeUrl(url) {
  if (!url) return '';
  try { const u = new URL(url); if (u.protocol !== 'http:' && u.protocol !== 'https:') return ''; return u.href; }
  catch { return ''; }
}

function truncUrl(url) {
  if (!url) return '';
  try { const u = new URL(url); return u.hostname + u.pathname.slice(0, 30) + (u.pathname.length > 30 ? '...' : ''); }
  catch { return url.slice(0, 50); }
}

function renderTable(rows) {
  const body = document.getElementById('specBody');
  body.innerHTML = rows.map(d => {
    const href = safeUrl(d.source_url);
    const srcCell = href
      ? `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer">${esc(truncUrl(href))}</a>`
      : '';
    return `<tr><td>${esc(d.brand)}</td><td>${esc(d.model)}</td><td>${esc(d.parameter)}</td>` +
      `<td>${esc(d.value)}</td><td>${esc(d.unit)}</td><td>${confBadge(d.confidence)}</td>` +
      `<td>${srcCell}</td></tr>`;
  }).join('');
  document.getElementById('rowCount').textContent = `Showing ${rows.length} of ${DATA.length} specs`;
}

function applyFilters() {
  const brand = fBrand.value;
  const param = fParam.value;
  const conf = parseFloat(document.getElementById('fConf').value);
  const model = document.getElementById('fModel').value.toLowerCase();
  const filtered = DATA.filter(d =>
    (!brand || d.brand === brand) &&
    (!param || d.parameter === param) &&
    (d.confidence >= conf) &&
    (!model || d.model.toLowerCase().includes(model))
  );
  renderTable(filtered);
}

// Sort
let sortCol = -1, sortAsc = true;
function sortTable(col) {
  if (sortCol === col) sortAsc = !sortAsc; else { sortCol = col; sortAsc = true; }
  const keys = ['brand','model','parameter','value','unit','confidence','source_url'];
  const key = keys[col];
  const rows = [...document.getElementById('specBody').rows];
  const body = document.getElementById('specBody');
  rows.sort((a, b) => {
    let va = a.cells[col].textContent, vb = b.cells[col].textContent;
    if (col === 5) { va = parseFloat(va); vb = parseFloat(vb); }
    if (va < vb) return sortAsc ? -1 : 1;
    if (va > vb) return sortAsc ? 1 : -1;
    return 0;
  });
  rows.forEach(r => body.appendChild(r));
}

renderTable(DATA);

// === Charts ===
const weightData = DATA.filter(d => d.parameter === 'peso_operativo' && d.value);
const powerData = DATA.filter(d => d.parameter === 'potencia_motor' && d.value);

// Bar: Operating weight by model
if (weightData.length) {
  Plotly.newPlot('chartWeight', [{
    x: weightData.map(d => d.model),
    y: weightData.map(d => parseFloat(d.value)),
    type: 'bar', marker: { color: '#3b82f6' },
    text: weightData.map(d => d.unit), hoverinfo: 'x+y+text'
  }], { title: 'Operating Weight by Model', xaxis: { tickangle: -45 },
        yaxis: { title: 'Weight' }, margin: { b: 120 } },
  { responsive: true });
} else {
  document.getElementById('chartWeight').innerHTML = '<p style="color:#888;padding:40px;text-align:center">No weight data available</p>';
}

// Bar: Engine power by model
if (powerData.length) {
  Plotly.newPlot('chartPower', [{
    x: powerData.map(d => d.model),
    y: powerData.map(d => parseFloat(d.value)),
    type: 'bar', marker: { color: '#f59e0b' },
    text: powerData.map(d => d.unit), hoverinfo: 'x+y+text'
  }], { title: 'Engine Power by Model', xaxis: { tickangle: -45 },
        yaxis: { title: 'Power' }, margin: { b: 120 } },
  { responsive: true });
} else {
  document.getElementById('chartPower').innerHTML = '<p style="color:#888;padding:40px;text-align:center">No power data available</p>';
}

// Scatter: Weight vs Power
const scatterModels = [...new Set([...weightData.map(d=>d.model), ...powerData.map(d=>d.model)])];
const scatterPts = scatterModels.map(m => {
  const w = weightData.find(d => d.model === m);
  const p = powerData.find(d => d.model === m);
  if (w && p) return { model: m, weight: parseFloat(w.value), power: parseFloat(p.value) };
  return null;
}).filter(Boolean);

if (scatterPts.length) {
  Plotly.newPlot('chartScatter', [{
    x: scatterPts.map(d => d.weight),
    y: scatterPts.map(d => d.power),
    text: scatterPts.map(d => d.model),
    mode: 'markers+text', type: 'scatter',
    textposition: 'top center', textfont: { size: 9 },
    marker: { size: 12, color: '#10b981' }
  }], { title: 'Weight vs Power', xaxis: { title: 'Operating Weight' },
        yaxis: { title: 'Engine Power' }, margin: { t: 40 } },
  { responsive: true });
} else {
  document.getElementById('chartScatter').innerHTML = '<p style="color:#888;padding:40px;text-align:center">Not enough data for scatter</p>';
}

// Heatmap: Confidence by parameter and model
const hModels = [...new Set(DATA.map(d => d.model))].sort();
const hParams = [...new Set(DATA.map(d => d.parameter))].sort();
const zData = hParams.map(p => hModels.map(m => {
  const match = DATA.find(d => d.model === m && d.parameter === p);
  return match ? match.confidence : null;
}));

if (hModels.length && hParams.length) {
  Plotly.newPlot('chartHeatmap', [{
    z: zData, x: hModels, y: hParams, type: 'heatmap',
    colorscale: [[0,'#fee2e2'],[0.5,'#fef9c3'],[1,'#dcfce7']],
    hoverongaps: false
  }], { title: 'Confidence Heatmap', margin: { l: 140, b: 120 },
        xaxis: { tickangle: -45 } },
  { responsive: true });
} else {
  document.getElementById('chartHeatmap').innerHTML = '<p style="color:#888;padding:40px;text-align:center">No data for heatmap</p>';
}

// === Rimpull Curves Chart ===
const GEAR_ORDER = {'1st':1,'2nd':2,'3rd':3,'4th':4,'5th':5,'6th':6,'7th':7,'Direct':8,'Reverse':9};
if (RIMPULL_DATA.length) {
  const rModels = [...new Set(RIMPULL_DATA.map(d => d.brand + ' ' + d.model))];
  const colors = ['#3b82f6','#ef4444','#10b981','#f59e0b','#8b5cf6','#ec4899','#06b6d4','#84cc16'];
  const rTraces = rModels.map((key, i) => {
    const pts = RIMPULL_DATA.filter(d => (d.brand + ' ' + d.model) === key)
      .sort((a,b) => (GEAR_ORDER[a.gear]||99) - (GEAR_ORDER[b.gear]||99));
    return {
      x: pts.map(d => d.gear),
      y: pts.map(d => d.force_kn),
      name: key,
      mode: 'lines+markers',
      type: 'scatter',
      marker: { size: 8, color: colors[i % colors.length] },
      line: { color: colors[i % colors.length] },
      text: pts.map(d => `${key}<br>Gear: ${d.gear}<br>Force: ${d.force_kn} kN` +
        (d.speed_kmh ? `<br>Speed: ${d.speed_kmh} km/h` : '')),
      hoverinfo: 'text'
    };
  });
  Plotly.newPlot('chartRimpull', rTraces, {
    title: 'Rimpull Curves by Model',
    xaxis: { title: 'Gear', categoryorder: 'array',
             categoryarray: Object.keys(GEAR_ORDER) },
    yaxis: { title: 'Force (kN)' },
    margin: { b: 80 },
    legend: { orientation: 'h', y: -0.2 }
  }, { responsive: true });

  // Force vs Speed chart (if speed data available)
  const speedPts = RIMPULL_DATA.filter(d => d.speed_kmh != null);
  if (speedPts.length) {
    const sModels = [...new Set(speedPts.map(d => d.brand + ' ' + d.model))];
    const sTraces = sModels.map((key, i) => {
      const pts = speedPts.filter(d => (d.brand + ' ' + d.model) === key)
        .sort((a,b) => a.speed_kmh - b.speed_kmh);
      return {
        x: pts.map(d => d.speed_kmh),
        y: pts.map(d => d.force_kn),
        name: key,
        mode: 'lines+markers',
        type: 'scatter',
        marker: { size: 8, color: colors[i % colors.length] },
        line: { color: colors[i % colors.length] },
        text: pts.map(d => `${key}<br>Gear: ${d.gear}<br>Force: ${d.force_kn} kN<br>Speed: ${d.speed_kmh} km/h`),
        hoverinfo: 'text'
      };
    });
    Plotly.newPlot('chartRimpullSpeed', sTraces, {
      title: 'Rimpull: Force vs Speed',
      xaxis: { title: 'Speed (km/h)' },
      yaxis: { title: 'Force (kN)' },
      margin: { b: 80 },
      legend: { orientation: 'h', y: -0.2 }
    }, { responsive: true });
  } else {
    document.getElementById('chartRimpullSpeed').innerHTML = '<p style="color:#888;padding:40px;text-align:center">No speed data available for rimpull curves</p>';
  }
} else {
  document.getElementById('chartRimpull').innerHTML = '<p style="color:#888;padding:40px;text-align:center">No rimpull curve data available</p>';
  document.getElementById('chartRimpullSpeed').innerHTML = '';
}
</script>
</body>
</html>"""


def _safe_json_for_html(data, **kwargs) -> str:
    """Serialize data to JSON safe for embedding in HTML <script> tags.

    Escapes sequences that could break out of a script context:
    - '</' → '<\\/' (prevents </script> injection)
    - '<!--' → '<\\!--' (prevents HTML comment injection)
    """
    raw = json.dumps(data, ensure_ascii=False, default=str, **kwargs)
    raw = raw.replace("</", r"<\/")
    raw = raw.replace("<!--", r"<\!--")
    return raw


class HTMLReportGenerator:
    """Genera un reporte HTML interactivo standalone con tabla filtrable y graficos Plotly."""

    def __init__(self, db: DatabaseManager, output_dir: str = "data/reports"):
        self.db = db
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self) -> str:
        """Genera el reporte HTML y retorna la ruta del archivo."""
        df = self.db.get_all_specs_dataframe()

        if df.empty:
            logger.warning("No hay datos para generar reporte HTML")
            return ""

        # Prepare data as list of dicts for JSON embedding
        records = df.to_dict(orient="records")
        # Ensure confidence is float
        for r in records:
            r["confidence"] = float(r.get("confidence", 0) or 0)

        # Compute summary stats
        total_brands = df["brand"].nunique()
        total_models = df["model"].nunique()
        total_specs = len(df)
        avg_confidence = df["confidence"].mean()

        # Build HTML from template
        html = HTML_TEMPLATE
        html = html.replace("{{generated_at}}", datetime.now().strftime("%Y-%m-%d %H:%M"))
        html = html.replace("{{total_brands}}", str(total_brands))
        html = html.replace("{{total_models}}", str(total_models))
        html = html.replace("{{total_specs}}", str(total_specs))
        html = html.replace("{{avg_confidence}}", f"{avg_confidence:.2f}")
        html = html.replace("{{data_json}}", _safe_json_for_html(records))

        # Rimpull curve data
        try:
            rimpull_df = self.db.get_rimpull_curves_dataframe()
            rimpull_records = rimpull_df.to_dict(orient="records") if not rimpull_df.empty else []
        except Exception:
            rimpull_records = []
        html = html.replace("{{rimpull_json}}", _safe_json_for_html(rimpull_records))

        output_path = self.output_dir / "equipment_report.html"
        output_path.write_text(html, encoding="utf-8")
        logger.info(f"Reporte HTML generado: {output_path}")
        return str(output_path)
