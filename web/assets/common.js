// Query string helper
function qs(name, fallback){
  const v = new URLSearchParams(location.search).get(name);
  return v ?? fallback;
}
// Default API inference
function defaultApi(){
  const qsApi = qs('api', null);
  if (qsApi) return qsApi;
  return (location.hostname ? `http://${location.hostname}:8000` : 'http://localhost:8000');
}

function nodeColor(t){
  switch (t){
    case 'Website': return '#2563eb';
    case 'Server': return '#4f46e5';
    case 'Software': return '#7c3aed';
    case 'Directory': return '#0ea5e9';
    case 'Feed': return '#10b981';
    case 'PDE': return '#f59e0b';
    case 'FlowRun': return '#ef4444';
    default: return '#64748b';
  }
}
function nodeShape(t){
  switch (t){
    case 'Website': return 'round-rectangle';
    case 'Server': return 'rectangle';
    case 'Software': return 'diamond';
    case 'Directory': return 'round-rectangle';
    case 'Feed': return 'ellipse';
    case 'PDE': return 'hexagon';
    case 'FlowRun': return 'octagon';
    default: return 'ellipse';
  }
}
function labelText(ele){
  const d = ele.data();
  return `${d.type}\n${d.name || d.url || d.fqdn || d.path || d.site_key || d.server_key || d.soft_key || d.dir_key || d.feed_key || d.pde_key}`;
}
function edgeColor(lbl, op){
  if (lbl === 'FLOWS_TO'){
    if (op === 'mask') return '#f59e0b';
    if (op === 'transform') return '#22d3ee';
    if (op === 'surrogate_join') return '#eab308';
    return '#fb7185';
  }
  if (lbl === 'READS') return '#60a5fa';
  if (lbl === 'WRITES') return '#34d399';
  if (lbl === 'HOSTED_ON' || lbl === 'RUNS' || lbl === 'USES' || lbl === 'EXPOSES' || lbl === 'HAS') return '#94a3b8';
  return '#a3a3a3';
}
function edgeLabel(d){ return d.op ? `${d.label}:${d.op}` : (d.label || ''); }

function baseStyle(){
  return [
    {
      selector: 'node',
      style: {
        'background-color': ele => nodeColor(ele.data('type')),
        'label': ele => labelText(ele),
        'color': '#e5e7eb',
        'font-size': '10px',
        'text-wrap': 'wrap',
        'text-max-width': '120px',
        'text-valign': 'center',
        'text-halign': 'center',
        'width': 'label',
        'height': 'label',
        'padding': '12px',
        'shape': ele => nodeShape(ele.data('type')),
        'border-width': 1,
        'border-color': '#0ea5e9'
      }
    },
    {
      selector: 'edge',
      style: {
        'line-color': ele => edgeColor(ele.data('label'), ele.data('op')),
        'target-arrow-color': ele => edgeColor(ele.data('label'), ele.data('op')),
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier',
        'width': 2,
        'label': ele => edgeLabel(ele.data()),
        'font-size': '9px',
        'color': '#a3a3a3',
        'text-background-color': '#060a13',
        'text-background-opacity': 0.8,
        'text-background-padding': 2
      }
    },
    { selector: ':selected', style: { 'border-width': 3, 'border-color': '#f59e0b' } }
  ];
}

// Fetch and add to cy
async function loadGraph(cy, api, mode, key, hops){
  const url = new URL(`${api}/lineage`);
  if (mode === 'pde') url.searchParams.set('pde_key', key);
  else url.searchParams.set('site_key', key);
  url.searchParams.set('max_hops', String(hops));
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`API ${res.status}`);
  const data = await res.json();
  cy.elements().remove();
  cy.add(data.nodes);
  cy.add(data.edges);
}

// Export helpers
function exportPng(cy){
  const png64 = cy.png({ full: true, scale: 2, bg: '#0b1220' });
  const a = document.createElement('a');
  a.href = png64;
  a.download = 'lineage.png';
  a.click();
}
function exportJson(cy){
  const json = cy.json();
  const blob = new Blob([JSON.stringify(json, null, 2)], {type: 'application/json'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'lineage.json';
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 5000);
}

// Legend builder
function buildLegend(el){
  const items = [
    ['Website', nodeColor('Website')],
    ['Server', nodeColor('Server')],
    ['Software', nodeColor('Software')],
    ['Directory', nodeColor('Directory')],
    ['Feed', nodeColor('Feed')],
    ['PDE', nodeColor('PDE')],
    ['FlowRun', nodeColor('FlowRun')]
  ];
  items.forEach(([name, color]) => {
    const sw = document.createElement('div');
    sw.className = 'swatch';
    sw.style.backgroundColor = color;
    const label = document.createElement('div');
    label.textContent = name;
    el.appendChild(sw); el.appendChild(label);
  });
}
