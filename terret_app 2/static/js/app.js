/* TÉRRET · app.js */

// ── Estado global ──────────────────────────────────────
const STATE = {
  master: false,
  producto: null,
  tab: null,
};

// ── Auth ───────────────────────────────────────────────
async function login(password) {
  const res = await fetch("/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password })
  });
  const data = await res.json();
  if (data.ok) {
    STATE.master = data.master;
    document.getElementById("auth-overlay")?.remove();
    location.reload();
  } else {
    document.getElementById("auth-error").textContent = "Contraseña incorrecta";
  }
}

// ── Navegación SPA ─────────────────────────────────────
function navigate(page) {
  const main = document.getElementById("main-content");
  if (!main) return;

  // Actualizar nav activo
  document.querySelectorAll(".nav-item").forEach(el => {
    el.classList.toggle("active", el.dataset.page === page);
  });

  // Cargar la página
  main.innerHTML = '<div class="flex-center" style="height:200px;color:var(--text3);font-size:0.8rem;">Cargando...</div>';

  fetch(`/pages/${page}`)
    .then(r => r.text())
    .then(html => {
      main.innerHTML = html;
      // Ejecutar scripts inline si los hay
      main.querySelectorAll("script").forEach(s => {
        const ns = document.createElement("script");
        ns.textContent = s.textContent;
        document.body.appendChild(ns);
        ns.remove();
      });
    })
    .catch(() => {
      main.innerHTML = '<div class="alert alert-error">Error cargando la página</div>';
    });

  // Guardar en URL
  history.pushState({ page }, "", `/${page}`);
}

// ── API helpers ────────────────────────────────────────
async function apiGet(endpoint) {
  const res = await fetch(`/api/${endpoint}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function apiPost(endpoint, data) {
  const res = await fetch(`/api/${endpoint}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data)
  });
  return res.json();
}

async function apiPostForm(endpoint, formData) {
  const res = await fetch(`/api/${endpoint}`, {
    method: "POST",
    body: formData
  });
  return res.json();
}

// ── Toast notifications ────────────────────────────────
function toast(message, type = "info", duration = 3000) {
  const el = document.createElement("div");
  el.className = `alert alert-${type}`;
  el.style.cssText = `
    position:fixed; bottom:20px; right:20px; z-index:9999;
    max-width:320px; animation: fadeIn 0.2s ease;
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
  `;
  el.textContent = message;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), duration);
}

// ── Selector de producto ────────────────────────────────
function setProducto(producto) {
  STATE.producto = producto;
  document.querySelectorAll(".producto-display").forEach(el => {
    el.textContent = producto;
  });
  // Recargar página actual con nuevo producto
  const active = document.querySelector(".nav-item.active");
  if (active) navigate(active.dataset.page);
}

// ── Semáforo helper ────────────────────────────────────
function semaforo(valor, bueno, malo) {
  if (!valor || valor === 0) return "dim";
  if (valor >= bueno) return "green";
  if (valor <= malo)  return "red";
  return "amber";
}

function semHtml(valor, bueno, malo, fmt) {
  const cls  = semaforo(valor, bueno, malo);
  const text = fmt ? fmt(valor) : valor;
  return `<span class="sem ${cls}"><span class="sem-dot"></span>${text}</span>`;
}

// ── Formatters ────────────────────────────────────────
const fmt = {
  cop:  v => `$${Number(v).toLocaleString("es-CO")}`,
  pct:  v => `${Number(v).toFixed(1)}%`,
  pct2: v => `${Number(v).toFixed(2)}%`,
  roas: v => `${Number(v).toFixed(2)}x`,
  num:  v => Number(v).toLocaleString("es-CO"),
};

// ── Decision card builder ──────────────────────────────
function buildDecisionCard(ad) {
  const HOOK_BUENO = 20, HOOK_MALO = 10;
  const HOLD_BUENO = 8,  HOLD_MALO = 3.5;
  const CTR_BUENO  = 1.5,CTR_MALO  = 0.7;
  const ROAS_ESC   = 2.5, ROAS_PAU  = 1.0;

  let decClass = "optimizar", decText = "OPTIMIZAR";
  if (ad.roas >= ROAS_ESC) { decClass = "escalar";  decText = "ESCALAR"; }
  if (ad.roas < ROAS_PAU && ad.inversion >= 20) { decClass = "pausar"; decText = "PAUSAR"; }

  const diag = [];
  if (ad.hook_rate > 0 && ad.hook_rate < HOOK_MALO)  diag.push(`Hook Rate ${fmt.pct(ad.hook_rate)} bajo → cambiar Hook`);
  else if (ad.hook_rate > 0 && ad.hook_rate < HOOK_BUENO) diag.push(`Hook Rate ${fmt.pct(ad.hook_rate)} en el límite`);
  else if (ad.hook_rate >= HOOK_BUENO) diag.push(`Hook Rate ${fmt.pct(ad.hook_rate)} funcionando bien`);

  if (ad.hold_rate > 0 && ad.hold_rate < HOLD_MALO) diag.push(`Hold Rate ${fmt.pct(ad.hold_rate)} muy bajo → revisar Body`);
  else if (ad.hold_rate > 0 && ad.hold_rate < HOLD_BUENO) diag.push(`Hold Rate ${fmt.pct(ad.hold_rate)} regular`);
  else if (ad.hold_rate >= HOLD_BUENO) diag.push(`Hold Rate ${fmt.pct(ad.hold_rate)} retiene bien`);

  if (ad.ctr > 0 && ad.ctr < CTR_MALO) diag.push(`CTR ${fmt.pct2(ad.ctr)} bajo → cambiar CTA`);
  else if (ad.ctr >= CTR_BUENO) diag.push(`CTR ${fmt.pct2(ad.ctr)} convirtiendo bien`);

  if (ad.roas >= ROAS_ESC) diag.push(`ROAS ${fmt.roas(ad.roas)} → escalar presupuesto`);
  else if (ad.roas < ROAS_PAU && ad.inversion >= 20) diag.push(`ROAS ${fmt.roas(ad.roas)} → pausar`);

  const shortName = ad.nombre_anuncio.length > 65
    ? ad.nombre_anuncio.slice(0, 65) + "…"
    : ad.nombre_anuncio;

  return `
  <div class="decision-card ${decClass}">
    <div class="decision-header">
      <span class="decision-name" title="${ad.nombre_anuncio}">${shortName}</span>
      <span class="decision-tag ${decClass}">${decText}</span>
    </div>
    <div class="decision-metrics">
      <div class="decision-metric">
        <div class="dm-label">Inversión</div>
        <div class="dm-value">${fmt.cop(ad.inversion)}</div>
      </div>
      <div class="decision-metric">
        <div class="dm-label">ROAS</div>
        <div class="dm-value" style="color:var(--${semaforo(ad.roas, ROAS_ESC, ROAS_PAU) === 'green' ? 'green' : semaforo(ad.roas, ROAS_ESC, ROAS_PAU) === 'red' ? 'red' : 'amber'})">${fmt.roas(ad.roas)}</div>
      </div>
      <div class="decision-metric">
        <div class="dm-label">Hook Rate</div>
        <div class="dm-value" style="color:var(--${semaforo(ad.hook_rate, HOOK_BUENO, HOOK_MALO) === 'green' ? 'green' : semaforo(ad.hook_rate, HOOK_BUENO, HOOK_MALO) === 'red' ? 'red' : 'amber'})">${fmt.pct(ad.hook_rate)}</div>
      </div>
      <div class="decision-metric">
        <div class="dm-label">Hold Rate</div>
        <div class="dm-value" style="color:var(--${semaforo(ad.hold_rate, HOLD_BUENO, HOLD_MALO) === 'green' ? 'green' : semaforo(ad.hold_rate, HOLD_BUENO, HOLD_MALO) === 'red' ? 'red' : 'amber'})">${fmt.pct(ad.hold_rate)}</div>
      </div>
      <div class="decision-metric">
        <div class="dm-label">CTR</div>
        <div class="dm-value" style="color:var(--${semaforo(ad.ctr, CTR_BUENO, CTR_MALO) === 'green' ? 'green' : semaforo(ad.ctr, CTR_BUENO, CTR_MALO) === 'red' ? 'red' : 'amber'})">${fmt.pct2(ad.ctr)}</div>
      </div>
    </div>
    <div class="decision-diag">${diag.join(" · ") || "Sin métricas de video aún"}</div>
  </div>`;
}

// ── Init ───────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  // Auth form
  const authForm = document.getElementById("auth-form");
  if (authForm) {
    authForm.addEventListener("submit", e => {
      e.preventDefault();
      login(document.getElementById("auth-password").value);
    });
  }

  // Navegación inicial
  const path = window.location.pathname.replace("/", "") || "dashboard";
  document.querySelector(`[data-page="${path}"]`)?.classList.add("active");

  // Popstate (navegación con back/forward)
  window.addEventListener("popstate", e => {
    if (e.state?.page) navigate(e.state.page);
  });

  // Selector de producto
  document.getElementById("producto-select")?.addEventListener("change", e => {
    setProducto(e.target.value);
  });
});
