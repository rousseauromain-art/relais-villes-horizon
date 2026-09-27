/* =====================================================================
   Relais « Villes a l'horizon » — Cloudflare Worker (offre gratuite)
   ---------------------------------------------------------------------
   Detient les secrets cote serveur, jamais dans le navigateur :
     - AIS_KEY                : cle AISStream (bateaux)
     - OPENSKY_CLIENT_ID      : client API OpenSky (avions, OAuth2)
     - OPENSKY_CLIENT_SECRET
   Variable (non secrete) :
     - ALLOWED_ORIGIN         : site autorise, ex. https://rousseauromain-art.github.io

   Routes (GET) :
     /health                                  -> etat du relais et des secrets
     /planes?lamin=&lomin=&lamax=&lomax=      -> avions (OpenSky, jeton gere ici)
     /ships?lamin=&lomin=&lamax=&lomax=       -> navires (ecoute AISStream ~10 s)

   AISStream refuse les connexions directes depuis un navigateur : le relais
   ouvre la connexion lui-meme, ecoute brievement la zone, puis renvoie un
   resume JSON. L'application interroge le relais toutes les ~30 s et cumule.
   ===================================================================== */

const DEF = {
  OPENSKY_TOKEN_URL: 'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token',
  OPENSKY_STATES_URL: 'https://opensky-network.org/api/states/all',
  AIS_URL: 'https://stream.aisstream.io/v0/stream', // les Workers ouvrent un WebSocket via https:// + en-tete Upgrade
  AIS_LISTEN_MS: 10000,
};
const MAX_SPAN = 3.5;        // degres max par cote de zone : protege les quotas
const PLANE_CACHE_MS = 10000;
const SHIP_TTL_MS = 20 * 60000;

// ---------------- pylones et points hauts telecom (France uniquement, source ANFR) ----------------
// ANFR ne propose pas de recherche par rayon geographique : on resout les communes couvertes par
// le rayon demande via l'API officielle "decoupage administratif" (geo.api.gouv.fr), puis on
// interroge ANFR filtre sur ces communes (refine.code_insee, seul filtre geographique documente
// qui fonctionne reellement aupres d'ANFR). Resultat regroupe par support (un pylone = plusieurs
// lignes, une par operateur x systeme radio).
const ANFR_RESOURCE = '88ef0887-6b0f-4d3f-8545-6d64c8f597da';
const ANFR_BASE = 'https://data.anfr.fr/d4c/api/records/2.0/resource/';
const GEO_BASE = 'https://geo.api.gouv.fr/communes';
const TPO_URL = 'https://data.anfr.fr/sites/default/files/dataset/dd1/1fac6-4531-4a27-9c8c-a3a9e4ec2107/sup_proprietaire_0.txt';
const NAT_URL = 'https://data.anfr.fr/sites/default/files/dataset/dd1/1fac6-4531-4a27-9c8c-a3a9e4ec2107/sup_nature_0.txt';
const TELECOM_MAX_KM = 15, TELECOM_MAX_COMMUNES = 12, TELECOM_CACHE_MS = 5 * 60000;
let tpoMap = null, natMap = null, lookupsAt = 0;
const telecomCache = new Map();

async function loadLookup(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error('lookup HTTP ' + r.status);
  const txt = await r.text(), map = new Map();
  txt.trim().split('\n').slice(1).forEach(line => {
    const i = line.indexOf(';'); if (i < 0) return;
    map.set(line.slice(0, i).trim(), line.slice(i + 1).trim());
  });
  return map;
}
async function ensureLookups() {
  if (tpoMap && natMap && Date.now() - lookupsAt < 24 * 3600000) return;
  const [tpo, nat] = await Promise.all([loadLookup(TPO_URL), loadLookup(NAT_URL)]);
  tpoMap = tpo; natMap = nat; lookupsAt = Date.now();
}
function haversineKm(la1, lo1, la2, lo2) {
  const R = 6371, D = Math.PI / 180;
  const dla = (la2 - la1) * D, dlo = (lo2 - lo1) * D;
  const a = Math.sin(dla / 2) ** 2 + Math.cos(la1 * D) * Math.cos(la2 * D) * Math.sin(dlo / 2) ** 2;
  return R * 2 * Math.asin(Math.sqrt(a));
}
async function communesInRadius(lat, lon, radiusKm) {
  const pts = [[lat, lon]];
  for (let a = 0; a < 360; a += 45) {
    const rad = a * Math.PI / 180;
    pts.push([lat + (radiusKm / 111) * Math.cos(rad), lon + (radiusKm / (111 * Math.max(0.2, Math.cos(lat * Math.PI / 180)))) * Math.sin(rad)]);
  }
  const codes = new Set();
  await Promise.all(pts.map(async ([la, lo]) => {
    try {
      const r = await fetch(`${GEO_BASE}?lat=${la.toFixed(5)}&lon=${lo.toFixed(5)}&fields=code&format=json`);
      if (!r.ok) return;
      const j = await r.json();
      if (Array.isArray(j)) for (const c of j) if (c.code) codes.add(c.code);
    } catch (e) {}
  }));
  return [...codes];
}
async function telecom(env, lat, lon, radiusKm, json) {
  radiusKm = Math.min(TELECOM_MAX_KM, Math.max(0.5, radiusKm || 5));
  const ckey = `${lat.toFixed(3)},${lon.toFixed(3)},${radiusKm}`;
  const hit = telecomCache.get(ckey);
  if (hit && Date.now() - hit.t < TELECOM_CACHE_MS) return json({ ...hit.body, cache: true });

  const communes = await communesInRadius(lat, lon, radiusKm);
  if (!communes.length) {
    const body = { anfrOk: true, hors_france: true, sites: [], communes: [] };
    telecomCache.set(ckey, { t: Date.now(), body });
    return json(body);
  }
  const used = communes.slice(0, TELECOM_MAX_COMMUNES);
  await ensureLookups();

  const qs = new URLSearchParams({ format: 'json', resource_id: ANFR_RESOURCE });
  for (const c of used) qs.append('refine.code_insee', c);
  const r = await fetch(ANFR_BASE + '?' + qs.toString());
  if (!r.ok) return json({ anfrOk: false, error: 'ANFR a repondu ' + r.status + ' (leur systeme de donnees connait parfois des pannes)' }, 502);
  let rows;
  try { rows = await r.json(); } catch (e) { return json({ anfrOk: false, error: 'reponse ANFR illisible' }, 502); }
  if (!Array.isArray(rows)) return json({ anfrOk: false, error: 'format ANFR inattendu' }, 502);

  const bySup = new Map();
  for (const row of rows) {
    const f = row.fields; if (!f || !f.sup_id) continue;
    const coords = row.geometry && row.geometry.coordinates; if (!coords) continue;
    const [slon, slat] = coords;
    const dist = haversineKm(lat, lon, slat, slon);
    if (dist > radiusKm) continue;
    let s = bySup.get(f.sup_id);
    if (!s) {
      s = {
        sup_id: f.sup_id, anfr: f.sta_nm_anfr || null,
        name: f.adr_lb_lieu || null,
        address: [f.adr_lb_add1, f.adr_lb_add2, f.adr_lb_add3].filter(Boolean).join(', ') || null,
        cp: f.adr_nm_cp || null,
        lat: slat, lon: slon,
        height_m: f.sup_nm_haut ? parseFloat(String(f.sup_nm_haut).replace(',', '.')) : null,
        owner: tpoMap.get(String(f.tpo_id)) || null, owner_code: f.tpo_id,
        nature: natMap.get(String(f.nat_id)) || null, nature_code: f.nat_id,
        operators: new Map(),
      };
      bySup.set(f.sup_id, s);
    }
    let op = s.operators.get(f.adm_lb_nom);
    if (!op) { op = { name: f.adm_lb_nom, systems: [] }; s.operators.set(f.adm_lb_nom, op); }
    op.systems.push({ system: f.emr_lb_systeme, generation: f.generation, status: f.statut, date: f.emr_dt });
  }
  const sites = [...bySup.values()].map(s => ({ ...s, operators: [...s.operators.values()] }));
  const body = { anfrOk: true, sites, communes: used };
  telecomCache.set(ckey, { t: Date.now(), body });
  if (telecomCache.size > 200) telecomCache.delete(telecomCache.keys().next().value);
  return json(body);
}


// memoire de l'isolat (conservee tant que Cloudflare le garde actif, sans garantie)
let token = null, tokenExp = 0;
const planeCache = new Map();
const shipMemo = new Map();

const cfg = (env, k) => (env && env[k]) || DEF[k];

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const origin = req.headers.get('Origin') || '';
    const allowed = (env.ALLOWED_ORIGIN || 'https://rousseauromain-art.github.io').split(',').map(s => s.trim()).filter(Boolean);
    const cors = {
      'Access-Control-Allow-Origin': allowed.includes(origin) ? origin : allowed[0],
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Max-Age': '86400',
      'Vary': 'Origin',
    };
    const json = (obj, status = 200) => new Response(JSON.stringify(obj), {
      status, headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
    });

    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });
    if (req.method !== 'GET') return json({ error: 'methode non autorisee' }, 405);

    if (url.pathname === '/' || url.pathname === '/health') {
      return json({
        ok: true, relais: 'villes-horizon',
        ais: !!env.AIS_KEY,
        opensky: !!(env.OPENSKY_CLIENT_ID && env.OPENSKY_CLIENT_SECRET),
        origines: allowed,
      });
    }

    // /telecom : pylones et points hauts ANFR (France uniquement) ; parametres lat/lon/radius_km,
    // distincts du format lamin/lomin/lamax/lomax utilise par /planes et /ships
    if (url.pathname === '/telecom') return handleTelecom(req, env, allowed);

    // /check : verifie reellement les identifiants (pas seulement leur presence) aupres
    // des deux services, pour que le diagnostic de l'app distingue "absent" de "invalide".
    // Les deux verifications tournent EN PARALLELE (Promise.all) et sont chacune bornees dans
    // le temps (CHECK_TIMEOUT_MS) : avant ce correctif, elles s'enchainaient et OpenSky pouvait
    // rester bloque tres longtemps (522 Cloudflare = timeout cote origine, souvent 30-100 s avant
    // l'echec), ce qui faisait largement depasser le delai cote client (20 s) de la page de debug —
    // "signal is aborted without reason" — avant meme que le relais ait fini de repondre
    // (retour terrain du 26/09/2026).
    if (url.pathname === '/check') {
      const [opensky, ais] = await Promise.all([checkOpenSky(env), checkAis(env)]);
      return json({ relais: true, opensky, ais });
    }

    // un navigateur envoie toujours Origin : les autres sites sont refuses
    if (origin && !allowed.includes(origin)) return json({ error: 'origine non autorisee : ' + origin }, 403);

    const box = parseBox(url);
    if (!box) return json({ error: `zone invalide : lamin, lomin, lamax, lomax requis, ${MAX_SPAN}° max par cote` }, 400);

    try {
      if (url.pathname === '/planes') return await planes(env, box, json);
      if (url.pathname === '/ships') return await ships(env, box, json);
    } catch (e) {
      return json({ error: 'erreur du relais : ' + (e && e.message || e) }, 502);
    }
    return json({ error: 'route inconnue' }, 404);
  },
};

async function handleTelecom(req, env, allowed) {
  const url = new URL(req.url);
  const origin = req.headers.get('Origin') || '';
  const cors = {
    'Access-Control-Allow-Origin': allowed.includes(origin) ? origin : allowed[0],
    'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin',
  };
  const json = (obj, status = 200) => new Response(JSON.stringify(obj), {
    status, headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
  });
  const lat = parseFloat(url.searchParams.get('lat')), lon = parseFloat(url.searchParams.get('lon'));
  const radius = parseFloat(url.searchParams.get('radius_km'));
  if (!isFinite(lat) || !isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) return json({ error: 'lat/lon invalides' }, 400);
  try { return await telecom(env, lat, lon, radius, json); }
  catch (e) { return json({ anfrOk: false, error: 'erreur du relais : ' + (e && e.message || e) }, 502); }
}

function parseBox(url) {
  const n = k => parseFloat(url.searchParams.get(k));
  const b = { lamin: n('lamin'), lomin: n('lomin'), lamax: n('lamax'), lomax: n('lomax') };
  if (Object.values(b).some(v => !isFinite(v))) return null;
  if (b.lamin >= b.lamax || b.lomin >= b.lomax) return null;
  if (b.lamin < -90 || b.lamax > 90 || b.lomin < -180 || b.lomax > 180) return null;
  if (b.lamax - b.lamin > MAX_SPAN || b.lomax - b.lomin > MAX_SPAN) return null;
  return b;
}

// ---------------- avions : OpenSky, OAuth2 client credentials ----------------
// serveur d'authentification OpenSky injoignable (522 Cloudflare, parfois 30-100 s avant l'echec) :
// la demande de jeton est bornee a TOKEN_TIMEOUT_MS et, en cas d'echec, on n'essaie plus pendant
// TOKEN_BACKOFF_MS — /planes repart aussitot en acces anonyme au lieu de bloquer jusqu'au delai
// de l'appli ("relais : delai depasse", retour terrain du 26/09/2026)
const TOKEN_TIMEOUT_MS = 6000, TOKEN_BACKOFF_MS = 10 * 60000;
let tokenFailUntil = 0, tokenFailMsg = '';
async function getToken(env) {
  if (!env.OPENSKY_CLIENT_ID || !env.OPENSKY_CLIENT_SECRET) return null;
  if (token && Date.now() < tokenExp) return token;
  if (Date.now() < tokenFailUntil) throw new Error(tokenFailMsg + ' — nouvel essai dans ' + Math.ceil((tokenFailUntil - Date.now()) / 60000) + ' min, acces anonyme en attendant');
  let r;
  try {
    r = await fetchTimeout(cfg(env, 'OPENSKY_TOKEN_URL'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'client_credentials', client_id: env.OPENSKY_CLIENT_ID, client_secret: env.OPENSKY_CLIENT_SECRET }),
    }, TOKEN_TIMEOUT_MS);
  } catch (e) {
    tokenFailUntil = Date.now() + TOKEN_BACKOFF_MS;
    tokenFailMsg = (e && e.name === 'AbortError') ? `serveur d'authentification OpenSky muet (${TOKEN_TIMEOUT_MS / 1000} s)` : 'authentification OpenSky injoignable';
    throw new Error(tokenFailMsg);
  }
  if (!r.ok) { tokenFailUntil = Date.now() + TOKEN_BACKOFF_MS; tokenFailMsg = 'authentification OpenSky refusee (' + r.status + ')'; throw new Error(tokenFailMsg); }
  const j = await r.json();
  token = j.access_token;
  tokenExp = Date.now() + Math.max(60, (j.expires_in || 1800) - 60) * 1000;
  return token;
}

// ---------------- /check : verifications individuelles, bornees dans le temps ----------------
const CHECK_TIMEOUT_MS = 8000; // chaque verification echoue proprement plutot que de bloquer /check
function fetchTimeout(url, opts, ms) {
  const ctrl = new AbortController(), t = setTimeout(() => ctrl.abort(), ms);
  return fetch(url, { ...opts, signal: ctrl.signal }).finally(() => clearTimeout(t));
}
async function checkOpenSky(env) {
  try {
    if (!env.OPENSKY_CLIENT_ID || !env.OPENSKY_CLIENT_SECRET) return 'absent (secrets non configures : acces anonyme)';
    // jeton dedie (pas getToken/le cache partage) : un depassement de CHECK_TIMEOUT_MS ne doit
    // pas laisser un jeton partiellement obtenu polluer le cache utilise par /planes
    const r0 = await fetchTimeout(cfg(env, 'OPENSKY_TOKEN_URL'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'client_credentials', client_id: env.OPENSKY_CLIENT_ID, client_secret: env.OPENSKY_CLIENT_SECRET }),
    }, CHECK_TIMEOUT_MS);
    if (!r0.ok) return `echec (authentification OpenSky refusee (${r0.status}))`;
    const tok = (await r0.json()).access_token;
    const r = await fetchTimeout(cfg(env, 'OPENSKY_STATES_URL') + '?lamin=0&lomin=0&lamax=0.5&lomax=0.5', { headers: { Authorization: 'Bearer ' + tok } }, CHECK_TIMEOUT_MS);
    return r.ok ? 'ok (jeton valide)' : `echec (OpenSky a repondu ${r.status})`;
  } catch (e) {
    return 'echec (' + ((e && e.name === 'AbortError') ? `pas de reponse en ${CHECK_TIMEOUT_MS / 1000} s` : (e && e.message || e)) + ')';
  }
}
async function checkAis(env) {
  if (!env.AIS_KEY) return 'absent (secret AIS_KEY non configure)';
  try {
    const resp = await fetchTimeout(cfg(env, 'AIS_URL'), { headers: { Upgrade: 'websocket' } }, CHECK_TIMEOUT_MS);
    const ws = resp.webSocket;
    if (!ws) return `echec (AISStream a repondu ${resp.status})`;
    ws.accept();
    ws.send(JSON.stringify({ APIKey: env.AIS_KEY, BoundingBoxes: [[[0, 0], [0.1, 0.1]]] }));
    const out = await new Promise(resolve => {
      const done = v => { clearTimeout(t); resolve(v); };
      const t = setTimeout(() => done('ok (cle acceptee, aucune erreur en 3 s — ne garantit pas la reception de donnees, voir /ships)'), 3000);
      ws.addEventListener('message', ev => {
        try { const m = JSON.parse(typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data));
          if (m.error) done('echec (cle refusee : ' + m.error + ')'); else done('ok (cle acceptee, donnees recues)');
        } catch (e) {}
      });
      ws.addEventListener('close', () => done('echec (connexion fermee sans confirmation)'));
      ws.addEventListener('error', () => done('echec (erreur de connexion)'));
    });
    try { ws.close(1000, 'verification'); } catch (e) {}
    return out;
  } catch (e) {
    return 'echec (' + ((e && e.name === 'AbortError') ? `pas de reponse en ${CHECK_TIMEOUT_MS / 1000} s` : (e && e.message || e)) + ')';
  }
}

async function planes(env, b, json) {
  const key = [b.lamin, b.lomin, b.lamax, b.lomax].map(v => v.toFixed(2)).join(',');
  const hit = planeCache.get(key);
  if (hit && Date.now() - hit.t < PLANE_CACHE_MS) return json({ ...hit.body, cache: true });

  let tok = null, authError = null;
  try { tok = await getToken(env); } catch (e) { authError = e.message; }

  const q = new URLSearchParams({ lamin: b.lamin, lomin: b.lomin, lamax: b.lamax, lomax: b.lomax });
  let r;
  try { r = await fetchTimeout(cfg(env, 'OPENSKY_STATES_URL') + '?' + q, { headers: tok ? { Authorization: 'Bearer ' + tok } : {} }, 8000); } // jeton (6 s max) + etats (8 s) < delai appli (15 s)
  catch (e) { return json({ error: (e && e.name === 'AbortError') ? 'OpenSky ne repond pas (8 s)' : 'OpenSky injoignable', authError }, 504); }
  if (r.status === 401 && tok) token = null; // jeton refuse : renouvele au prochain appel
  if (r.status === 429) {
    return json({ error: 'quota OpenSky atteint', retryAfter: +(r.headers.get('x-rate-limit-retry-after-seconds') || 0) }, 429);
  }
  if (!r.ok) return json({ error: 'OpenSky a repondu ' + r.status, authError }, 502);

  const j = await r.json();
  const body = {
    time: j.time, states: j.states || [],
    auth: tok ? 'client API' : 'anonyme', authError,
    creditsRestants: r.headers.get('x-rate-limit-remaining'),
  };
  planeCache.set(key, { t: Date.now(), body });
  if (planeCache.size > 50) planeCache.delete(planeCache.keys().next().value);
  return json(body);
}

// ---------------- navires : ecoute breve d'AISStream ----------------
async function ships(env, b, json) {
  if (!env.AIS_KEY) return json({ error: 'cle AISStream absente du relais (secret AIS_KEY)' }, 503);

  const cellKey = [b.lamin, b.lomin, b.lamax, b.lomax].map(v => v.toFixed(1)).join(',');
  let memo = shipMemo.get(cellKey);
  if (!memo) { memo = new Map(); shipMemo.set(cellKey, memo); if (shipMemo.size > 20) shipMemo.delete(shipMemo.keys().next().value); }

  const listenMs = +cfg(env, 'AIS_LISTEN_MS');
  let upstreamError = null, received = 0;
  const t0 = Date.now();

  const resp = await fetch(cfg(env, 'AIS_URL'), { headers: { Upgrade: 'websocket' } });
  const ws = resp.webSocket;
  if (!ws) return json({ error: 'AISStream a refuse la connexion (' + resp.status + ')' }, 502);
  ws.accept();
  // l'abonnement doit partir dans les 3 s suivant la connexion
  ws.send(JSON.stringify({
    APIKey: env.AIS_KEY,
    BoundingBoxes: [[[b.lamin, b.lomin], [b.lamax, b.lomax]]],
    FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport', 'ShipStaticData'],
  }));

  await new Promise(resolve => {
    const done = () => { clearTimeout(timer); resolve(); };
    const timer = setTimeout(done, listenMs);
    const dec = new TextDecoder();
    ws.addEventListener('message', ev => {
      let m;
      try { m = JSON.parse(typeof ev.data === 'string' ? ev.data : dec.decode(ev.data)); } catch (e) { return; }
      if (m.error) { upstreamError = String(m.error); done(); return; }
      received++;
      absorb(memo, m);
      if (received >= 500) done();
    });
    ws.addEventListener('close', done);
    ws.addEventListener('error', done);
  });
  try { ws.close(1000, 'fin'); } catch (e) {}

  const now = Date.now(), list = [];
  for (const [mmsi, v] of memo) {
    if (now - v.t > SHIP_TTL_MS) { memo.delete(mmsi); continue; }
    if (v.lat == null) continue;
    list.push({ mmsi, ...v });
  }
  return json({ ships: list, received, listenMs: now - t0, error: upstreamError });
}

function absorb(memo, m) {
  const md = m.MetaData || {}, msg = m.Message || {};
  const mmsi = md.MMSI != null ? String(md.MMSI) : null;
  if (!mmsi) return;
  const cur = memo.get(mmsi) || { name: '', lat: null, lon: null, sog: 0, cog: 0, type: null, t: 0 };
  const nm = (md.ShipName || '').trim();
  if (nm) cur.name = nm;
  if (m.MessageType === 'ShipStaticData') {
    const s = msg.ShipStaticData || {};
    if (s.Name && s.Name.trim()) cur.name = s.Name.trim();
    if (s.Type != null) cur.type = s.Type;
    memo.set(mmsi, cur);
    return;
  }
  const p = msg[m.MessageType] || {};
  const lat = p.Latitude != null ? p.Latitude : (md.latitude != null ? md.latitude : md.Latitude);
  const lon = p.Longitude != null ? p.Longitude : (md.longitude != null ? md.longitude : md.Longitude);
  if (lat == null || lon == null || Math.abs(lat) > 90 || Math.abs(lon) > 180) return;
  cur.lat = lat; cur.lon = lon;
  cur.sog = p.Sog != null && p.Sog < 102.3 ? p.Sog : 0;   // 102,3 = vitesse non disponible
  cur.cog = p.Cog != null && p.Cog < 360 ? p.Cog : 0;     // 360 = cap non disponible
  cur.t = Date.now();
  memo.set(mmsi, cur);
}
