/* =====================================================================
   Relais « Villes a l'horizon » — Cloudflare Worker (offre gratuite)
   ---------------------------------------------------------------------
   Detient les secrets cote serveur, jamais dans le navigateur :
     - AIS_KEY                : cle AISStream (bateaux)
     - OPENSKY_CLIENT_ID      : client API OpenSky (avions, OAuth2)
     - OPENSKY_CLIENT_SECRET
     - GEOAPIFY_KEY           : cle Geoapify Places (villes/sommets/POI, source supplementaire)
     - GEONAMES_USER          : identifiant GeoNames (pas une cle : un nom de compte, service web
                                a activer sur la page du compte geonames.org)
   Variable (non secrete) :
     - ALLOWED_ORIGIN         : site autorise, ex. https://rousseauromain-art.github.io

   Routes (GET) :
     /health                                  -> etat du relais et des secrets
     /planes?lamin=&lomin=&lamax=&lomax=      -> avions (OpenSky, jeton gere ici)
     /ships?lamin=&lomin=&lamax=&lomax=       -> navires (ecoute AISStream ~10 s)
     /telecom?lat=&lon=&radius_km=            -> pylones/points hauts telecom (ANFR, France uniquement)
              [&debug=1|all]                     debug=1 : detail de chaque route ANFR essayee ;
                                                  debug=all : les essaie TOUTES (comparaison)
     /geoapify?lat=&lon=&radius_km=&kind=     -> villes/sommets/POI (Geoapify Places), source
                                                  supplementaire en parallele d'Overpass/OpenFreeMap ;
                                                  kind = villes|sommets|lighthouse|view|heritage|beach
     /geonames?lat=&lon=&radius_km=&kind=     -> villes/sommets (GeoNames), autre source independante ;
                                                  kind = villes|sommets

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
// Constat du 28/09/2026 (tests manuels) : l'ancienne route "records/2.0/resource" repond 404 ; le jeu
// de donnees existe toujours (meme UUID de ressource, CSV du 24/09/2026). data.anfr.fr est un portail
// "d4c" : facade facon OpenDataSoft (records/1.0/search) posee sur un CKAN (records/1.0/download =
// datastore_search). Deux routes repondent : search v1 (par nom de jeu) et download (par UUID).
// Comme ANFR change parfois ses routes sans prevenir, le relais essaie plusieurs strategies dans
// l'ordre, garde en memoire celle qui a marche, et sait tout detailler (?debug=1 ou ?debug=all).
// Autres pieges constates :
//  - les lignes n'ont plus de "geometry" : coordonnees dans un texte "48.10 , -1.70" (ou DMS) ;
//  - un seul refine.code_insee est pris en compte par requete (les suivants sont ignores) ;
//  - Paris/Lyon/Marseille : ANFR range par ARRONDISSEMENT (75108...), jamais sous 75056 -> 0 resultat.
const ANFR_RESOURCE = '88ef0887-6b0f-4d3f-8545-6d64c8f597da';
const ANFR_DATASET = 'observatoire_2g_3g_4g';
const ANFR_API = 'https://data.anfr.fr/d4c/api/records/';
const GEO_BASE = 'https://geo.api.gouv.fr/communes';
const TPO_URL = 'https://data.anfr.fr/sites/default/files/dataset/dd1/1fac6-4531-4a27-9c8c-a3a9e4ec2107/sup_proprietaire_0.txt';
const NAT_URL = 'https://data.anfr.fr/sites/default/files/dataset/dd1/1fac6-4531-4a27-9c8c-a3a9e4ec2107/sup_nature_0.txt';
const TELECOM_MAX_KM = 15, TELECOM_MAX_COMMUNES = 12, TELECOM_CACHE_MS = 5 * 60000;
const ANFR_ROWS = 5000;          // lignes max par requete (une ligne = un operateur x un systeme radio)
const ANFR_TIMEOUT_MS = 12000;
const ANFR_WINNER_MS = 3600000;  // on reessaie d'abord la route gagnante pendant 1 h
const PLM = new Set(['75056', '69123', '13055']); // Paris, Lyon, Marseille : codes "commune" jamais utilises par ANFR
const ANFR_FIELDS = 'sup_id,adm_lb_nom,emr_lb_systeme,generation,statut,emr_dt,sta_nm_anfr,nat_id,sup_nm_haut,tpo_id,adr_lb_lieu,adr_lb_add1,adr_lb_add2,adr_lb_add3,adr_nm_cp,code_insee,coordonnees,coord';
let tpoMap = null, natMap = null, lookupsAt = 0, lookupsErr = null;
let anfrWinner = null, anfrWinnerAt = 0;
const telecomCache = new Map();

async function fetchTO(url, ms) {
  const ctrl = new AbortController(); const id = setTimeout(() => ctrl.abort(), ms);
  try { return await fetch(url, { signal: ctrl.signal, headers: { 'Accept': 'application/json' } }); }
  finally { clearTimeout(id); }
}
async function loadLookup(url) {
  const r = await fetchTO(url, 8000);
  if (!r.ok) throw new Error('lookup HTTP ' + r.status);
  const txt = await r.text(), map = new Map();
  txt.trim().split('\n').slice(1).forEach(line => {
    const i = line.indexOf(';'); if (i < 0) return;
    map.set(line.slice(0, i).trim(), line.slice(i + 1).trim());
  });
  return map;
}
// tables proprietaire/nature : utiles mais pas indispensables (sans elles on garde les sites, sans libelle)
async function ensureLookups() {
  if (tpoMap && natMap && Date.now() - lookupsAt < 24 * 3600000) return;
  try {
    const [tpo, nat] = await Promise.all([loadLookup(TPO_URL), loadLookup(NAT_URL)]);
    tpoMap = tpo; natMap = nat; lookupsAt = Date.now(); lookupsErr = null;
  } catch (e) { lookupsErr = String(e && e.message || e); tpoMap = tpoMap || new Map(); natMap = natMap || new Map(); }
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
    const base = `${GEO_BASE}?lat=${la.toFixed(5)}&lon=${lo.toFixed(5)}&fields=code&format=json`;
    try {
      const r = await fetchTO(base, 6000);
      if (!r.ok) return;
      const j = await r.json();
      if (!Array.isArray(j)) return;
      for (const c of j) {
        if (!c.code) continue;
        if (!PLM.has(c.code)) { codes.add(c.code); continue; }
        // Paris/Lyon/Marseille : on demande l'arrondissement de ce point, seul code connu d'ANFR
        try {
          const r2 = await fetchTO(base + '&type=arrondissement-municipal', 6000);
          const j2 = r2.ok ? await r2.json() : [];
          if (Array.isArray(j2)) for (const a2 of j2) if (a2.code) codes.add(a2.code);
        } catch (e) {}
      }
    } catch (e) {}
  }));
  return [...codes];
}

// coordonnees : geometry GeoJSON, geo_point [lat,lon], texte "lat , lon" ou DMS "48°6'2''N 1°42'16''W"
function parseDMS(s) {
  const m = /(\d+)°\s*(\d+)'\s*([\d.]+)(?:''|")\s*([NS])\s+(\d+)°\s*(\d+)'\s*([\d.]+)(?:''|")\s*([EW])/i.exec(s);
  if (!m) return null;
  const v = (d, mi, se, h) => (+d + mi / 60 + se / 3600) * (/[SW]/i.test(h) ? -1 : 1);
  return [v(m[1], m[2], m[3], m[4]), v(m[5], m[6], m[7], m[8])];
}
function coordsOf(f, r) {
  const g = r && r.geometry && r.geometry.coordinates;
  if (Array.isArray(g) && g.length >= 2 && isFinite(g[0]) && isFinite(g[1])) return [+g[1], +g[0]];
  for (const k of ['geo_point_2d', 'coord', 'coordonnees']) {
    const v = f[k];
    if (Array.isArray(v) && v.length === 2 && isFinite(v[0]) && isFinite(v[1])) return [+v[0], +v[1]];
    if (v && typeof v === 'object' && isFinite(v.lat) && isFinite(v.lon)) return [+v.lat, +v.lon];
  }
  if (typeof f.coordonnees === 'string') {
    const p = f.coordonnees.split(/\s*[,;]\s*/).map(x => parseFloat(x));
    if (p.length === 2 && isFinite(p[0]) && isFinite(p[1]) && Math.abs(p[0]) <= 90) return p;
  }
  if (typeof f.coord === 'string') return parseDMS(f.coord);
  return null;
}
// les 3 formats de reponse rencontres : tableau (ancienne 2.0), OpenDataSoft v1 (records/nhits),
// CKAN datastore (success/result.records/result.total). HTTP 200 + "status":"error" = echec.
function extractRows(j) {
  if (Array.isArray(j)) return { rows: j.map(r => ({ f: r.fields || r, r })), total: j.length };
  if (!j || typeof j !== 'object') throw new Error('reponse vide');
  if (j.success === false) throw new Error('refus CKAN : ' + JSON.stringify(j.error || {}).slice(0, 160));
  if (j.status === 'error') throw new Error('requete refusee par ANFR ("status":"error")');
  if (j.result && Array.isArray(j.result.records)) return { rows: j.result.records.map(r => ({ f: r, r })), total: +j.result.total || j.result.records.length };
  if (Array.isArray(j.records)) return { rows: j.records.map(r => ({ f: r.fields || r, r })), total: isFinite(j.nhits) ? +j.nhits : j.records.length };
  throw new Error('format de reponse inattendu (' + Object.keys(j).slice(0, 5).join(',') + ')');
}

// Strategies, de la plus economique a la plus lourde. "urls" recoit le contexte et renvoie la liste
// des URL a interroger (une seule, ou une par commune). Les virgules et deux-points restent en clair
// dans geofilter (autorises dans une query string ; certains serveurs ne les re-decodent pas).
const enc = encodeURIComponent;
const ANFR_STRATEGIES = [
  { id: 'search-geofilter', label: 'Search v1 + filtre par rayon (1 requete, sans communes)', communes: false,
    urls: c => [`${ANFR_API}1.0/search/?dataset=${ANFR_DATASET}&rows=${ANFR_ROWS}&geofilter.distance=${c.lat.toFixed(5)},${c.lon.toFixed(5)},${Math.round(c.radiusKm * 1000)}`],
    // si ANFR ignore le filtre, il renvoie toute la France (~830 000 lignes) : c'est un echec deguise
    sane: (tot) => tot < 200000 },
  { id: 'download-filters', label: 'Download (CKAN) + filtre JSON multi-communes (1 requete)', communes: true,
    urls: c => [`${ANFR_API}1.0/download/?resource_id=${ANFR_RESOURCE}&limit=${ANFR_ROWS * 3}&fields=${ANFR_FIELDS}&filters=${enc(JSON.stringify({ code_insee: c.communes }))}`],
    sane: (tot, c, rows) => rows.every(x => !x.f.code_insee || c.communes.includes(String(x.f.code_insee))) },
  { id: 'download-refine', label: 'Download (CKAN) par commune', communes: true,
    urls: c => c.communes.map(k => `${ANFR_API}1.0/download/?resource_id=${ANFR_RESOURCE}&limit=${ANFR_ROWS}&fields=${ANFR_FIELDS}&refine.code_insee=${enc(k)}`) },
  { id: 'search-refine', label: 'Search v1 par commune', communes: true,
    urls: c => c.communes.map(k => `${ANFR_API}1.0/search/?dataset=${ANFR_DATASET}&rows=${ANFR_ROWS}&refine.code_insee=${enc(k)}`) },
  { id: 'legacy-resource', label: 'Ancienne route records/2.0/resource (morte le 28/09/2026)', communes: true,
    urls: c => c.communes.map(k => `${ANFR_API}2.0/resource/?format=json&resource_id=${ANFR_RESOURCE}&refine.code_insee=${enc(k)}`) },
];

async function runStrategy(s, ctx) {
  const t0 = Date.now(), att = { strategy: s.id, label: s.label, calls: [] };
  if (s.communes && !ctx.communes.length) { att.ok = false; att.error = 'aucune commune trouvee'; return { att }; }
  const urls = s.urls(ctx);
  const results = await Promise.all(urls.map(async u => {
    const call = { url: u };
    try {
      const r = await fetchTO(u, ANFR_TIMEOUT_MS);
      call.http = r.status;
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const txt = await r.text(); call.bytes = txt.length;
      let j; try { j = JSON.parse(txt); } catch (e) { throw new Error('pas du JSON (' + txt.slice(0, 60).replace(/\s+/g, ' ') + ')'); }
      const out = extractRows(j);
      call.rows = out.rows.length; call.total = out.total;
      if (out.total > out.rows.length) call.truncated = true;
      return { ok: true, out, call };
    } catch (e) {
      call.error = e && e.name === 'AbortError' ? 'delai depasse' : String(e && e.message || e);
      return { ok: false, call };
    }
  }));
  att.calls = results.map(x => x.call); att.ms = Date.now() - t0;
  const good = results.filter(x => x.ok);
  // echec si la moindre requete tombe en 404 / refus (route cassee), ou si rien n'a marche
  const broken = results.find(x => !x.ok && (/HTTP 4\d\d|refus|status|format|JSON/.test(x.call.error)));
  if (!good.length || broken) { att.ok = false; att.error = (broken || results[0]).call.error; return { att }; }
  const rows = good.flatMap(x => x.out.rows);
  const total = good.reduce((a, x) => a + (x.out.total || 0), 0);
  if (s.sane && !s.sane(total, ctx, rows)) { att.ok = false; att.error = `filtre ignore par ANFR (${total} lignes pour toute la zone)`; return { att }; }
  att.ok = true; att.rows = rows.length; att.total = total;
  if (good.length < results.length) att.partial = `${results.length - good.length} requete(s) sur ${results.length} en echec`;
  return { att, rows };
}

function groupSites(rows, lat, lon, radiusKm) {
  const bySup = new Map(); let noCoord = 0;
  for (const { f, r } of rows) {
    if (!f || !f.sup_id) continue;
    const ll = coordsOf(f, r); if (!ll) { noCoord++; continue; }
    const [slat, slon] = ll;
    if (haversineKm(lat, lon, slat, slon) > radiusKm) continue;
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
  return { sites: [...bySup.values()].map(s => ({ ...s, operators: [...s.operators.values()] })), noCoord };
}

// debug : '' (normal), '1' (detail des tentatives), 'all' (essaie TOUTES les strategies, pour comparer)
async function telecom(env, lat, lon, radiusKm, json, debug) {
  radiusKm = Math.min(TELECOM_MAX_KM, Math.max(0.5, radiusKm || 5));
  const ckey = `${lat.toFixed(3)},${lon.toFixed(3)},${radiusKm}`;
  const hit = telecomCache.get(ckey);
  if (!debug && hit && Date.now() - hit.t < TELECOM_CACHE_MS) return json({ ...hit.body, cache: true });

  const tc = Date.now();
  const communesAll = await communesInRadius(lat, lon, radiusKm);
  const communesMs = Date.now() - tc;
  if (!communesAll.length) {
    const body = { anfrOk: true, hors_france: true, sites: [], communes: [] };
    telecomCache.set(ckey, { t: Date.now(), body });
    return json(debug ? { ...body, debug: { communesMs, note: 'geo.api.gouv.fr : aucune commune (hors de France, ou service en panne)' } } : body);
  }
  const communes = communesAll.slice(0, TELECOM_MAX_COMMUNES);
  await ensureLookups();
  const ctx = { lat, lon, radiusKm, communes };

  // ordre : route gagnante recente d'abord, puis les autres dans l'ordre de preference
  let order = ANFR_STRATEGIES.slice();
  if (anfrWinner && Date.now() - anfrWinnerAt < ANFR_WINNER_MS && debug !== 'all') {
    order = [order.find(s => s.id === anfrWinner), ...order.filter(s => s.id !== anfrWinner)].filter(Boolean);
  }
  const attempts = []; let rows = null, used = null;
  for (const s of order) {
    const { att, rows: got } = await runStrategy(s, ctx);
    attempts.push(att);
    if (att.ok && !rows) { rows = got; used = s; if (debug !== 'all') break; }
  }
  const dbg = debug ? { communes: communesAll, communesMs, lookups: lookupsErr || 'ok', attempts } : undefined;
  if (!rows) {
    anfrWinner = null;
    const resume = attempts.map(a => `${a.strategy} : ${a.error}`).join(' | ');
    return json({ anfrOk: false, error: 'toutes les routes ANFR ont echoue — ' + resume, debug: dbg }, 502);
  }
  anfrWinner = used.id; anfrWinnerAt = Date.now();
  const { sites, noCoord } = groupSites(rows, lat, lon, radiusKm);
  const winAtt = attempts.find(a => a.strategy === used.id);
  const body = { anfrOk: true, sites, communes, route: used.id,
    truncated: !!(winAtt && winAtt.calls.some(c => c.truncated)), noCoord: noCoord || undefined };
  telecomCache.set(ckey, { t: Date.now(), body });
  if (telecomCache.size > 200) telecomCache.delete(telecomCache.keys().next().value);
  return json(debug ? { ...body, debug: dbg } : body);
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
        geoapify: !!env.GEOAPIFY_KEY,
        geonames: !!env.GEONAMES_USER,
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

    // /osm : sonde de diagnostic vers les miroirs Overpass (villes/lieux/reperes hauts).
    // L'appli elle-meme n'utilise PAS cette route (elle interroge Overpass directement depuis le
    // navigateur, pour beneficier du cache/anti-429 cote client) : cette route sert uniquement a
    // interroger les 3 miroirs de l'exterieur (sans dependre du reseau du telephone) pendant un
    // diagnostic, ex. GET /osm?q=<requete Overpass QL encodee>&mirror=de (optionnel, sinon les 3
    // miroirs en parallele). Retour terrain du 27/09/2026 : les 3 miroirs ont refuse la meme requete
    // "villes" (rayon 60 km + toutes les categories de POI), 2 en delai depasse (20 s), 1 en 504
    // apres 9 s — cette route permet de reproduire et d'isoler ce genre de panne sans aller-retour.
    if (url.pathname === '/osm') return await osmDebug(env, url, json);

    // un navigateur envoie toujours Origin : les autres sites sont refuses
    if (origin && !allowed.includes(origin)) return json({ error: 'origine non autorisee : ' + origin }, 403);

    // /geoapify et /geonames consomment un quota nominatif (credits/jour lies au compte du relais,
    // contrairement a ANFR/Overpass qui sont anonymes et gratuits) : places APRES le controle
    // d'origine ci-dessus, volontairement, pour limiter leur usage au site autorise.
    if (url.pathname === '/geoapify') return handleGeoapify(req, env, allowed);
    if (url.pathname === '/geonames') return handleGeonames(req, env, allowed);

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
  const debug = url.searchParams.get('debug') === 'all' ? 'all' : url.searchParams.get('debug') ? '1' : '';
  try { return await telecom(env, lat, lon, radius, json, debug); }
  catch (e) { return json({ anfrOk: false, error: 'erreur du relais : ' + (e && e.message || e) }, 502); }
}

// ---------------- /geoapify : villes/sommets/POI supplementaires (Geoapify Places API) ----------------
// Autre source independante d'Overpass/OpenFreeMap : Geoapify agrege plusieurs fournisseurs sur sa
// propre infrastructure (offre gratuite : 3000 credits/jour, 1 credit = jusqu'a 20 resultats, aucune
// carte bancaire requise). Cle GEOAPIFY_KEY, jamais exposee au navigateur. Categories Geoapify sans
// equivalent documente pour "cape" et "rock" (les pointes/rochers du groupe POI de l'appli) : ces
// deux groupes ne sont donc PAS couverts ici, Overpass/OpenFreeMap restent les seules sources pour eux.
const GEOAPIFY_URL = 'https://api.geoapify.com/v2/places';
const GEOAPIFY_GROUPS = {
  villes: ['populated_place.city', 'populated_place.town', 'populated_place.village', 'populated_place.hamlet'],
  sommets: ['natural.mountain.peak'],
  lighthouse: ['man_made.lighthouse'],
  view: ['tourism.attraction.viewpoint', 'tourism.attraction'],
  heritage: ['heritage', 'heritage.unesco', 'tourism.sights.castle', 'tourism.sights.fort', 'tourism.sights.ruines'],
  beach: ['beach', 'beach.beach_resort'],
};
// sous-categorie -> "kind" attendu cote appli (voir kindOf() dans villes-horizon.html) ; pour les
// groupes a kind unique (sommets, lighthouse, view, heritage, beach) le kind est fixe, voir plus bas
const GEOAPIFY_KIND_BY_CAT = {
  'populated_place.city': 'city', 'populated_place.town': 'town',
  'populated_place.village': 'village', 'populated_place.hamlet': 'hamlet',
};
const GEOAPIFY_MAX_KM = 200, GEOAPIFY_CACHE_MS = 10 * 60000;
const geoapifyCache = new Map();
async function handleGeoapify(req, env, allowed) {
  const url = new URL(req.url);
  const origin = req.headers.get('Origin') || '';
  const cors = {
    'Access-Control-Allow-Origin': allowed.includes(origin) ? origin : allowed[0],
    'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin',
  };
  const json = (obj, status = 200) => new Response(JSON.stringify(obj), {
    status, headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
  });
  if (!env.GEOAPIFY_KEY) return json({ error: 'cle Geoapify absente du relais (secret GEOAPIFY_KEY)' }, 503);

  const lat = parseFloat(url.searchParams.get('lat')), lon = parseFloat(url.searchParams.get('lon'));
  const kind = url.searchParams.get('kind');
  const cats = GEOAPIFY_GROUPS[kind];
  if (!isFinite(lat) || !isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) return json({ error: 'lat/lon invalides' }, 400);
  if (!cats) return json({ error: 'kind inconnu, attendu : ' + Object.keys(GEOAPIFY_GROUPS).join(', ') }, 400);
  const radiusKm = Math.min(GEOAPIFY_MAX_KM, Math.max(0.5, parseFloat(url.searchParams.get('radius_km')) || 30));
  const limit = Math.min(200, Math.max(1, parseInt(url.searchParams.get('limit'), 10) || 100));

  const ckey = `${kind},${lat.toFixed(3)},${lon.toFixed(3)},${radiusKm},${limit}`;
  const hit = geoapifyCache.get(ckey);
  if (hit && Date.now() - hit.t < GEOAPIFY_CACHE_MS) return json({ ...hit.body, cache: true });

  const qs = new URLSearchParams({
    categories: cats.join(','),
    filter: `circle:${lon},${lat},${Math.round(radiusKm * 1000)}`,
    limit: String(limit),
    apiKey: env.GEOAPIFY_KEY,
  });
  let r;
  try { r = await fetchTimeout(GEOAPIFY_URL + '?' + qs, {}, 10000); }
  catch (e) { return json({ error: (e && e.name === 'AbortError') ? 'Geoapify ne repond pas (10 s)' : 'Geoapify injoignable' }, 504); }
  if (!r.ok) {
    let msg = ''; try { msg = (await r.json()).message || ''; } catch (e) {}
    return json({ error: 'Geoapify a repondu ' + r.status + (msg ? ' : ' + msg : '') }, 502);
  }
  let j; try { j = await r.json(); } catch (e) { return json({ error: 'reponse Geoapify illisible' }, 502); }

  const seen = new Set(), got = [];
  for (const f of (j.features || [])) {
    const p = f.properties || {}; const name = p.name; if (!name) continue;
    const lt = p.lat, ln = p.lon; if (lt == null || ln == null) continue;
    let placeKind = null;
    for (const c of (p.categories || [])) if (GEOAPIFY_KIND_BY_CAT[c]) { placeKind = GEOAPIFY_KIND_BY_CAT[c]; break; }
    const resolvedKind = placeKind || (kind === 'sommets' ? 'peak' : kind);
    const key = name + '@' + lt.toFixed(4) + ',' + ln.toFixed(4);
    if (seen.has(key)) continue; seen.add(key);
    // Geoapify Places ne fournit pas d'altitude dans les champs standard : ele reste null (la fusion
    // cote appli la complete depuis une autre source si elle la connait, cf. mergeByNameProximity)
    got.push({ name, lat: lt, lon: ln, kind: resolvedKind, pop: +(p.population || 0), ele: (typeof p.elevation === 'number') ? p.elevation : null });
  }
  const body = { places: got };
  geoapifyCache.set(ckey, { t: Date.now(), body });
  if (geoapifyCache.size > 300) geoapifyCache.delete(geoapifyCache.keys().next().value);
  return json(body);
}

// ---------------- /geonames : villes/sommets, autre source independante (GeoNames) ----------------
// Necessite un compte GRATUIT sur geonames.org avec le service web active (page du compte, case
// "Enable"), distinct d'une simple cle API : GEONAMES_USER est cet identifiant de compte, jamais
// expose au navigateur. Cout en "credits" (quota par defaut du compte gratuit, non verifie ici) :
// findNearbyPlaceNameJSON = 3 credits/appel, findNearbyJSON = 4/appel.
const GEONAMES_BASE = 'https://secure.geonames.org';
const GEONAMES_MAX_KM = 200, GEONAMES_CACHE_MS = 10 * 60000;
const geonamesCache = new Map();
// GeoNames ne classe pas ses lieux avec la meme finesse que le tag OSM "place" (city/town/village/
// hamlet) : seuil approximatif par population, uniquement pour choisir un rang d'affichage cote appli
function geonamesKindFromPop(pop) {
  if (pop >= 100000) return 'city';
  if (pop >= 10000) return 'town';
  if (pop >= 1000) return 'village';
  return 'hamlet';
}
async function handleGeonames(req, env, allowed) {
  const url = new URL(req.url);
  const origin = req.headers.get('Origin') || '';
  const cors = {
    'Access-Control-Allow-Origin': allowed.includes(origin) ? origin : allowed[0],
    'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Max-Age': '86400', 'Vary': 'Origin',
  };
  const json = (obj, status = 200) => new Response(JSON.stringify(obj), {
    status, headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' },
  });
  if (!env.GEONAMES_USER) return json({ error: 'identifiant GeoNames absent du relais (secret GEONAMES_USER)' }, 503);

  const lat = parseFloat(url.searchParams.get('lat')), lon = parseFloat(url.searchParams.get('lon'));
  const kind = url.searchParams.get('kind');
  if (!isFinite(lat) || !isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) return json({ error: 'lat/lon invalides' }, 400);
  if (kind !== 'villes' && kind !== 'sommets') return json({ error: 'kind inconnu, attendu : villes, sommets' }, 400);
  const radiusKm = Math.min(GEONAMES_MAX_KM, Math.max(1, parseFloat(url.searchParams.get('radius_km')) || 30));
  const maxRows = Math.min(100, Math.max(1, parseInt(url.searchParams.get('max'), 10) || 30));

  const ckey = `${kind},${lat.toFixed(3)},${lon.toFixed(3)},${radiusKm},${maxRows}`;
  const hit = geonamesCache.get(ckey);
  if (hit && Date.now() - hit.t < GEONAMES_CACHE_MS) return json({ ...hit.body, cache: true });

  const qs = new URLSearchParams({ lat: String(lat), lng: String(lon), radius: String(radiusKm), maxRows: String(maxRows), style: 'FULL', username: env.GEONAMES_USER });
  let endpoint;
  if (kind === 'villes') endpoint = GEONAMES_BASE + '/findNearbyPlaceNameJSON?' + qs;
  else { qs.append('featureClass', 'T'); qs.append('featureCode', 'PK'); qs.append('featureCode', 'MT'); endpoint = GEONAMES_BASE + '/findNearbyJSON?' + qs; }

  let r;
  try { r = await fetchTimeout(endpoint, {}, 10000); }
  catch (e) { return json({ error: (e && e.name === 'AbortError') ? 'GeoNames ne repond pas (10 s)' : 'GeoNames injoignable' }, 504); }
  if (!r.ok) return json({ error: 'GeoNames a repondu ' + r.status }, 502);
  let j; try { j = await r.json(); } catch (e) { return json({ error: 'reponse GeoNames illisible' }, 502); }
  if (j.status) return json({ error: 'GeoNames : ' + (j.status.message || 'erreur inconnue') }, 502);

  const seen = new Set(), got = [];
  for (const g of (j.geonames || [])) {
    const name = g.name || g.toponymName; if (!name) continue;
    const lt = parseFloat(g.lat), ln = parseFloat(g.lng); if (!isFinite(lt) || !isFinite(ln)) continue;
    const key = name + '@' + lt.toFixed(4) + ',' + ln.toFixed(4);
    if (seen.has(key)) continue; seen.add(key);
    const pop = +(g.population || 0);
    const rawEle = g.elevation != null ? g.elevation : (g.srtm3 != null ? g.srtm3 : g.astergdem);
    const ele = (typeof rawEle === 'number' && rawEle > -1000) ? rawEle : null; // -32768 = "sans donnee" (sentinelle SRTM)
    got.push({ name, lat: lt, lon: ln, kind: kind === 'sommets' ? 'peak' : geonamesKindFromPop(pop), pop, ele });
  }
  const body = { places: got };
  geonamesCache.set(ckey, { t: Date.now(), body });
  if (geonamesCache.size > 300) geonamesCache.delete(geonamesCache.keys().next().value);
  return json(body);
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

// ---------------- /osm : sonde de diagnostic vers les miroirs Overpass ----------------
// OSM_TIMEOUT_MS volontairement bien plus court que le delai cote appli (20 s/miroir) : cette
// route est appelee par des outils externes (ex. recuperation web automatisee) dont le delai de
// lecture est souvent < 15-16 s. Quand un miroir reste totalement muet (ni erreur, ni reponse),
// Promise.all attend le plus lent des 3 : mieux vaut echouer proprement en quelques secondes,
// avec le detail de chaque miroir, que de faire "timeout" cote appelant sans aucune info (retour
// terrain du 27/09/2026 : meme une requete minuscule restait bloquee 20 s, un miroir ne repondant
// jamais). Reglable via ?ms=2000..15000 si besoin d'attendre un peu plus longtemps.
const OSM_ENDPOINTS = ['https://overpass.private.coffee/api/interpreter', 'https://maps.mail.ru/osm/tools/overpass/api/interpreter', 'https://overpass-api.de/api/interpreter'];
const OSM_TIMEOUT_MS = 8000, OSM_MAX_Q = 4000;
async function osmDebug(env, url, json) {
  const q = url.searchParams.get('q');
  if (!q || q.length > OSM_MAX_Q) return json({ error: `parametre q manquant ou trop long (max ${OSM_MAX_Q} caracteres, recu ${q ? q.length : 0})` }, 400);
  const which = url.searchParams.get('mirror'); // fragment d'hote optionnel (ex. "de", "mail.ru") ; sinon les 3 en parallele
  const msParam = parseInt(url.searchParams.get('ms'), 10);
  const timeoutMs = isFinite(msParam) ? Math.min(15000, Math.max(2000, msParam)) : OSM_TIMEOUT_MS;
  const targets = which ? OSM_ENDPOINTS.filter(e => e.includes(which)) : OSM_ENDPOINTS;
  if (!targets.length) return json({ error: 'mirror inconnu', options: OSM_ENDPOINTS.map(e => new URL(e).hostname) }, 400);

  const results = await Promise.all(targets.map(async ep => {
    const host = new URL(ep).hostname, t0 = Date.now();
    try {
      const r = await fetchTimeout(ep, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: 'data=' + encodeURIComponent(q) }, timeoutMs);
      const ms = Date.now() - t0;
      if (!r.ok) return { mirror: host, ok: false, ms, error: 'HTTP ' + r.status };
      let j; try { j = await r.json(); } catch (e) { return { mirror: host, ok: false, ms, error: 'reponse illisible (pas du JSON)' }; }
      const elements = j.elements || [];
      const named = elements.map(e => e.tags && (e.tags['name:fr'] || e.tags.name)).filter(Boolean);
      return { mirror: host, ok: true, ms, elements: elements.length, named: named.length, sample: named.slice(0, 25) };
    } catch (e) {
      const ms = Date.now() - t0;
      return { mirror: host, ok: false, ms, error: (e && e.name === 'AbortError') ? `delai depasse (${timeoutMs / 1000}s)` : (e && e.message || String(e)) };
    }
  }));
  return json({ q, timeoutMs, results });
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
