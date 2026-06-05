import fs from 'fs/promises';
import path from 'path';
import yaml from 'js-yaml';
import { parse as parseCsv } from 'csv-parse/sync';
import Fastify, { FastifyReply, FastifyRequest } from 'fastify';
import { createClient, RedisClientType } from 'redis';

const app = Fastify({ logger: true });
const ROOT_DIR = path.resolve(__dirname, '..');
const PROVIDERS_PATH = path.join(ROOT_DIR, 'providers.yml');
const PROVIDERS_FALLBACK_PATH = path.join(ROOT_DIR, 'providers.example.yml');

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const CACHE_TTL = Number(process.env.CACHE_TTL_SECONDS ?? '840');
const USER_AGENT = process.env.USER_AGENT ?? 'TRMNL-Skywatch-Plugin/1.0';
const GEO_CACHE_TTL = 30 * 24 * 3600;
const ENABLE_IP_WHITELIST =
  String(process.env.ENABLE_IP_WHITELIST ?? 'false').toLowerCase() === 'true';
const IP_REFRESH_HOURS = Number(process.env.IP_REFRESH_HOURS ?? '24');
const MAX_QUEUE_SIZE = 20;
const QUEUE_TIMEOUT = 5000;
const MAX_PLANES = 30;
const MAX_CACHE_PLANES = 250;
const AIRPORT_CACHE_TTL = 24 * 3600;
const ROUTE_CACHE_TTL = 4 * 3600;
const OURAIRPORTS_CSV_URL =
  'https://davidmegginson.github.io/ourairports-data/airports.csv';
const OURAIRPORTS_CACHE_KEY = 'skywatch:ourairports';
const STATS_STARTED_AT_KEY = 'skywatch:stats:started_at';
const STATS_KEY = 'skywatch:stats';
const RADIUS_NM = Number(process.env.RADIUS_NM ?? '50');
const FETCH_RADIUS_NM = RADIUS_NM + 25.0;

interface Provider {
  name: string;
  url: string;
  ac_key?: string;
  cooldown_ms: number;
}

interface AirportRecord {
  iata: string;
  icao: string;
  name: string;
  municipality?: string;
  country?: string;
  lat: number;
  lon: number;
}

interface RouteAirport {
  icao: string;
  code: string;
  name: string;
  municipality: string;
  country: string;
  lat: number;
  lon: number;
}

interface RouteData {
  origin: RouteAirport;
  destination: RouteAirport;
}

interface ApiTask {
  latKey: number;
  lonKey: number;
  showGround: boolean;
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
}

const redisClient: RedisClientType = createClient({ url: REDIS_URL });
let providers: Provider[] = [];
let providerLastCall = new Map<string, number>();
let queue: ApiTask[] = [];
let queueNotifier: (() => void) | null = null;
let inflight = new Map<string, Promise<unknown>>();
let backoffUntil = 0;
let adsbdbBackoffUntil = 0;
let trmnlIps = new Set<string>();

class Semaphore {
  private available: number;
  private waiting: Array<() => void> = [];

  constructor(max: number) {
    this.available = max;
  }

  async acquire(): Promise<void> {
    if (this.available > 0) {
      this.available -= 1;
      return;
    }
    await new Promise<void>((resolve) => this.waiting.push(resolve));
  }

  release(): void {
    this.available += 1;
    const next = this.waiting.shift();
    if (next) {
      this.available -= 1;
      next();
    }
  }
}

const routeSemaphore = new Semaphore(5);

function tileKey(lat: number, lon: number): [number, number] {
  return [Math.round(lat * 2), Math.round(lon * 2)];
}

function tileCenter(latKey: number, lonKey: number): [number, number] {
  return [latKey / 2.0, lonKey / 2.0];
}

function cacheKey(latKey: number, lonKey: number, showGround: boolean): string {
  return `skywatch:planes:${latKey}:${lonKey}:${showGround ? 1 : 0}`;
}

function airportCacheKey(latKey: number, lonKey: number): string {
  return `skywatch:airports:${latKey}:${lonKey}`;
}

function geoKey(address: string): string {
  return `skywatch:geo:${address.toLowerCase().trim()}`;
}

function routeKey(callsign: string): string {
  return `skywatch:route:${callsign.trim().toLowerCase()}`;
}

function haversineNm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const R = 3440.065;
  const phi1 = toRadians(lat1);
  const phi2 = toRadians(lat2);
  const dphi = toRadians(lat2 - lat1);
  const dlambda = toRadians(lon2 - lon1);
  const a =
    Math.sin(dphi / 2) ** 2 +
    Math.cos(phi1) * Math.cos(phi2) * Math.sin(dlambda / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function toRadians(value: number): number {
  return (value * Math.PI) / 180;
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit = {},
  timeoutMs = 10000,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

function selectProvider(): Provider | null {
  const now = Date.now() / 1000;
  for (const provider of providers) {
    const last = providerLastCall.get(provider.name) ?? 0;
    if (now - last >= provider.cooldown_ms / 1000.0) {
      return provider;
    }
  }
  return null;
}

async function getFromCache(
  latKey: number,
  lonKey: number,
  showGround: boolean,
): Promise<any | null> {
  const raw = await redisClient.get(cacheKey(latKey, lonKey, showGround));
  return raw ? JSON.parse(raw) : null;
}

async function setCache(
  latKey: number,
  lonKey: number,
  showGround: boolean,
  data: unknown,
): Promise<void> {
  if (CACHE_TTL <= 0) {
    return;
  }
  await redisClient.setEx(
    cacheKey(latKey, lonKey, showGround),
    CACHE_TTL,
    JSON.stringify(data),
  );
}

async function incrementStat(field: string, amount = 1): Promise<void> {
  await redisClient.hIncrBy(STATS_KEY, field, amount);
}

async function loadProviders(): Promise<Provider[]> {
  let contents: string;
  try {
    contents = await fs.readFile(PROVIDERS_PATH, 'utf8');
  } catch {
    contents = await fs.readFile(PROVIDERS_FALLBACK_PATH, 'utf8');
  }
  const data = yaml.load(contents) as { providers?: Provider[] } | null;
  return data?.providers ?? [];
}

async function fetchTrmnlIps(): Promise<Set<string>> {
  try {
    const response = await fetchWithTimeout(
      'https://trmnl.com/api/ips',
      { headers: { 'User-Agent': USER_AGENT } },
      10000,
    );
    if (!response.ok) {
      app.log.error(
        `Failed to fetch TRMNL IPs: ${response.status} ${response.statusText}`,
      );
      return new Set();
    }
    const data = (await response.json()) as any;
    const ips = new Set<string>();
    for (const ip of data?.data?.ipv4 ?? []) {
      ips.add(String(ip));
    }
    for (const ip of data?.data?.ipv6 ?? []) {
      ips.add(String(ip));
    }
    app.log.info(`Fetched ${ips.size} TRMNL IPs`);
    return ips;
  } catch (error) {
    app.log.error(`Failed to fetch TRMNL IPs: ${error}`);
    return new Set();
  }
}

function parseClientIp(request: FastifyRequest): string | undefined {
  const forwarded = request.headers['x-forwarded-for'];
  if (typeof forwarded === 'string' && forwarded.length > 0) {
    return forwarded.split(',')[0].trim();
  }
  const cfIp = request.headers['cf-connecting-ip'];
  if (typeof cfIp === 'string' && cfIp.length > 0) {
    return cfIp;
  }
  return request.ip;
}

function checkIpWhitelist(request: FastifyRequest): boolean {
  if (!ENABLE_IP_WHITELIST) {
    return true;
  }
  const clientIp = parseClientIp(request);
  return clientIp ? trmnlIps.has(clientIp) : false;
}

async function loadOurAirports(): Promise<AirportRecord[]> {
  const raw = await redisClient.get(OURAIRPORTS_CACHE_KEY);
  if (!raw) {
    return [];
  }
  return JSON.parse(raw) as AirportRecord[];
}

async function refreshOurAirports(): Promise<void> {
  app.log.info('Refreshing OurAirports CSV...');
  try {
    const response = await fetchWithTimeout(
      OURAIRPORTS_CSV_URL,
      { headers: { 'User-Agent': USER_AGENT } },
      30000,
    );
    if (!response.ok) {
      app.log.error(
        `OurAirports fetch error: ${response.status} ${response.statusText}`,
      );
      return;
    }
    const text = await response.text();
    const records = parseCsv(text, {
      columns: true,
      skip_empty_lines: true,
    }) as Record<string, string>[];
    const airports: AirportRecord[] = [];
    for (const row of records) {
      if (row.type !== 'large_airport' && row.type !== 'medium_airport') {
        continue;
      }
      const iata = (row.iata_code ?? '').trim();
      if (!iata) {
        continue;
      }
      const latitude = Number(row.latitude_deg);
      const longitude = Number(row.longitude_deg);
      if (Number.isNaN(latitude) || Number.isNaN(longitude)) {
        continue;
      }
      airports.push({
        iata,
        icao: (row.gps_code || row.ident || '').trim(),
        name: (row.name || '').trim(),
        lat: latitude,
        lon: longitude,
      });
    }
    await redisClient.set(OURAIRPORTS_CACHE_KEY, JSON.stringify(airports));
    app.log.info(
      `OurAirports: ${airports.length} large/medium airports cached`,
    );
  } catch (error) {
    app.log.error(`OurAirports refresh failed: ${error}`);
  }
}

async function fetchAirports(
  latKey: number,
  lonKey: number,
): Promise<AirportRecord[]> {
  const key = airportCacheKey(latKey, lonKey);
  const raw = await redisClient.get(key);
  if (raw) {
    return JSON.parse(raw) as AirportRecord[];
  }
  const allAirports = await loadOurAirports();
  if (!allAirports.length) {
    return [];
  }
  const [tLat, tLon] = tileCenter(latKey, lonKey);
  const nearby = allAirports.filter(
    (airport) =>
      haversineNm(tLat, tLon, airport.lat, airport.lon) <= FETCH_RADIUS_NM,
  );
  await redisClient.setEx(key, AIRPORT_CACHE_TTL, JSON.stringify(nearby));
  app.log.info(
    `Airports for tile ${latKey},${lonKey}: ${nearby.length} within ${FETCH_RADIUS_NM}nm`,
  );
  return nearby;
}

async function geocodeAddress(
  address: string,
): Promise<{ lat: number; lon: number } | null> {
  const key = geoKey(address);
  const raw = await redisClient.get(key);
  if (raw) {
    return JSON.parse(raw) as { lat: number; lon: number };
  }
  const url = new URL('https://nominatim.openstreetmap.org/search');
  url.searchParams.set('q', address);
  url.searchParams.set('format', 'json');
  url.searchParams.set('limit', '1');
  try {
    const response = await fetchWithTimeout(
      url.toString(),
      { headers: { 'User-Agent': USER_AGENT } },
      10000,
    );
    if (!response.ok) {
      return null;
    }
    const data = (await response.json()) as any[];
    if (!Array.isArray(data) || !data.length) {
      return null;
    }
    const result = { lat: Number(data[0].lat), lon: Number(data[0].lon) };
    await redisClient.setEx(key, GEO_CACHE_TTL, JSON.stringify(result));
    return result;
  } catch (error) {
    app.log.error(`Geocoding error: ${error}`);
    return null;
  }
}

function reducePayload(
  rawData: any,
  centerLat: number,
  centerLon: number,
  showGround: boolean,
  acKey = 'ac',
): any {
  const acList = Array.isArray(rawData[acKey]) ? rawData[acKey] : [];
  const processed: any[] = [];
  for (const a of acList) {
    const pLat = a?.lat;
    const pLon = a?.lon;
    const alt = a?.alt_baro;
    if (pLat == null || pLon == null) {
      continue;
    }
    if (!showGround && alt === 'ground') {
      continue;
    }
    const dist = haversineNm(pLat, pLon, centerLat, centerLon);
    if (dist > FETCH_RADIUS_NM) {
      continue;
    }
    processed.push({
      hex: a?.hex ?? '',
      flight: String(a?.flight ?? '').trim(),
      r: a?.r ?? '',
      t: a?.t ?? '',
      cat: a?.category,
      desc: a?.desc ?? '',
      alt_baro: alt,
      gs: a?.gs,
      track: a?.track,
      baro_rate: a?.baro_rate ?? 0,
      squawk: a?.squawk ?? '',
      lat: pLat,
      lon: pLon,
      _dist: dist,
    });
  }
  processed.sort((a, b) => a._dist - b._dist);
  const closest = processed.slice(0, MAX_CACHE_PLANES);
  for (const plane of closest) {
    delete plane._dist;
  }
  return {
    ac: closest,
    total: typeof rawData.total === 'number' ? rawData.total : closest.length,
  };
}

async function doApiCall(
  latKey: number,
  lonKey: number,
  showGround: boolean,
): Promise<any> {
  const provider = selectProvider();
  if (!provider) {
    throw new Error('All providers on cooldown');
  }
  const [lat, lon] = tileCenter(latKey, lonKey);
  const url = provider.url
    .replace('{lat}', String(lat))
    .replace('{lon}', String(lon))
    .replace('{radius}', String(Math.round(FETCH_RADIUS_NM)));
  providerLastCall.set(provider.name, Date.now() / 1000);
  const start = Date.now();
  const response = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': USER_AGENT } },
    4000,
  );
  const elapsedMs = Date.now() - start;
  if (response.status === 429) {
    const retryAfter = Number(
      response.headers.get('Retry-After') ?? provider.cooldown_ms / 1000.0,
    );
    providerLastCall.set(
      provider.name,
      Date.now() / 1000 + retryAfter - provider.cooldown_ms / 1000.0,
    );
    backoffUntil = Date.now() / 1000 + 1.0;
    await incrementStat(`rate_limited:${provider.name}`);
    app.log.warn(
      `RATE LIMITED: ${provider.name} retry_after=${retryAfter}s tile=${latKey},${lonKey}`,
    );
    throw new Error(`429 rate limited on ${provider.name}`);
  }
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(
      `Provider ${provider.name} error ${response.status} ${body}`,
    );
  }
  const rawData = await response.json();
  const acCount = Array.isArray(rawData[provider.ac_key ?? 'ac'])
    ? rawData[provider.ac_key ?? 'ac'].length
    : 0;
  await incrementStat(`calls:${provider.name}`);
  app.log.info(
    `API: ${provider.name} tile=${latKey},${lonKey} ac=${acCount} elapsed=${elapsedMs}ms`,
  );
  const reduced = reducePayload(
    rawData,
    lat,
    lon,
    showGround,
    provider.ac_key ?? 'ac',
  );
  reduced.fetched_at_utc = new Date().toISOString();
  reduced.provider = provider.name;
  await setCache(latKey, lonKey, showGround, reduced);
  return reduced;
}

async function getQueueItem(): Promise<ApiTask> {
  while (queue.length === 0) {
    await new Promise<void>((resolve) => {
      queueNotifier = resolve;
    });
  }
  const item = queue.shift()!;
  queueNotifier = null;
  return item;
}

function queueSize(): number {
  return queue.length;
}

function enqueueApiTask(task: ApiTask): boolean {
  if (queue.length >= MAX_QUEUE_SIZE) {
    return false;
  }
  queue.push(task);
  if (queueNotifier) {
    queueNotifier();
    queueNotifier = null;
  }
  return true;
}

function timeout<T>(ms: number, message: string): Promise<T> {
  return new Promise<T>((_, reject) => {
    setTimeout(() => reject(new Error(message)), ms);
  });
}

async function apiWorker(): Promise<void> {
  while (true) {
    const task = await getQueueItem();
    const key = `${task.latKey}:${task.lonKey}:${task.showGround ? 1 : 0}`;
    try {
      const cached = await getFromCache(
        task.latKey,
        task.lonKey,
        task.showGround,
      );
      if (cached !== null) {
        app.log.info(`WORKER CACHE HIT (dedup): ${task.latKey},${task.lonKey}`);
        task.resolve(cached);
        continue;
      }
      const now = Date.now() / 1000;
      const delay = Math.max(0, backoffUntil - now);
      if (delay > 0) {
        await sleep(delay * 1000);
      }
      while (selectProvider() === null) {
        const waits = providers.map((provider) => {
          const last = providerLastCall.get(provider.name) ?? 0;
          return provider.cooldown_ms / 1000.0 - (now - last);
        });
        const minWait = Math.max(0.05, Math.min(...waits));
        app.log.info(
          `All providers on cooldown, waiting ${minWait.toFixed(2)}s`,
        );
        await sleep(minWait * 1000);
      }
      const data = await doApiCall(task.latKey, task.lonKey, task.showGround);
      task.resolve(data);
    } catch (error) {
      app.log.error(
        `API worker error for ${task.latKey},${task.lonKey}: ${error}`,
      );
      await incrementStat('api_errors');
      const message = String(error instanceof Error ? error.message : error);
      for (const provider of providers) {
        if (message.includes(provider.name)) {
          await incrementStat(`errors:${provider.name}`);
          break;
        }
      }
      const stale = await getFromCache(
        task.latKey,
        task.lonKey,
        task.showGround,
      );
      task.resolve(stale);
    } finally {
      inflight.delete(key);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchPlanes(
  lat: number,
  lon: number,
  showGround: boolean,
): Promise<any | null> {
  const [latKey, lonKey] = tileKey(lat, lon);
  const inflightKey = `${latKey}:${lonKey}:${showGround ? 1 : 0}`;
  const cached = await getFromCache(latKey, lonKey, showGround);
  if (cached !== null) {
    app.log.info(`CACHE HIT: ${latKey},${lonKey} ground=${showGround}`);
    await incrementStat('cache_hits');
    return cached;
  }
  app.log.info(`CACHE MISS: ${latKey},${lonKey} ground=${showGround}`);
  await incrementStat('cache_misses');
  if (inflight.has(inflightKey)) {
    app.log.info(`IN-FLIGHT HIT: ${latKey},${lonKey}`);
    await incrementStat('inflight_hits');
    try {
      return await Promise.race([
        inflight.get(inflightKey)!,
        timeout(QUEUE_TIMEOUT, 'queue timeout'),
      ]);
    } catch {
      return await getFromCache(latKey, lonKey, showGround);
    }
  }
  if (
    !enqueueApiTask(createDeferredTask(latKey, lonKey, showGround, inflightKey))
  ) {
    app.log.warn(`Queue full, returning stale cache for ${latKey},${lonKey}`);
    return await getFromCache(latKey, lonKey, showGround);
  }
  const promise = inflight.get(inflightKey)!;
  try {
    return await Promise.race([
      promise,
      timeout(QUEUE_TIMEOUT, 'queue timeout'),
    ]);
  } catch {
    app.log.warn(
      `Queue timeout, returning stale cache for ${latKey},${lonKey}`,
    );
    return await getFromCache(latKey, lonKey, showGround);
  }
}

function createDeferredTask(
  latKey: number,
  lonKey: number,
  showGround: boolean,
  inflightKey: string,
): ApiTask {
  let resolve!: (value: unknown) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<unknown>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  inflight.set(inflightKey, promise);
  return { latKey, lonKey, showGround, resolve, reject };
}

function validCallsign(callsign: string): boolean {
  return Boolean(
    callsign && callsign.length >= 4 && /^[A-Z]{2,3}\d/.test(callsign),
  );
}

async function fetchRoute(
  callsign: string,
): Promise<[RouteData | null, boolean]> {
  callsign = callsign.trim();
  if (!validCallsign(callsign)) {
    return [null, false];
  }
  const key = routeKey(callsign);
  const raw = await redisClient.get(key);
  if (raw !== null) {
    return [JSON.parse(raw) as RouteData | null, true];
  }
  if (Date.now() / 1000 < adsbdbBackoffUntil) {
    return [null, false];
  }
  try {
    await routeSemaphore.acquire();
    const response = await fetchWithTimeout(
      `https://api.adsbdb.com/v0/callsign/${encodeURIComponent(callsign)}`,
      {
        headers: { 'User-Agent': USER_AGENT },
      },
      5000,
    );
    if (response.status === 429) {
      const retryAfter = Number(response.headers.get('Retry-After') ?? '60');
      adsbdbBackoffUntil = Date.now() / 1000 + retryAfter;
      app.log.warn(`adsbdb rate limited, backoff ${retryAfter}s`);
      return [null, false];
    }
    if (response.status === 200) {
      const body = (await response.json()) as any;
      const data = body?.response?.flightroute;
      if (data) {
        const route = {
          origin: airportInfo(data.origin ?? {}),
          destination: airportInfo(data.destination ?? {}),
        };
        await redisClient.setEx(key, ROUTE_CACHE_TTL, JSON.stringify(route));
        return [route, false];
      }
    }
    await redisClient.setEx(key, ROUTE_CACHE_TTL, JSON.stringify(null));
    if (![400, 404, 200, 429].includes(response.status)) {
      app.log.warn(`adsbdb route ${callsign}: HTTP ${response.status}`);
    }
  } catch (error) {
    app.log.debug(`Route fetch error ${callsign}: ${error}`);
  } finally {
    routeSemaphore.release();
  }
  return [null, false];
}

function airportInfo(a: any): RouteAirport {
  return {
    icao: String(a.icao_code ?? ''),
    code: String(a.iata_code ?? a.icao_code ?? ''),
    name: String(a.name ?? ''),
    municipality: String(a.municipality ?? ''),
    country: String(a.country_iso_name ?? ''),
    lat: Number(a.latitude ?? 0),
    lon: Number(a.longitude ?? 0),
  };
}

function routeProgress(plane: any, route: RouteData): number | null {
  try {
    const o = route.origin;
    const d = route.destination;
    const olat = o.lat;
    const olon = o.lon;
    const dlat = d.lat;
    const dlon = d.lon;
    const plat = plane.lat;
    const plon = plane.lon;
    const total = Math.sqrt((dlat - olat) ** 2 + (dlon - olon) ** 2);
    if (total < 1e-6) {
      return null;
    }
    const covered = Math.sqrt((plat - olat) ** 2 + (plon - olon) ** 2);
    return Math.round(Math.max(0, Math.min(1, covered / total)) * 1000) / 1000;
  } catch {
    return null;
  }
}

function airportLabel(airport: RouteAirport, routeDisplay: string): string {
  if (routeDisplay === 'hidden') {
    return '';
  }
  if (routeDisplay === 'cities') {
    const city = (airport.municipality || '').slice(0, 20).toUpperCase();
    const country = airport.country || '';
    if (city && country) {
      return `${city} (${country})`;
    }
    return city || country;
  }
  return airport.code || airport.icao || '';
}

async function enrichWithRoutes(
  aircraft: any[],
  routeDisplay: string,
): Promise<void> {
  const callsigns = aircraft.map((plane) => String(plane.flight ?? '').trim());
  if (!callsigns.some((cs) => cs.length)) {
    return;
  }
  const keys = callsigns.map((cs) => (cs ? routeKey(cs) : ''));
  const rawValues = await redisClient.mGet(keys);
  const routes: Array<RouteData | null> = [];
  const fetchIndices: number[] = [];
  let hits = 0;
  for (let i = 0; i < callsigns.length; i += 1) {
    const cs = callsigns[i];
    const raw = rawValues[i];
    if (!cs) {
      routes.push(null);
    } else if (raw !== null) {
      routes.push(JSON.parse(raw) as RouteData | null);
      hits += 1;
    } else {
      routes.push(null);
      fetchIndices.push(i);
    }
  }
  if (fetchIndices.length) {
    await Promise.all(
      fetchIndices.map(async (index) => {
        const [route] = await fetchRoute(callsigns[index]);
        routes[index] = route;
      }),
    );
  }
  let resolved = 0;
  for (let i = 0; i < aircraft.length; i += 1) {
    const plane = aircraft[i];
    const route = routes[i];
    if (!route) {
      continue;
    }
    const origin = airportLabel(route.origin, routeDisplay);
    const destination = airportLabel(route.destination, routeDisplay);
    const progress = routeProgress(plane, route);
    if (origin) {
      plane.origin = origin;
    }
    if (destination) {
      plane.dest = destination;
    }
    if (progress !== null) {
      plane.progress = progress;
    }
    if (origin || destination) {
      resolved += 1;
    }
  }
  const totalCs = callsigns.filter((cs) => cs).length;
  app.log.info(
    `routes: ${totalCs} w/ callsign — ${hits} cached, ${fetchIndices.length} fetched, ${resolved} resolved`,
  );
}

app.get('/', async (request: FastifyRequest, reply: FastifyReply) => {
  if (!checkIpWhitelist(request)) {
    return reply.code(403).send({ error: 'Access denied' });
  }
  const query = request.query as Record<string, string | undefined>;
  let lat = query.lat ? Number(query.lat) : NaN;
  let lon = query.lon ? Number(query.lon) : NaN;
  const address = query.address;
  const showGround =
    String(query.show_ground ?? 'false').toLowerCase() === 'true';
  const routeDisplay = String(query.route_display ?? 'codes');

  if (address) {
    const geo = await geocodeAddress(address);
    if (geo) {
      lat = geo.lat;
      lon = geo.lon;
    } else {
      return reply.code(400).send({ error: 'Location not found' });
    }
  }

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return reply.code(400).send({ error: 'Missing lat/lon or address' });
  }

  await incrementStat('requests');
  const [latKey, lonKey] = tileKey(lat, lon);
  const [rawData, tileAirports] = await Promise.all([
    fetchPlanes(lat, lon, showGround),
    fetchAirports(latKey, lonKey),
  ]);

  if (rawData) {
    const userAc = (rawData.ac as any[])
      .map((p) => {
        const dist = haversineNm(lat, lon, p.lat, p.lon);
        return { ...p, _dist: dist };
      })
      .filter((p) => p._dist <= RADIUS_NM);
    userAc.sort((a, b) => a._dist - b._dist);
    for (const p of userAc) {
      delete p._dist;
    }
    const data = {
      ac: userAc.slice(0, MAX_PLANES),
      total: userAc.length,
      lat,
      lon,
      provider: rawData.provider,
      fetched_at_utc: rawData.fetched_at_utc,
      airports: tileAirports.filter(
        (airport) =>
          haversineNm(lat, lon, airport.lat, airport.lon) <= RADIUS_NM,
      ),
    };
    await enrichWithRoutes(data.ac, routeDisplay);
    return reply.send({ data });
  }
  return reply.code(500).send({ error: 'Failed to fetch data' });
});

app.get(
  '/debug/airports',
  async (request: FastifyRequest, reply: FastifyReply) => {
    const query = request.query as Record<string, string | undefined>;
    const lat = query.lat ? Number(query.lat) : 34.0;
    const lon = query.lon ? Number(query.lon) : -118.0;
    const [latKey, lonKey] = tileKey(lat, lon);
    const airports = await fetchAirports(latKey, lonKey);
    const filtered = airports.filter(
      (airport) => haversineNm(lat, lon, airport.lat, airport.lon) <= RADIUS_NM,
    );
    return reply.send({
      user: [lat, lon],
      tile: [latKey, lonKey],
      total_in_tile: airports.length,
      count_near_user: filtered.length,
      airports: filtered,
    });
  },
);

app.get('/health', async (_request: FastifyRequest, reply: FastifyReply) => {
  let redisOk = false;
  try {
    await redisClient.ping();
    redisOk = true;
  } catch {
    redisOk = false;
  }
  return reply.send({
    status: redisOk ? 'healthy' : 'degraded',
    redis: redisOk,
    ip_whitelist: ENABLE_IP_WHITELIST,
    queue_size: queueSize(),
    inflight: inflight.size,
  });
});

async function startup(): Promise<void> {
  await redisClient.connect();
  await redisClient.ping();
  app.log.info(`Redis connected: ${REDIS_URL}`);
  providers = await loadProviders();
  app.log.info(
    `Loaded ${providers.length} providers: ${providers.map((p) => p.name).join(', ')}`,
  );
  if (!(await redisClient.exists(STATS_STARTED_AT_KEY))) {
    await redisClient.set(STATS_STARTED_AT_KEY, new Date().toISOString());
  }
  for (let index = 0; index < 3; index += 1) {
    apiWorker().catch((error) => app.log.error(`API worker failed: ${error}`));
  }
  refreshOurAirports().catch((error) =>
    app.log.error(`OurAirports startup failed: ${error}`),
  );
  setInterval(
    () =>
      refreshOurAirports().catch((error) =>
        app.log.error(`OurAirports refresh failed: ${error}`),
      ),
    AIRPORT_CACHE_TTL * 1000,
  );
  setInterval(
    () =>
      logStats().catch((error) =>
        app.log.error(`Stats logger failed: ${error}`),
      ),
    3600 * 1000,
  );
  if (ENABLE_IP_WHITELIST) {
    trmnlIps = await fetchTrmnlIps();
    setInterval(
      () =>
        fetchTrmnlIps()
          .then((ips) => {
            trmnlIps = ips;
          })
          .catch((err) => app.log.error(`IP refresh failed: ${err}`)),
      IP_REFRESH_HOURS * 3600 * 1000,
    );
  }
  app.log.info('Startup complete — Fastify server ready');
}

async function logStats(): Promise<void> {
  const raw = await redisClient.hGetAll(STATS_KEY);
  const stats: Record<string, number> = {};
  for (const [key, value] of Object.entries(raw)) {
    stats[key] = Number(value ?? '0');
  }
  const total = stats.requests ?? 0;
  const hits = stats.cache_hits ?? 0;
  const errors = stats.api_errors ?? 0;
  const hitPct = total ? `${((hits / total) * 100).toFixed(1)}%` : 'n/a';
  const errPct = total ? `${((errors / total) * 100).toFixed(2)}%` : 'n/a';
  const startedStr = await redisClient.get(STATS_STARTED_AT_KEY);
  let periodTag = 'period unknown';
  let reqDay = 0;
  let reqHr = 0;
  let upDay = 0;
  let upHr = 0;
  if (startedStr) {
    const started = new Date(startedStr);
    const periodSecs = Math.max(1, (Date.now() - started.getTime()) / 1000);
    const periodDays = periodSecs / 86400;
    const periodHrs = periodSecs / 3600;
    const upstream = providers.reduce(
      (sum, provider) => sum + (stats[`calls:${provider.name}`] ?? 0),
      0,
    );
    reqDay = Math.floor(total / periodDays);
    reqHr = Math.floor(total / periodHrs);
    upDay = Math.floor(upstream / periodDays);
    upHr = Math.floor(upstream / periodHrs);
    periodTag = `since ${started.toISOString().slice(0, 16)}Z (${periodDays.toFixed(1)}d)`;
  }
  app.log.info(
    `STATS [${periodTag}] | requests=${total} cache_hits=${hits}(${hitPct}) misses=${stats.cache_misses ?? 0} inflight_dedup=${stats.inflight_hits ?? 0} api_errors=${errors}(${errPct})`,
  );
  app.log.info(
    `  rates: req/day=${reqDay} req/hr=${reqHr} upstream/day=${upDay} upstream/hr=${upHr}`,
  );
  for (const provider of providers) {
    const name = provider.name;
    app.log.info(
      `  ${name}: calls=${stats[`calls:${name}`] ?? 0} rate_limited=${stats[`rate_limited:${name}`] ?? 0} errors=${stats[`errors:${name}`] ?? 0}`,
    );
  }
}

async function main(): Promise<void> {
  await startup();
  const port = Number(process.env.PORT ?? '8080');
  await app.listen({ port, host: '0.0.0.0' });
}

main().catch((error) => {
  app.log.error(error);
  process.exit(1);
});
