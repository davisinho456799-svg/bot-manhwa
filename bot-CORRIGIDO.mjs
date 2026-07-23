/**
 * bot.mjs — Bot de Manhwa/Anime para Discord
 * ES Module nativo. Importa dependências direto do node_modules.
 *
 * Fontes de busca:
 *   AniList, Kitsu, MangaDex, Comick, MangaUpdates, Exa AI (existentes)
 *   Jikan (MyAnimeList) — NOVO
 *   AniDB (via dump de títulos) — NOVO (requer ANIDB_CLIENT + ANIDB_CLIENTVER)
 */

import {
  Client, GatewayIntentBits, Events, SlashCommandBuilder, EmbedBuilder,
  ActionRowBuilder, StringSelectMenuBuilder, ComponentType, REST, Routes,
  PermissionFlagsBits, ChannelType, TextChannel,
} from 'discord.js';
import pino from 'pino';
import { drizzle } from 'drizzle-orm/node-postgres';
import { pgTable, text, integer, timestamp, real } from 'drizzle-orm/pg-core';
import { eq, and, sql } from 'drizzle-orm';
import pkg from 'pg';
import { createGunzip } from 'node:zlib';
import { Writable } from 'node:stream';

const { Pool } = pkg;

// ─── Logger ──────────────────────────────────────────────────────────────────

const isProduction = process.env.NODE_ENV === 'production';
const logger = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  ...isProduction ? {} : { transport: { target: 'pino-pretty', options: { colorize: true } } },
});

// ─── Cache ───────────────────────────────────────────────────────────────────

class Cache {
  store = new Map();
  set(key, value, ttlMs) { this.store.set(key, { value, expiresAt: Date.now() + ttlMs }); }
  get(key) {
    const e = this.store.get(key);
    if (!e) return null;
    if (Date.now() > e.expiresAt) { this.store.delete(key); return null; }
    return e.value;
  }
  has(key) { return this.get(key) !== null; }
  delete(key) { this.store.delete(key); }
  prune() {
    const now = Date.now();
    for (const [k, e] of this.store) if (now > e.expiresAt) this.store.delete(k);
  }
  async getOrSet(key, fn, ttlMs) {
    const c = this.get(key);
    if (c !== null) return c;
    const v = await fn();
    this.set(key, v, ttlMs);
    return v;
  }
}
const cache = new Cache();
setInterval(() => cache.prune(), 10 * 60 * 1000);

const TTL = {
  SEARCH:    20 * 60 * 1000,
  ID_LOOKUP: 60 * 60 * 1000,
  PT_BR:     30 * 60 * 1000,
  TRANSLATE: 60 * 60 * 1000,
};

// ─── Helpers comuns ───────────────────────────────────────────────────────────

function cleanDescription(raw) {
  if (!raw) return '';
  return raw
    .replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#039;/g, "'").trim();
}

function statusLabel(status) {
  const map = {
    FINISHED: 'Finalizado', RELEASING: 'Em lançamento',
    NOT_YET_RELEASED: 'Ainda não lançado', CANCELLED: 'Cancelado', HIATUS: 'Em hiato',
  };
  return status ? (map[status] ?? status) : 'Desconhecido';
}

function buildAlternativeTitles(m) {
  const titles = new Set();
  if (m.title.english) titles.add(m.title.english);
  if (m.title.romaji)  titles.add(m.title.romaji);
  if (m.title.native)  titles.add(m.title.native);
  for (const s of m.synonyms ?? []) if (s) titles.add(s);
  const main = m.title.english ?? m.title.romaji ?? m.title.native ?? '';
  titles.delete(main);
  if (titles.size === 0) return null;
  return [...titles].slice(0, 6).join('\n');
}

// ─── Tradução (MyMemory) ──────────────────────────────────────────────────────

async function translateToPtBr(text) {
  if (!text) return 'Sem sinopse disponível.';
  const MAX = 500;
  const truncated = text.slice(0, MAX);
  const key = `translate:${truncated}`;
  return cache.getOrSet(key, async () => {
    try {
      const url = `https://api.mymemory.translated.net/get?q=${encodeURIComponent(truncated)}&langpair=en|pt-BR`;
      const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
      if (!res.ok) throw new Error('MyMemory error');
      const json = await res.json();
      if (json.responseStatus !== 200) throw new Error('Translation failed');
      const t = json.responseData.translatedText;
      if (t.toLowerCase() === truncated.toLowerCase()) return truncated + (text.length > MAX ? '...' : '');
      return t + (text.length > MAX ? '...' : '');
    } catch {
      return truncated + (text.length > MAX ? '...' : '');
    }
  }, TTL.TRANSLATE);
}

async function translateToEnglish(query) {
  try {
    const url = `https://api.mymemory.translated.net/get?q=${encodeURIComponent(query)}&langpair=pt|en`;
    const res = await fetch(url, { signal: AbortSignal.timeout(4000) });
    if (!res.ok) return null;
    const json = await res.json();
    if (json.responseStatus !== 200) return null;
    const t = json.responseData.translatedText;
    if (t.toLowerCase().trim() === query.toLowerCase().trim()) return null;
    return t;
  } catch { return null; }
}

// ─── AniList ──────────────────────────────────────────────────────────────────

const ANILIST_API = 'https://graphql.anilist.co';
const MANGA_FIELDS = `
  id title { romaji english native } synonyms description(asHtml: false)
  coverImage { large color } averageScore genres chapters status siteUrl startDate { year month day }
`;
const ANIME_FIELDS = `
  id title { romaji english native } synonyms description(asHtml: false)
  coverImage { large color } averageScore genres episodes status siteUrl startDate { year month day }
`;

async function anilistRequest(query, variables) {
  const res = await fetch(ANILIST_API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`AniList API error: ${res.status}`);
  const json = await res.json();
  if (json.errors?.length) throw new Error(json.errors[0].message);
  return json.data;
}

const SEARCH_MANHWA_QUERY = `query SearchManhwa($search: String!, $page: Int) { Page(page: $page, perPage: 8) { media(search: $search, type: MANGA, countryOfOrigin: KR, sort: SEARCH_MATCH) { ${MANGA_FIELDS} } } }`;
const SEARCH_MANGA_QUERY  = `query SearchManga($search: String!, $page: Int) { Page(page: $page, perPage: 8) { media(search: $search, type: MANGA, sort: SEARCH_MATCH) { ${MANGA_FIELDS} } } }`;
const SEARCH_ANIME_QUERY  = `query SearchAnime($search: String!, $page: Int) { Page(page: $page, perPage: 8) { media(search: $search, type: ANIME, sort: SEARCH_MATCH) { ${ANIME_FIELDS} } } }`;
const ID_MANGA_QUERY = `query GetManga($id: Int!) { Media(id: $id, type: MANGA) { ${MANGA_FIELDS} } }`;
const ID_ANIME_QUERY = `query GetAnime($id: Int!) { Media(id: $id, type: ANIME) { ${ANIME_FIELDS} } }`;

async function searchManhwa(search) {
  return cache.getOrSet(`anilist:manhwa:${search.toLowerCase().trim()}`, async () => {
    const d = await anilistRequest(SEARCH_MANHWA_QUERY, { search, page: 1 });
    return (d.Page.media ?? []).map(m => ({ ...m, type: 'MANGA' }));
  }, TTL.SEARCH);
}
async function searchManga(search) {
  return cache.getOrSet(`anilist:manga:${search.toLowerCase().trim()}`, async () => {
    const d = await anilistRequest(SEARCH_MANGA_QUERY, { search, page: 1 });
    return (d.Page.media ?? []).map(m => ({ ...m, type: 'MANGA' }));
  }, TTL.SEARCH);
}
async function searchAnime(search) {
  return cache.getOrSet(`anilist:anime:${search.toLowerCase().trim()}`, async () => {
    const d = await anilistRequest(SEARCH_ANIME_QUERY, { search, page: 1 });
    return (d.Page.media ?? []).map(m => ({ ...m, type: 'ANIME', chapters: null }));
  }, TTL.SEARCH);
}
async function getManhwaById(id) {
  return cache.getOrSet(`anilist:id:manga:${id}`, async () => {
    try {
      const d = await anilistRequest(ID_MANGA_QUERY, { id });
      return d.Media ? { ...d.Media, type: 'MANGA' } : null;
    } catch { return null; }
  }, TTL.ID_LOOKUP);
}
async function getAnimeById(id) {
  return cache.getOrSet(`anilist:id:anime:${id}`, async () => {
    try {
      const d = await anilistRequest(ID_ANIME_QUERY, { id });
      return d.Media ? { ...d.Media, type: 'ANIME', chapters: null } : null;
    } catch { return null; }
  }, TTL.ID_LOOKUP);
}

// ─── Kitsu ────────────────────────────────────────────────────────────────────

const KITSU_API = 'https://kitsu.io/api/edge';
function kitsuTitle(a) {
  return a.titles?.en ?? a.canonicalTitle ?? a.titles?.en_jp ?? a.titles?.ja_jp ?? 'Sem título';
}
function kitsuToUnified(a, id) {
  const main = kitsuTitle(a);
  const synonyms = [];
  if (a.titles) for (const k of Object.keys(a.titles)) { const t = a.titles[k]; if (t && t !== main && !synonyms.includes(t)) synonyms.push(t); }
  if (Array.isArray(a.abbreviatedTitles)) for (const t of a.abbreviatedTitles) { if (t && t !== main && !synonyms.includes(t)) synonyms.push(t); }
  return {
    source: 'kitsu', id: String(id), mainTitle: main,
    nativeTitle: a.titles?.ja_jp ?? null, romajiTitle: a.titles?.en_jp ?? null, synonyms,
    description: a.synopsis ?? a.description ?? null,
    coverUrl: a.posterImage?.large ?? a.posterImage?.original ?? null,
    accentColor: 14512137,
    score: a.averageRating ? Math.round(parseFloat(a.averageRating)) : null,
    genres: [], chapters: null, episodes: a.episodeCount ?? null,
    status: a.status ?? null,
    siteUrl: `https://kitsu.io/anime/${a.slug}`,
    year: a.startDate ? parseInt(a.startDate.slice(0, 4), 10) : null,
    ptBrUrl: null, mediaType: 'anime',
  };
}
async function searchKitsu(query) {
  return cache.getOrSet(`kitsu:search:${query.toLowerCase().trim()}`, async () => {
    const res = await fetch(`${KITSU_API}/anime?filter[text]=${encodeURIComponent(query)}&page[limit]=6`, {
      headers: { Accept: 'application/vnd.api+json' }, signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) throw new Error(`Kitsu error: ${res.status}`);
    const json = await res.json();
    return (json.data ?? []).map(d => kitsuToUnified(d.attributes, d.id));
  }, TTL.SEARCH);
}
async function getKitsuById(id) {
  return cache.getOrSet(`kitsu:id:${id}`, async () => {
    try {
      const res = await fetch(`${KITSU_API}/anime/${id}`, {
        headers: { Accept: 'application/vnd.api+json' }, signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return null;
      const json = await res.json();
      return json.data ? kitsuToUnified(json.data.attributes, json.data.id) : null;
    } catch { return null; }
  }, TTL.ID_LOOKUP);
}

// ─── MangaDex ─────────────────────────────────────────────────────────────────

const MANGADEX_API = 'https://api.mangadex.org';
function pickTitle(titles, preferred) {
  for (const lang of preferred) if (titles[lang]) return titles[lang];
  const first = Object.keys(titles)[0];
  return first ? (titles[first] ?? null) : null;
}
function mapStatus(s) {
  const map = { ongoing: 'RELEASING', completed: 'FINISHED', hiatus: 'HIATUS', cancelled: 'CANCELLED' };
  return s ? (map[s] ?? s.toUpperCase()) : null;
}
function buildCoverUrl(manga) {
  const rel = manga.relationships.find(r => r.type === 'cover_art');
  if (!rel?.attributes?.fileName) return null;
  return `https://uploads.mangadex.org/covers/${manga.id}/${rel.attributes.fileName}.512.jpg`;
}
function extractGenres(tags) {
  return tags.filter(t => t.attributes.group === 'genre' || t.attributes.group === 'theme')
    .map(t => pickTitle(t.attributes.name, ['en', 'pt-br', 'pt']) ?? '').filter(Boolean).slice(0, 8);
}
function extractSynonyms(altTitles, mainTitle) {
  const seen = new Set([mainTitle.toLowerCase()]); const result = [];
  for (const alt of altTitles) {
    for (const lang of ['en', 'ko', 'ja', 'pt-br', 'pt', 'ro', 'ja-ro']) {
      const t = alt[lang];
      if (t && !seen.has(t.toLowerCase())) { seen.add(t.toLowerCase()); result.push(t); }
    }
    if (result.length >= 8) break;
  }
  return result;
}
function parseManga(manga) {
  const a = manga.attributes;
  const mainTitle = pickTitle(a.title, ['en', 'ko', 'ja-ro', 'ja']) ?? 'Sem título';
  const chapters = a.lastChapter ? (parseInt(a.lastChapter, 10) || null) : null;
  return {
    source: 'mangadex', id: manga.id, mainTitle,
    nativeTitle: a.title['ko'] ?? a.title['ja'] ?? null,
    romajiTitle: a.title['ja-ro'] ?? null,
    synonyms: extractSynonyms(a.altTitles, mainTitle),
    description: a.description['en'] ?? a.description['pt-br'] ?? null,
    coverUrl: buildCoverUrl(manga), score: null,
    genres: extractGenres(a.tags),
    chapters: chapters && isNaN(chapters) ? null : chapters,
    status: mapStatus(a.status),
    siteUrl: `https://mangadex.org/title/${manga.id}`,
    year: a.year,
  };
}
async function getMangaDexById(id) {
  return cache.getOrSet(`mangadex:id:${id}`, async () => {
    try {
      const res = await fetch(`${MANGADEX_API}/manga/${encodeURIComponent(id)}?includes[]=cover_art`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return null;
      const json = await res.json();
      if (json.result !== 'ok' || !json.data) return null;
      return parseManga(json.data);
    } catch { return null; }
  }, TTL.ID_LOOKUP);
}
async function searchMangaDex(query) {
  return cache.getOrSet(`mangadex:search:${query.toLowerCase().trim()}`, async () => {
    try {
      const params = new URLSearchParams({ title: query, limit: '8', 'order[relevance]': 'desc', 'includes[]': 'cover_art' });
      const res = await fetch(`${MANGADEX_API}/manga?${params}`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return [];
      const json = await res.json();
      if (json.result !== 'ok' || !json.data) return [];
      return json.data.map(parseManga);
    } catch { return []; }
  }, TTL.SEARCH);
}
async function hasPtBrChapters(mangadexId) {
  return cache.getOrSet(`mangadex:ptbr:${mangadexId}`, async () => {
    try {
      const params = new URLSearchParams({ manga: mangadexId, 'translatedLanguage[]': 'pt-br', limit: '1' });
      const res = await fetch(`${MANGADEX_API}/chapter?${params}`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) return false;
      const json = await res.json();
      return json.result === 'ok' && json.total > 0;
    } catch { return false; }
  }, TTL.PT_BR);
}
async function findMangaDexIdByTitle(title) {
  return cache.getOrSet(`mangadex:findid:${title.toLowerCase().trim()}`, async () => {
    try {
      const params = new URLSearchParams({ title, limit: '1', 'order[relevance]': 'desc' });
      const res = await fetch(`${MANGADEX_API}/manga?${params}`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) return null;
      const json = await res.json();
      if (json.result !== 'ok' || !json.data?.length) return null;
      return json.data[0].id;
    } catch { return null; }
  }, TTL.PT_BR);
}

// ─── Comick ───────────────────────────────────────────────────────────────────

const COMICK_BASE = 'https://api.comick.io';
const COMICK_COVER = 'https://meo.comick.pictures';
const COMICK_STATUS = { 1: 'RELEASING', 2: 'FINISHED', 3: 'CANCELLED', 4: 'HIATUS' };
function comickCoverUrl(r) {
  const c = r.md_covers?.[0];
  return c?.b2key ? `${COMICK_COVER}/${c.b2key}` : null;
}
async function searchComick(query) {
  const params = new URLSearchParams({ q: query, limit: '6', country: 'ko' });
  const res = await fetch(`${COMICK_BASE}/v1.0/search?${params}`, {
    headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
  });
  if (!res.ok) throw new Error(`Comick error: ${res.status}`);
  const json = await res.json();
  return Array.isArray(json) ? json : [];
}
async function getComickBySlug(slug) {
  try {
    const res = await fetch(`${COMICK_BASE}/comic/${slug}`, {
      headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) return null;
    const json = await res.json();
    const comic = json.comic ?? json;
    if (!comic?.hid) return null;
    return comic;
  } catch { return null; }
}

// ─── MangaUpdates ─────────────────────────────────────────────────────────────

const MU_BASE = 'https://api.mangaupdates.com/v1';
const MU_TYPE_COUNTRY = { Manhwa: 'KR', Manhua: 'CN', Manga: 'JP', Doujinshi: 'JP' };
function muStatusToAnilist(s) {
  if (!s) return null;
  const sl = s.toLowerCase();
  if (sl.includes('ongoing') || sl.includes('publishing')) return 'RELEASING';
  if (sl.includes('complete')) return 'FINISHED';
  if (sl.includes('hiatus')) return 'HIATUS';
  if (sl.includes('cancel')) return 'CANCELLED';
  return null;
}
function muToRecord(r) {
  return {
    id: String(r.series_id), title: r.title, url: r.url,
    description: r.description ?? null,
    coverUrl: r.image?.url?.original ?? null,
    score: r.rating?.rating ? Math.round(r.rating.rating * 10) : null,
    genres: (r.genres ?? []).map(g => g.genre),
    status: muStatusToAnilist(r.status),
    year: r.year ? parseInt(r.year, 10) : null,
    country: MU_TYPE_COUNTRY[r.type ?? ''] ?? null,
  };
}
async function getMangaUpdatesById(id) {
  try {
    const res = await fetch(`${MU_BASE}/series/${id}`, {
      headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) return null;
    const r = await res.json();
    return r?.series_id ? muToRecord(r) : null;
  } catch { return null; }
}
async function searchMangaUpdates(query, type) {
  const body = { search: query, perpage: 6 };
  if (type) body.type = type;
  const res = await fetch(`${MU_BASE}/series/search`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(8000),
  });
  if (!res.ok) throw new Error(`MangaUpdates error: ${res.status}`);
  const json = await res.json();
  return (json.results ?? []).map(({ record: r }) => muToRecord(r));
}

// ─── Exa AI ───────────────────────────────────────────────────────────────────

const EXA_BASE = 'https://api.exa.ai';
function extractAnilistId(url) { const m = url.match(/anilist\.co\/manga\/(\d+)/i); return m ? parseInt(m[1], 10) : null; }
function extractMangadexId(url) { const m = url.match(/mangadex\.org\/title\/([0-9a-f-]{36})/i); return m ? m[1] : null; }

const DIRECT_SCAN_SITES = [
  { name: 'BlackoutComics', domain: 'blackoutcomics.com', fallbackUrl: null, fallbackLabel: null },
  { name: 'TiaManhwa',      domain: 'tiamanhwa.com',      fallbackUrl: null, fallbackLabel: null },
  { name: 'Hiper.cool',     domain: 'hiper.cool',          fallbackUrl: null, fallbackLabel: null },
];

async function findDirectLinks(title) {
  const apiKey = process.env.EXA_API_KEY;
  const makeFallback = s => ({
    name: s.name, domain: s.domain,
    url: s.fallbackUrl ?? `https://${s.domain}/?s=${encodeURIComponent(title)}`,
    direct: false, fallbackLabel: s.fallbackLabel,
  });
  const fallbacks = DIRECT_SCAN_SITES.map(makeFallback);
  if (!apiKey) return fallbacks;
  try {
    const res = await fetch(`${EXA_BASE}/search`, {
      method: 'POST',
      headers: { 'x-api-key': apiKey, 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ query: title, numResults: 9, type: 'keyword', includeDomains: DIRECT_SCAN_SITES.map(s => s.domain) }),
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) return fallbacks;
    const json = await res.json();
    const hits = json.results ?? [];
    return DIRECT_SCAN_SITES.map(site => {
      const match = hits.find(h => { try { return new URL(h.url).hostname.replace(/^www\./, '') === site.domain; } catch { return false; } });
      return match ? { name: site.name, domain: site.domain, url: match.url, direct: true, fallbackLabel: null } : makeFallback(site);
    });
  } catch { return fallbacks; }
}

async function searchExaManhwa(query) {
  const apiKey = process.env.EXA_API_KEY;
  if (!apiKey) return [];
  const res = await fetch(`${EXA_BASE}/search`, {
    method: 'POST',
    headers: { 'x-api-key': apiKey, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({
      query: `${query} manhwa manga`, numResults: 6, type: 'neural', useAutoprompt: true,
      includeDomains: ['anilist.co', 'mangadex.org'],
      contents: { text: { maxCharacters: 400 }, highlights: { numSentences: 2, highlightsPerUrl: 1 } },
    }),
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`Exa error: ${res.status}`);
  const json = await res.json();
  return (json.results ?? []).map(r => ({
    url: r.url, title: r.title ?? '', snippet: r.highlights?.[0] ?? r.text?.slice(0, 300) ?? null,
    anilistId: extractAnilistId(r.url), mangadexId: extractMangadexId(r.url),
  }));
}

async function searchExaSynopsis(synopsis, mode, synopsisEn) {
  const apiKey = process.env.EXA_API_KEY;
  if (!apiKey) return [];
  const mediaLabel = mode === 'anime' ? 'anime' : 'manhwa manga webtoon';
  const domains = mode === 'anime'
    ? ['anilist.co', 'myanimelist.net', 'anime-planet.com']
    : ['anilist.co', 'mangadex.org', 'myanimelist.net'];
  const mainText = synopsisEn ?? synopsis;
  const makeRequest = (queryText, framing) => fetch(`${EXA_BASE}/search`, {
    method: 'POST',
    headers: { 'x-api-key': apiKey, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify({
      query: `${mediaLabel} ${framing}: ${queryText}`, numResults: 8, type: 'neural', useAutoprompt: true,
      includeDomains: domains,
      contents: { text: { maxCharacters: 500 }, highlights: { numSentences: 3, highlightsPerUrl: 2 } },
    }),
    signal: AbortSignal.timeout(12000),
  });
  const [r1, r2] = await Promise.allSettled([makeRequest(mainText, 'with plot'), makeRequest(mainText, 'story about')]);
  const seen = new Set(); const allHits = [];
  for (const r of [r1, r2]) {
    if (r.status !== 'fulfilled' || !r.value.ok) continue;
    const json = await r.value.json();
    for (const hit of json.results ?? []) {
      if (seen.has(hit.url)) continue;
      seen.add(hit.url);
      allHits.push({ url: hit.url, title: hit.title ?? '', snippet: hit.highlights?.[0] ?? hit.text?.slice(0, 300) ?? null,
        anilistId: extractAnilistId(hit.url), mangadexId: extractMangadexId(hit.url) });
    }
  }
  return allHits;
}

// ─── Jikan (MyAnimeList) — NOVO ───────────────────────────────────────────────

const JIKAN_BASE = 'https://api.jikan.moe/v4';

function jikanStatusToAnilist(s) {
  if (!s) return null;
  const sl = s.toLowerCase();
  if (sl.includes('airing') || sl.includes('publishing') || sl.includes('ongoing')) return 'RELEASING';
  if (sl.includes('finished') || sl.includes('completed')) return 'FINISHED';
  if (sl.includes('hiatus') || sl.includes('on hiatus')) return 'HIATUS';
  if (sl.includes('discontinued') || sl.includes('cancelled')) return 'CANCELLED';
  return null;
}

function jikanAnimeToUnified(item) {
  return {
    source: 'jikan',
    id: String(item.mal_id),
    mainTitle: item.title_english ?? item.title ?? item.title_japanese ?? 'Sem título',
    nativeTitle: item.title_japanese ?? null,
    romajiTitle: item.title ?? null,
    synonyms: item.title_synonyms ?? [],
    description: item.synopsis ?? null,
    coverUrl: item.images?.jpg?.large_image_url ?? item.images?.jpg?.image_url ?? null,
    accentColor: 0x2E51A2,
    score: item.score ? Math.round(item.score * 10) : null,
    genres: (item.genres ?? []).map(g => g.name),
    chapters: null,
    episodes: item.episodes ?? null,
    status: jikanStatusToAnilist(item.status),
    siteUrl: item.url ?? `https://myanimelist.net/anime/${item.mal_id}`,
    year: item.aired?.prop?.from?.year ?? null,
    ptBrUrl: null,
    mediaType: 'anime',
  };
}

function jikanMangaToUnified(item) {
  return {
    source: 'jikan',
    id: String(item.mal_id),
    mainTitle: item.title_english ?? item.title ?? item.title_japanese ?? 'Sem título',
    nativeTitle: item.title_japanese ?? null,
    romajiTitle: item.title ?? null,
    synonyms: item.title_synonyms ?? [],
    description: item.synopsis ?? null,
    coverUrl: item.images?.jpg?.large_image_url ?? item.images?.jpg?.image_url ?? null,
    accentColor: 0x2E51A2,
    score: item.score ? Math.round(item.score * 10) : null,
    genres: (item.genres ?? []).map(g => g.name),
    chapters: item.chapters ?? null,
    episodes: null,
    status: jikanStatusToAnilist(item.status),
    siteUrl: item.url ?? `https://myanimelist.net/manga/${item.mal_id}`,
    year: item.published?.prop?.from?.year ?? null,
    ptBrUrl: null,
    mediaType: 'manga',
  };
}

async function searchJikanAnime(query) {
  return cache.getOrSet(`jikan:anime:${query.toLowerCase().trim()}`, async () => {
    try {
      const params = new URLSearchParams({ q: query, limit: '6', type: 'tv', order_by: 'relevance' });
      const res = await fetch(`${JIKAN_BASE}/anime?${params}`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return [];
      const json = await res.json();
      return (json.data ?? []).map(jikanAnimeToUnified);
    } catch { return []; }
  }, TTL.SEARCH);
}

async function searchJikanManga(query) {
  return cache.getOrSet(`jikan:manga:${query.toLowerCase().trim()}`, async () => {
    try {
      const params = new URLSearchParams({ q: query, limit: '6', order_by: 'relevance' });
      const res = await fetch(`${JIKAN_BASE}/manga?${params}`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return [];
      const json = await res.json();
      return (json.data ?? []).map(jikanMangaToUnified);
    } catch { return []; }
  }, TTL.SEARCH);
}

async function getJikanAnimeById(id) {
  return cache.getOrSet(`jikan:id:anime:${id}`, async () => {
    try {
      const res = await fetch(`${JIKAN_BASE}/anime/${id}`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return null;
      const json = await res.json();
      return json.data ? jikanAnimeToUnified(json.data) : null;
    } catch { return null; }
  }, TTL.ID_LOOKUP);
}

async function getJikanMangaById(id) {
  return cache.getOrSet(`jikan:id:manga:${id}`, async () => {
    try {
      const res = await fetch(`${JIKAN_BASE}/manga/${id}`, {
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return null;
      const json = await res.json();
      return json.data ? jikanMangaToUnified(json.data) : null;
    } catch { return null; }
  }, TTL.ID_LOOKUP);
}

// ─── AniDB — NOVO ─────────────────────────────────────────────────────────────
// Usa o dump público de títulos (não requer autenticação).
// Para busca por ID completa, usa a HTTP API (requer ANIDB_CLIENT + ANIDB_CLIENTVER).

let anidbIndex = null; // Map<string(normalizado), {aid, titles: string[]}>[]
let anidbLastFetch = 0;
const ANIDB_TTL = 24 * 60 * 60 * 1000; // 24 horas
const ANIDB_DUMP_URL = 'http://anidb.net/api/anime-titles.xml.gz';

async function loadAniDBDump() {
  if (anidbIndex && (Date.now() - anidbLastFetch) < ANIDB_TTL) return;
  const client = process.env.ANIDB_CLIENT;
  const clientver = process.env.ANIDB_CLIENTVER;
  // Baixa o dump mesmo sem client (dump é público)
  try {
    logger.info('AniDB: baixando dump de títulos...');
    const res = await fetch(ANIDB_DUMP_URL, {
      headers: { 'User-Agent': client ? `${client}/${clientver}` : 'manhwabot/1' },
      signal: AbortSignal.timeout(30000),
    });
    if (!res.ok) { logger.warn(`AniDB dump retornou ${res.status}`); return; }
    // Descomprime o gzip em memória
    const arrayBuf = await res.arrayBuffer();
    const xmlText = await decompressGzip(Buffer.from(arrayBuf));
    anidbIndex = parseAniDBTitles(xmlText);
    anidbLastFetch = Date.now();
    logger.info({ count: anidbIndex.length }, 'AniDB: dump carregado');
  } catch (err) {
    logger.warn({ err: err.message }, 'AniDB: falha ao baixar dump');
  }
}

async function decompressGzip(buf) {
  return new Promise((resolve, reject) => {
    const gunzip = createGunzip();
    const chunks = [];
    const writer = new Writable({
      write(chunk, _enc, cb) { chunks.push(chunk); cb(); },
    });
    gunzip.on('error', reject);
    writer.on('finish', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    writer.on('error', reject);
    gunzip.pipe(writer);
    gunzip.end(buf);
  });
}

function parseAniDBTitles(xml) {
  const entries = [];
  // Divide por blocos <anime aid="...">...</anime>
  const animeRe = /<anime aid="(\d+)">([\s\S]*?)<\/anime>/g;
  const titleRe = /<title[^>]*>([^<]+)<\/title>/g;
  let m;
  while ((m = animeRe.exec(xml)) !== null) {
    const aid = m[1];
    const block = m[2];
    const titles = [];
    let tm;
    const tmpRe = new RegExp(titleRe.source, titleRe.flags);
    while ((tm = tmpRe.exec(block)) !== null) {
      const t = tm[1].trim();
      if (t) titles.push(t);
    }
    if (titles.length) entries.push({ aid, titles });
  }
  return entries;
}

function anidbNormalize(s) {
  return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9\s]/g, '').trim();
}

async function searchAniDB(query) {
  if (!anidbIndex) return [];
  const qNorm = anidbNormalize(query);
  const qTokens = qNorm.split(/\s+/).filter(w => w.length > 1);
  if (!qTokens.length) return [];

  const scored = [];
  for (const entry of anidbIndex) {
    let best = 0;
    for (const title of entry.titles) {
      const tNorm = anidbNormalize(title);
      if (tNorm === qNorm) { best = 1; break; }
      if (tNorm.includes(qNorm) || qNorm.includes(tNorm)) { best = Math.max(best, 0.85); continue; }
      const tTokens = new Set(tNorm.split(/\s+/).filter(w => w.length > 1));
      let inter = 0;
      for (const w of qTokens) if (tTokens.has(w)) inter++;
      const sim = (2 * inter) / (qTokens.length + tTokens.size);
      if (sim > best) best = sim;
    }
    if (best >= 0.5) scored.push({ entry, score: best });
  }
  scored.sort((a, b) => b.score - a.score);

  return scored.slice(0, 5).map(({ entry }) => {
    const mainTitle = entry.titles[0] ?? 'Sem título';
    return {
      source: 'anidb',
      id: entry.aid,
      mainTitle,
      nativeTitle: null, romajiTitle: null,
      synonyms: entry.titles.slice(1, 6),
      description: null,
      coverUrl: null,
      accentColor: 0x1A2B4C,
      score: null,
      genres: [],
      chapters: null, episodes: null,
      status: null,
      siteUrl: `https://anidb.net/anime/${entry.aid}`,
      year: null,
      ptBrUrl: null,
      mediaType: 'anime',
    };
  });
}

async function getAniDBById(aid) {
  const client = process.env.ANIDB_CLIENT;
  const clientver = process.env.ANIDB_CLIENTVER;
  if (!client || !clientver) {
    // Retorna dados básicos do índice
    if (!anidbIndex) return null;
    const entry = anidbIndex.find(e => e.aid === String(aid));
    if (!entry) return null;
    return {
      source: 'anidb', id: entry.aid, mainTitle: entry.titles[0] ?? 'Sem título',
      nativeTitle: null, romajiTitle: null, synonyms: entry.titles.slice(1, 6),
      description: null, coverUrl: null, accentColor: 0x1A2B4C, score: null,
      genres: [], chapters: null, episodes: null, status: null,
      siteUrl: `https://anidb.net/anime/${aid}`, year: null, ptBrUrl: null, mediaType: 'anime',
    };
  }
  return cache.getOrSet(`anidb:id:${aid}`, async () => {
    try {
      // Respeita rate limit: 1 req / 2s por cliente (não faz requests paralelos)
      await new Promise(r => setTimeout(r, 2100));
      const url = `http://api.anidb.net:9001/httpapi?client=${encodeURIComponent(client)}&clientver=${encodeURIComponent(clientver)}&protover=1&request=anime&aid=${aid}`;
      const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
      if (!res.ok) return null;
      const xml = await res.text();
      // Parse básico do XML de resposta
      const getTag = (tag) => { const m = xml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`)); return m ? m[1].trim() : null; };
      const titleMatch = xml.match(/<title[^>]*xml:lang="en"[^>]*>([^<]+)<\/title>/)
        ?? xml.match(/<title[^>]*type="main"[^>]*>([^<]+)<\/title>/);
      const mainTitle = titleMatch ? titleMatch[1].trim() : 'Sem título';
      const episodesStr = getTag('episodecount');
      const episodes = episodesStr ? parseInt(episodesStr, 10) || null : null;
      const yearMatch = xml.match(/<startdate>(\d{4})-/);
      const year = yearMatch ? parseInt(yearMatch[1], 10) : null;
      const description = getTag('description');
      return {
        source: 'anidb', id: String(aid), mainTitle,
        nativeTitle: null, romajiTitle: null, synonyms: [],
        description: description ? cleanDescription(description) : null,
        coverUrl: null, accentColor: 0x1A2B4C, score: null,
        genres: [], chapters: null, episodes, status: null,
        siteUrl: `https://anidb.net/anime/${aid}`, year, ptBrUrl: null, mediaType: 'anime',
      };
    } catch { return null; }
  }, TTL.ID_LOOKUP);
}

// ─── Unified conversion ───────────────────────────────────────────────────────

function anilistToUnified(m) {
  const isAnime = m.type === 'ANIME';
  return {
    source: isAnime ? 'anilist-anime' : 'anilist',
    id: String(m.id),
    mainTitle: m.title.english ?? m.title.romaji ?? m.title.native ?? 'Sem título',
    nativeTitle: m.title.native ?? null,
    romajiTitle: m.title.romaji ?? null,
    synonyms: m.synonyms ?? [],
    description: m.description,
    coverUrl: m.coverImage.large,
    accentColor: m.coverImage.color ? parseInt(m.coverImage.color.replace('#', ''), 16) : 8087790,
    score: m.averageScore,
    genres: m.genres,
    chapters: isAnime ? null : m.chapters ?? null,
    episodes: isAnime ? m.episodes ?? null : null,
    status: m.status,
    siteUrl: m.siteUrl,
    year: m.startDate?.year ?? null,
    ptBrUrl: null, mediaType: isAnime ? 'anime' : 'manga',
  };
}

function mangadexToUnified(m) {
  return {
    source: 'mangadex', id: m.id, mainTitle: m.mainTitle,
    nativeTitle: m.nativeTitle, romajiTitle: m.romajiTitle, synonyms: m.synonyms,
    description: m.description, coverUrl: m.coverUrl, accentColor: 15106362,
    score: null, genres: m.genres, chapters: m.chapters, episodes: null,
    status: m.status, siteUrl: m.siteUrl, year: m.year, ptBrUrl: null, mediaType: 'manga',
  };
}

function comickToUnified(m) {
  return {
    source: 'comick', id: m.slug, mainTitle: m.title,
    nativeTitle: null, romajiTitle: null, synonyms: (m.md_titles ?? []).map(t => t.title),
    description: m.desc ?? null, coverUrl: comickCoverUrl(m), accentColor: 2533018,
    score: m.rating ? Math.round(parseFloat(m.rating) * 10) : null,
    genres: (m.genres ?? []).map(g => g.name), chapters: m.last_chapter ?? null, episodes: null,
    status: m.status !== null ? (COMICK_STATUS[m.status] ?? null) : null,
    siteUrl: `https://comick.io/comic/${m.slug}`, year: m.year ?? null, ptBrUrl: null, mediaType: 'manga',
  };
}

function mangaupdatesToUnified(m) {
  return {
    source: 'mangaupdates', id: m.id, mainTitle: m.title,
    nativeTitle: null, romajiTitle: null, synonyms: [],
    description: m.description, coverUrl: m.coverUrl, accentColor: 1402304,
    score: m.score, genres: m.genres, chapters: null, episodes: null,
    status: m.status, siteUrl: m.url, year: m.year, ptBrUrl: null, mediaType: 'manga',
  };
}

// ─── Enrich com PT-BR ─────────────────────────────────────────────────────────

async function enrichWithPtBr(result) {
  if (result.mediaType === 'anime') return result;
  const key = `ptbr:${result.source}:${result.id}`;
  const cached = cache.get(key);
  if (cached !== null) return cached ? { ...result, ptBrUrl: cached } : result;
  try {
    let mangadexId = result.source === 'mangadex' ? result.id : await findMangaDexIdByTitle(result.mainTitle);
    if (!mangadexId) { cache.set(key, false, TTL.PT_BR); return result; }
    const hasPtBr = await hasPtBrChapters(mangadexId);
    if (!hasPtBr) { cache.set(key, false, TTL.PT_BR); return result; }
    const url = `https://mangadex.org/title/${mangadexId}`;
    cache.set(key, url, TTL.PT_BR);
    return { ...result, ptBrUrl: url };
  } catch { cache.set(key, false, TTL.PT_BR); return result; }
}

// ─── Deduplicação e ranking ───────────────────────────────────────────────────

function normalize(s) {
  return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9\s]/g, '').trim();
}
function tokenize(s) {
  const stop = new Set(['the','a','an','of','in','on','at','to','o','de','da','do']);
  return new Set(normalize(s).split(/\s+/).filter(w => w.length > 1 && !stop.has(w)));
}
function stringSimilarity(a, b) {
  const na = normalize(a), nb = normalize(b);
  if (na === nb) return 1;
  if (na.includes(nb) || nb.includes(na)) return 0.9;
  const ta = tokenize(a), tb = tokenize(b);
  if (ta.size === 0 || tb.size === 0) return 0;
  let inter = 0; for (const w of ta) if (tb.has(w)) inter++;
  return 2 * inter / (ta.size + tb.size);
}
function bestTitleSimilarity(query, result) {
  const candidates = [result.mainTitle, result.romajiTitle, result.nativeTitle, ...result.synonyms].filter(Boolean);
  let best = 0;
  for (const t of candidates) { const s = stringSimilarity(query, t); if (s > best) best = s; if (best >= 1) break; }
  return best;
}
function isDuplicate(existing, candidate) {
  const normMain = normalize(candidate.mainTitle);
  return existing.some(r => {
    if (r.id === candidate.id && r.source === candidate.source) return true;
    if (stringSimilarity(r.mainTitle, candidate.mainTitle) > 0.85) return true;
    if (candidate.nativeTitle && r.nativeTitle && normalize(candidate.nativeTitle) === normalize(r.nativeTitle)) return true;
    if (candidate.romajiTitle && r.romajiTitle && stringSimilarity(candidate.romajiTitle, r.romajiTitle) > 0.9) return true;
    if (r.synonyms.some(s => normalize(s) === normMain)) return true;
    return false;
  });
}
function rankAndFilter(query, results) {
  return results.map(r => {
    let score = bestTitleSimilarity(query, r);
    if (r.source === 'anilist' || r.source === 'anilist-anime') score += 0.05;
    if (r.score && r.score > 60) score += 0.02;
    if (r.ptBrUrl) score += 0.03;
    return { result: r, score };
  }).sort((a, b) => b.score - a.score).map(s => s.result);
}

function isSynopsisQuery(query) {
  if (query.trim().split(/\s+/).length >= 7) return true;
  if (query.includes(',')) return true;
  return /\b(reencarna|acorda|descobre|consegue|torna[-\s]se|vira|entra|nasce|morre|luta|tenta|foge|salva|busca|procura|encontra|protege|derrota|precisa|decide|aprende|sobrevive|desperta|ganha|perde|volta|vai|chega|é (um|uma|o|a)\b)/i.test(query);
}

async function exaHitsToUnified(hits) {
  const anilistIds = hits.map(h => h.anilistId).filter(id => id !== null);
  const mangadexIds = hits.map(h => h.mangadexId).filter(id => id !== null);
  const [af, mf] = await Promise.allSettled([
    Promise.all(anilistIds.slice(0, 5).map(id => getManhwaById(id))),
    Promise.all(mangadexIds.slice(0, 5).map(id => getMangaDexById(id))),
  ]);
  return [
    ...(af.status === 'fulfilled' ? af.value.flatMap(m => m ? [anilistToUnified(m)] : []) : []),
    ...(mf.status === 'fulfilled' ? mf.value.flatMap(m => m ? [mangadexToUnified(m)] : []) : []),
  ];
}

// ─── Variantes de query para busca fuzzy ─────────────────────────────────────

function generateQueryVariants(query) {
  const variants = new Set();
  const q = query.trim();
  variants.add(q);

  // "Drop Out" → "Dropout"
  const noSpace = q.replace(/\s+/g, '');
  if (noSpace !== q && noSpace.length > 0) variants.add(noSpace);

  // "Drop Out" → "Drop-Out"
  const hyphenated = q.replace(/\s+/g, '-');
  if (hyphenated !== q) variants.add(hyphenated);

  // Remove pontuação, mantém letras/números/espaços
  const noPunct = q.replace(/[^\w\s]/g, ' ').replace(/\s+/g, ' ').trim();
  if (noPunct !== q && noPunct.length > 0) variants.add(noPunct);

  // Sem pontuação e sem espaços
  const noPunctNoSpace = noPunct.replace(/\s+/g, '');
  if (noPunctNoSpace !== q && noPunctNoSpace.length > 0) variants.add(noPunctNoSpace);

  // Remove artigos iniciais: "The Rising..." → "Rising..."
  const withoutArticle = q.replace(/^(the|a|an)\s+/i, '').trim();
  if (withoutArticle !== q && withoutArticle.length > 0) variants.add(withoutArticle);

  return [...variants].filter(v => v.length >= 2);
}

// ─── TMDB (opcional — só usa se TMDB_API_KEY estiver configurado) ─────────────

async function searchTMDB(query) {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey) return [];
  return cache.getOrSet(`tmdb:search:${query.toLowerCase().trim()}`, async () => {
    try {
      const params = new URLSearchParams({ api_key: apiKey, query, include_adult: 'false', language: 'en-US' });
      const res = await fetch(`https://api.themoviedb.org/3/search/tv?${params}`, {
        signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return [];
      const json = await res.json();
      return (json.results ?? [])
        .filter(r => r.origin_country?.includes('JP') || r.original_language === 'ja')
        .slice(0, 5)
        .map(r => ({
          source: 'tmdb',
          id: String(r.id),
          mainTitle: r.name ?? r.original_name,
          nativeTitle: r.original_language === 'ja' ? r.original_name : null,
          romajiTitle: null,
          synonyms: [],
          description: r.overview ?? null,
          coverUrl: r.poster_path ? `https://image.tmdb.org/t/p/w300${r.poster_path}` : null,
          accentColor: 0x01B4E4,
          score: r.vote_average ? Math.round(r.vote_average * 10) : null,
          genres: [],
          chapters: null, episodes: null,
          status: null,
          siteUrl: `https://www.themoviedb.org/tv/${r.id}`,
          year: r.first_air_date ? parseInt(r.first_air_date.slice(0, 4), 10) : null,
          ptBrUrl: null,
          mediaType: 'anime',
        }));
    } catch { return []; }
  }, TTL.SEARCH);
}

// ─── searchAllSources (central, inclui Jikan + AniDB) ────────────────────────

async function searchAllSources(query, mode = 'manhwa') {
  const cacheKey = `unified:${mode}:${query.toLowerCase().trim()}`;
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  const isSynopsis = isSynopsisQuery(query);
  const merged = [];

  if (mode === 'anime') {
    // ── Passo 1: busca em paralelo em todas as fontes com a query original ──
    const variants = generateQueryVariants(query);
    const [animeResults, translatedQuery, kitsuResults, jikanResults, anidbResults, tmdbResults] = await Promise.all([
      searchAnime(query).catch(() => []),
      translateToEnglish(query),
      searchKitsu(query).catch(() => []),
      searchJikanAnime(query).catch(() => []),
      searchAniDB(query).catch(() => []),
      searchTMDB(query).catch(() => []),
    ]);

    const exaSynopsisHits = isSynopsis
      ? await searchExaSynopsis(query, 'anime', translatedQuery).catch(() => [])
      : [];

    for (const r of animeResults.map(anilistToUnified)) if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of kitsuResults)  if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of jikanResults)  if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of anidbResults)  if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of tmdbResults)   if (!isDuplicate(merged, r)) merged.push(r);

    if (isSynopsis && exaSynopsisHits.length > 0) {
      const exaResults = await exaHitsToUnified(exaSynopsisHits).catch(() => []);
      for (const r of exaResults) if (!isDuplicate(merged, r)) merged.push(r);
    }

    // ── Passo 2: se poucos resultados, tenta todas as variantes de query ──
    if (merged.length < 3) {
      // Variantes + query traduzida
      const fallbackQueries = new Set([...variants, translatedQuery].filter(Boolean));
      fallbackQueries.delete(query); // já buscamos a original

      await Promise.all([...fallbackQueries].map(async (q) => {
        const [r1, r2, r3, r4] = await Promise.all([
          searchAnime(q).catch(() => []),
          searchKitsu(q).catch(() => []),
          searchJikanAnime(q).catch(() => []),
          searchTMDB(q).catch(() => []),
        ]);
        for (const r of r1.map(anilistToUnified)) if (!isDuplicate(merged, r)) merged.push(r);
        for (const r of r2) if (!isDuplicate(merged, r)) merged.push(r);
        for (const r of r3) if (!isDuplicate(merged, r)) merged.push(r);
        for (const r of r4) if (!isDuplicate(merged, r)) merged.push(r);
      }));
    }

    // ── Passo 3: extrai títulos alternativos dos resultados e busca por eles ──
    if (merged.length < 3) {
      const altTitles = new Set();
      for (const r of merged) {
        if (r.nativeTitle) altTitles.add(r.nativeTitle);
        if (r.romajiTitle)  altTitles.add(r.romajiTitle);
        for (const s of r.synonyms ?? []) if (s && s.length > 1) altTitles.add(s);
      }
      // Remove títulos muito parecidos com a query original (já tentados)
      const qNorm = normalize(query);
      const newAlts = [...altTitles].filter(t => normalize(t) !== qNorm && t.length >= 2).slice(0, 4);

      if (newAlts.length > 0) {
        await Promise.all(newAlts.map(async (alt) => {
          const [r1, r2, r3] = await Promise.all([
            searchAnime(alt).catch(() => []),
            searchJikanAnime(alt).catch(() => []),
            searchAniDB(alt).catch(() => []),
          ]);
          for (const r of r1.map(anilistToUnified)) if (!isDuplicate(merged, r)) merged.push(r);
          for (const r of r2) if (!isDuplicate(merged, r)) merged.push(r);
          for (const r of r3) if (!isDuplicate(merged, r)) merged.push(r);
        }));
      }
    }

    // ── Passo 4: último recurso — Exa AI ──
    if (merged.length < 3) {
      const exaHits = await searchExaManhwa(query).catch(() => []);
      const exaResults = await exaHitsToUnified(exaHits).catch(() => []);
      for (const r of exaResults) if (!isDuplicate(merged, r)) merged.push(r);
    }

  } else {
    // ── Passo 1: busca em paralelo em todas as fontes com a query original ──
    const variants = generateQueryVariants(query);
    const [manhwaResults, mangadexResults, comickResults, muResults, jikanResults, translatedForExa] = await Promise.all([
      (mode === 'all' ? searchManga(query) : searchManhwa(query)).catch(() => []),
      searchMangaDex(query).catch(() => []),
      searchComick(query).catch(() => []),
      searchMangaUpdates(query).catch(() => []),
      searchJikanManga(query).catch(() => []),
      translateToEnglish(query),
    ]);

    const exaSynopsisHits = isSynopsis
      ? await searchExaSynopsis(query, mode, translatedForExa).catch(() => [])
      : [];

    for (const r of manhwaResults.map(anilistToUnified)) if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of mangadexResults.map(mangadexToUnified)) if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of comickResults.map(comickToUnified))     if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of muResults.map(mangaupdatesToUnified))   if (!isDuplicate(merged, r)) merged.push(r);
    for (const r of jikanResults) if (!isDuplicate(merged, r)) merged.push(r);

    if (isSynopsis && exaSynopsisHits.length > 0) {
      const exaResults = await exaHitsToUnified(exaSynopsisHits).catch(() => []);
      for (const r of exaResults) if (!isDuplicate(merged, r)) merged.push(r);
    }

    // ── Passo 2: se poucos resultados, tenta variantes + query traduzida ──
    if (merged.length < 3) {
      const fallbackQueries = new Set([...variants, translatedForExa].filter(Boolean));
      fallbackQueries.delete(query);

      await Promise.all([...fallbackQueries].map(async (q) => {
        const [m1, m2, m3, m4] = await Promise.all([
          (mode === 'all' ? searchManga(q) : searchManhwa(q)).catch(() => []),
          searchMangaDex(q).catch(() => []),
          searchComick(q).catch(() => []),
          searchJikanManga(q).catch(() => []),
        ]);
        for (const r of m1.map(anilistToUnified))    if (!isDuplicate(merged, r)) merged.push(r);
        for (const r of m2.map(mangadexToUnified))   if (!isDuplicate(merged, r)) merged.push(r);
        for (const r of m3.map(comickToUnified))     if (!isDuplicate(merged, r)) merged.push(r);
        for (const r of m4) if (!isDuplicate(merged, r)) merged.push(r);
      }));
    }

    // ── Passo 3: extrai títulos alternativos dos resultados e busca por eles ──
    if (merged.length < 3) {
      const altTitles = new Set();
      for (const r of merged) {
        if (r.nativeTitle) altTitles.add(r.nativeTitle);
        if (r.romajiTitle)  altTitles.add(r.romajiTitle);
        for (const s of r.synonyms ?? []) if (s && s.length > 1) altTitles.add(s);
      }
      const qNorm = normalize(query);
      const newAlts = [...altTitles].filter(t => normalize(t) !== qNorm && t.length >= 2).slice(0, 4);

      if (newAlts.length > 0) {
        await Promise.all(newAlts.map(async (alt) => {
          const [m1, m2] = await Promise.all([
            (mode === 'all' ? searchManga(alt) : searchManhwa(alt)).catch(() => []),
            searchMangaDex(alt).catch(() => []),
          ]);
          for (const r of m1.map(anilistToUnified))  if (!isDuplicate(merged, r)) merged.push(r);
          for (const r of m2.map(mangadexToUnified)) if (!isDuplicate(merged, r)) merged.push(r);
        }));
      }
    }

    // ── Passo 4: último recurso — Exa AI ──
    if (merged.length < 3) {
      try {
        const exaHits = await searchExaManhwa(query);
        const exaResults = await exaHitsToUnified(exaHits);
        for (const r of exaResults) if (!isDuplicate(merged, r)) merged.push(r);
      } catch { /* silencioso */ }
    }
  }

  const ranked = rankAndFilter(query, merged).slice(0, 10);
  cache.set(cacheKey, ranked, TTL.SEARCH);
  return ranked;
}

async function getUnifiedById(source, id) {
  let result = null;
  if      (source === 'anilist')       { const m = await getManhwaById(parseInt(id, 10)); result = m ? anilistToUnified(m) : null; }
  else if (source === 'anilist-anime') { const m = await getAnimeById(parseInt(id, 10));  result = m ? anilistToUnified(m) : null; }
  else if (source === 'mangadex')      { const m = await getMangaDexById(id);             result = m ? mangadexToUnified(m) : null; }
  else if (source === 'comick')        { const m = await getComickBySlug(id);             result = m ? comickToUnified(m) : null; }
  else if (source === 'mangaupdates')  { const m = await getMangaUpdatesById(id);         result = m ? mangaupdatesToUnified(m) : null; }
  else if (source === 'kitsu')         { result = await getKitsuById(id); }
  else if (source === 'jikan')         { result = await getJikanAnimeById(id); }
  else if (source === 'jikan-manga')   { result = await getJikanMangaById(id); }
  else if (source === 'anidb')         { result = await getAniDBById(id); }
  if (!result) return null;
  return enrichWithPtBr(result);
}

// ─── DB Schema ────────────────────────────────────────────────────────────────

if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL não configurado.');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const db = drizzle(pool);

const favoritosTable = pgTable('favoritos', {
  id:            integer('id').primaryKey().generatedAlwaysAsIdentity(),
  discordUserId: text('discord_user_id').notNull(),
  manhwaId:      text('manhwa_id').notNull(),
  source:        text('source').notNull(),
  title:         text('title').notNull(),
  coverUrl:      text('cover_url'),
  siteUrl:       text('site_url').notNull(),
  genres:        text('genres'),
  score:         text('score'),
  addedAt:       timestamp('added_at').defaultNow().notNull(),
});

const STATUS_OPCOES = ['lendo', 'concluido', 'planejo', 'pausado', 'abandonado'];
const STATUS_LABELS = {
  lendo: '📖 Lendo', concluido: '✅ Concluído',
  planejo: '🔖 Planejo Ler', pausado: '⏸️ Pausado', abandonado: '🗑️ Abandonado',
};

const listaLeituraTable = pgTable('lista_leitura', {
  id:            integer('id').primaryKey().generatedAlwaysAsIdentity(),
  discordUserId: text('discord_user_id').notNull(),
  manhwaId:      text('manhwa_id').notNull(),
  source:        text('source').notNull(),
  title:         text('title').notNull(),
  coverUrl:      text('cover_url'),
  siteUrl:       text('site_url').notNull(),
  genres:        text('genres'),
  score:         text('score'),
  status:        text('status').notNull(),
  addedAt:       timestamp('added_at').defaultNow().notNull(),
  updatedAt:     timestamp('updated_at').defaultNow().notNull(),
});

const notificacaoCanaisTable = pgTable('notificacao_canais', {
  id:           integer('id').primaryKey().generatedAlwaysAsIdentity(),
  guildId:      text('guild_id').notNull().unique(),
  channelId:    text('channel_id').notNull(),
  configuredAt: timestamp('configured_at').defaultNow().notNull(),
});

const capitulosRastreados = pgTable('capitulos_rastreados', {
  id:           integer('id').primaryKey().generatedAlwaysAsIdentity(),
  manhwaId:     text('manhwa_id').notNull().unique(),
  source:       text('source').notNull(),
  title:        text('title').notNull(),
  coverUrl:     text('cover_url'),
  siteUrl:      text('site_url').notNull(),
  lastChapters: real('last_chapters'),
  lastChecked:  timestamp('last_checked').defaultNow(),
});

// ─── Sites de leitura BR ─────────────────────────────────────────────────────

const FALLBACK_SITES = [
  { name: 'NexusToons',   url: 'https://nexustoons.com',          search: '/?s=' },
  { name: 'InkApk',       url: 'https://inkapk.net',              search: '/?s=' },
  { name: 'ReMangas',     url: 'https://remangas.net',            search: '/?s=' },
  { name: 'MangaHost',    url: 'https://mangahost4.com',          search: '/find/' },
  { name: 'UnionMangas',  url: 'https://unionleitor.top',         search: '/lista-mangas/0/0/0/0/1/0/0?busca=' },
  { name: 'MangaLivre',   url: 'https://mangalivre.net',          search: '/series/index/busca=' },
];
function buildFallbackLinks(title) {
  const enc = encodeURIComponent(title);
  return FALLBACK_SITES.map(s => `[${s.name}](${s.url}${s.search}${enc})`).join(' • ');
}
function buildScanLinksExternal(title) { return buildFallbackLinks(title); }

// ─── buildEmbed ───────────────────────────────────────────────────────────────

const SOURCE_LABELS = {
  anilist: 'AniList', 'anilist-anime': 'AniList', mangadex: 'MangaDex',
  comick: 'Comick.io', mangaupdates: 'MangaUpdates',
  kitsu: 'Kitsu', jikan: 'MyAnimeList', 'jikan-manga': 'MyAnimeList', anidb: 'AniDB',
};
const SOURCE_ICONS = {
  anilist: '🟣', 'anilist-anime': '🟣', mangadex: '🟠', comick: '🟢',
  mangaupdates: '🔵', kitsu: '🟡', jikan: '🔵', 'jikan-manga': '🔵', anidb: '🔷',
};

function buildAltTitles(r) {
  const seen = new Set([r.mainTitle.toLowerCase()]);
  const titles = [];
  for (const t of [r.nativeTitle, r.romajiTitle, ...r.synonyms]) {
    if (t && !seen.has(t.toLowerCase())) { seen.add(t.toLowerCase()); titles.push(t); }
  }
  return titles.length ? titles.slice(0, 6).join('\n') : null;
}

async function buildEmbed(r) {
  const score    = r.score    ? `⭐ ${(r.score / 10).toFixed(1)}/10` : '⭐ N/A';
  const chapters = r.chapters ? `📖 ${r.chapters} capítulos` : '📖 Desconhecido';
  const status   = `📌 ${statusLabel(r.status)}`;
  const genres   = r.genres.length ? r.genres.slice(0, 6).join(' • ') : 'Sem gêneros';
  const altTitles = buildAltTitles(r);
  const sourceLabel = SOURCE_LABELS[r.source] ?? r.source;
  const sourceIcon  = SOURCE_ICONS[r.source] ?? '🔵';
  const rawDesc = cleanDescription(r.description);
  const [synopsis, directLinks] = await Promise.all([
    translateToPtBr(rawDesc),
    findDirectLinks(r.mainTitle),
  ]);
  const embed = new EmbedBuilder()
    .setTitle(r.mainTitle).setURL(r.siteUrl)
    .setDescription(synopsis || 'Sem sinopse disponível.')
    .setColor(r.accentColor)
    .addFields(
      { name: 'Avaliação', value: score, inline: true },
      { name: 'Capítulos', value: chapters, inline: true },
      { name: 'Status', value: status, inline: true },
      { name: 'Gêneros', value: genres, inline: false },
    );
  if (r.coverUrl) embed.setThumbnail(r.coverUrl);
  if (altTitles)  embed.addFields({ name: 'Títulos alternativos', value: altTitles, inline: false });
  if (r.year)     embed.addFields({ name: 'Ano de início', value: String(r.year), inline: true });
  if (r.ptBrUrl)  embed.addFields({ name: '🇧🇷 Leitura em PT-BR (MangaDex)', value: `[Ler em Português](${r.ptBrUrl})`, inline: false });
  const directFound = directLinks.filter(l => l.direct);
  if (directFound.length > 0) embed.addFields({ name: '🔗 Sites de leitura (direto)', value: directFound.map(l => `[${l.name}](${l.url})`).join(' • '), inline: false });
  embed.addFields({ name: '🔎 Buscar nos sites BR', value: buildFallbackLinks(r.mainTitle), inline: false });
  embed.addFields({ name: `${sourceIcon} Fonte`, value: `[${sourceLabel}](${r.siteUrl})`, inline: true });
  embed.setFooter({ text: 'Dados via AniList/MangaDex/MAL/AniDB • Sinopse traduzida automaticamente' });
  const directFallback = directLinks.filter(l => !l.direct);
  if (directFallback.length > 0 && directFound.length === 0)
    embed.addFields({ name: '🔗 Sites de scan', value: directFallback.map(l => `[${l.name}](${l.url})`).join(' • '), inline: false });
  return embed;
}

// ─── Autocomplete ─────────────────────────────────────────────────────────────

async function respondAutocomplete(interaction, focusedValue, mode = 'manhwa') {
  const query = focusedValue.trim();
  if (query.length < 2) { await interaction.respond([]); return; }
  try {
    const seen = new Set(); const suggestions = [];
    if (mode === 'anime') {
      const animeRaw = await searchAnime(query).catch(() => []);
      for (const m of animeRaw) {
        const title = m.title.english ?? m.title.romaji ?? m.title.native ?? '';
        if (title && !seen.has(title.toLowerCase())) {
          seen.add(title.toLowerCase());
          suggestions.push({ name: title.slice(0, 100), value: `anilist-anime:${m.id}` });
        }
      }
    } else {
      const [anilistRaw, comickRaw] = await Promise.allSettled([searchManhwa(query), searchComick(query)]);
      if (anilistRaw.status === 'fulfilled') {
        for (const m of anilistRaw.value) {
          const title = m.title.english ?? m.title.romaji ?? m.title.native ?? '';
          if (title && !seen.has(title.toLowerCase())) {
            seen.add(title.toLowerCase());
            suggestions.push({ name: title.slice(0, 100), value: `anilist:${m.id}` });
          }
        }
      }
      if (comickRaw.status === 'fulfilled') {
        for (const m of comickRaw.value) {
          if (m.title && !seen.has(m.title.toLowerCase())) {
            seen.add(m.title.toLowerCase());
            suggestions.push({ name: m.title.slice(0, 100), value: `comick:${m.slug}` });
          }
        }
      }
    }
    await interaction.respond(suggestions.slice(0, 25));
  } catch { await interaction.respond([]); }
}

// ─── Comandos ─────────────────────────────────────────────────────────────────

// /obra — pesquisa manhwa
const cmdObra = {
  data: new SlashCommandBuilder().setName('obra').setDescription('Pesquisa um manhwa/mangá no AniList, MangaDex e MAL com sinopse traduzida')
    .addStringOption(o => o.setName('titulo').setDescription('Nome do manhwa para pesquisar').setRequired(true).setAutocomplete(true)),
  async autocomplete(interaction) { await respondAutocomplete(interaction, interaction.options.getFocused(), 'manhwa'); },
  async execute(interaction) {
    const input = interaction.options.getString('titulo', true);
    await interaction.deferReply();
    if (input.includes(':')) {
      const colonIdx = input.indexOf(':');
      const source = input.slice(0, colonIdx), id = input.slice(colonIdx + 1);
      const validSources = ['anilist','anilist-anime','mangadex','comick','mangaupdates','kitsu','jikan','jikan-manga','anidb'];
      if (validSources.includes(source)) {
        await interaction.editReply({ content: '⏳ Carregando...' });
        const detail = await getUnifiedById(source, id);
        if (detail) { const embed = await buildEmbed(detail); await interaction.editReply({ content: null, embeds: [embed] }); return; }
      }
    }
    let results = [];
    try { results = await searchAllSources(input, 'manhwa'); }
    catch { await interaction.editReply('❌ Erro ao buscar o manhwa. Tente novamente.'); return; }
    if (!results.length) {
      await interaction.editReply(`❌ Nenhum manhwa encontrado para **${input}**.\n\n💡 Dicas:\n• Tente o nome em inglês ou coreano\n• Use parte do título\n• Tente o comando \`/serie\` se for um anime`);
      return;
    }
    if (results.length === 1) {
      await interaction.editReply({ content: '⏳ Carregando detalhes...' });
      const detail = await getUnifiedById(results[0].source, results[0].id);
      const embed = await buildEmbed(detail ?? results[0]);
      await interaction.editReply({ content: null, embeds: [embed] }); return;
    }
    const options = results.slice(0, 10).map(r => ({
      label: r.mainTitle.slice(0, 100),
      description: `${SOURCE_ICONS[r.source] ?? '🔵'}${r.score ? ` ⭐${(r.score / 10).toFixed(1)}` : ''}${r.year ? ` (${r.year})` : ''} • ${r.genres.slice(0, 2).join(', ') || 'Manhwa'}`.slice(0, 100),
      value: `${r.source}:${r.id}`,
    }));
    const row = new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder().setCustomId('manhwa_select').setPlaceholder('Escolha o manhwa correto...').addOptions(options)
    );
    await interaction.editReply({ content: `🔍 Encontrei **${results.length}** resultados para **${input}**. Qual você quer?`, components: [row] });
    try {
      const collector = interaction.channel?.createMessageComponentCollector({
        componentType: ComponentType.StringSelect,
        filter: i => i.customId === 'manhwa_select' && i.user.id === interaction.user.id,
        time: 30000, max: 1,
      });
      if (!collector) { await interaction.editReply({ content: '❌ Erro ao criar seletor.', components: [] }); return; }
      collector.on('collect', async sel => {
        await sel.deferUpdate();
        const ci = sel.values[0].indexOf(':');
        const src = sel.values[0].slice(0, ci), id = sel.values[0].slice(ci + 1);
        await interaction.editReply({ content: '⏳ Carregando detalhes...', components: [] });
        const detail = await getUnifiedById(src, id);
        const embed = await buildEmbed(detail ?? results.find(r => r.id === id));
        await interaction.editReply({ content: null, embeds: [embed], components: [] });
      });
      collector.on('end', async collected => {
        if (collected.size === 0) await interaction.editReply({ content: '⏱️ Tempo esgotado. Use o comando novamente.', components: [] }).catch(() => null);
      });
    } catch { await interaction.editReply({ content: '❌ Erro ao processar seleção.', components: [] }); }
  },
};

// /serie — pesquisa anime
const cmdSerie = {
  data: new SlashCommandBuilder().setName('serie').setDescription('Pesquisa um anime no AniList, Kitsu e MAL com sinopse traduzida')
    .addStringOption(o => o.setName('titulo').setDescription('Nome do anime para pesquisar').setRequired(true).setAutocomplete(true)),
  async autocomplete(interaction) {
    const focused = interaction.options.getFocused();
    if (!focused || focused.length < 2) { await interaction.respond([]); return; }
    try {
      const results = await searchAllSources(focused, 'anime');
      await interaction.respond(results.slice(0, 10).map(r => ({ name: r.mainTitle.slice(0, 100), value: `${r.source}:${r.id}` })));
    } catch { await interaction.respond([]); }
  },
  async execute(interaction) {
    const input = interaction.options.getString('titulo', true);
    await interaction.deferReply();
    if (input.includes(':')) {
      const parts = input.split(':');
      const source = parts[0], id = parts.slice(1).join(':');
      const validSources = ['anilist-anime','anilist','kitsu','jikan','anidb'];
      if (validSources.includes(source)) {
        const detail = await getUnifiedById(source, id);
        if (detail) {
          const score = detail.score ? `⭐ ${(detail.score / 10).toFixed(1)}/10` : '⭐ N/A';
          const eps = detail.episodes ? `🎬 ${detail.episodes} episódios` : '🎬 Episódios desconhecidos';
          const status = `📌 ${statusLabel(detail.status)}`;
          const genres = detail.genres.length ? detail.genres.slice(0, 6).join(' • ') : 'Sem gêneros';
          const synopsis = await translateToPtBr(cleanDescription(detail.description));
          const embed = new EmbedBuilder().setTitle(detail.mainTitle).setURL(detail.siteUrl)
            .setDescription(synopsis || 'Sem sinopse disponível.').setColor(detail.accentColor)
            .addFields({ name: 'Avaliação', value: score, inline: true }, { name: 'Episódios', value: eps, inline: true }, { name: 'Status', value: status, inline: true }, { name: 'Gêneros', value: genres, inline: false });
          if (detail.coverUrl) embed.setThumbnail(detail.coverUrl);
          if (detail.year) embed.addFields({ name: 'Ano de início', value: String(detail.year), inline: true });
          const src = SOURCE_ICONS[detail.source] ?? '🔵';
          embed.addFields({ name: `${src} Fonte`, value: `[${SOURCE_LABELS[detail.source] ?? detail.source}](${detail.siteUrl})`, inline: true });
          embed.setFooter({ text: 'Dados via AniList/Kitsu/MAL/AniDB • Sinopse traduzida automaticamente' });
          await interaction.editReply({ embeds: [embed] }); return;
        }
      }
    }
    let results = [];
    try { results = await searchAllSources(input, 'anime'); }
    catch { await interaction.editReply('❌ Erro ao buscar o anime. Tente novamente.'); return; }
    if (!results.length) { await interaction.editReply(`❌ Nenhum anime encontrado para **${input}**.\n\nDica: tente o nome em inglês ou japonês!`); return; }
    if (results.length === 1) {
      const detail = await getUnifiedById(results[0].source, results[0].id);
      const r = detail ?? results[0];
      const synopsis = await translateToPtBr(cleanDescription(r.description));
      const embed = new EmbedBuilder().setTitle(r.mainTitle).setURL(r.siteUrl)
        .setDescription(synopsis || 'Sem sinopse disponível.').setColor(r.accentColor)
        .addFields({ name: 'Avaliação', value: r.score ? `⭐ ${(r.score/10).toFixed(1)}/10` : '⭐ N/A', inline: true },
          { name: 'Episódios', value: r.episodes ? `🎬 ${r.episodes}` : '🎬 ?', inline: true },
          { name: 'Status', value: `📌 ${statusLabel(r.status)}`, inline: true },
          { name: 'Gêneros', value: r.genres.join(' • ') || 'Sem gêneros', inline: false });
      if (r.coverUrl) embed.setThumbnail(r.coverUrl);
      if (r.year) embed.addFields({ name: 'Ano', value: String(r.year), inline: true });
      await interaction.editReply({ embeds: [embed] }); return;
    }
    const options = results.slice(0, 10).map(r => ({
      label: r.mainTitle.slice(0, 100),
      description: `${SOURCE_ICONS[r.source] ?? '🔵'}${r.score ? ` ⭐${(r.score/10).toFixed(1)}` : ''}${r.year ? ` (${r.year})` : ''} • ${r.genres.slice(0, 2).join(', ') || 'Anime'}`.slice(0, 100),
      value: `${r.source}:${r.id}`,
    }));
    const row = new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder().setCustomId('anime_select').setPlaceholder('Escolha o anime correto...').addOptions(options)
    );
    await interaction.editReply({ content: `🔍 Encontrei **${results.length}** resultados para **${input}**. Qual você quer?`, components: [row] });
    try {
      const collector = interaction.channel?.createMessageComponentCollector({
        componentType: ComponentType.StringSelect,
        filter: i => i.customId === 'anime_select' && i.user.id === interaction.user.id,
        time: 30000, max: 1,
      });
      if (!collector) { await interaction.editReply({ content: '❌ Erro ao criar seletor.', components: [] }); return; }
      collector.on('collect', async sel => {
        await sel.deferUpdate();
        const ci = sel.values[0].indexOf(':');
        const src = sel.values[0].slice(0, ci), id = sel.values[0].slice(ci + 1);
        await interaction.editReply({ content: '⏳ Carregando...', components: [] });
        const detail = await getUnifiedById(src, id);
        const r = detail ?? results.find(r => r.id === id);
        const synopsis = await translateToPtBr(cleanDescription(r.description));
        const embed = new EmbedBuilder().setTitle(r.mainTitle).setURL(r.siteUrl)
          .setDescription(synopsis || 'Sem sinopse disponível.').setColor(r.accentColor)
          .addFields({ name: 'Avaliação', value: r.score ? `⭐ ${(r.score/10).toFixed(1)}/10` : '⭐ N/A', inline: true },
            { name: 'Episódios', value: r.episodes ? `🎬 ${r.episodes}` : '🎬 ?', inline: true },
            { name: 'Status', value: `📌 ${statusLabel(r.status)}`, inline: true },
            { name: 'Gêneros', value: r.genres.join(' • ') || 'Sem gêneros', inline: false });
        if (r.coverUrl) embed.setThumbnail(r.coverUrl);
        await interaction.editReply({ content: null, embeds: [embed], components: [] });
      });
      collector.on('end', async collected => {
        if (collected.size === 0) await interaction.editReply({ content: '⏱️ Tempo esgotado. Use o comando novamente.', components: [] }).catch(() => null);
      });
    } catch { await interaction.editReply({ content: '❌ Erro ao processar seleção.', components: [] }); }
  },
};

// /tops — top 10 manhwas
const cmdTops = {
  data: new SlashCommandBuilder().setName('tops').setDescription('Lista os 10 manhwas mais bem avaliados no AniList'),
  async execute(interaction) {
    await interaction.deferReply();
    try {
      const res = await fetch(ANILIST_API, {
        method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ query: `query { Page(page: 1, perPage: 10) { media(type: MANGA, countryOfOrigin: KR, sort: SCORE_DESC, status_not: NOT_YET_RELEASED) { id title { romaji english } averageScore genres chapters status siteUrl coverImage { color } } } }`, variables: {} }),
      });
      if (!res.ok) throw new Error(`AniList error: ${res.status}`);
      const json = await res.json();
      const list = json.data.Page.media;
      const description = list.map((m, i) => {
        const title = m.title.english ?? m.title.romaji;
        const score = m.averageScore ? `⭐ ${(m.averageScore / 10).toFixed(1)}` : '⭐ N/A';
        const chapters = m.chapters ? `📖 ${m.chapters} caps` : '';
        return `**${i + 1}.** [${title}](${m.siteUrl}) — ${score} ${chapters ? `| ${chapters}` : ''}\n> ${m.genres.slice(0, 2).join(', ')}`;
      }).join('\n\n');
      await interaction.editReply({ embeds: [new EmbedBuilder().setTitle('🏆 Top 10 Manhwas — AniList').setDescription(description).setColor(8087790).setFooter({ text: 'Fonte: AniList • Ordenado por nota média' })] });
    } catch { await interaction.editReply('❌ Erro ao buscar os top manhwas. Tente novamente.'); }
  },
};

// /indicar — recomendações por gênero
const GENRES_LIST = [
  { label: '⚔️ Ação', value: 'Action' }, { label: '🗺️ Aventura', value: 'Adventure' },
  { label: '😂 Comédia', value: 'Comedy' }, { label: '😢 Drama', value: 'Drama' },
  { label: '🧙 Fantasia', value: 'Fantasy' }, { label: '😱 Horror', value: 'Horror' },
  { label: '🔍 Mistério', value: 'Mystery' }, { label: '🧠 Psicológico', value: 'Psychological' },
  { label: '💕 Romance', value: 'Romance' }, { label: '🚀 Ficção Científica', value: 'Sci-Fi' },
  { label: '☕ Slice of Life', value: 'Slice of Life' }, { label: '👻 Sobrenatural', value: 'Supernatural' },
  { label: '😰 Thriller', value: 'Thriller' }, { label: '🏆 Esportes', value: 'Sports' },
  { label: '🤖 Mecha', value: 'Mecha' }, { label: '🎵 Música', value: 'Music' },
  { label: '⏰ Reencarnação', value: 'Reincarnation' }, { label: '🎮 Game', value: 'Video Games' },
  { label: '🧟 Zumbi', value: 'Zombies' }, { label: '🗡️ Survival', value: 'Survival' },
  { label: '🏫 Escola', value: 'School Life' },
];
const cmdIndicar = {
  data: new SlashCommandBuilder().setName('indicar').setDescription('Recomenda manhwas por gênero — selecione até 5 gêneros'),
  async execute(interaction) {
    const row = new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder().setCustomId('genre_select').setPlaceholder('Selecione de 1 a 5 gêneros').setMinValues(1).setMaxValues(5).addOptions(GENRES_LIST)
    );
    await interaction.reply({ content: '🎭 Escolha os gêneros para receber recomendações de manhwa:', components: [row], ephemeral: false });
    const collector = interaction.channel?.createMessageComponentCollector({
      componentType: ComponentType.StringSelect,
      filter: i => i.customId === 'genre_select' && i.user.id === interaction.user.id,
      time: 45000, max: 1,
    });
    collector?.on('collect', async sel => {
      await sel.deferUpdate();
      const selected = sel.values;
      await interaction.editReply({ content: '⏳ Buscando recomendações...', components: [] });
      try {
        const res = await fetch(ANILIST_API, {
          method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ query: `query($g:[String],$p:Int){Page(page:$p,perPage:6){media(type:MANGA,countryOfOrigin:KR,genre_in:$g,sort:SCORE_DESC,averageScore_greater:70){id title{romaji english native}description(asHtml:false)coverImage{large color}averageScore genres chapters status siteUrl startDate{year}}}}`, variables: { g: selected, p: 1 } }),
          signal: AbortSignal.timeout(10000),
        });
        if (!res.ok) throw new Error();
        const json = await res.json();
        const results = json.data?.Page?.media ?? [];
        if (!results.length) { await interaction.editReply({ content: '❌ Nenhum manhwa encontrado para essa combinação. Tente outros!' }); return; }
        const lines = await Promise.all(results.map(async m => {
          const title = m.title.english ?? m.title.romaji ?? 'Sem título';
          const score = m.averageScore ? `⭐ ${(m.averageScore / 10).toFixed(1)}` : '⭐ N/A';
          const chapters = m.chapters ? `📖 ${m.chapters} caps` : '';
          const rawDesc = cleanDescription(m.description ?? '').slice(0, 200);
          const desc = rawDesc ? await translateToPtBr(rawDesc) : 'Sem sinopse.';
          return `**[${title}](${m.siteUrl})** — ${score} ${chapters ? `| ${chapters}` : ''} | ${statusLabel(m.status)}\n> ${desc.slice(0, 120)}${desc.length > 120 ? '...' : ''}\n> 🔎 ${buildScanLinksExternal(title)}`;
        }));
        const genreLabels = selected.map(g => GENRES_LIST.find(x => x.value === g)?.label ?? g).join(', ');
        await interaction.editReply({ content: null, embeds: [new EmbedBuilder().setTitle('📚 Recomendações de Manhwa').setDescription(`**Gêneros selecionados:** ${genreLabels}\n\n${lines.join('\n\n')}`).setColor(8087790).setFooter({ text: 'Fonte: AniList • Sinopses traduzidas automaticamente' })] });
      } catch { await interaction.editReply({ content: '❌ Erro ao buscar recomendações. Tente novamente.' }); }
    });
    collector?.on('end', async (_c, reason) => {
      if (reason === 'time') await interaction.editReply({ content: '⏱️ Tempo esgotado. Use `/indicar` novamente.', components: [] });
    });
  },
};

// /guia — ajuda
const cmdGuia = {
  data: new SlashCommandBuilder().setName('guia').setDescription('Lista todos os comandos do bot e suas funções'),
  async execute(interaction) {
    const embed = new EmbedBuilder().setTitle('📖 Comandos do Bot de Manhwa').setColor(8087790)
      .setDescription('Aqui estão todos os comandos disponíveis:')
      .addFields(
        { name: '🔍 /obra <título>', value: 'Pesquisa um manhwa no **AniList, MangaDex, MAL e mais** com sinopse em PT-BR, nota, gêneros e links BR.', inline: false },
        { name: '🎌 /serie <título>', value: 'Pesquisa um anime no **AniList, Kitsu, MAL e AniDB** com sinopse traduzida.', inline: false },
        { name: '🏆 /tops', value: 'Lista os **10 manhwas mais bem avaliados** do AniList.', inline: false },
        { name: '🎭 /indicar', value: 'Recomenda manhwas com base em **até 5 gêneros** escolhidos.', inline: false },
        { name: '🎲 /surpresa', value: 'Retorna um **manhwa aleatório** bem avaliado.', inline: false },
        { name: '📡 /novidades', value: 'Lista os manhwas mais populares **em lançamento**.', inline: false },
        { name: '❤️ /curtidas adicionar/listar/remover', value: 'Gerencia sua lista de manhwas favoritos.', inline: false },
        { name: '⚔️ /versus <manhwa1> <manhwa2>', value: 'Compara dois manhwas lado a lado.', inline: false },
        { name: '✍️ /criador <nome>', value: 'Busca um autor e lista toda a obra dele.', inline: false },
        { name: '🔔 /alertas canal/status/desativar', value: 'Configura notificações de novos capítulos.', inline: false },
        { name: '📋 /leituras adicionar/ver/mover/remover', value: 'Sua lista de leitura pessoal com status.', inline: false },
        { name: '🏆 /populares', value: 'Top 10 manhwas mais favoritados no servidor.', inline: false },
        { name: '📊 /estatisticas [@usuário]', value: 'Exibe suas estatísticas de leitura no bot.', inline: false },
        { name: '🎯 /parecidos <título>', value: 'Encontra manhwas parecidos com um que você gosta.', inline: false },
        { name: '🔎 /filtrar', value: 'Busca avançada com filtros combinados (gênero, status, ano, nota, tipo).', inline: false },
        { name: '📖 /sinopse <descrição>', value: 'Encontra uma obra descrevendo o enredo com suas próprias palavras (IA).', inline: false },
      )
      .addFields({ name: '🇧🇷 Sites de leitura BR', value: 'NexusToons • InkApk • ReMangas • MangaHost • UnionMangas • MangaLivre', inline: false })
      .setFooter({ text: 'Fontes: AniList, MangaDex, Comick, MangaUpdates, MAL (Jikan), AniDB, Kitsu' });
    await interaction.reply({ embeds: [embed] });
  },
};

// /surpresa — manhwa aleatório
const cmdSurpresa = {
  data: new SlashCommandBuilder().setName('surpresa').setDescription('Retorna um manhwa aleatório bem avaliado para descobrir obras novas'),
  async execute(interaction) {
    await interaction.deferReply();
    try {
      const randomPage = Math.floor(Math.random() * 40) + 1;
      const res = await fetch(ANILIST_API, {
        method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ query: `query($p:Int){Page(page:$p,perPage:1){media(type:MANGA,countryOfOrigin:KR,sort:SCORE_DESC,averageScore_greater:75,status_not:NOT_YET_RELEASED){id title{romaji english native}synonyms description(asHtml:false)coverImage{large color}averageScore genres chapters status siteUrl startDate{year}}}}`, variables: { p: randomPage } }),
        signal: AbortSignal.timeout(10000),
      });
      if (!res.ok) throw new Error();
      const json = await res.json();
      const media = json.data?.Page?.media;
      if (!media?.length) { await interaction.editReply('❌ Não foi possível encontrar um manhwa. Tente novamente!'); return; }
      const m = media[0];
      const title = m.title.english ?? m.title.romaji ?? m.title.native ?? 'Sem título';
      const synopsis = await translateToPtBr(cleanDescription(m.description));
      const altTitles = buildAlternativeTitles(m);
      const color = m.coverImage.color ? parseInt(m.coverImage.color.replace('#', ''), 16) : 8087790;
      const embed = new EmbedBuilder().setTitle(`🎲 ${title}`).setURL(m.siteUrl)
        .setDescription(synopsis || 'Sem sinopse disponível.').setThumbnail(m.coverImage.large).setColor(color)
        .addFields(
          { name: 'Avaliação', value: m.averageScore ? `⭐ ${(m.averageScore / 10).toFixed(1)}/10` : '⭐ N/A', inline: true },
          { name: 'Capítulos', value: m.chapters ? `📖 ${m.chapters} capítulos` : '📖 Desconhecido', inline: true },
          { name: 'Status', value: `📌 ${statusLabel(m.status)}`, inline: true },
          { name: 'Gêneros', value: m.genres.slice(0, 6).join(' • ') || 'Sem gêneros', inline: false },
        );
      if (altTitles) embed.addFields({ name: 'Títulos alternativos', value: altTitles, inline: false });
      if (m.startDate?.year) embed.addFields({ name: 'Ano de início', value: String(m.startDate.year), inline: true });
      embed.addFields({ name: '🔎 Buscar nos sites BR', value: buildScanLinksExternal(title), inline: false });
      embed.setFooter({ text: '🎲 Manhwa aleatório • Fonte: AniList • Sinopse traduzida automaticamente' });
      await interaction.editReply({ embeds: [embed] });
    } catch { await interaction.editReply('❌ Erro ao buscar manhwa aleatório. Tente novamente!'); }
  },
};

// /novidades — lançamentos
const cmdNovidades = {
  data: new SlashCommandBuilder().setName('novidades').setDescription('Lista os manhwas mais populares que estão em lançamento agora'),
  async execute(interaction) {
    await interaction.deferReply();
    try {
      const res = await fetch(ANILIST_API, {
        method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ query: `query($p:Int){Page(page:$p,perPage:10){media(type:MANGA,countryOfOrigin:KR,status:RELEASING,sort:POPULARITY_DESC){id title{romaji english}averageScore genres chapters popularity siteUrl coverImage{color}startDate{year}}}}`, variables: { p: 1 } }),
        signal: AbortSignal.timeout(10000),
      });
      if (!res.ok) throw new Error();
      const json = await res.json();
      const list = json.data?.Page?.media ?? [];
      if (!list.length) { await interaction.editReply('❌ Não foi possível obter os lançamentos agora. Tente novamente!'); return; }
      const description = list.map((m, i) => {
        const title = m.title.english ?? m.title.romaji;
        const score = m.averageScore ? `⭐ ${(m.averageScore / 10).toFixed(1)}` : '⭐ N/A';
        const chapters = m.chapters ? `📖 ${m.chapters} caps` : '📖 Em andamento';
        const year = m.startDate?.year ? `(${m.startDate.year})` : '';
        return `**${i + 1}.** [${title}](${m.siteUrl}) ${year} — ${score} | ${chapters}\n> 🏷️ ${m.genres.slice(0, 2).join(', ') || '—'}`;
      }).join('\n\n');
      await interaction.editReply({ embeds: [new EmbedBuilder().setTitle('📡 Manhwas em Lançamento').setDescription(description).setColor(3066993).setFooter({ text: 'Fonte: AniList • Ordenado por popularidade • Status: Em lançamento' })] });
    } catch { await interaction.editReply('❌ Erro ao buscar lançamentos. Tente novamente!'); }
  },
};

// /favorito — favoritos de manhwa e anime
    const ANIME_SOURCES_SET = new Set(['anilist-anime', 'jikan', 'kitsu', 'anidb']);
    const isAnimeFav = source => ANIME_SOURCES_SET.has(source);

    const cmdFavorito = {
    data: new SlashCommandBuilder()
      .setName('favorito')
      .setDescription('Gerencie seus favoritos de manhwa e anime')
      .addSubcommandGroup(group =>
        group.setName('adicionar').setDescription('Adiciona um título aos favoritos')
          .addSubcommand(sub => sub.setName('manhwa').setDescription('Adiciona um manhwa aos favoritos')
            .addStringOption(o => o.setName('titulo').setDescription('Nome do manhwa').setRequired(true).setAutocomplete(true)))
          .addSubcommand(sub => sub.setName('anime').setDescription('Adiciona um anime aos favoritos')
            .addStringOption(o => o.setName('titulo').setDescription('Nome do anime').setRequired(true).setAutocomplete(true)))
      )
      .addSubcommand(sub => sub.setName('listar').setDescription('Mostra todos os seus favoritos (manhwa e anime)'))
      .addSubcommand(sub => sub.setName('remover').setDescription('Remove um título dos seus favoritos')
        .addStringOption(o => o.setName('titulo').setDescription('Nome ou parte do título').setRequired(true))),

    async autocomplete(interaction) {
      const group   = interaction.options.getSubcommandGroup(false);
      const focused = interaction.options.getFocused();
      const mode    = (group === 'adicionar' && interaction.options.getSubcommand() === 'anime') ? 'anime' : 'manhwa';
      await respondAutocomplete(interaction, focused, mode);
    },

    async execute(interaction) {
      const userId = interaction.user.id;
      const group  = interaction.options.getSubcommandGroup(false);
      const sub    = interaction.options.getSubcommand();

      // ── LISTAR ──
      if (sub === 'listar') {
        await interaction.deferReply({ ephemeral: true });
        const lista = await db.select().from(favoritosTable)
          .where(eq(favoritosTable.discordUserId, userId))
          .orderBy(favoritosTable.addedAt);
        if (!lista.length) {
          await interaction.editReply({ content: '📭 Você ainda não tem favoritos! Use `/favorito adicionar manhwa` ou `/favorito adicionar anime`.' });
          return;
        }
        const linhas = lista.map((fav, i) => {
          const icon   = isAnimeFav(fav.source) ? '🎌' : (fav.source === 'anilist' ? '🟣' : '🟠');
          const score  = fav.score  ? `⭐ ${fav.score}`                            : '⭐ N/A';
          const genres = fav.genres ? fav.genres.split(',').slice(0, 2).join(', ') : '—';
          return `**${i + 1}.** ${icon} [${fav.title}](${fav.siteUrl}) — ${score}\n> 🏷️ ${genres}`;
        });
        const manhwaCount = lista.filter(f => !isAnimeFav(f.source)).length;
        const animeCount  = lista.filter(f =>  isAnimeFav(f.source)).length;
        await interaction.editReply({ embeds: [
          new EmbedBuilder()
            .setTitle(`⭐ Favoritos de ${interaction.user.displayName}`)
            .setDescription(linhas.join('\n\n'))
            .setColor(15844367)
            .setFooter({ text: `${manhwaCount} manhwa(s) • ${animeCount} anime(s)` }),
        ]});

      // ── ADICIONAR MANHWA ou ANIME ──
      } else if (group === 'adicionar') {
        const tipo  = sub; // 'manhwa' | 'anime'
        const input = interaction.options.getString('titulo', true);
        await interaction.deferReply({ ephemeral: true });

        // Autocomplete selection: source:id format
        if (input.includes(':')) {
          const [source, ...idParts] = input.split(':');
          const id             = idParts.join(':');
          const validAnime     = ['anilist-anime', 'jikan', 'kitsu', 'anidb'];
          const validManhwa    = ['anilist', 'mangadex', 'comick', 'mangaupdates'];
          const validSources   = tipo === 'anime' ? validAnime : validManhwa;
          if (validSources.includes(source)) {
            const detail = await getUnifiedById(source, id).catch(() => null);
            if (detail) {
              const already = await db.select({ id: favoritosTable.id }).from(favoritosTable)
                .where(and(eq(favoritosTable.discordUserId, userId), eq(favoritosTable.manhwaId, String(detail.id))))
                .limit(1);
              if (already.length) {
                await interaction.editReply({ content: `⚠️ **${detail.mainTitle}** já está nos seus favoritos!` });
                return;
              }
              await db.insert(favoritosTable).values({
                discordUserId: userId, manhwaId: String(detail.id), source: detail.source,
                title: detail.mainTitle, coverUrl: detail.coverUrl ?? null, siteUrl: detail.siteUrl,
                genres: detail.genres?.join(',') ?? null,
                score: detail.score ? (detail.score / 10).toFixed(1) : null,
              });
              const embed = new EmbedBuilder()
                .setTitle(`✅ ${detail.mainTitle}`).setURL(detail.siteUrl)
                .setDescription(`${tipo === 'anime' ? '🎌' : '📖'} Adicionado aos seus favoritos!`)
                .setColor(15844367);
              if (detail.coverUrl) embed.setThumbnail(detail.coverUrl);
              await interaction.editReply({ embeds: [embed] });
              return;
            }
          }
        }

        // Text search fallback
        let results = [];
        try { results = await searchAllSources(input, tipo); }
        catch { await interaction.editReply('❌ Erro ao buscar. Tente novamente.'); return; }
        if (!results.length) {
          await interaction.editReply(`❌ Nenhum ${tipo === 'anime' ? 'anime' : 'manhwa'} encontrado para **${input}**.`);
          return;
        }
        const existingIds = new Set(
          (await db.select({ manhwaId: favoritosTable.manhwaId }).from(favoritosTable)
            .where(eq(favoritosTable.discordUserId, userId))).map(r => r.manhwaId)
        );
        const available = results.filter(r => !existingIds.has(String(r.id)));
        if (!available.length) { await interaction.editReply('⚠️ Todos os resultados já estão nos seus favoritos!'); return; }

        const doInsert = async m => db.insert(favoritosTable).values({
          discordUserId: userId, manhwaId: String(m.id), source: m.source,
          title: m.mainTitle, coverUrl: m.coverUrl ?? null, siteUrl: m.siteUrl,
          genres: m.genres?.join(',') ?? null,
          score: m.score ? (m.score / 10).toFixed(1) : null,
        });

        if (available.length === 1) {
          await doInsert(available[0]);
          await interaction.editReply({ content: `✅ **${available[0].mainTitle}** adicionado aos seus favoritos!` });
          return;
        }
        const options = available.slice(0, 8).map(r => ({
          label:       r.mainTitle.slice(0, 100),
          description: `${SOURCE_ICONS[r.source] ?? '🔵'} ${SOURCE_LABELS[r.source] ?? r.source} • ${r.genres?.slice(0, 2).join(', ') || 'Sem gêneros'}`.slice(0, 100),
          value:       `${r.source}:${r.id}`,
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder().setCustomId('favorito_add_select')
            .setPlaceholder(`Selecione o ${tipo === 'anime' ? 'anime' : 'manhwa'} para adicionar`)
            .addOptions(options)
        );
        await interaction.editReply({
          content: `🔍 Encontrei **${available.length}** resultados para **${input}**. Qual adicionar?`,
          components: [row],
        });
        const collector = interaction.channel?.createMessageComponentCollector({
          componentType: ComponentType.StringSelect,
          filter: i => i.customId === 'favorito_add_select' && i.user.id === interaction.user.id,
          time: 30000, max: 1,
        });
        collector?.on('collect', async sel => {
          await sel.deferUpdate();
          const [selSrc, ...selIdParts] = sel.values[0].split(':');
          const detail = await getUnifiedById(selSrc, selIdParts.join(':')).catch(() => null);
          const chosen = detail ?? available.find(r => String(r.id) === selIdParts.join(':'));
          if (!chosen) { await interaction.editReply({ content: '❌ Erro ao obter detalhes.', components: [] }); return; }
          await doInsert(chosen);
          await interaction.editReply({ content: `✅ **${chosen.mainTitle}** adicionado aos seus favoritos!`, components: [] });
        });
        collector?.on('end', async (_c, reason) => {
          if (reason === 'time') await interaction.editReply({ content: '⏱️ Tempo esgotado.', components: [] });
        });

      // ── REMOVER ──
      } else if (sub === 'remover') {
        const titulo = interaction.options.getString('titulo', true);
        await interaction.deferReply({ ephemeral: true });
        const matches = await db.select().from(favoritosTable)
          .where(and(eq(favoritosTable.discordUserId, userId), sql`lower(${favoritosTable.title}) like lower(${'%' + titulo + '%'})`));
        if (!matches.length) { await interaction.editReply(`❌ Nenhum favorito com **${titulo}** encontrado.`); return; }
        if (matches.length === 1) {
          await db.delete(favoritosTable).where(and(eq(favoritosTable.discordUserId, userId), eq(favoritosTable.id, matches[0].id)));
          await interaction.editReply(`🗑️ **${matches[0].title}** removido dos seus favoritos.`);
          return;
        }
        const options = matches.slice(0, 8).map(f => ({
          label:       f.title.slice(0, 100),
          description: `${isAnimeFav(f.source) ? '🎌 Anime' : '📖 Manhwa'} • ${(f.genres ?? '').split(',').slice(0, 2).join(', ') || 'Sem gêneros'}`.slice(0, 100),
          value:       String(f.id),
        }));
        const row = new ActionRowBuilder().addComponents(
          new StringSelectMenuBuilder().setCustomId('favorito_remove_select').setPlaceholder('Selecione qual remover').addOptions(options)
        );
        await interaction.editReply({ content: `⚠️ Encontrei **${matches.length}** favorito(s) com esse nome. Qual remover?`, components: [row] });
        const collector = interaction.channel?.createMessageComponentCollector({
          componentType: ComponentType.StringSelect,
          filter: i => i.customId === 'favorito_remove_select' && i.user.id === interaction.user.id,
          time: 30000, max: 1,
        });
        collector?.on('collect', async sel => {
          await sel.deferUpdate();
          const favId = parseInt(sel.values[0], 10);
          const fav   = matches.find(f => f.id === favId);
          if (!fav) { await interaction.editReply({ content: '❌ Erro ao remover.', components: [] }); return; }
          await db.delete(favoritosTable).where(and(eq(favoritosTable.discordUserId, userId), eq(favoritosTable.id, favId)));
          await interaction.editReply({ content: `🗑️ **${fav.title}** removido dos seus favoritos.`, components: [] });
        });
        collector?.on('end', async (_c, reason) => {
          if (reason === 'time') await interaction.editReply({ content: '⏱️ Tempo esgotado.', components: [] });
        });
      }
    },
    };

    
// /versus — comparar manhwas
const cmdVersus = {
  data: new SlashCommandBuilder().setName('versus').setDescription('Compara dois manhwas lado a lado para ajudar a decidir qual ler')
    .addStringOption(o => o.setName('manhwa1').setDescription('Primeiro manhwa').setRequired(true).setAutocomplete(true))
    .addStringOption(o => o.setName('manhwa2').setDescription('Segundo manhwa').setRequired(true).setAutocomplete(true)),
  async autocomplete(interaction) {
    const focused = interaction.options.getFocused(true);
    await respondAutocomplete(interaction, focused.value, 'manhwa');
  },
  async execute(interaction) {
    const q1 = interaction.options.getString('manhwa1', true);
    const q2 = interaction.options.getString('manhwa2', true);
    await interaction.deferReply();
    await interaction.editReply({ content: '⏳ Buscando os dois manhwas...' });
    const [r1, r2] = await Promise.all([
      searchAllSources(q1).catch(() => []),
      searchAllSources(q2).catch(() => []),
    ]);
    if (!r1.length) { await interaction.editReply(`❌ Nenhum resultado para **${q1}**.`); return; }
    if (!r2.length) { await interaction.editReply(`❌ Nenhum resultado para **${q2}**.`); return; }
    const pickOne = async (results, query, customId, slot) => {
      if (results.length === 1) return results[0];
      const options = results.slice(0, 8).map(r => ({
        label: r.mainTitle.slice(0, 100),
        description: `${SOURCE_ICONS[r.source] ?? '🔵'} ${SOURCE_LABELS[r.source] ?? r.source} • ${r.genres.slice(0, 2).join(', ') || 'Sem gêneros'}`.slice(0, 100),
        value: `${r.source}:${r.id}`,
      }));
      const row = new ActionRowBuilder().addComponents(new StringSelectMenuBuilder().setCustomId(customId).setPlaceholder(`Selecione o manhwa ${slot} correto`).addOptions(options));
      await interaction.editReply({ content: `🔍 Vários resultados para o **manhwa ${slot}** ("${query}"). Selecione o correto:`, components: [row], embeds: [] });
      return new Promise(resolve => {
        const collector = interaction.channel?.createMessageComponentCollector({
          componentType: ComponentType.StringSelect, filter: i => i.customId === customId && i.user.id === interaction.user.id, time: 30000, max: 1,
        });
        collector?.on('collect', async sel => {
          await sel.deferUpdate();
          const ci = sel.values[0].indexOf(':'); const src = sel.values[0].slice(0, ci), id = sel.values[0].slice(ci + 1);
          resolve(results.find(r => r.source === src && r.id === id) ?? null);
        });
        collector?.on('end', (_c, reason) => { if (reason === 'time') resolve(null); });
      });
    };
    let a = await pickOne(r1, q1, 'compare_select_1', 1);
    if (!a) { await interaction.editReply({ content: '⏱️ Tempo esgotado ou erro. Tente novamente.', components: [] }); return; }
    let b = await pickOne(r2, q2, 'compare_select_2', 2);
    if (!b) { await interaction.editReply({ content: '⏱️ Tempo esgotado ou erro. Tente novamente.', components: [] }); return; }
    const scoreBar = s => { if (!s) return '▱▱▱▱▱▱▱▱▱▱ N/A'; const f = Math.round(s/10); return '▰'.repeat(f) + '▱'.repeat(10-f) + ` ${(s/10).toFixed(1)}`; };
    const winner = (a, b) => { if (a===null&&b===null) return ['',''];  if (a===null) return ['','🏆']; if (b===null) return ['🏆','']; if (a>b) return ['🏆','']; if (b>a) return ['','🏆']; return ['🤝','🤝']; };
    const [ws1,ws2] = winner(a.score, b.score);
    const [wc1,wc2] = winner(a.chapters, b.chapters);
    const embed = new EmbedBuilder().setTitle('⚔️ Comparação de Manhwas').setColor(8087790)
      .setDescription(`Comparando **[${a.mainTitle}](${a.siteUrl})** vs **[${b.mainTitle}](${b.siteUrl})**\n\n> 🏆 = melhor nesse critério | 🤝 = empatados`)
      .addFields(
        { name: `1️⃣ ${a.mainTitle}`, value: `**Avaliação:** ${scoreBar(a.score)} ${ws1}\n**Capítulos:** ${a.chapters ? `📖 ${a.chapters} caps` : '📖 Desconhecido'} ${wc1}\n**Status:** 📌 ${statusLabel(a.status)}\n**Gêneros:** ${a.genres.slice(0,4).join(' • ') || '—'}\n**Ano:** ${a.year ?? '—'}\n**Fonte:** ${SOURCE_LABELS[a.source] ?? a.source}`, inline: true },
        { name: `2️⃣ ${b.mainTitle}`, value: `**Avaliação:** ${scoreBar(b.score)} ${ws2}\n**Capítulos:** ${b.chapters ? `📖 ${b.chapters} caps` : '📖 Desconhecido'} ${wc2}\n**Status:** 📌 ${statusLabel(b.status)}\n**Gêneros:** ${b.genres.slice(0,4).join(' • ') || '—'}\n**Ano:** ${b.year ?? '—'}\n**Fonte:** ${SOURCE_LABELS[b.source] ?? b.source}`, inline: true },
      );
    const shared = a.genres.filter(g => b.genres.includes(g));
    if (shared.length) embed.addFields({ name: '🔗 Gêneros em comum', value: shared.join(' • '), inline: false });
    if (a.coverUrl) embed.setThumbnail(a.coverUrl);
    embed.setFooter({ text: 'Dados via AniList, MangaDex e MAL' });
    await interaction.editReply({ content: null, embeds: [embed], components: [] });
  },
};

// /criador — autor/artista
const cmdCriador = {
  data: new SlashCommandBuilder().setName('criador').setDescription('Busca todos os manhwas de um autor ou artista')
    .addStringOption(o => o.setName('nome').setDescription('Nome do autor ou artista').setRequired(true)),
  async execute(interaction) {
    const nome = interaction.options.getString('nome', true);
    await interaction.deferReply();
    await interaction.editReply({ content: `🔍 Buscando autor **${nome}**...` });
    const SEARCH_STAFF = `query SearchStaff($search: String!) { Page(page: 1, perPage: 6) { staff(search: $search) { id name { full native } image { medium } description siteUrl } } }`;
    const STAFF_WORKS = `query StaffWorks($id: Int!, $page: Int!) { Staff(id: $id) { id name { full native } image { large } description siteUrl staffMedia(type: MANGA, page: $page, perPage: 25, sort: START_DATE_DESC) { pageInfo { hasNextPage } edges { staffRole node { id title { romaji english } countryOfOrigin averageScore genres chapters status siteUrl startDate { year } coverImage { color } } } } } }`;
    const anilistFetch = async (q, v) => {
      const res = await fetch(ANILIST_API, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ query: q, variables: v }), signal: AbortSignal.timeout(10000) });
      if (!res.ok) throw new Error(`AniList ${res.status}`);
      const json = await res.json();
      if (json.errors?.length) throw new Error(json.errors[0].message);
      return json.data;
    };
    let staffList;
    try { const d = await anilistFetch(SEARCH_STAFF, { search: nome }); staffList = d.Page.staff ?? []; }
    catch { await interaction.editReply('❌ Erro ao buscar o autor. Tente novamente.'); return; }
    if (!staffList.length) { await interaction.editReply(`❌ Nenhum autor encontrado com o nome **${nome}**.`); return; }
    let chosenStaff;
    if (staffList.length === 1) { chosenStaff = staffList[0]; }
    else {
      const options = staffList.slice(0, 6).map(s => ({ label: s.name.full.slice(0, 100), description: (s.name.native ?? 'Nome nativo desconhecido').slice(0, 100), value: String(s.id) }));
      const row = new ActionRowBuilder().addComponents(new StringSelectMenuBuilder().setCustomId('autor_select').setPlaceholder('Selecione o autor correto').addOptions(options));
      await interaction.editReply({ content: `👥 Encontrei **${staffList.length}** autores com esse nome. Selecione o correto:`, components: [row] });
      const selected = await new Promise(resolve => {
        const collector = interaction.channel?.createMessageComponentCollector({
          componentType: ComponentType.StringSelect, filter: i => i.customId === 'autor_select' && i.user.id === interaction.user.id, time: 30000, max: 1,
        });
        collector?.on('collect', async sel => { await sel.deferUpdate(); resolve(staffList.find(s => String(s.id) === sel.values[0]) ?? null); });
        collector?.on('end', (_c, reason) => { if (reason === 'time') resolve(null); });
      });
      if (!selected) { await interaction.editReply({ content: '⏱️ Tempo esgotado. Use `/criador` novamente.', components: [] }); return; }
      chosenStaff = selected;
    }
    await interaction.editReply({ content: `⏳ Buscando obras de **${chosenStaff.name.full}**...`, components: [] });
    let staffFull;
    try { const d = await anilistFetch(STAFF_WORKS, { id: chosenStaff.id, page: 1 }); staffFull = d.Staff; }
    catch { await interaction.editReply('❌ Erro ao buscar as obras do autor. Tente novamente.'); return; }
    const edges = staffFull.staffMedia.edges ?? [];
    const manhwas = edges.filter(e => e.node.countryOfOrigin === 'KR' || !e.node.countryOfOrigin).slice(0, 12);
    const allWorks = edges.slice(0, 12);
    const worksToShow = manhwas.length >= 3 ? manhwas : allWorks;
    if (!worksToShow.length) { await interaction.editReply(`❌ **${staffFull.name.full}** não tem obras listadas no AniList.`); return; }
    const cleanD = (raw, max=200) => !raw ? '' : raw.replace(/<br\s*\/?>/gi,' ').replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&quot;/g,'"').replace(/&#039;/g,"'").trim().slice(0, max);
    const authorDesc = cleanD(staffFull.description, 180);
    const nativeName = staffFull.name.native ? ` (${staffFull.name.native})` : '';
    const workLines = worksToShow.map(e => {
      const title = e.node.title.english ?? e.node.title.romaji;
      return `**[${title}](${e.node.siteUrl})** ${e.node.startDate?.year ? `(${e.node.startDate.year})` : ''} ${e.staffRole ? `*${e.staffRole}*` : ''}\n> ${e.node.averageScore ? `⭐ ${(e.node.averageScore / 10).toFixed(1)}` : '⭐ N/A'} | ${e.node.chapters ? `📖 ${e.node.chapters} caps` : '📖 Em andamento'} | ${statusLabel(e.node.status)}\n> 🏷️ ${e.node.genres.slice(0, 2).join(', ') || '—'}\n> 🔎 ${buildScanLinksExternal(title)}`;
    });
    const hasMore = staffFull.staffMedia.pageInfo.hasNextPage && worksToShow.length >= 12;
    const embed = new EmbedBuilder().setTitle(`✍️ ${staffFull.name.full}${nativeName}`).setURL(staffFull.siteUrl).setColor(15105570)
      .setDescription((authorDesc ? `*${authorDesc}...*\n\n` : '') + workLines.join('\n\n') + (hasMore ? '\n\n*...e mais obras no AniList*' : ''))
      .setFooter({ text: `${worksToShow.length} obra(s) listada(s) • Fonte: AniList` });
    if (staffFull.image.large) embed.setThumbnail(staffFull.image.large);
    await interaction.editReply({ content: null, embeds: [embed], components: [] });
  },
};

// /alertas — notificações
const cmdAlertas = {
  data: new SlashCommandBuilder().setName('alertas').setDescription('Configura notificações de novos capítulos dos seus favoritos')
    .addSubcommand(sub => sub.setName('canal').setDescription('Define o canal onde o bot avisará sobre novos capítulos').addChannelOption(o => o.setName('canal').setDescription('Canal de texto').addChannelTypes(ChannelType.GuildText).setRequired(true)))
    .addSubcommand(sub => sub.setName('status').setDescription('Mostra o canal de notificações configurado'))
    .addSubcommand(sub => sub.setName('desativar').setDescription('Desativa as notificações de novos capítulos'))
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageChannels),
  async execute(interaction) {
    const sub = interaction.options.getSubcommand();
    if (sub === 'canal') {
      if (!interaction.guildId) { await interaction.reply({ content: '❌ Este comando só pode ser usado em servidores.', ephemeral: true }); return; }
      const canal = interaction.options.getChannel('canal', true);
      await interaction.deferReply({ ephemeral: true });
      await db.insert(notificacaoCanaisTable).values({ guildId: interaction.guildId, channelId: canal.id }).onConflictDoUpdate({
        target: notificacaoCanaisTable.guildId, set: { channelId: canal.id, configuredAt: new Date() },
      });
      await interaction.editReply({ embeds: [new EmbedBuilder().setTitle('🔔 Notificações Configuradas!').setColor(3066993).setDescription(`O bot vai avisar em ${canal} sempre que um manhwa da lista de favoritos de alguém tiver **novos capítulos**.\n\n> ✅ A verificação acontece automaticamente a cada **2 horas**.\n> ✅ Somente manhwas marcados como favoritos com status "Em lançamento" são monitorados.\n> ✅ Cada manhwa só gera uma notificação por atualização de capítulo.`).setFooter({ text: 'Use /alertas desativar para parar as notificações' })] });
    } else if (sub === 'status') {
      if (!interaction.guildId) { await interaction.reply({ content: '❌ Este comando só pode ser usado em servidores.', ephemeral: true }); return; }
      await interaction.deferReply({ ephemeral: true });
      const [config] = await db.select().from(notificacaoCanaisTable).where(eq(notificacaoCanaisTable.guildId, interaction.guildId));
      if (!config) { await interaction.editReply({ content: '📭 Nenhum canal de notificações configurado.\nUse `/alertas canal #canal` para configurar.' }); return; }
      await interaction.editReply({ embeds: [new EmbedBuilder().setTitle('🔔 Status das Notificações').setColor(3447003).addFields({ name: 'Canal configurado', value: `<#${config.channelId}>`, inline: true }, { name: 'Configurado em', value: config.configuredAt.toLocaleDateString('pt-BR'), inline: true }, { name: 'Frequência de verificação', value: 'A cada 2 horas', inline: false }).setFooter({ text: 'Use /alertas desativar para parar as notificações' })] });
    } else {
      if (!interaction.guildId) { await interaction.reply({ content: '❌ Este comando só pode ser usado em servidores.', ephemeral: true }); return; }
      await interaction.deferReply({ ephemeral: true });
      const deleted = await db.delete(notificacaoCanaisTable).where(eq(notificacaoCanaisTable.guildId, interaction.guildId)).returning();
      if (!deleted.length) { await interaction.editReply({ content: '📭 Não havia notificações configuradas neste servidor.' }); return; }
      await interaction.editReply({ content: '🔕 Notificações desativadas com sucesso.' });
    }
  },
};

// /leituras — lista de leitura
const CORES = { lendo: 3066993, concluido: 3447003, planejo: 10181046, pausado: 15965202, abandonado: 15158332 };
const EMOJIS = { lendo: '📖', concluido: '✅', planejo: '🔖', pausado: '⏸️', abandonado: '🗑️' };
const cmdLeituras = {
  data: new SlashCommandBuilder().setName('leituras').setDescription('Sua lista de leitura pessoal com status por manhwa')
    .addSubcommand(s => s.setName('adicionar').setDescription('Adiciona um manhwa à sua lista').addStringOption(o => o.setName('titulo').setDescription('Nome do manhwa').setRequired(true)).addStringOption(o => o.setName('status').setDescription('Status de leitura').setRequired(true).addChoices({ name: '📖 Lendo', value: 'lendo' }, { name: '✅ Concluído', value: 'concluido' }, { name: '🔖 Planejo Ler', value: 'planejo' }, { name: '⏸️ Pausado', value: 'pausado' }, { name: '🗑️ Abandonado', value: 'abandonado' })))
    .addSubcommand(s => s.setName('ver').setDescription('Exibe sua lista de leitura').addStringOption(o => o.setName('status').setDescription('Filtrar por status (padrão: todos)').setRequired(false).addChoices({ name: '📖 Lendo', value: 'lendo' }, { name: '✅ Concluído', value: 'concluido' }, { name: '🔖 Planejo Ler', value: 'planejo' }, { name: '⏸️ Pausado', value: 'pausado' }, { name: '🗑️ Abandonado', value: 'abandonado' })))
    .addSubcommand(s => s.setName('mover').setDescription('Muda o status de um manhwa na sua lista').addStringOption(o => o.setName('titulo').setDescription('Nome do manhwa').setRequired(true)).addStringOption(o => o.setName('status').setDescription('Novo status').setRequired(true).addChoices({ name: '📖 Lendo', value: 'lendo' }, { name: '✅ Concluído', value: 'concluido' }, { name: '🔖 Planejo Ler', value: 'planejo' }, { name: '⏸️ Pausado', value: 'pausado' }, { name: '🗑️ Abandonado', value: 'abandonado' })))
    .addSubcommand(s => s.setName('remover').setDescription('Remove um manhwa da sua lista').addStringOption(o => o.setName('titulo').setDescription('Nome do manhwa').setRequired(true))),
  async execute(interaction) {
    const sub = interaction.options.getSubcommand();
    if (sub === 'adicionar') {
      const titulo = interaction.options.getString('titulo', true);
      const status = interaction.options.getString('status', true);
      await interaction.deferReply({ ephemeral: true });
      const results = await searchAllSources(titulo).catch(() => []);
      if (!results.length) { await interaction.editReply(`❌ Nenhum manhwa encontrado para **${titulo}**.`); return; }
      const notInList = [];
      for (const r of results.slice(0, 10)) {
        const [existing] = await db.select().from(listaLeituraTable).where(and(eq(listaLeituraTable.discordUserId, interaction.user.id), eq(listaLeituraTable.manhwaId, r.id)));
        if (!existing) notInList.push(r);
      }
      if (!notInList.length) { await interaction.editReply('⚠️ Todos os resultados já estão na sua lista!'); return; }
      const doInsert = async m => db.insert(listaLeituraTable).values({ discordUserId: interaction.user.id, manhwaId: m.id, source: m.source, title: m.mainTitle, coverUrl: m.coverUrl ?? null, siteUrl: m.siteUrl, genres: m.genres.join(', '), score: m.score?.toString() ?? null, status });
      if (notInList.length === 1) {
        await doInsert(notInList[0]);
        await interaction.editReply(`${EMOJIS[status]} **${notInList[0].mainTitle}** adicionado à lista como **${STATUS_LABELS[status]}**!`); return;
      }
      const options = notInList.slice(0, 10).map(r => ({ label: r.mainTitle.slice(0, 100), description: (r.genres.slice(0, 3).join(', ') || 'Sem gênero').slice(0, 100), value: `${r.source}:${r.id}` }));
      const row = new ActionRowBuilder().addComponents(new StringSelectMenuBuilder().setCustomId('lista_add_select').setPlaceholder('Selecione o manhwa correto').addOptions(options));
      await interaction.editReply({ content: `📚 Encontrei **${notInList.length}** resultados. Qual deseja adicionar?`, components: [row] });
      const collector = interaction.channel?.createMessageComponentCollector({ componentType: ComponentType.StringSelect, filter: i => i.customId === 'lista_add_select' && i.user.id === interaction.user.id, time: 30000, max: 1 });
      collector?.on('collect', async sel => {
        await sel.deferUpdate();
        const ci = sel.values[0].indexOf(':'); const src = sel.values[0].slice(0,ci), id = sel.values[0].slice(ci+1);
        const chosen = notInList.find(r => r.source === src && r.id === id);
        if (!chosen) { await interaction.editReply({ content: '❌ Erro ao selecionar.', components: [] }); return; }
        await doInsert(chosen);
        await interaction.editReply({ content: `${EMOJIS[status]} **${chosen.mainTitle}** adicionado à lista como **${STATUS_LABELS[status]}**!`, components: [] });
      });
      collector?.on('end', async (_c, reason) => { if (reason === 'time') await interaction.editReply({ content: '⏱️ Tempo esgotado.', components: [] }); });
    } else if (sub === 'ver') {
      const statusFiltro = interaction.options.getString('status');
      await interaction.deferReply({ ephemeral: true });
      const rows = await db.select().from(listaLeituraTable).where(
        statusFiltro ? and(eq(listaLeituraTable.discordUserId, interaction.user.id), eq(listaLeituraTable.status, statusFiltro)) : eq(listaLeituraTable.discordUserId, interaction.user.id)
      );
      if (!rows.length) { await interaction.editReply(statusFiltro ? `📭 Você não tem nenhum manhwa com status **${STATUS_LABELS[statusFiltro]}**.` : '📭 Sua lista está vazia. Use `/leituras adicionar` para começar!'); return; }
      const grouped = {};
      for (const r of rows) { if (!grouped[r.status]) grouped[r.status] = []; grouped[r.status].push(r); }
      const statusOrder = ['lendo', 'concluido', 'planejo', 'pausado', 'abandonado'];
      const fields = statusOrder.filter(s => grouped[s]?.length).map(s => {
        const items = grouped[s].slice(0, 15);
        const extra = grouped[s].length > 15 ? `\n> *...e mais ${grouped[s].length - 15}*` : '';
        return { name: `${STATUS_LABELS[s]} — ${grouped[s].length}`, value: items.map(r => `> [${r.title}](${r.siteUrl})`).join('\n') + extra, inline: false };
      });
      const cor = statusFiltro ? CORES[statusFiltro] : 8087790;
      await interaction.editReply({ embeds: [new EmbedBuilder().setTitle(`📚 Lista de Leitura de ${interaction.user.displayName}`).setColor(cor).addFields(fields).setFooter({ text: `${rows.length} manhwa(s) na lista` })] });
    } else if (sub === 'mover') {
      const titulo = interaction.options.getString('titulo', true);
      const novoStatus = interaction.options.getString('status', true);
      await interaction.deferReply({ ephemeral: true });
      const todos = await db.select().from(listaLeituraTable).where(eq(listaLeituraTable.discordUserId, interaction.user.id));
      const matches = todos.filter(r => r.title.toLowerCase().includes(titulo.toLowerCase()));
      if (!matches.length) { await interaction.editReply('❌ Nenhum manhwa com esse nome na sua lista.'); return; }
      if (matches.length === 1) {
        await db.update(listaLeituraTable).set({ status: novoStatus }).where(eq(listaLeituraTable.id, matches[0].id));
        await interaction.editReply(`✅ **${matches[0].title}** movido para **${STATUS_LABELS[novoStatus]}**!`); return;
      }
      const options = matches.slice(0, 10).map(r => ({ label: r.title.slice(0, 100), description: STATUS_LABELS[r.status] ?? r.status, value: String(r.id) }));
      const row = new ActionRowBuilder().addComponents(new StringSelectMenuBuilder().setCustomId('lista_mover_select').setPlaceholder('Selecione o manhwa').addOptions(options));
      await interaction.editReply({ content: '📋 Vários resultados encontrados. Qual deseja mover?', components: [row] });
      const collector = interaction.channel?.createMessageComponentCollector({ componentType: ComponentType.StringSelect, filter: i => i.customId === 'lista_mover_select' && i.user.id === interaction.user.id, time: 30000, max: 1 });
      collector?.on('collect', async sel => {
        await sel.deferUpdate();
        const chosen = matches.find(r => String(r.id) === sel.values[0]);
        if (!chosen) { await interaction.editReply({ content: '❌ Erro.', components: [] }); return; }
        await db.update(listaLeituraTable).set({ status: novoStatus }).where(eq(listaLeituraTable.id, chosen.id));
        await interaction.editReply({ content: `✅ **${chosen.title}** movido para **${STATUS_LABELS[novoStatus]}**!`, components: [] });
      });
      collector?.on('end', async (_c, reason) => { if (reason === 'time') await interaction.editReply({ content: '⏱️ Tempo esgotado.', components: [] }); });
    } else {
      const titulo = interaction.options.getString('titulo', true);
      await interaction.deferReply({ ephemeral: true });
      const todos = await db.select().from(listaLeituraTable).where(eq(listaLeituraTable.discordUserId, interaction.user.id));
      const matches = todos.filter(r => r.title.toLowerCase().includes(titulo.toLowerCase()));
      if (!matches.length) { await interaction.editReply('❌ Nenhum manhwa com esse nome na sua lista.'); return; }
      if (matches.length === 1) {
        await db.delete(listaLeituraTable).where(eq(listaLeituraTable.id, matches[0].id));
        await interaction.editReply(`🗑️ **${matches[0].title}** removido da sua lista.`); return;
      }
      const options = matches.slice(0, 10).map(r => ({ label: r.title.slice(0, 100), description: STATUS_LABELS[r.status] ?? r.status, value: String(r.id) }));
      const row = new ActionRowBuilder().addComponents(new StringSelectMenuBuilder().setCustomId('lista_rem_select').setPlaceholder('Selecione o manhwa a remover').addOptions(options));
      await interaction.editReply({ content: '📋 Qual deseja remover?', components: [row] });
      const collector = interaction.channel?.createMessageComponentCollector({ componentType: ComponentType.StringSelect, filter: i => i.customId === 'lista_rem_select' && i.user.id === interaction.user.id, time: 30000, max: 1 });
      collector?.on('collect', async sel => {
        await sel.deferUpdate();
        const chosen = matches.find(r => String(r.id) === sel.values[0]);
        if (!chosen) { await interaction.editReply({ content: '❌ Erro.', components: [] }); return; }
        await db.delete(listaLeituraTable).where(eq(listaLeituraTable.id, chosen.id));
        await interaction.editReply({ content: `🗑️ **${chosen.title}** removido da sua lista.`, components: [] });
      });
      collector?.on('end', async (_c, reason) => { if (reason === 'time') await interaction.editReply({ content: '⏱️ Tempo esgotado.', components: [] }); });
    }
  },
};

// /populares — ranking
const cmdPopulares = {
  data: new SlashCommandBuilder().setName('populares').setDescription('Top 10 manhwas mais favoritados pelos membros deste servidor'),
  async execute(interaction) {
    await interaction.deferReply();
    const rows = await db.select({
      manhwaId: favoritosTable.manhwaId, title: favoritosTable.title, siteUrl: favoritosTable.siteUrl,
      coverUrl: favoritosTable.coverUrl, genres: favoritosTable.genres, score: favoritosTable.score,
      total: sql`cast(count(*) as int)`,
    }).from(favoritosTable).groupBy(favoritosTable.manhwaId, favoritosTable.title, favoritosTable.siteUrl, favoritosTable.coverUrl, favoritosTable.genres, favoritosTable.score).orderBy(sql`count(*) desc`).limit(10);
    if (!rows.length) { await interaction.editReply('📭 Ainda ninguém adicionou favoritos. Use `/curtidas adicionar` para começar!'); return; }
    const medals = ['🥇', '🥈', '🥉'];
    const lines = rows.map((r, i) => {
      const medal = medals[i] ?? `**${i + 1}.**`;
      const score = r.score ? `⭐ ${parseFloat(r.score).toFixed(1)}` : '';
      const genres = r.genres?.split(',').slice(0, 2).join(', ') || '';
      return `${medal} **[${r.title}](${r.siteUrl})** — 🤍 ${r.total} favorito(s)\n> ${[score, genres].filter(Boolean).join(' | ')}\n> 🔎 ${buildScanLinksExternal(r.title)}`;
    });
    const embed = new EmbedBuilder().setTitle('🏆 Ranking — Manhwas Mais Favoritados').setDescription(lines.join('\n\n')).setColor(15844367).setFooter({ text: `Top ${rows.length} • Baseado nos favoritos de todos os usuários` });
    if (rows[0].coverUrl) embed.setThumbnail(rows[0].coverUrl);
    await interaction.editReply({ embeds: [embed] });
  },
};

// /estatisticas — perfil do usuário
const cmdEstatisticas = {
  data: new SlashCommandBuilder().setName('estatisticas').setDescription('Exibe suas estatísticas de leitura no bot')
    .addUserOption(o => o.setName('usuario').setDescription('Ver perfil de outro usuário (opcional)').setRequired(false)),
  async execute(interaction) {
    await interaction.deferReply();
    const alvo = interaction.options.getUser('usuario') ?? interaction.user;
    const userId = alvo.id;
    const [favoritos, lista] = await Promise.all([
      db.select().from(favoritosTable).where(eq(favoritosTable.discordUserId, userId)),
      db.select().from(listaLeituraTable).where(eq(listaLeituraTable.discordUserId, userId)),
    ]);
    if (!favoritos.length && !lista.length) { await interaction.editReply(`📭 **${alvo.displayName}** ainda não tem favoritos nem lista de leitura.`); return; }
    const genreCount = {};
    for (const f of [...favoritos, ...lista]) { for (const g of (f.genres?.split(',').map(g => g.trim()).filter(Boolean) ?? [])) genreCount[g] = (genreCount[g] ?? 0) + 1; }
    const topGenres = Object.entries(genreCount).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([g]) => g);
    const scores = [...favoritos, ...lista].map(f => parseFloat(f.score ?? '')).filter(s => !isNaN(s));
    const avgScore = scores.length > 0 ? (scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(1) : null;
    const statusCount = {};
    for (const l of lista) statusCount[l.status] = (statusCount[l.status] ?? 0) + 1;
    const statusOrder = ['lendo', 'concluido', 'planejo', 'pausado', 'abandonado'];
    const listaResumo = statusOrder.filter(s => statusCount[s]).map(s => `${STATUS_LABELS[s]}: **${statusCount[s]}**`).join('\n');
    const totalUnicos = new Set([...favoritos.map(f => f.manhwaId), ...lista.map(l => l.manhwaId)]).size;
    const embed = new EmbedBuilder().setTitle(`📊 Perfil de ${alvo.displayName}`).setThumbnail(alvo.displayAvatarURL()).setColor(10181046)
      .addFields({ name: '📚 Total de títulos', value: `**${totalUnicos}** manhwa(s) únicos`, inline: true }, { name: '🤍 Favoritos', value: `**${favoritos.length}**`, inline: true }, { name: '⭐ Nota média', value: avgScore ? `**${avgScore}** / 10` : 'N/A', inline: true });
    if (listaResumo) embed.addFields({ name: '📋 Lista de leitura', value: listaResumo, inline: false });
    if (topGenres.length) embed.addFields({ name: '🏷️ Gêneros favoritos', value: topGenres.join(' • '), inline: false });
    embed.setFooter({ text: 'Use /leituras e /curtidas para gerenciar sua coleção' });
    await interaction.editReply({ embeds: [embed] });
  },
};

// /parecidos — similares
const cmdParecidos = {
  data: new SlashCommandBuilder().setName('parecidos').setDescription('Encontra manhwas parecidos com um que você gosta')
    .addStringOption(o => o.setName('titulo').setDescription('Nome do manhwa de referência').setRequired(true).setAutocomplete(true)),
  async autocomplete(interaction) { await respondAutocomplete(interaction, interaction.options.getFocused(), 'manhwa'); },
  async execute(interaction) {
    const titulo = interaction.options.getString('titulo', true);
    await interaction.deferReply();
    await interaction.editReply({ content: `🔍 Buscando **${titulo}**...` });
    const results = await searchAllSources(titulo).catch(() => []);
    const anilistResults = results.filter(r => r.source === 'anilist');
    if (!anilistResults.length) { await interaction.editReply(`❌ Nenhum resultado encontrado para **${titulo}** no AniList.`); return; }
    let chosenId, chosenTitle, chosenGenres;
    if (anilistResults.length === 1) { chosenId = parseInt(anilistResults[0].id, 10); chosenTitle = anilistResults[0].mainTitle; chosenGenres = anilistResults[0].genres; }
    else {
      const options = anilistResults.slice(0, 8).map(r => ({ label: r.mainTitle.slice(0, 100), description: (r.genres.slice(0, 3).join(', ') || 'Sem gênero').slice(0, 100), value: `${r.id}|${r.mainTitle}|${r.genres.join(',')}` }));
      const row = new ActionRowBuilder().addComponents(new StringSelectMenuBuilder().setCustomId('similar_select').setPlaceholder('Selecione o manhwa de referência').addOptions(options));
      await interaction.editReply({ content: `📋 Encontrei **${anilistResults.length}** resultados. Qual é o de referência?`, components: [row] });
      const chosen = await new Promise(resolve => {
        const collector = interaction.channel?.createMessageComponentCollector({ componentType: ComponentType.StringSelect, filter: i => i.customId === 'similar_select' && i.user.id === interaction.user.id, time: 30000, max: 1 });
        collector?.on('collect', async sel => { await sel.deferUpdate(); const [id, title, genresStr] = sel.values[0].split('|'); resolve({ id: parseInt(id, 10), title, genres: genresStr.split(',').filter(Boolean) }); });
        collector?.on('end', (_c, reason) => { if (reason === 'time') resolve(null); });
      });
      if (!chosen) { await interaction.editReply({ content: '⏱️ Tempo esgotado.', components: [] }); return; }
      chosenId = chosen.id; chosenTitle = chosen.title; chosenGenres = chosen.genres;
    }
    await interaction.editReply({ content: `⏳ Buscando similares a **${chosenTitle}**...`, components: [] });
    const anilistFetch = async (q, v) => { const res = await fetch(ANILIST_API, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ query: q, variables: v }), signal: AbortSignal.timeout(10000) }); if (!res.ok) throw new Error(); const json = await res.json(); if (json.errors?.length) throw new Error(); return json.data; };
    let similarList = [];
    try { const d = await anilistFetch(`query Similar($id:Int!){Media(id:$id,type:MANGA){recommendations(sort:RATING_DESC,perPage:8){nodes{mediaRecommendation{id title{romaji english}averageScore genres chapters status siteUrl countryOfOrigin coverImage{large color}}}}}}`, { id: chosenId }); similarList = d.Media.recommendations.nodes.map(n => n.mediaRecommendation).filter(Boolean); } catch {}
    if (similarList.length < 3 && chosenGenres.length > 0) {
      try { const gd = await anilistFetch(`query SimilarByGenre($genres:[String],$notId:Int!){Page(page:1,perPage:8){media(type:MANGA,countryOfOrigin:KR,genre_in:$genres,sort:SCORE_DESC,id_not:$notId,averageScore_greater:65){id title{romaji english}averageScore genres chapters status siteUrl coverImage{large color}}}}`, { genres: chosenGenres.slice(0, 3), notId: chosenId }); similarList = [...similarList, ...gd.Page.media.filter(m => !similarList.some(s => s.id === m.id))].slice(0, 8); } catch {}
    }
    if (!similarList.length) { await interaction.editReply(`❌ Não encontrei similares para **${chosenTitle}** no AniList.`); return; }
    const lines = similarList.slice(0, 8).map(m => {
      const title = m.title.english ?? m.title.romaji;
      return `**[${title}](${m.siteUrl})**\n> ${m.averageScore ? `⭐ ${(m.averageScore/10).toFixed(1)}` : '⭐ N/A'} | ${m.chapters ? `📖 ${m.chapters} caps` : '📖 Em andamento'} | ${statusLabel(m.status)}\n> 🏷️ ${m.genres.slice(0, 3).join(', ') || '—'}\n> 🔎 ${buildScanLinksExternal(title)}`;
    });
    const embed = new EmbedBuilder().setTitle(`🎯 Similares a: ${chosenTitle}`).setDescription(lines.join('\n\n')).setColor(1752220).setFooter({ text: `${similarList.length} recomendações • Fonte: AniList` });
    if (similarList[0]?.coverImage?.large) embed.setThumbnail(similarList[0].coverImage.large);
    await interaction.editReply({ content: null, embeds: [embed], components: [] });
  },
};

// /filtrar — busca avançada
const GENEROS_FILTRAR = ['Action','Adventure','Comedy','Drama','Fantasy','Horror','Mystery','Psychological','Romance','Sci-Fi','Slice of Life','Supernatural','Thriller','Sports','Mecha','Reincarnation','Survival','School Life','Video Games','Zombies'];
const GENEROS_PT_FILTRAR = { Action:'Ação', Adventure:'Aventura', Comedy:'Comédia', Drama:'Drama', Fantasy:'Fantasia', Horror:'Horror', Mystery:'Mistério', Psychological:'Psicológico', Romance:'Romance', 'Sci-Fi':'Ficção Científica', 'Slice of Life':'Slice of Life', Supernatural:'Sobrenatural', Thriller:'Thriller', Sports:'Esportes', Mecha:'Mecha', Reincarnation:'Reencarnação', Survival:'Survival', 'School Life':'Escola', 'Video Games':'Game', Zombies:'Zumbi' };
const COUNTRY_LABEL = { KR: '🇰🇷 Manhwa', CN: '🇨🇳 Manhua', JP: '🇯🇵 Manga' };
const cmdFiltrar = {
  data: new SlashCommandBuilder().setName('filtrar').setDescription('Busca manhwas com filtros avançados combinados')
    .addStringOption(o => o.setName('genero').setDescription('Filtrar por gênero').setRequired(false).addChoices(...GENEROS_FILTRAR.map(g => ({ name: GENEROS_PT_FILTRAR[g] ?? g, value: g }))))
    .addStringOption(o => o.setName('status').setDescription('Filtrar por status').setRequired(false).addChoices({ name: '📡 Em lançamento', value: 'RELEASING' }, { name: '✅ Finalizado', value: 'FINISHED' }, { name: '⏸️ Pausado / Hiato', value: 'HIATUS' }, { name: '❌ Cancelado', value: 'CANCELLED' }))
    .addIntegerOption(o => o.setName('ano_min').setDescription('Ano mínimo de início').setRequired(false).setMinValue(1990).setMaxValue(2030))
    .addIntegerOption(o => o.setName('ano_max').setDescription('Ano máximo de início').setRequired(false).setMinValue(1990).setMaxValue(2030))
    .addIntegerOption(o => o.setName('nota_min').setDescription('Nota mínima de 1 a 10').setRequired(false).setMinValue(1).setMaxValue(10))
    .addStringOption(o => o.setName('tipo').setDescription('Tipo de origem (padrão: Manhwa)').setRequired(false).addChoices({ name: '🇰🇷 Manhwa (Coreano)', value: 'KR' }, { name: '🇨🇳 Manhua (Chinês)', value: 'CN' }, { name: '🇯🇵 Manga (Japonês)', value: 'JP' })),
  async execute(interaction) {
    const genero = interaction.options.getString('genero');
    const status = interaction.options.getString('status');
    const anoMin = interaction.options.getInteger('ano_min');
    const anoMax = interaction.options.getInteger('ano_max');
    const notaMin = interaction.options.getInteger('nota_min');
    const tipo = interaction.options.getString('tipo') ?? 'KR';
    await interaction.deferReply();
    const filtrosAtivos = [];
    if (genero) filtrosAtivos.push(`🏷️ ${GENEROS_PT_FILTRAR[genero] ?? genero}`);
    if (status) filtrosAtivos.push({ RELEASING: '📡 Em lançamento', FINISHED: '✅ Finalizado', HIATUS: '⏸️ Pausado', CANCELLED: '❌ Cancelado' }[status] ?? status);
    if (anoMin) filtrosAtivos.push(`📅 A partir de ${anoMin}`);
    if (anoMax) filtrosAtivos.push(`📅 Até ${anoMax}`);
    if (notaMin) filtrosAtivos.push(`⭐ Nota ${notaMin}.0+`);
    filtrosAtivos.push(COUNTRY_LABEL[tipo] ?? tipo);
    const variables = { countryOfOrigin: tipo, page: 1 };
    if (genero) variables.genre = genero;
    if (status) variables.status = status;
    if (anoMin) variables.yearGreater = parseInt(`${anoMin}0000`, 10);
    if (anoMax) variables.yearLesser = parseInt(`${anoMax}1231`, 10);
    if (notaMin) variables.scoreGreater = notaMin * 10 - 1;
    const QUERY = `query BuscarFiltros($genre:String,$status:MediaStatus,$yearGreater:FuzzyDateInt,$yearLesser:FuzzyDateInt,$scoreGreater:Int,$countryOfOrigin:CountryCode,$page:Int){Page(page:$page,perPage:10){pageInfo{total hasNextPage}media(type:MANGA,countryOfOrigin:$countryOfOrigin,genre:$genre,status:$status,startDate_greater:$yearGreater,startDate_lesser:$yearLesser,averageScore_greater:$scoreGreater,sort:SCORE_DESC){id title{romaji english}averageScore genres chapters status siteUrl startDate{year}coverImage{large color}countryOfOrigin}}}`;
    let results, total = 0;
    try {
      const res = await fetch(ANILIST_API, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ query: QUERY, variables }), signal: AbortSignal.timeout(10000) });
      if (!res.ok) throw new Error();
      const json = await res.json();
      if (json.errors?.length) throw new Error();
      results = json.data.Page.media ?? []; total = json.data.Page.pageInfo.total ?? results.length;
    } catch { await interaction.editReply('❌ Erro ao buscar no AniList. Tente novamente.'); return; }
    if (!results.length) { await interaction.editReply(`❌ Nenhum resultado com os filtros:\n${filtrosAtivos.map(f => `> ${f}`).join('\n')}\n\nTente combinações menos restritivas.`); return; }
    const lines = results.map((m, i) => {
      const title = m.title.english ?? m.title.romaji;
      const score = m.averageScore ? `⭐ ${(m.averageScore/10).toFixed(1)}` : '⭐ N/A';
      return `**${i+1}.** **[${title}](${m.siteUrl})** ${m.startDate?.year ? `(${m.startDate.year})` : ''}\n> ${score} | ${m.chapters ? `📖 ${m.chapters} caps` : '📖 Em andamento'} | ${statusLabel(m.status)}\n> 🏷️ ${m.genres.slice(0, 3).join(', ') || '—'}\n> 🔎 ${buildScanLinksExternal(title)}`;
    });
    const cor = results[0].coverImage.color ? parseInt(results[0].coverImage.color.replace('#',''), 16) : 3447003;
    const embed = new EmbedBuilder().setTitle('🔎 Resultado da Busca Avançada').setDescription(lines.join('\n\n')).setColor(cor).addFields({ name: 'Filtros aplicados', value: filtrosAtivos.join('  •  '), inline: false }).setFooter({ text: `Exibindo 10 de ${total} resultado(s) • Ordenados por nota` });
    if (results[0].coverImage.large) embed.setThumbnail(results[0].coverImage.large);
    await interaction.editReply({ content: null, embeds: [embed] });
  },
};

// /sinopse — busca por IA
const cmdSinopse = {
  data: new SlashCommandBuilder().setName('sinopse').setDescription('Encontra um manhwa/anime descrevendo o enredo com suas próprias palavras')
    .addStringOption(o => o.setName('descricao').setDescription('Descreva o enredo, personagens ou situação da obra').setRequired(true).setMinLength(10))
    .addStringOption(o => o.setName('tipo').setDescription('Tipo de obra (padrão: manhwa)').setRequired(false).addChoices({ name: '🇰🇷 Manhwa / Mangá', value: 'manhwa' }, { name: '🎌 Anime', value: 'anime' }, { name: '🌐 Todos', value: 'all' })),
  async execute(interaction) {
    const descricao = interaction.options.getString('descricao', true);
    const tipo = interaction.options.getString('tipo') ?? 'manhwa';
    await interaction.deferReply();
    await interaction.editReply('🔍 Buscando por sinopse com IA...');
    let sinopseEn = null;
    try { sinopseEn = await translateToEnglish(descricao); } catch {}
    let hits = [];
    try { hits = await searchExaSynopsis(descricao, tipo, sinopseEn); }
    catch { await interaction.editReply('❌ Erro ao consultar a busca semântica. Tente novamente.'); return; }
    if (!hits.length) { await interaction.editReply('❌ Não encontrei nenhuma obra com essa descrição.\n\n💡 **Dicas:**\n• Tente descrever o enredo de outra forma\n• Mencione poderes, ambientação ou personagens\n• Quanto mais detalhes, melhor'); return; }
    const results = [];
    for (const hit of hits) {
      try {
        let result = null;
        if (hit.anilistId) result = await getUnifiedById(tipo === 'anime' ? 'anilist-anime' : 'anilist', String(hit.anilistId));
        else if (hit.mangadexId) result = await getUnifiedById('mangadex', hit.mangadexId);
        if (result && !results.some(r => r.id === result.id && r.source === result.source)) results.push(result);
      } catch {}
    }
    if (!results.length) { await interaction.editReply('❌ A IA encontrou referências mas não conseguiu carregar os dados completos.\n💡 Tente reformular a descrição.'); return; }
    if (results.length === 1) {
      await interaction.editReply({ content: '⏳ Carregando detalhes...' });
      const embed = await buildEmbed(await enrichWithPtBr(results[0]));
      await interaction.editReply({ content: null, embeds: [embed] }); return;
    }
    const SOURCE_ICONS_SYN = { anilist: '🟣', 'anilist-anime': '🟣', mangadex: '🟠', comick: '🟢', mangaupdates: '🔵' };
    const options = results.slice(0, 10).map(r => ({
      label: r.mainTitle.slice(0, 100),
      description: `${SOURCE_ICONS_SYN[r.source] ?? '🔵'}${r.score ? ` ⭐${(r.score/10).toFixed(1)}` : ''}${r.year ? ` (${r.year})` : ''} • ${r.genres.slice(0, 2).join(', ') || 'Obra'}`.slice(0, 100),
      value: `${r.source}:${r.id}`,
    }));
    const row = new ActionRowBuilder().addComponents(new StringSelectMenuBuilder().setCustomId('sinopse_select').setPlaceholder('Escolha a obra que você estava descrevendo...').addOptions(options));
    await interaction.editReply({ content: `🧠 Encontrei **${results.length}** possíveis obras para essa descrição. Qual é a certa?`, components: [row] });
    try {
      const collector = interaction.channel?.createMessageComponentCollector({ componentType: ComponentType.StringSelect, filter: i => i.customId === 'sinopse_select' && i.user.id === interaction.user.id, time: 30000, max: 1 });
      if (!collector) { await interaction.editReply({ content: '❌ Erro ao criar seletor.', components: [] }); return; }
      collector.on('collect', async sel => {
        await sel.deferUpdate();
        const ci = sel.values[0].indexOf(':'); const source = sel.values[0].slice(0, ci), id = sel.values[0].slice(ci + 1);
        await interaction.editReply({ content: '⏳ Carregando detalhes...', components: [] });
        const detail = await getUnifiedById(source, id);
        const embed = await buildEmbed(await enrichWithPtBr(detail ?? results.find(r => r.id === id)));
        await interaction.editReply({ content: null, embeds: [embed], components: [] });
      });
      collector.on('end', async collected => {
        if (collected.size === 0) await interaction.editReply({ content: '⏱️ Tempo esgotado. Use o comando novamente.', components: [] }).catch(() => null);
      });
    } catch { await interaction.editReply({ content: '❌ Erro ao processar seleção.', components: [] }); }
  },
};

// ─── Mapa de comandos ─────────────────────────────────────────────────────────

const commands = new Map([
  [cmdObra.data.name,         cmdObra],
  [cmdSerie.data.name,        cmdSerie],
  [cmdTops.data.name,         cmdTops],
  [cmdIndicar.data.name,      cmdIndicar],
  [cmdGuia.data.name,         cmdGuia],
  [cmdSurpresa.data.name,     cmdSurpresa],
  [cmdNovidades.data.name,    cmdNovidades],
  [cmdFavorito.data.name,      cmdFavorito],
  [cmdVersus.data.name,       cmdVersus],
  [cmdCriador.data.name,      cmdCriador],
  [cmdAlertas.data.name,      cmdAlertas],
  [cmdLeituras.data.name,     cmdLeituras],
  [cmdPopulares.data.name,    cmdPopulares],
  [cmdEstatisticas.data.name, cmdEstatisticas],
  [cmdParecidos.data.name,    cmdParecidos],
  [cmdFiltrar.data.name,      cmdFiltrar],
  [cmdSinopse.data.name,      cmdSinopse],
]);

// ─── Deploy de slash commands ─────────────────────────────────────────────────

async function deployCommands(clientId, token) {
  const body = [...commands.values()].map(c => c.data.toJSON());
  const rest = new REST().setToken(token);
  try {
    logger.info({ count: body.length }, 'Registrando slash commands...');
    await rest.put(Routes.applicationCommands(clientId), { body });
    logger.info('Slash commands registrados com sucesso.');
  } catch (err) {
    logger.error({ err }, 'Erro ao registrar slash commands');
    throw err;
  }
}

// ─── Serviço de notificações ──────────────────────────────────────────────────

const CHECK_INTERVAL_MS = 2 * 60 * 60 * 1000;

async function fetchChapters(mediaId, source) {
    if (source === 'anilist') {
      try {
        const res = await fetch(ANILIST_API, {
          method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ query: `query($id:Int!){Media(id:$id,type:MANGA){chapters status}}`, variables: { id: parseInt(mediaId, 10) } }),
          signal: AbortSignal.timeout(8000),
        });
        if (!res.ok) return null;
        const json = await res.json();
        return json.data?.Media?.chapters ?? null;
      } catch { return null; }
    }
    if (source === 'mangadex') {
      try {
        const params = new URLSearchParams({ manga: mediaId, 'translatedLanguage[]': 'pt-br', limit: '1', 'order[chapter]': 'desc' });
        const res = await fetch(`https://api.mangadex.org/chapter?${params}`, { signal: AbortSignal.timeout(8000) });
        if (!res.ok) return null;
        const json = await res.json();
        if (!json.data?.length) return null;
        const chap = json.data[0].attributes.chapter;
        return chap ? parseFloat(chap) : json.total;
      } catch { return null; }
    }
    if (source === 'anilist-anime') {
      try {
        const res = await fetch(ANILIST_API, {
          method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ query: `query($id:Int!){Media(id:$id,type:ANIME){episodes status}}`, variables: { id: parseInt(mediaId, 10) } }),
          signal: AbortSignal.timeout(8000),
        });
        if (!res.ok) return null;
        const json = await res.json();
        return json.data?.Media?.episodes ?? null;
      } catch { return null; }
    }
    if (source === 'jikan') {
      try {
        const res = await fetch(`https://api.jikan.moe/v4/anime/${encodeURIComponent(mediaId)}`, { signal: AbortSignal.timeout(8000) });
        if (!res.ok) return null;
        const json = await res.json();
        return json.data?.episodes ?? null;
      } catch { return null; }
    }
    if (source === 'kitsu') {
      try {
        const res = await fetch(`https://kitsu.app/api/edge/anime/${encodeURIComponent(mediaId)}`, {
          headers: { Accept: 'application/vnd.api+json' }, signal: AbortSignal.timeout(8000),
        });
        if (!res.ok) return null;
        const json = await res.json();
        return json.data?.attributes?.episodeCount ?? null;
      } catch { return null; }
    }
    return null;
    }
    
async function runCheck(client) {
    logger.info('Verificando atualizações...');
    const canais = await db.select().from(notificacaoCanaisTable);
    if (!canais.length) return;
    const favorites = await db.selectDistinctOn([favoritosTable.manhwaId], {
      manhwaId: favoritosTable.manhwaId, source: favoritosTable.source, title: favoritosTable.title,
      coverUrl: favoritosTable.coverUrl, siteUrl: favoritosTable.siteUrl,
    }).from(favoritosTable);
    if (!favorites.length) return;
    for (const m of favorites) {
      try {
        const newCount = await fetchChapters(m.manhwaId, m.source);
        if (newCount === null) continue;
        const isAnime = ANIME_SOURCES_SET.has(m.source);
        const [existing] = await db.select().from(capitulosRastreados).where(eq(capitulosRastreados.manhwaId, m.manhwaId));
        if (!existing) {
          await db.insert(capitulosRastreados).values({ manhwaId: m.manhwaId, source: m.source, title: m.title, coverUrl: m.coverUrl, siteUrl: m.siteUrl, lastChapters: newCount });
          continue;
        }
        const lastCount = existing.lastChapters ?? 0;
        if (newCount > lastCount) {
          const diff   = Math.floor(newCount) - Math.floor(lastCount);
          const unit   = isAnime ? 'Episódio(s)' : 'Capítulo(s)';
          const unitLc = isAnime ? 'episódio(s)' : 'capítulo(s)';
          const icon   = isAnime ? '🎌' : '📬';
          logger.info({ title: m.title, lastCount, newCount }, `Novo(s) ${unitLc} detectado(s)!`);
          for (const canal of canais) {
            try {
              const channel = await client.channels.fetch(canal.channelId);
              if (!channel || !(channel instanceof TextChannel)) continue;
              const extra = isAnime ? '' : `\n\n🔎 **Buscar nos sites BR:**\n${buildScanLinksExternal(m.title)}`;
              const embed = new EmbedBuilder()
                .setTitle(`${icon} Novo(s) ${unit}: ${m.title}`).setURL(m.siteUrl)
                .setColor(isAnime ? 0x4169E1 : 3066993)
                .setDescription(`**${diff > 0 ? diff : 'Alguns'}** novo(s) ${unitLc} disponível(eis)!\n\n📊 Total agora: **${Math.floor(newCount)}** ${unitLc}${extra}`)
                .setFooter({ text: 'Notificação automática • Bot de Manhwa/Anime' });
              if (m.coverUrl) embed.setThumbnail(m.coverUrl);
              await channel.send({ embeds: [embed] });
            } catch (err) { logger.error({ err, channelId: canal.channelId }, 'Erro ao enviar notificação'); }
          }
          await db.update(capitulosRastreados).set({ lastChapters: newCount, lastChecked: sql`now()` }).where(eq(capitulosRastreados.manhwaId, m.manhwaId));
        } else {
          await db.update(capitulosRastreados).set({ lastChecked: sql`now()` }).where(eq(capitulosRastreados.manhwaId, m.manhwaId));
        }
        await new Promise(r => setTimeout(r, 500));
      } catch (err) { logger.error({ err, titulo: m.title }, 'Erro ao verificar atualização'); }
    }
    logger.info('Verificação concluída.');
    }
    
function startNotificacaoService(client) {
  const runSafe = async () => { try { await runCheck(client); } catch (err) { logger.error({ err }, 'Erro no serviço de notificações'); } };
  setTimeout(runSafe, 60000);
  setInterval(runSafe, CHECK_INTERVAL_MS);
  logger.info({ intervalHoras: 2 }, 'Serviço de notificações iniciado');
}

// ─── Inicialização do bot ─────────────────────────────────────────────────────

async function startBot() {
  const token = process.env['DISCORD_BOT_TOKEN'];
  if (!token) { logger.error('DISCORD_BOT_TOKEN não configurado. Bot não iniciado.'); return; }

  // Carrega dump do AniDB em background (não bloqueia o startup)
  loadAniDBDump().catch(err => logger.warn({ err: err.message }, 'AniDB: falha no carregamento inicial'));

  const client = new Client({
    intents: [GatewayIntentBits.Guilds],
    rest: { retries: 5 },
  });

  client.once(Events.ClientReady, async readyClient => {
    logger.info({ tag: readyClient.user.tag, guilds: readyClient.guilds.cache.size }, 'Bot do Discord conectado');
    try { await deployCommands(readyClient.user.id, token); }
    catch (err) { logger.error({ err }, 'Falha ao registrar comandos'); }
    startNotificacaoService(readyClient);
  });

  client.on(Events.ShardDisconnect, (event, shardId) => logger.warn({ code: event.code, shardId }, 'Bot desconectado do Discord — reconectando...'));
  client.on(Events.ShardReconnecting, shardId => logger.info({ shardId }, 'Bot reconectando ao Discord...'));
  client.on(Events.ShardResume, (shardId, replayedEvents) => logger.info({ shardId, replayedEvents }, 'Bot reconectado ao Discord.'));
  client.on('error', err => logger.error({ err }, 'Erro no cliente do Discord'));

  client.on(Events.InteractionCreate, async interaction => {
    if (interaction.isAutocomplete()) {
      const command = commands.get(interaction.commandName);
      if (command?.autocomplete) { try { await command.autocomplete(interaction); } catch {} }
      return;
    }
    if (!interaction.isChatInputCommand()) return;
    const command = commands.get(interaction.commandName);
    if (!command) return;
    try { await command.execute(interaction); }
    catch (err) {
      if (err?.code === 10062) { logger.warn({ command: interaction.commandName }, 'Interação expirou (10062) — ignorando.'); return; }
      logger.error({ err, command: interaction.commandName }, 'Erro ao executar comando');
      const msg = { content: '❌ Ocorreu um erro ao executar esse comando.', ephemeral: true };
      try {
        if (interaction.replied || interaction.deferred) await interaction.followUp(msg);
        else await interaction.reply(msg);
      } catch (replyErr) {
        if (replyErr?.code === 10062 || replyErr?.code === 40060) logger.warn({ code: replyErr.code, command: interaction.commandName }, 'Não foi possível enviar mensagem de erro — interação já expirada.');
        else logger.error({ err: replyErr, command: interaction.commandName }, 'Erro ao tentar responder com mensagem de erro');
      }
    }
  });

  await client.login(token);
}

// ─── Tratamento global de erros ───────────────────────────────────────────────

process.on('unhandledRejection', reason => console.error('Erro não tratado (unhandledRejection) — bot continua rodando:', reason));
process.on('uncaughtException', err => console.error('Exceção não capturada (uncaughtException) — bot continua rodando:', err));

startBot().catch(err => { console.error('Erro fatal:', err); process.exit(1); });
