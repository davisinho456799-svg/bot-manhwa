import pg from "pg";
const { Pool } = pg;
if (!process.env.DATABASE_URL) { console.error("❌ DATABASE_URL não configurado."); process.exit(1); }
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const SQL = `
CREATE TABLE IF NOT EXISTS favoritos (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  discord_user_id TEXT NOT NULL, manhwa_id TEXT NOT NULL, source TEXT NOT NULL,
  title TEXT NOT NULL, cover_url TEXT, site_url TEXT NOT NULL, genres TEXT, score TEXT,
  added_at TIMESTAMP DEFAULT NOW() NOT NULL
);
CREATE TABLE IF NOT EXISTS lista_leitura (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  discord_user_id TEXT NOT NULL, manhwa_id TEXT NOT NULL, source TEXT NOT NULL,
  title TEXT NOT NULL, cover_url TEXT, site_url TEXT NOT NULL, genres TEXT, score TEXT,
  status TEXT NOT NULL, added_at TIMESTAMP DEFAULT NOW() NOT NULL, updated_at TIMESTAMP DEFAULT NOW() NOT NULL
);
CREATE TABLE IF NOT EXISTS notificacao_canais (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  guild_id TEXT NOT NULL UNIQUE, channel_id TEXT NOT NULL, configured_at TIMESTAMP DEFAULT NOW() NOT NULL
);
CREATE TABLE IF NOT EXISTS capitulos_rastreados (
  id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  manhwa_id TEXT NOT NULL UNIQUE, source TEXT NOT NULL, title TEXT NOT NULL,
  cover_url TEXT, site_url TEXT NOT NULL, last_chapters REAL, last_checked TIMESTAMP DEFAULT NOW()
);`;
try {
  await pool.query(SQL);
  console.log("✅ Tabelas criadas!");
} catch (err) {
  console.error("❌ Erro:", err.message); process.exit(1);
} finally { await pool.end(); }
