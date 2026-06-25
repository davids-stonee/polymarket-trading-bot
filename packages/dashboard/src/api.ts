import { DateTime } from "luxon";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import net from "node:net";
import {
  buildSlugFor,
  currentPeriodStart,
  isNamedPipe,
  parse1hPeriodStartEt,
  parseAsset,
  parseTimeFrame,
  readJsonlRangeSync,
  readJsonlTailSync,
  readLastJsonlRow,
  tfFolder,
  type Asset,
  type CollectorFrame,
  type TimeFrame,
} from "@pmt/shared";

export interface LiveFrame extends CollectorFrame {
  collector_connected: boolean;
}

export interface LiveState {
  frame: LiveFrame | null;
  connected: boolean;
  lastUpdateMs: number;
}

export function createLiveState(): LiveState {
  return { frame: null, connected: false, lastUpdateMs: 0 };
}

function toLiveFrame(cf: CollectorFrame, connected: boolean): LiveFrame {
  return { ...cf, collector_connected: connected };
}

export function spawnCollectorSubscriber(ipcPath: string, live: LiveState): void {
  const connect = () => {
    if (!isNamedPipe(ipcPath) && !existsSync(ipcPath)) {
      setTimeout(connect, 2000);
      return;
    }
    const socket = net.createConnection(ipcPath);
    let buffer = "";

    socket.on("connect", () => {
      live.connected = true;
    });

    socket.on("data", (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const cf = JSON.parse(line) as CollectorFrame;
          live.frame = toLiveFrame(cf, true);
          live.lastUpdateMs = Date.now();
          live.connected = true;
        } catch {
          // skip
        }
      }
    });

    socket.on("close", () => {
      live.connected = false;
      if (live.frame) live.frame.collector_connected = false;
      setTimeout(connect, 2000);
    });
    socket.on("error", () => {
      live.connected = false;
      if (live.frame) live.frame.collector_connected = false;
      socket.destroy();
      setTimeout(connect, 2000);
    });
  };
  connect();
}

export function spawnStalenessWatchdog(live: LiveState): void {
  setInterval(() => {
    if (live.lastUpdateMs > 0 && Date.now() - live.lastUpdateMs > 5000) {
      live.connected = false;
      if (live.frame) live.frame.collector_connected = false;
    }
  }, 2000);
}

function periodStartFromSlug(slug: string): number | null {
  const tail = slug.split("-").pop();
  const ts = tail ? parseInt(tail, 10) : NaN;
  if (ts > 1_000_000_000) return ts;
  return parse1hPeriodStartEt(slug);
}

function etDateFromPeriodStart(periodStart: number): string | null {
  const dt = DateTime.fromSeconds(periodStart, { zone: "America/New_York" });
  return dt.toFormat("yyyy-MM-dd");
}

function slugOnEtDate(periodStart: number | null, date: string): boolean {
  if (periodStart === null) return false;
  return etDateFromPeriodStart(periodStart) === date;
}

export interface SlugEntry {
  slug: string;
  period_start: number | null;
  line_count: number;
  is_current: boolean;
}

export function listSlugs(
  dataDir: string,
  asset: Asset,
  tf: TimeFrame,
  limit = 500,
  date?: string,
): SlugEntry[] {
  const dir = join(dataDir, "ask_bid_prices", tfFolder(tf), asset);
  if (!existsSync(dir)) return [];
  const currentSlug = buildSlugFor(asset, tf, currentPeriodStart(tf));
  const files = readdirSync(dir).filter((f) => f.endsWith(".jsonl"));

  let entries: SlugEntry[] = files.map((f) => {
    const slug = f.replace(/\.jsonl$/, "");
    const path = join(dir, f);
    const content = readFileSync(path, "utf8").trim();
    const line_count = content ? content.split("\n").length : 0;
    return {
      slug,
      period_start: periodStartFromSlug(slug),
      line_count,
      is_current: slug === currentSlug,
    };
  }).filter((e) => e.line_count > 0);

  if (date) {
    entries = entries.filter((e) => slugOnEtDate(e.period_start, date));
  }

  entries.sort((a, b) => {
    const pa = a.period_start ?? 0;
    const pb = b.period_start ?? 0;
    return pb - pa || b.slug.localeCompare(a.slug);
  });

  if (!date) entries = entries.slice(0, limit);
  return entries;
}

export function listSlugDates(
  dataDir: string,
  asset: Asset,
  tf: TimeFrame,
): Array<{ date: string; count: number }> {
  const entries = listSlugs(dataDir, asset, tf, 10_000);
  const byDate = new Map<string, number>();
  for (const e of entries) {
    if (e.period_start === null) continue;
    const d = etDateFromPeriodStart(e.period_start);
    if (!d) continue;
    byDate.set(d, (byDate.get(d) ?? 0) + 1);
  }
  return [...byDate.entries()]
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([date, count]) => ({ date, count }));
}

export function readPriceHistory(
  dataDir: string,
  asset: string,
  venue: string,
  limit: number,
  fromMs?: number,
  toMs?: number,
): unknown[] {
  const a = parseAsset(asset);
  if (!a) return [];
  const path = join(dataDir, "prices", venue.toLowerCase(), `${a}.jsonl`);
  if (fromMs !== undefined || toMs !== undefined) {
    return readJsonlRangeSync(path, limit, fromMs, toMs);
  }
  return readJsonlTailSync(path, limit);
}

export function readSpreadHistory(
  dataDir: string,
  asset: string,
  limit: number,
  fromMs?: number,
  toMs?: number,
): unknown[] {
  const a = parseAsset(asset);
  if (!a) return [];
  const path = join(dataDir, "spread", "binance_chainlink", `${a}.jsonl`);
  if (fromMs !== undefined || toMs !== undefined) {
    return readJsonlRangeSync(path, limit, fromMs, toMs);
  }
  return readJsonlTailSync(path, limit);
}

export function readClPtbHistory(
  dataDir: string,
  asset: string,
  tf: string,
  limit: number,
  fromMs?: number,
  toMs?: number,
): unknown[] {
  const a = parseAsset(asset);
  const t = parseTimeFrame(tf);
  if (!a || !t) return [];
  const path = join(dataDir, "spread", "cl_ptb_deviation", `${a}_${t}.jsonl`);
  if (fromMs !== undefined || toMs !== undefined) {
    return readJsonlRangeSync(path, limit, fromMs, toMs);
  }
  return readJsonlTailSync(path, limit);
}

export function readAskBidHistory(
  dataDir: string,
  asset: string,
  tf: string,
  limit: number,
  slug?: string,
): unknown[] {
  const a = parseAsset(asset);
  const t = parseTimeFrame(tf);
  if (!a || !t) return [];
  const s = slug ?? buildSlugFor(a, t, currentPeriodStart(t));
  if (s.includes("/") || s.includes("\\") || s.includes("..")) return [];
  const path = join(dataDir, "ask_bid_prices", tfFolder(t), a, `${s}.jsonl`);
  return readJsonlTailSync(path, limit);
}

export function readSpreadLatest(dataDir: string): unknown {
  const path = join(dataDir, "spread", "latest.json");
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

export function readOrderbookSnapshot(
  dataDir: string,
  asset: string,
  tf: string,
  slug: string,
): unknown | null {
  const a = parseAsset(asset);
  const t = parseTimeFrame(tf);
  if (!a || !t) return null;
  if (slug.includes("/") || slug.includes("\\") || slug.includes("..")) return null;
  const path = join(dataDir, "order_books", tfFolder(t), a, `${slug}.jsonl`);
  return readLastJsonlRow(path);
}

export function readPtbForSlug(
  dataDir: string,
  asset: string,
  tf: string,
  slug: string,
): { slug: string; price_to_beat: number; ptb_venue: string } | null {
  const a = parseAsset(asset);
  const t = parseTimeFrame(tf);
  if (!a || !t) return null;
  if (slug.includes("/") || slug.includes("\\") || slug.includes("..")) return null;
  const path = join(dataDir, "market_data", tfFolder(t), a, `${t}.jsonl`);
  if (!existsSync(path)) return null;
  const content = readFileSync(path, "utf8");
  const lines = content.split("\n").filter((l) => l.trim());
  for (let i = lines.length - 1; i >= 0; i--) {
    try {
      const row = JSON.parse(lines[i]!) as { slug?: string; open_price?: number; ptb_venue?: string };
      if (row.slug === slug) {
        return {
          slug,
          price_to_beat: row.open_price ?? 0,
          ptb_venue: row.ptb_venue ?? "",
        };
      }
    } catch {
      // skip
    }
  }
  return null;
}

export function listMarkets(live: LiveState): unknown[] {
  if (!live.frame) return [];
  const markets = [];
  for (const [key, ptb] of Object.entries(live.frame.price_to_beat)) {
    markets.push({
      market_key: key,
      price_to_beat: ptb,
      yes_best_bid: live.frame.yes_best_bid[key],
      yes_best_ask: live.frame.yes_best_ask[key],
      no_best_bid: live.frame.no_best_bid[key],
      no_best_ask: live.frame.no_best_ask[key],
    });
  }
  return markets;
}

export function statDataDir(dataDir: string): { size_mb: number; files: number } {
  let files = 0;
  let bytes = 0;
  const walk = (dir: string) => {
    if (!existsSync(dir)) return;
    for (const entry of readdirSync(dir)) {
      const p = join(dir, entry);
      const st = statSync(p);
      if (st.isDirectory()) walk(p);
      else { files++; bytes += st.size; }
    }
  };
  walk(dataDir);
  return { size_mb: Math.round(bytes / 1024 / 1024 * 10) / 10, files };
}

export function getWsFrame(live: LiveState): LiveFrame | null {
  if (!live.frame) return null;
  return {
    ...live.frame,
    collector_connected: live.connected && live.frame.collector_connected,
  };
}
