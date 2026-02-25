#!/usr/bin/env node
require('dotenv').config();

const { WebClient } = require('@slack/web-api');
const { loadConfig } = require('./config');
const { parseReport, looksLikeReport } = require('./report-parser');
const { addReportLog, hasReportLogBySlackUrl, checkReportLogDatabaseAccess } = require('./notion');

function parseArgs() {
  const args = process.argv.slice(2);
  const parsed = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--channel' && args[i + 1]) parsed.channel = args[++i];
    else if (args[i] === '--since' && args[i + 1]) parsed.since = args[++i];
    else if (args[i] === '--until' && args[i + 1]) parsed.until = args[++i];
    else if (args[i] === '--max' && args[i + 1]) parsed.max = Number(args[++i]);
    else if (args[i] === '--dry-run') parsed.dryRun = true;
  }
  return parsed;
}

function resolveReportLogDatabaseId(config, toolKey, channelId) {
  const routes = config.report_log_databases || {};
  const byTool = routes.tools && toolKey ? routes.tools[toolKey] : null;
  const byChannel = routes.channels && channelId ? routes.channels[channelId] : null;
  return byTool || byChannel || routes.default || process.env.NOTION_REPORT_LOG_DB_ID || null;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function toSlackMessageUrl(channelId, ts) {
  return `https://app.slack.com/archives/${channelId}/p${String(ts).replace('.', '')}`;
}

async function fetchMessages(client, channel, oldest, latest, max) {
  const list = [];
  let cursor;
  do {
    const res = await client.conversations.history({
      channel,
      oldest: String(oldest),
      latest: latest ? String(latest) : undefined,
      inclusive: true,
      limit: 200,
      cursor,
    });

    for (const m of (res.messages || [])) {
      list.push(m);
      if (max && list.length >= max) return list;
    }

    cursor = res.response_metadata?.next_cursor || '';
    if (cursor) await sleep(200);
  } while (cursor);

  return list;
}

async function main() {
  const { channel, since, until, max, dryRun } = parseArgs();
  if (!channel || !since) {
    console.error('使い方: node src/backfill-report-logs.js --channel <CHANNEL_ID> --since <YYYY-MM-DD> [--until <YYYY-MM-DD>] [--max <N>] [--dry-run]');
    process.exit(1);
  }

  const sinceDate = new Date(`${since}T00:00:00Z`);
  if (Number.isNaN(sinceDate.getTime())) {
    console.error(`[ERROR] --since の形式が不正です: ${since}`);
    process.exit(1);
  }

  let untilTs = null;
  if (until) {
    const untilDate = new Date(`${until}T23:59:59Z`);
    if (Number.isNaN(untilDate.getTime())) {
      console.error(`[ERROR] --until の形式が不正です: ${until}`);
      process.exit(1);
    }
    untilTs = untilDate.getTime() / 1000;
  }

  const token = process.env.SLACK_USER_TOKEN || process.env.SLACK_BOT_TOKEN;
  if (!token) {
    console.error('[ERROR] SLACK_BOT_TOKEN または SLACK_USER_TOKEN を設定してください');
    process.exit(1);
  }

  const config = loadConfig();
  const toolKey = 'report_detect';
  const databaseId = resolveReportLogDatabaseId(config, toolKey, channel);
  if (!databaseId) {
    console.error('[ERROR] 起票先DBが未設定です。NOTION_REPORT_LOG_DB_ID もしくは report_log_databases を設定してください');
    process.exit(1);
  }

  const dbCheck = await checkReportLogDatabaseAccess(databaseId);
  if (!dbCheck.ok) {
    console.error(`[ERROR] Notion DB接続失敗: ${databaseId}`);
    console.error(`[ERROR] ${dbCheck.message}`);
    process.exit(1);
  }

  const client = new WebClient(token);
  const oldestTs = sinceDate.getTime() / 1000;

  console.log(`バックフィル開始: channel=${channel} since=${since} until=${until || 'latest'} db=${databaseId} dryRun=${Boolean(dryRun)}`);
  const messages = await fetchMessages(client, channel, oldestTs, untilTs, max);
  messages.sort((a, b) => parseFloat(a.ts) - parseFloat(b.ts));
  console.log(`取得件数: ${messages.length}`);

  const userCache = new Map();
  let lastContext = null; // { company, group }
  let scanned = 0;
  let reportLike = 0;
  let parsedReports = 0;
  let skippedExisting = 0;
  let written = 0;

  for (const msg of messages) {
    scanned++;
    if (msg.subtype || msg.bot_id) continue;
    const text = msg.text || '';
    if (!looksLikeReport(text)) continue;
    reportLike++;

    const slackUrl = toSlackMessageUrl(channel, msg.ts);
    const exists = await hasReportLogBySlackUrl(slackUrl, { databaseId });
    if (exists) {
      skippedExisting++;
      continue;
    }

    let userName = msg.user || 'unknown';
    if (msg.user) {
      if (userCache.has(msg.user)) {
        userName = userCache.get(msg.user);
      } else {
        try {
          const userInfo = await client.users.info({ user: msg.user });
          userName = userInfo.user?.profile?.display_name || userInfo.user?.real_name || userInfo.user?.name || msg.user;
        } catch {
          userName = msg.user;
        }
        userCache.set(msg.user, userName);
      }
    }

    const items = await parseReport(text, userName, lastContext);
    if (!items.length) continue;
    parsedReports++;
    const date = new Date(parseFloat(msg.ts) * 1000).toISOString().slice(0, 10);

    for (const item of items) {
      if (item.company && item.company !== '不明') {
        lastContext = { company: item.company, group: item.group || null };
      }
      if (!dryRun) {
        await addReportLog(item, slackUrl, date, { databaseId });
      }
      written++;
    }
    await sleep(150);
  }

  console.log('バックフィル完了');
  console.log(`- scanned: ${scanned}`);
  console.log(`- reportLike: ${reportLike}`);
  console.log(`- parsedReports: ${parsedReports}`);
  console.log(`- skippedExisting: ${skippedExisting}`);
  console.log(`- written: ${written}`);
}

main().catch((err) => {
  console.error('[ERROR] バックフィル失敗:', err.message);
  process.exit(1);
});
