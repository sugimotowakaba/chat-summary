require('dotenv').config();

const { App, ExpressReceiver } = require('@slack/bolt');
const { loadConfig } = require('./config');
const { addMessage, getMessages, clearMessages, getChannelMessages, clearChannelMessages, getBufferStatus } = require('./buffer');
const { summarize } = require('./openai');
const { createPage, appendToPage, addReportLog, checkReportLogDatabaseAccess } = require('./notion');
const { parseReport, looksLikeReport } = require('./report-parser');

// 必須の環境変数チェック
const required = [
  'SLACK_BOT_TOKEN',
  'SLACK_SIGNING_SECRET',
  'OPENAI_API_KEY',
  'NOTION_TOKEN',
  'NOTION_DATABASE_ID',
];
for (const key of required) {
  if (!process.env[key]) {
    console.error(`[ERROR] 環境変数 ${key} が設定されていません`);
    process.exit(1);
  }
}

const receiver = new ExpressReceiver({
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  processBeforeResponse: false,
});

// まとめ処理中のスレッドを管理（重複実行防止）
const processingThreads = new Set();

// スレッドごとの Notion ページ管理（差分追記用）
// key: threadKey, value: { id, url }
const pageStore = new Map();
// 報告ログの重複処理防止（Slack再送対策）
const processedReportMessages = new Map();
const REPORT_DEDUP_TTL_MS = 6 * 60 * 60 * 1000;
// チャンネルごとの直前コンテキスト（会社名・グループ名、メッセージをまたいだ引き継ぎ用）
const lastContextByChannel = new Map();

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  receiver,
});

// ==================
// メッセージを取得してスレッドキーと本文を返すユーティリティ
// ==================
async function fetchMessage(client, channelId, ts) {
  const result = await client.conversations.history({
    channel: channelId,
    latest: ts,
    inclusive: true,
    limit: 1,
  });

  if (!result.messages || result.messages.length === 0) return null;
  const msg = result.messages[0];

  // スレッド返信の場合はスレッドから取得
  if (msg.ts !== ts && msg.thread_ts) {
    const threadResult = await client.conversations.replies({
      channel: channelId,
      ts: msg.thread_ts,
      latest: ts,
      inclusive: true,
      limit: 1,
    });
    const threadMsg = threadResult.messages?.find((m) => m.ts === ts);
    if (threadMsg) return threadMsg;
  }

  return msg;
}

// ==================
// ユーザー名キャッシュ（Slack user ID → 表示名）
// ==================
const userNameCache = new Map();

async function resolveUserName(client, userId) {
  if (userNameCache.has(userId)) return userNameCache.get(userId);
  try {
    const res = await client.users.info({ user: userId });
    const name = res.user?.profile?.display_name || res.user?.real_name || res.user?.name || userId;
    userNameCache.set(userId, name);
    return name;
  } catch {
    return userId;
  }
}

function resolveReportLogDatabaseId(config, toolKey, channelId) {
  const routes = config.report_log_databases || {};
  const byTool = routes.tools && toolKey ? routes.tools[toolKey] : null;
  const byChannel = routes.channels && channelId ? routes.channels[channelId] : null;
  return byTool || byChannel || routes.default || process.env.NOTION_REPORT_LOG_DB_ID || null;
}

function collectReportLogDatabaseTargets(config) {
  const routes = config.report_log_databases || {};
  const targets = [];
  const seenIds = new Set();

  const pushTarget = (label, id) => {
    if (!id || seenIds.has(id)) return;
    seenIds.add(id);
    targets.push({ label, id });
  };

  pushTarget('env.NOTION_REPORT_LOG_DB_ID', process.env.NOTION_REPORT_LOG_DB_ID);
  pushTarget('config.report_log_databases.default', routes.default);

  if (routes.tools) {
    Object.entries(routes.tools).forEach(([tool, id]) => {
      pushTarget(`config.report_log_databases.tools.${tool}`, id);
    });
  }
  if (routes.channels) {
    Object.entries(routes.channels).forEach(([channel, id]) => {
      pushTarget(`config.report_log_databases.channels.${channel}`, id);
    });
  }

  return targets;
}

function reserveReportMessage(message) {
  const key = `${message.channel}:${message.ts}`;
  const now = Date.now();

  // 期限切れを掃除
  for (const [k, expiresAt] of processedReportMessages.entries()) {
    if (expiresAt <= now) processedReportMessages.delete(k);
  }

  const expiresAt = processedReportMessages.get(key);
  if (expiresAt && expiresAt > now) {
    return false;
  }
  processedReportMessages.set(key, now + REPORT_DEDUP_TTL_MS);
  return true;
}

// ==================
// 確定作業報告の自動検出・ログ
// ==================
app.message(async ({ message, client, logger }) => {
  // Bot自身の投稿、編集、削除は無視
  if (message.subtype) return;
  if (message.bot_id) return;

  const config = loadConfig();
  const watchChannels = config.report_watch_channels || [];
  if (!watchChannels.includes(message.channel)) return;

  const text = message.text || '';
  if (!looksLikeReport(text)) return;
  if (!reserveReportMessage(message)) {
    logger.info(`[report-detect] 重複イベントをスキップ: channel=${message.channel} ts=${message.ts}`);
    return;
  }

  logger.info(`[report-detect] 報告メッセージを検出: ${text.slice(0, 60)}...`);

  try {
    const userName = await resolveUserName(client, message.user);
    const lastContext = lastContextByChannel.get(message.channel) || null;
    const items = await parseReport(text, userName, lastContext);

    if (items.length === 0) {
      logger.info(`[report-detect] パース結果: 報告アイテムなし`);
      return;
    }

    logger.info(`[report-detect] ${items.length}件の報告アイテムを検出`);

    // Slack URL を構築
    const pTs = message.ts.replace('.', '');
    const slackUrl = `https://app.slack.com/archives/${message.channel}/p${pTs}`;

    // 日付
    const date = new Date(parseFloat(message.ts) * 1000).toISOString().slice(0, 10);

    // 種別ごとのリアクションを決定（最も優先度の高いものを付ける）
    const reportReactions = config.report_reactions || {};
    const typePriority = ['question', 'allergen_leak', 'bracket_missing', 'tag_error', 'status_change', 'info'];
    const detectedTypes = new Set(items.map((i) => i.type));
    let reactionToAdd = 'white_check_mark';
    for (const t of typePriority) {
      if (detectedTypes.has(t) && reportReactions[t]) {
        reactionToAdd = reportReactions[t];
        break;
      }
    }

    // リアクションを付ける
    try {
      await client.reactions.add({
        channel: message.channel,
        timestamp: message.ts,
        name: reactionToAdd,
      });
    } catch (reactionErr) {
      // already_reacted は無視
      if (reactionErr.data?.error !== 'already_reacted') {
        logger.warn(`[report-detect] リアクション付与失敗:`, reactionErr.message);
      }
    }

    // Notion に各アイテムを登録
    const toolKey = 'report_detect';
    const reportLogDatabaseId = resolveReportLogDatabaseId(config, toolKey, message.channel);
    logger.info(`[report-detect] Notion DB route: tool=${toolKey} channel=${message.channel} db=${reportLogDatabaseId}`);

    let loggedCount = 0;
    for (const item of items) {
      if (item.company && item.company !== '不明') {
        lastContextByChannel.set(message.channel, { company: item.company, group: item.group || null });
      }
      try {
        await addReportLog(item, slackUrl, date, { databaseId: reportLogDatabaseId });
        loggedCount++;
      } catch (notionErr) {
        logger.error(
          `[report-detect] Notion登録失敗: code=${notionErr.code || 'unknown'} status=${notionErr.status || 'unknown'} message=${notionErr.message}`
        );
      }
    }

    logger.info(`[report-detect] ${loggedCount}/${items.length}件をNotionに登録完了`);
  } catch (err) {
    logger.error(`[report-detect] 処理エラー:`, err);
  }
});

// ==================
// reaction_added イベントハンドラ
// ==================
app.event('reaction_added', async ({ event, client, logger }) => {
  const config = loadConfig();
  const { reaction, item } = event;

  // メッセージへのリアクションのみ対象（ファイルなどは除外）
  if (item.type !== 'message') return;

  const channelId = item.channel;
  const ts = item.ts;

  const label = config.reactions[reaction];
  const isTrigger = reaction === config.trigger_reaction;
  const isThreadCollect = reaction === config.thread_collect_reaction;
  const isGlobalTrigger = reaction === config.global_trigger_reaction;

  // 設定に含まれないリアクションは無視
  if (!label && !isTrigger && !isThreadCollect && !isGlobalTrigger) return;

  // メッセージを取得してスレッドキーを決定
  let msg;
  try {
    msg = await fetchMessage(client, channelId, ts);
  } catch (err) {
    logger.error(`[${channelId}] メッセージ取得エラー:`, err);
    return;
  }

  if (!msg) {
    logger.warn(`[${channelId}] メッセージが見つかりませんでした (ts: ${ts})`);
    return;
  }

  // threadKey: スレッド内なら親tsで統一、単独メッセージは自身のts
  const threadKey = `${channelId}:${msg.thread_ts || msg.ts}`;

  // ==================
  // スレッド全収集リアクション → スレッド内全件をバッファに追加
  // ==================
  if (isThreadCollect) {
    const collectLabel = config.thread_collect_label || 'スレッド';
    const parentTs = msg.thread_ts || msg.ts;

    try {
      const threadResult = await client.conversations.replies({
        channel: channelId,
        ts: parentTs,
      });

      const threadMsgs = threadResult.messages || [];
      for (const m of threadMsgs) {
        if (!m.text) continue;
        addMessage(threadKey, { label: collectLabel, text: m.text, ts: m.ts, user: m.user });
      }
      logger.info(`[${threadKey}] スレッド全収集: ${threadMsgs.length}件をバッファに追加`);
    } catch (err) {
      logger.error(`[${threadKey}] スレッド取得エラー:`, err);
    }
    return;
  }

  // ==================
  // 全体トリガーリアクション → チャンネル全バッファをまとめ実行
  // ==================
  if (isGlobalTrigger) {
    const globalKey = `${channelId}:global`;

    if (processingThreads.has(globalKey)) {
      logger.info(`[${channelId}] 全体まとめ処理中のため重複リクエストを無視`);
      return;
    }

    const messages = getChannelMessages(channelId);

    if (messages.length === 0) {
      logger.info(`[${channelId}] 全体トリガーが押されましたが、バッファが空です`);
      return;
    }

    processingThreads.add(globalKey);
    logger.info(`[${channelId}] 全体まとめ開始: ${messages.length}件のメッセージ`);

    try {
      const summary = await summarize(messages, config);

      const sorted = [...messages].sort((a, b) => parseFloat(a.ts) - parseFloat(b.ts));
      const sourceLines = sorted.map((m) => {
        const slackLink = `https://app.slack.com/archives/${channelId}/p${m.ts.replace('.', '')}`;
        const date = new Date(parseFloat(m.ts) * 1000).toLocaleString('ja-JP', {
          month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
        });
        const userRef = m.user ? `<@${m.user}>` : '不明';
        const preview = m.text?.replace(/\n/g, ' ').slice(0, 80) || '';
        return `- [${m.label}] ${date} ${userRef}: ${preview} → [Slackで確認](${slackLink})`;
      });
      const sourceSection = '\n\n---\n\n## 元メッセージ\n\n' + sourceLines.join('\n');

      clearChannelMessages(channelId);

      const now = new Date();
      const dateStr = now.toLocaleDateString('ja-JP', { year: 'numeric', month: '2-digit', day: '2-digit' });
      const timeStr = now.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' });

      let pageUrl;

      if (pageStore.has(globalKey)) {
        const { id, url } = pageStore.get(globalKey);
        const appendContent = `\n\n---\n\n## 追記 ${dateStr} ${timeStr}\n\n` + summary + sourceSection;
        await appendToPage(id, appendContent);
        pageUrl = url;
        logger.info(`[${channelId}] 全体Notionページに追記しました: ${pageUrl}`);
      } else {
        const prefix = config.notion_title_prefix || 'Slackまとめ';
        const title = `${prefix}（全体） ${dateStr}`;
        const fullContent = summary + sourceSection;
        const page = await createPage(title, fullContent);
        pageStore.set(globalKey, page);
        pageUrl = page.url;
        logger.info(`[${channelId}] 全体NotionページをNotionに作成しました: ${pageUrl}`);
      }

      try {
        await client.chat.postMessage({
          channel: channelId,
          text: `チャンネル全体のまとめをNotionに保存しました :white_check_mark:\n${pageUrl}`,
        });
      } catch (notifyErr) {
        logger.warn('Slack通知の送信に失敗しました:', notifyErr.message);
      }
    } catch (err) {
      logger.error('全体まとめ処理でエラーが発生しました:', err);
    } finally {
      processingThreads.delete(globalKey);
    }

    return;
  }

  // ==================
  // トリガーリアクション → まとめ実行
  // ==================
  if (isTrigger) {
    if (processingThreads.has(threadKey)) {
      logger.info(`[${threadKey}] まとめ処理中のため重複リクエストを無視`);
      return;
    }

    const messages = getMessages(threadKey);

    if (messages.length === 0) {
      logger.info(`[${threadKey}] トリガーが押されましたが、バッファが空です`);
      return;
    }

    processingThreads.add(threadKey);
    logger.info(`[${threadKey}] まとめ開始: ${messages.length}件のメッセージ`);

    try {
      // OpenAI で要約
      const summary = await summarize(messages, config);

      // 元メッセージセクションを作成
      const sorted = [...messages].sort((a, b) => parseFloat(a.ts) - parseFloat(b.ts));
      const sourceLines = sorted.map((m) => {
        const slackLink = `https://app.slack.com/archives/${channelId}/p${m.ts.replace('.', '')}`;
        const date = new Date(parseFloat(m.ts) * 1000).toLocaleString('ja-JP', {
          month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
        });
        const userRef = m.user ? `<@${m.user}>` : '不明';
        const preview = m.text?.replace(/\n/g, ' ').slice(0, 80) || '';
        return `- [${m.label}] ${date} ${userRef}: ${preview} → [Slackで確認](${slackLink})`;
      });
      const sourceSection = '\n\n---\n\n## 元メッセージ\n\n' + sourceLines.join('\n');

      // バッファをクリア
      clearMessages(threadKey);

      const now = new Date();
      const dateStr = now.toLocaleDateString('ja-JP', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
      });
      const timeStr = now.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' });

      let pageUrl;

      if (pageStore.has(threadKey)) {
        // ==================
        // 既存ページに追記
        // ==================
        const { id, url } = pageStore.get(threadKey);
        const appendContent = `\n\n---\n\n## 追記 ${dateStr} ${timeStr}\n\n` + summary + sourceSection;
        await appendToPage(id, appendContent);
        pageUrl = url;
        logger.info(`[${threadKey}] Notionページに追記しました: ${pageUrl}`);
      } else {
        // ==================
        // 新規ページを作成
        // ==================
        const prefix = config.notion_title_prefix || 'Slackまとめ';
        const title = `${prefix} ${dateStr}`;
        const fullContent = summary + sourceSection;
        const page = await createPage(title, fullContent);
        pageStore.set(threadKey, page);
        pageUrl = page.url;
        logger.info(`[${threadKey}] Notionにページを作成しました: ${pageUrl}`);
      }

      // Slack に完了通知（失敗しても全体は止めない）
      try {
        await client.chat.postMessage({
          channel: channelId,
          text: `まとめをNotionに保存しました :white_check_mark:\n${pageUrl}`,
        });
      } catch (notifyErr) {
        logger.warn('Slack通知の送信に失敗しました（chat:write スコープを確認してください）:', notifyErr.message);
      }
    } catch (err) {
      logger.error('まとめ処理でエラーが発生しました:', err);
    } finally {
      processingThreads.delete(threadKey);
    }

    return;
  }

  // ==================
  // 通常のリアクション → バッファにメッセージを追加
  // ==================
  addMessage(threadKey, { label, text: msg.text, ts: msg.ts, user: msg.user });
  logger.info(`[${threadKey}] バッファに追加: [${label}] ${msg.text?.slice(0, 60)}`);
});

// ==================
// デバッグ用エンドポイント（バッファ状態確認）
// ==================
receiver.router.get('/status', (req, res) => {
  res.json({
    status: 'ok',
    buffer: getBufferStatus(),
    pages: Object.fromEntries(pageStore),
  });
});

// ==================
// ヘルスチェック（Railway用）
// ==================
receiver.router.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// ==================
// 起動
// ==================
const port = process.env.PORT || 3000;

(async () => {
  await app.start(port);
  console.log(`Slack→Notion まとめBot 起動中 (port: ${port})`);
  console.log(`Events URL: POST /slack/events`);

  const config = loadConfig();
  const reportDbTargets = collectReportLogDatabaseTargets(config);
  if (reportDbTargets.length === 0) {
    console.error('[startup] Notion報告ログDB接続NG');
    console.error('[startup] NOTION_REPORT_LOG_DB_ID または report_log_databases を設定してください');
  }
  for (const target of reportDbTargets) {
    const reportLogDbCheck = await checkReportLogDatabaseAccess(target.id);
    if (reportLogDbCheck.ok) {
      console.log(`[startup] Notion報告ログDB接続OK: ${target.label} -> ${reportLogDbCheck.databaseId}`);
    } else {
      console.error(`[startup] Notion報告ログDB接続NG: ${target.label}`);
      if (reportLogDbCheck.databaseId) {
        console.error(`[startup] DB ID: ${reportLogDbCheck.databaseId}`);
        console.error('[startup] 対象DBをIntegrationに共有しているか確認してください');
      }
      console.error(`[startup] 詳細: ${reportLogDbCheck.message}`);
    }
  }
})();
