const { Client } = require('@notionhq/client');

const notion = new Client({ auth: process.env.NOTION_TOKEN });
const reportLogDbCache = new Map();

/**
 * インライン Markdown リンク [text](url) を Notion rich_text 配列に変換する
 * @param {string} text
 * @returns {object[]}
 */
function inlineToRichText(text) {
  const parts = [];
  const linkRegex = /\[([^\]]+)\]\(([^)]+)\)/g;
  let lastIndex = 0;
  let match;

  while ((match = linkRegex.exec(text)) !== null) {
    if (match.index > lastIndex) {
      parts.push({ type: 'text', text: { content: text.slice(lastIndex, match.index) } });
    }
    parts.push({ type: 'text', text: { content: match[1], link: { url: match[2] } } });
    lastIndex = match.index + match[0].length;
  }

  if (lastIndex < text.length) {
    parts.push({ type: 'text', text: { content: text.slice(lastIndex) } });
  }

  return parts.length > 0 ? parts : [{ type: 'text', text: { content: text } }];
}

/**
 * Markdown テキストを Notion ブロック配列に変換する
 * @param {string} markdown
 * @returns {object[]}
 */
function markdownToBlocks(markdown) {
  const lines = markdown.split('\n');
  const blocks = [];

  for (const line of lines) {
    if (!line.trim()) {
      blocks.push({
        object: 'block',
        type: 'paragraph',
        paragraph: { rich_text: [] },
      });
      continue;
    }

    if (line.startsWith('### ')) {
      blocks.push({
        object: 'block',
        type: 'heading_3',
        heading_3: { rich_text: inlineToRichText(line.slice(4)) },
      });
    } else if (line.startsWith('## ')) {
      blocks.push({
        object: 'block',
        type: 'heading_2',
        heading_2: { rich_text: inlineToRichText(line.slice(3)) },
      });
    } else if (line.startsWith('# ')) {
      blocks.push({
        object: 'block',
        type: 'heading_1',
        heading_1: { rich_text: inlineToRichText(line.slice(2)) },
      });
    } else if (line.match(/^[-*] /)) {
      blocks.push({
        object: 'block',
        type: 'bulleted_list_item',
        bulleted_list_item: { rich_text: inlineToRichText(line.slice(2)) },
      });
    } else if (line.match(/^\d+\. /)) {
      blocks.push({
        object: 'block',
        type: 'numbered_list_item',
        numbered_list_item: { rich_text: inlineToRichText(line.replace(/^\d+\. /, '')) },
      });
    } else if (line.startsWith('> ')) {
      blocks.push({
        object: 'block',
        type: 'quote',
        quote: { rich_text: inlineToRichText(line.slice(2)) },
      });
    } else {
      blocks.push({
        object: 'block',
        type: 'paragraph',
        paragraph: { rich_text: inlineToRichText(line) },
      });
    }
  }

  // Notion APIはリクエスト1回あたり最大100ブロックまで
  return blocks.slice(0, 100);
}

/**
 * Notionデータベースに新規ページを作成する
 * @param {string} title - ページタイトル
 * @param {string} markdownContent - Markdown形式のコンテンツ
 * @param {string} channelId - SlackチャンネルID（メタ情報として記録）
 * @returns {Promise<string>} 作成されたページのURL
 */
async function createPage(title, markdownContent, channelId) {
  const databaseId = process.env.NOTION_DATABASE_ID;
  const blocks = markdownToBlocks(markdownContent);

  const response = await notion.pages.create({
    parent: { database_id: databaseId },
    properties: {
      名前: {
        title: [{ text: { content: title } }],
      },
    },
    children: blocks,
  });

  return { id: response.id, url: response.url };
}

/**
 * 既存の Notion ページにブロックを追記する
 * @param {string} pageId - 追記先のページ ID
 * @param {string} markdownContent - 追記する Markdown コンテンツ
 */
async function appendToPage(pageId, markdownContent) {
  const blocks = markdownToBlocks(markdownContent);
  await notion.blocks.children.append({
    block_id: pageId,
    children: blocks,
  });
}

/**
 * 報告ログDBに1行追加する
 * @param {object} item - parseReport() が返すアイテム
 * @param {string} item.customer - 顧客名
 * @param {string} item.product - 商品名
 * @param {string} item.type - 種別
 * @param {string} item.detail - 詳細
 * @param {string|null} item.allergen - アレルゲン
 * @param {string} item.reporter - 報告者名
 * @param {string} slackUrl - Slackメッセージへのリンク
 * @param {string} date - 日付文字列 (YYYY-MM-DD)
 */
async function addReportLog(item, slackUrl, date, options = {}) {
  const databaseId = options.databaseId || process.env.NOTION_REPORT_LOG_DB_ID;
  if (!databaseId) {
    console.warn('[report-log] NOTION_REPORT_LOG_DB_ID が未設定のためスキップ');
    return null;
  }

  const db = await getReportLogDatabase(databaseId);
  const schema = getReportLogSchema(db);
  if (!schema.titlePropName) {
    throw new Error('Notion DBにtitleプロパティが見つかりません');
  }
  const { titlePropName, dateProp, reporterProp, typeProp, detailProp, slackProp, allergenProp } = schema;

  const typeLabels = {
    bracket_missing: '【】漏れ',
    tag_error: 'タグ誤認識',
    allergen_leak: 'アレルゲン漏れ',
    status_change: 'ステータス変更',
    question: '質問・相談',
    info: '情報共有',
  };

  const titleParts = [item.company];
  if (item.group) titleParts.push(item.group);
  titleParts.push(item.product);
  const titleText = titleParts.join(' / ');

  const properties = {
    [titlePropName]: {
      title: [{ text: { content: titleText } }],
    },
  };

  if (dateProp) properties[dateProp] = { date: { start: date } };
  if (reporterProp) properties[reporterProp] = { rich_text: [{ text: { content: item.reporter } }] };
  if (typeProp) properties[typeProp] = { select: { name: typeLabels[item.type] || item.type } };
  if (detailProp) properties[detailProp] = { rich_text: [{ text: { content: item.detail || '' } }] };
  if (slackProp) properties[slackProp] = { url: slackUrl };

  // 会社名・グループ名・品物名（列があれば）
  if (schema.companyProp && item.company) {
    properties[schema.companyProp] = { rich_text: [{ text: { content: item.company } }] };
  }
  if (schema.groupProp && item.group) {
    properties[schema.groupProp] = { rich_text: [{ text: { content: item.group } }] };
  }
  if (schema.productProp && item.product) {
    properties[schema.productProp] = { rich_text: [{ text: { content: item.product } }] };
  }

  // アレルゲンがある場合のみ追加
  if (item.allergen && allergenProp) {
    properties[allergenProp] = {
      rich_text: [{ text: { content: item.allergen } }],
    };
  }

  const response = await notion.pages.create({
    parent: { database_id: databaseId },
    properties,
  });

  return { id: response.id, url: response.url };
}

async function getReportLogDatabase(databaseId) {
  if (reportLogDbCache.has(databaseId)) {
    return reportLogDbCache.get(databaseId);
  }
  const db = await notion.databases.retrieve({ database_id: databaseId });
  reportLogDbCache.set(databaseId, db);
  return db;
}

function getReportLogSchema(db) {
  const dbProps = db.properties || {};
  const titleProp = Object.entries(dbProps).find(([, p]) => p.type === 'title');
  return {
    titlePropName: titleProp?.[0] || null,
    dateProp: findPropertyName(dbProps, ['日付', 'Date'], 'date'),
    reporterProp: findPropertyName(dbProps, ['報告者', 'Reporter'], 'rich_text'),
    typeProp: findPropertyName(dbProps, ['種別', 'Type'], 'select'),
    detailProp: findPropertyName(dbProps, ['詳細', 'Detail'], 'rich_text'),
    slackProp: findPropertyName(dbProps, ['Slack', 'URL', 'Link'], 'url'),
    allergenProp: findPropertyName(dbProps, ['アレルゲン', 'Allergen'], 'rich_text'),
    companyProp: findPropertyName(dbProps, ['会社名', 'Company'], 'rich_text'),
    groupProp: findPropertyName(dbProps, ['グループ名', 'Group'], 'rich_text'),
    productProp: findPropertyName(dbProps, ['品物名', '商品名', 'Product'], 'rich_text'),
  };
}

function findPropertyName(properties, candidates, expectedType) {
  for (const name of candidates) {
    if (properties[name] && properties[name].type === expectedType) {
      return name;
    }
  }
  return null;
}

async function hasReportLogBySlackUrl(slackUrl, options = {}) {
  const databaseId = options.databaseId || process.env.NOTION_REPORT_LOG_DB_ID;
  if (!databaseId || !slackUrl) return false;

  const db = await getReportLogDatabase(databaseId);
  const schema = getReportLogSchema(db);
  if (!schema.slackProp) return false;

  const res = await notion.databases.query({
    database_id: databaseId,
    filter: {
      property: schema.slackProp,
      url: { equals: slackUrl },
    },
    page_size: 1,
  });

  return Array.isArray(res.results) && res.results.length > 0;
}

/**
 * 報告ログDBへのアクセス可否を確認する
 * @returns {Promise<{ ok: boolean, databaseId?: string, message?: string }>}
 */
async function checkReportLogDatabaseAccess(databaseId = process.env.NOTION_REPORT_LOG_DB_ID) {
  if (!databaseId) {
    return {
      ok: false,
      message: 'NOTION_REPORT_LOG_DB_ID が未設定です',
    };
  }

  try {
    await getReportLogDatabase(databaseId);
    return { ok: true, databaseId };
  } catch (err) {
    return {
      ok: false,
      databaseId,
      message: err.message,
    };
  }
}

module.exports = { createPage, appendToPage, addReportLog, hasReportLogBySlackUrl, checkReportLogDatabaseAccess };
