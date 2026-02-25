#!/usr/bin/env node
/**
 * OCR確定作業CSV × Notionミスログを照合してNotionに一括起票する
 *
 * Usage:
 *   node src/reconcile-to-notion.js --work-csv <作業詳細.csv> [--since YYYY-MM-DD] [--db-id <dbId>] [--dry-run]
 *
 * 環境変数:
 *   NOTION_TOKEN
 *   NOTION_REPORT_LOG_DB_ID  - ミスログDB（照合元。Slackボットが書き込む）
 *   NOTION_RECONCILE_DB_ID   - 起票先DB（省略時は自動作成）
 */
require('dotenv').config();

const fs = require('fs');
const { Client } = require('@notionhq/client');

const notion = new Client({ auth: process.env.NOTION_TOKEN });

// ---------- CLI引数 ----------
function parseArgs() {
  const args = process.argv.slice(2);
  const parsed = { csvFiles: [], since: null, dbId: null, dryRun: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--work-csv' && args[i + 1]) parsed.csvFiles.push(args[++i]);
    else if (args[i] === '--since'   && args[i + 1]) parsed.since = args[++i];
    else if (args[i] === '--db-id'   && args[i + 1]) parsed.dbId  = args[++i];
    else if (args[i] === '--dry-run') parsed.dryRun = true;
  }
  return parsed;
}

// ---------- CSV パーサー（ダブルクォート対応） ----------
function parseCSV(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8').replace(/^\uFEFF/, ''); // BOM除去
  const lines = content.trim().split('\n');
  const headers = lines[0].split(',').map((h) => h.trim());
  return lines.slice(1).filter((l) => l.trim()).map((l) => {
    const values = [];
    let cur = '', inQ = false;
    for (let i = 0; i < l.length; i++) {
      if      (l[i] === '"' && !inQ)               { inQ = true; }
      else if (l[i] === '"' && inQ && l[i+1] === '"') { cur += '"'; i++; }
      else if (l[i] === '"' && inQ)                 { inQ = false; }
      else if (l[i] === ',' && !inQ)                { values.push(cur.trim()); cur = ''; }
      else                                           { cur += l[i]; }
    }
    values.push(cur.trim());
    const row = {};
    headers.forEach((h, i) => { row[h] = values[i] || ''; });
    return row;
  });
}

// ---------- 文字列正規化（表記揺れ吸収） ----------
function normalize(s) {
  return (s || '')
    .replace(/[\s　]/g, '')
    .replace(/＆/g, '&')
    .toLowerCase();
}

// ---------- Notion 報告ログDB を全件取得 ----------
async function queryReportLog(dbId, since) {
  const entries = [];
  let cursor;
  const filter = since
    ? { property: '日付', date: { on_or_after: since } }
    : undefined;
  do {
    const res = await notion.databases.query({
      database_id: dbId,
      filter,
      page_size: 100,
      start_cursor: cursor,
    });
    entries.push(...res.results);
    cursor = res.has_more ? res.next_cursor : null;
  } while (cursor);
  return entries;
}

// ---------- Notionページから照合キーを抽出 ----------
function parseReportPage(page) {
  const p = page.properties;

  // 新スキーマ（会社名・グループ名・品物名カラム）優先、旧スキーマ（タイトルのみ）はフォールバック
  const company = p['会社名']?.rich_text?.[0]?.text?.content || '';
  const group   = p['グループ名']?.rich_text?.[0]?.text?.content || '';
  const product = p['品物名']?.rich_text?.[0]?.text?.content
               || p['商品名']?.rich_text?.[0]?.text?.content
               || '';

  // フォールバック: タイトルを分解
  let groupFb = group, productFb = product;
  if (!group || !product) {
    const titleProp = Object.values(p).find((v) => v.type === 'title');
    const title = titleProp?.title?.[0]?.text?.content || '';
    const parts = title.split(' / ');
    if (parts.length >= 2) {
      groupFb   = groupFb   || parts[parts.length - 2].trim();
      productFb = productFb || parts[parts.length - 1].trim();
    }
  }

  const date     = p['日付']?.date?.start || '';
  const type     = p['種別']?.select?.name || '';
  const reporter = p['報告者']?.rich_text?.[0]?.text?.content || '';

  return {
    company,
    group:   groupFb,
    product: productFb,
    date,
    type,
    reporter,
    pageId: page.id,
  };
}

// ---------- 作業CSVから最も一致する行を返す ----------
function findBestWorkRow(entry, workRows, 作業種別) {
  const ng = normalize(entry.group);
  const nf = normalize(entry.product);

  const scored = workRows
    .filter((r) => r['作業種別'] === 作業種別)
    .map((r) => {
      const wg = normalize(r['グループ名']);
      const wf = normalize(r['加工品名/生鮮品名']);
      let score = 0;
      if (ng && wg) {
        if (ng === wg)                          score += 10;
        else if (wg.includes(ng) || ng.includes(wg)) score += 5;
      }
      if (nf && wf) {
        if (nf === wf)                          score += 10;
        else if (wf.includes(nf) || nf.includes(wf)) score += 4;
        else if (nf.length > 4 && wf.includes(nf.slice(0, 4))) score += 2;
      }
      return { row: r, score };
    })
    .filter((x) => x.score >= 6);

  if (scored.length === 0) return null;

  if (entry.date) {
    const t = new Date(entry.date);
    scored.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return Math.abs(new Date(a.row['作業日時']) - t) - Math.abs(new Date(b.row['作業日時']) - t);
    });
  } else {
    scored.sort((a, b) => b.score - a.score);
  }

  return scored[0].row;
}

// ---------- 起票先DB のスキーマ ----------
const RECONCILE_DB_SCHEMA = {
  名前:               { title: {} },
  法人名:             { rich_text: {} },
  'グループ/店舗名':  { rich_text: {} },
  食べ物名:           { rich_text: {} },
  ミス種別: {
    select: {
      options: [
        { name: '【】漏れ',      color: 'yellow' },
        { name: 'アレルゲン漏れ', color: 'red'    },
        { name: 'タグ誤認識',    color: 'orange'  },
        { name: 'ステータス変更', color: 'blue'   },
        { name: '質問・相談',    color: 'purple'  },
        { name: '情報共有',      color: 'gray'    },
      ],
    },
  },
  OCR作業者:       { rich_text: {} },
  OCR作業日:       { date: {} },
  確定者:          { rich_text: {} },
  最終ステータス: {
    select: {
      options: [
        { name: '判定済',         color: 'green'  },
        { name: '未確定',         color: 'gray'   },
        { name: '要確認',         color: 'yellow' },
        { name: '問い合わせ依頼', color: 'orange' },
      ],
    },
  },
  起票日:   { date: {} },
  商品ID:   { rich_text: {} }, // 重複チェック用
};

// ---------- DB作成（存在しない場合） ----------
async function ensureDatabase(dbId, reportLogDbId) {
  if (dbId) {
    // 存在確認
    try {
      await notion.databases.retrieve({ database_id: dbId });
      return dbId;
    } catch {
      console.error(`[ERROR] 指定のDB (${dbId}) にアクセスできません`);
      process.exit(1);
    }
  }

  // 親ページを報告ログDBから取得
  let parentPageId;
  try {
    const logDb = await notion.databases.retrieve({ database_id: reportLogDbId });
    parentPageId = logDb.parent?.page_id || logDb.parent?.block_id;
  } catch {
    console.error('[ERROR] 報告ログDBの親ページを取得できませんでした。--db-id か NOTION_RECONCILE_DB_ID を設定してください。');
    process.exit(1);
  }

  const today = new Date().toISOString().slice(0, 7); // YYYY-MM
  const res = await notion.databases.create({
    parent: { type: 'page_id', page_id: parentPageId },
    title: [{ type: 'text', text: { content: `OCRミス作業者レポート ${today}` } }],
    properties: RECONCILE_DB_SCHEMA,
  });
  console.log(`  新規DB作成: ${res.id}`);
  console.log(`  → 次回以降は NOTION_RECONCILE_DB_ID=${res.id} を .env に設定してください`);
  return res.id;
}

// ---------- 既存の商品IDセットを取得（重複チェック用） ----------
async function fetchExistingProductIds(dbId) {
  const ids = new Set();
  let cursor;
  do {
    const res = await notion.databases.query({ database_id: dbId, page_size: 100, start_cursor: cursor });
    for (const page of res.results) {
      const id = page.properties['商品ID']?.rich_text?.[0]?.text?.content;
      if (id) ids.add(id);
    }
    cursor = res.has_more ? res.next_cursor : null;
  } while (cursor);
  return ids;
}

// ---------- 1件挿入 ----------
function rich(text) {
  return { rich_text: [{ text: { content: String(text || '') } }] };
}

async function insertRecord(dbId, record) {
  const props = {
    名前:            { title: [{ text: { content: `${record.group} / ${record.product}` } }] },
    法人名:          rich(record.company),
    'グループ/店舗名': rich(record.group),
    食べ物名:        rich(record.product),
    OCR作業者:       rich(record.ocrWorker || '不明'),
    確定者:          rich(record.confirmer),
    商品ID:          rich(record.productId),
  };

  if (record.mistakeType) props['ミス種別'] = { select: { name: record.mistakeType } };
  if (record.finalStatus) props['最終ステータス'] = { select: { name: record.finalStatus } };
  if (record.date) props['起票日'] = { date: { start: record.date } };
  if (record.ocrWorkDate) {
    try {
      const d = new Date(record.ocrWorkDate.replace(' ', 'T'));
      if (!isNaN(d)) props['OCR作業日'] = { date: { start: d.toISOString() } };
    } catch { /* skip */ }
  }

  await notion.pages.create({ parent: { database_id: dbId }, properties: props });
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ---------- メイン ----------
async function main() {
  const { csvFiles, since, dbId: dbIdArg, dryRun } = parseArgs();

  if (csvFiles.length === 0) {
    console.error('使い方: node src/reconcile-to-notion.js --work-csv <作業詳細.csv> [--since YYYY-MM-DD] [--db-id <dbId>] [--dry-run]');
    process.exit(1);
  }

  const reportLogDbId = process.env.NOTION_REPORT_LOG_DB_ID;
  if (!reportLogDbId) {
    console.error('[ERROR] NOTION_REPORT_LOG_DB_ID が未設定です');
    process.exit(1);
  }

  const reconcileDbId = dbIdArg || process.env.NOTION_RECONCILE_DB_ID || null;

  // 作業CSVを読み込み
  const allWorkRows = [];
  for (const f of csvFiles) {
    const rows = parseCSV(f);
    console.log(`作業CSV: ${f}  ${rows.length}件`);
    allWorkRows.push(...rows);
  }
  const ocrRows  = allWorkRows.filter((r) => r['作業種別'] === 'OCR確認作業');
  const confRows = allWorkRows.filter((r) => r['作業種別'] === '確定作業');
  console.log(`  OCR確認: ${ocrRows.length}件 / 確定作業: ${confRows.length}件`);

  // 報告ログDBからミスログを取得
  console.log(`\nNotionミスログDB (${reportLogDbId}) を取得中...${since ? ` since=${since}` : ''}`);
  const pages = await queryReportLog(reportLogDbId, since);
  const entries = pages.map(parseReportPage);
  console.log(`  ${entries.length}件`);

  // 照合
  let matchedOcr = 0, unmatchedOcr = 0;
  const records = entries.map((entry) => {
    const ocrRow  = findBestWorkRow(entry, ocrRows,  'OCR確認作業');
    const confRow = findBestWorkRow(entry, confRows, '確定作業');
    if (ocrRow) matchedOcr++; else unmatchedOcr++;

    return {
      company:      entry.company || ocrRow?.['会社名'] || confRow?.['会社名'] || '',
      group:        entry.group,
      product:      entry.product,
      date:         entry.date,
      mistakeType:  entry.type,
      confirmer:    entry.reporter,
      ocrWorker:    ocrRow?.['作業者名'] || null,
      ocrWorkDate:  ocrRow?.['作業日時'] || '',
      finalStatus:  confRow?.['変更後ステータス'] || ocrRow?.['変更後ステータス'] || '',
      productId:    ocrRow?.['商品ID'] || confRow?.['商品ID'] || '',
    };
  });

  console.log(`\n照合結果: OCR作業者特定=${matchedOcr}件 / 不明=${unmatchedOcr}件`);

  if (unmatchedOcr > 0) {
    console.log('  未マッチ:');
    records.filter((r) => !r.ocrWorker).slice(0, 10).forEach((r) =>
      console.log(`    - [${r.mistakeType}] ${r.group} / ${r.product}`)
    );
    if (unmatchedOcr > 10) console.log(`    ... 他${unmatchedOcr - 10}件`);
  }

  if (dryRun) {
    console.log('\n--- DRY RUN: 書き込みをスキップ ---');
    console.log('先頭5件プレビュー:');
    records.slice(0, 5).forEach((r) =>
      console.log(`  ${r.date} | ${r.mistakeType} | ${r.group} / ${r.product} | OCR: ${r.ocrWorker || '不明'} | 確定: ${r.confirmer}`)
    );
    return;
  }

  // 起票先DBの確保
  console.log('\n起票先DBを確認中...');
  const targetDbId = await ensureDatabase(reconcileDbId, reportLogDbId);

  // 既存の商品IDを取得（重複スキップ）
  console.log('既存レコードを確認中...');
  const existingIds = await fetchExistingProductIds(targetDbId);
  console.log(`  既存: ${existingIds.size}件`);

  // 挿入
  const toInsert = records.filter((r) => !r.productId || !existingIds.has(r.productId));
  const skipped  = records.length - toInsert.length;
  console.log(`\n${toInsert.length}件を起票します（${skipped}件はスキップ）...`);

  let ok = 0, ng = 0;
  for (const record of toInsert) {
    try {
      await insertRecord(targetDbId, record);
      ok++;
      if (ok % 10 === 0) process.stdout.write(`\r  ${ok}/${toInsert.length}件完了...`);
      await sleep(150);
    } catch (err) {
      ng++;
      console.error(`\n  [ERROR] ${record.group} / ${record.product}: ${err.message}`);
    }
  }

  console.log(`\n\n完了! 起票=${ok}件 / スキップ=${skipped}件 / エラー=${ng}件`);
  console.log(`Notion DB: https://notion.so/${targetDbId.replace(/-/g, '')}`);
}

main().catch((err) => {
  console.error('[ERROR]', err.message);
  process.exit(1);
});
