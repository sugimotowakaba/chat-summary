#!/usr/bin/env node
/**
 * 報告ログDBの全ページをアーカイブ（削除）する一時スクリプト
 * Usage: node src/delete-all-report-logs.js [--db-id <dbId>]
 */
require('dotenv').config();
const { Client } = require('@notionhq/client');

const args = process.argv.slice(2);
const dbIdArg = args.indexOf('--db-id');
const databaseId = dbIdArg >= 0 ? args[dbIdArg + 1] : process.env.NOTION_REPORT_LOG_DB_ID;

if (!databaseId) {
  console.error('DB IDが指定されていません。--db-id か NOTION_REPORT_LOG_DB_ID を設定してください。');
  process.exit(1);
}

const notion = new Client({ auth: process.env.NOTION_TOKEN });

async function deleteAll() {
  console.log(`DB: ${databaseId} の全ページをアーカイブします...`);
  let total = 0;
  let cursor;

  do {
    const res = await notion.databases.query({
      database_id: databaseId,
      start_cursor: cursor,
      page_size: 100,
    });

    for (const page of res.results) {
      await notion.pages.update({ page_id: page.id, archived: true });
      total++;
      if (total % 10 === 0) process.stdout.write(`\r  削除済み: ${total}`);
    }

    cursor = res.has_more ? res.next_cursor : undefined;
  } while (cursor);

  console.log(`\n完了: ${total} 件削除しました。`);
}

deleteAll().catch((err) => {
  console.error('エラー:', err.message);
  process.exit(1);
});
