#!/usr/bin/env node
/**
 * 報告ログDBに会社名・グループ名・品物名の列を追加する一時スクリプト
 */
require('dotenv').config();
const { Client } = require('@notionhq/client');

const databaseId = process.argv[2] || process.env.NOTION_REPORT_LOG_DB_ID;
if (!databaseId) {
  console.error('DB IDが指定されていません。');
  process.exit(1);
}

const notion = new Client({ auth: process.env.NOTION_TOKEN });

async function main() {
  const db = await notion.databases.retrieve({ database_id: databaseId });
  const existing = Object.keys(db.properties);
  console.log('既存の列:', existing.join(', '));

  const toAdd = {};
  if (!existing.includes('会社名')) toAdd['会社名'] = { rich_text: {} };
  if (!existing.includes('グループ名')) toAdd['グループ名'] = { rich_text: {} };
  if (!existing.includes('品物名')) toAdd['品物名'] = { rich_text: {} };

  if (Object.keys(toAdd).length === 0) {
    console.log('追加する列はありません（全て既存）。');
    return;
  }

  await notion.databases.update({
    database_id: databaseId,
    properties: toAdd,
  });

  console.log('追加しました:', Object.keys(toAdd).join(', '));
}

main().catch((err) => {
  console.error('エラー:', err.message);
  process.exit(1);
});
