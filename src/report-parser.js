const OpenAI = require('openai');

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

/**
 * 確定者の報告メッセージを構造化された報告アイテムに分解する
 * @param {string} text - Slackメッセージのテキスト
 * @param {string} userName - 投稿者名
 * @returns {Promise<object[]>} 構造化された報告アイテムの配列
 */
async function parseReport(text, userName, lastContext = null) {
  const systemPrompt = `あなたはSlackの確定作業チャンネルの投稿を構造化するアシスタントです。
食品アレルギー判定の確定作業における報告メッセージを解析し、個別の報告アイテムに分解してJSON形式で出力してください。

## 報告の種別（type）
- bracket_missing: 【】（親切表示/アレルギー別記）の記載漏れ・追記
- tag_error: AIタグの誤認識（間違ったアレルゲンが付く、タグが付かない等）
- allergen_leak: アレルゲンの漏れ（【】以外の理由でアレルゲンが抜けている）
- status_change: ステータス変更（要確認で返却、問い合わせ依頼等）
- question: 質問・相談
- info: 情報共有・その他

## 出力形式
JSON形式で以下の構造のオブジェクトを出力してください：
{ "items": [ { "company": "会社名", "group": "グループ名またはnull", "product": "品物名", "type": "種別", "detail": "1文の説明", "allergen": "アレルゲン名またはnull" } ] }

## ルール
- 1つのメッセージに複数の報告が含まれる場合、それぞれ別のアイテムにする
- 「作業完了しました」「報告です」等の挨拶部分はスキップ
- 会社名・グループ名が省略されている場合は、同一メッセージ内の直前に登場した値を引き継ぐ
- 会社名が最後まで不明な場合のみ"不明"とする
- group は組織内のブランド・グループ・チェーン名。単独の会社で区分けがない場合はnull
- allergenは食品表示法の義務・推奨アレルゲン品目（卵・乳・小麦・えび・かに・落花生・そば・くるみ・いくら・キウイフルーツ・牛肉・豚肉・鶏肉・さけ・大豆・ごま等）のみ。きのこ・野菜カテゴリ等はnull
- 雑談・挨拶のみ・知識を問う質問・フィードバック意見・シフト代行依頼は items=[] で返す`;

  const contextHint = lastContext
    ? `直前のメッセージの会社名: ${lastContext.company}${lastContext.group ? ` / グループ名: ${lastContext.group}` : ''}\n\n`
    : '';

  const response = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: `${contextHint}${text}\n\nOutput in JSON format.` },
    ],
    temperature: 0,
    response_format: { type: 'json_object' },
  });

  const content = response.choices[0].message.content;
  let parsed;
  try {
    parsed = JSON.parse(content);
  } catch {
    return [];
  }

  const items = Array.isArray(parsed) ? parsed : (parsed.items || parsed.reports || []);

  return items.map((item) => ({
    company:  item.company  || lastContext?.company || '不明',
    group:    item.group    || lastContext?.group   || null,
    product:  item.product  || '不明',
    type:     item.type     || 'info',
    detail:   item.detail   || '',
    allergen: item.allergen || null,
    reporter: userName,
  }));
}

/**
 * メッセージが確定作業の報告っぽいかを簡易判定する（OpenAI呼び出し前のフィルタ）
 * @param {string} text
 * @returns {boolean}
 */
function looksLikeReport(text) {
  if (!text || text.length < 20) return false;

  // 除外: 質問・意見・雑談パターン（OpenAI呼び出しを節約）
  const excludePatterns = [
    /質問です/,
    /教えていただけ/,
    /みなさんどう思/,
    /ご存知の方/,
    /感想.*意見/,
    /フィードバック/,
    /代行.*お願い/,
    /代行でき/,
  ];
  if (excludePatterns.some((p) => p.test(text))) return false;

  // 報告特有パターン（いずれかにマッチすれば報告とみなす）
  const reportPatterns = [
    /【.*?】.*(?:追記|記載|漏れ|なし|抜け)/,  // 【】と問題の組み合わせ
    /記載[漏もな]れ/,
    /【】(?:追記|記載|漏れ|なし|未記入|未入力)/,
    /タグ.*(?:付き|付か|なし|ない)/,
    /チェック.*(?:外|入|は[ず])/,
    /アレルギー.*(?:漏れ|抜け|なし|外)/,
    /確定作業.*(?:完了|報告)/,
    /本日.*報告/,
    /以下.*報告/,
    /要確認.*(?:返却|で返)/,
    /問い合わせ.*(?:依頼|対象|先)/,
    /判定(?:済|根拠|保留)/,
    /規格書.*(?:【|漏|抜|追)/,
    /[＜■〇].+/,                 // ＜顧客名＞ or ■顧客名 or 〇商品名
    /未確定/,
    /確定しました/,
  ];

  return reportPatterns.some((pattern) => pattern.test(text));
}

module.exports = { parseReport, looksLikeReport };
