/**
 * backfill-closing-period.cjs
 * 締め請求(closing/monthly)で closingPeriod が欠落している既存請求書に、
 * closingPeriod を「追加」する補正スクリプト（加算のみ・既存値は上書きしない）。
 *
 * モード:
 *   node scripts/backfill-closing-period.cjs backup   … invoices全件をJSONバックアップ（読み取りのみ）
 *   node scripts/backfill-closing-period.cjs dryrun    … 補正候補を一覧表示（書き込みなし）※デフォルト
 *   node scripts/backfill-closing-period.cjs apply --yes … 本適用（自動バックアップ→auto候補のみ書き込み）
 *
 * 復元は App.jsx の getClosingPeriod / getAllClosingPeriods を移植して整合性を担保。
 * 期間が一意に定まらない請求書は自動補正せず「要手動確認(MANUAL)」に隔離する。
 */
const fs = require("fs");
const path = require("path");

const MODE = (process.argv[2] || "dryrun").toLowerCase();
const CONFIRM = process.argv.includes("--yes");
const APP_ROOT = path.join(__dirname, "..");
const BACKUP_DIR = path.join(APP_ROOT, "_backups");

// ── 認証情報を .env.production から取得（秘密はログに出さない） ──────────────
function loadServiceAccount() {
  const envPath = path.join(APP_ROOT, ".env.production");
  const raw = fs.readFileSync(envPath, "utf8");
  const line = raw.split(/\r?\n/).find((l) => l.startsWith("FIREBASE_SERVICE_ACCOUNT="));
  if (!line) throw new Error("FIREBASE_SERVICE_ACCOUNT が .env.production に見つかりません");
  let v = line.slice("FIREBASE_SERVICE_ACCOUNT=".length).trim();
  const tries = [];
  tries.push(v); // そのまま
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
    const inner = v.slice(1, -1);
    tries.push(inner);
    tries.push(inner.replace(/\\"/g, '"').replace(/\\n/g, "\n").replace(/\\\\/g, "\\"));
  }
  for (const t of tries) {
    try { return JSON.parse(t); } catch (_) { /* next */ }
  }
  throw new Error("FIREBASE_SERVICE_ACCOUNT のJSON解析に失敗しました");
}

// ── App.jsx から移植（挙動を一致させる） ──────────────────────────────────
function getClosingPeriod(yearMonth, closingDay, prevClosingDay) {
  const [y, m] = yearMonth.split("-").map(Number);
  const pad = (n) => String(n).padStart(2, "0");
  const lastDay = new Date(y, m, 0).getDate();
  const endDay = closingDay === 0 ? lastDay : Math.min(closingDay, lastDay);
  const end = `${y}-${pad(m)}-${pad(endDay)}`;
  let start;
  if (prevClosingDay === undefined || prevClosingDay === null) {
    start = `${y}-${pad(m)}-01`;
  } else {
    const startDay = (prevClosingDay === 0 ? new Date(y, m - 1, 0).getDate() : prevClosingDay) + 1;
    if (prevClosingDay === 0) start = `${y}-${pad(m)}-01`;
    else if (startDay > lastDay) start = end;
    else start = `${y}-${pad(m)}-${pad(startDay)}`;
  }
  return { start, end, closingDay };
}
function getAllClosingPeriods(yearMonth, closingDays) {
  if (!closingDays || !closingDays.length) return [];
  const sorted = [...closingDays].sort((a, b) => (a === 0 ? 32 : a) - (b === 0 ? 32 : b));
  return sorted.map((cd, i) => getClosingPeriod(yearMonth, cd, i === 0 ? null : sorted[i - 1]));
}

function ym(dateStr) { return (dateStr || "").slice(0, 7); }

async function main() {
  const admin = require("firebase-admin");
  if (!admin.apps.length) admin.initializeApp({ credential: admin.credential.cert(loadServiceAccount()) });
  const db = admin.firestore();

  const [invSnap, delSnap, cliSnap] = await Promise.all([
    db.collection("invoices").get(),
    db.collection("deliveries").get(),
    db.collection("clients").get(),
  ]);
  const invoices = invSnap.docs.map((d) => ({ id: d.id, ...d.data() }));
  const delByDocNo = {};
  delSnap.docs.forEach((d) => { const x = d.data(); if (x.docNo) delByDocNo[x.docNo] = x; });
  const cliById = {};
  cliSnap.docs.forEach((d) => { cliById[d.id] = d.data(); });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");

  console.log(`\n[接続OK] invoices=${invoices.length} deliveries=${delSnap.size} clients=${cliSnap.size}`);

  // ── バックアップ（backup / apply 時） ──
  if (MODE === "backup" || MODE === "apply") {
    if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });
    const file = path.join(BACKUP_DIR, `invoices-backup-${stamp}.json`);
    fs.writeFileSync(file, JSON.stringify(invoices, null, 2), "utf8");
    console.log(`[バックアップ] invoices 全${invoices.length}件 → ${file}`);
    if (MODE === "backup") return;
  }

  // ── 補正候補の抽出と復元 ──
  const targets = invoices.filter(
    (i) => (i.billingType === "closing" || i.billingType === "monthly") &&
           !(i.closingPeriod && i.closingPeriod.start && i.closingPeriod.end)
  );

  const auto = [], manual = [];
  for (const inv of targets) {
    const clientName = (cliById[inv.clientId] && cliById[inv.clientId].name) || inv.clientId || "?";
    const refs = inv.deliveryRefs && inv.deliveryRefs.length ? inv.deliveryRefs
      : (inv.deliveryRef ? [inv.deliveryRef] : []);
    const dates = refs.map((r) => delByDocNo[r] && delByDocNo[r].date).filter(Boolean).sort();
    const rec = { docNo: inv.docNo, id: inv.id, clientName, issueDate: inv.date, closingDay: inv.closingDay,
      delRange: dates.length ? `${dates[0]}~${dates[dates.length - 1]}` : "(納品日なし)" };

    if (!dates.length) { manual.push({ ...rec, reason: "紐づく納品書の日付が取得できない" }); continue; }
    const months = [...new Set(dates.map(ym))];
    if (months.length !== 1) { manual.push({ ...rec, reason: `納品が複数月にまたがる(${months.join(",")})` }); continue; }

    const closingDays = (cliById[inv.clientId] && cliById[inv.clientId].closingDays && cliById[inv.clientId].closingDays.length)
      ? cliById[inv.clientId].closingDays : [0];
    const periods = getAllClosingPeriods(months[0], closingDays);
    const contains = periods.filter((p) => dates.every((d) => d >= p.start && d <= p.end));
    let chosen = contains.find((p) => p.closingDay === inv.closingDay) || (contains.length === 1 ? contains[0] : null);

    if (!chosen) { manual.push({ ...rec, reason: `期間が一意に定まらない(候補${contains.length}件)` }); continue; }
    auto.push({ ...rec, period: { start: chosen.start, end: chosen.end } });
  }

  // ── 出力 ──
  console.log(`\n===== 補正候補 (closing/monthly で closingPeriod 欠落) =====`);
  console.log(`対象総数: ${targets.length}  → 自動補正可: ${auto.length}  要手動確認: ${manual.length}\n`);
  console.log(`--- 自動補正可 (${auto.length}件) ---`);
  auto.forEach((r) => console.log(
    `  ${r.docNo} | ${r.clientName} | 発行:${r.issueDate} 締日:${r.closingDay} | 納品:${r.delRange} | → 締め期間 ${r.period.start}~${r.period.end}`));
  if (manual.length) {
    console.log(`\n--- 要手動確認 (${manual.length}件・自動補正しません) ---`);
    manual.forEach((r) => console.log(`  ${r.docNo} | ${r.clientName} | 発行:${r.issueDate} | 納品:${r.delRange} | 理由:${r.reason}`));
  }

  // 影響レポート（売上集計の月移動）
  const moves = auto.filter((r) => ym(r.issueDate) !== ym(r.period.end));
  console.log(`\n[影響] 売上集計で計上月が変わる請求書: ${moves.length}件`);
  moves.forEach((r) => console.log(`  ${r.docNo} | ${r.clientName} | ${ym(r.issueDate)} → ${ym(r.period.end)}`));

  if (MODE === "dryrun") {
    console.log(`\n[dryrun] 書き込みはしていません。適用は apply --yes で。`);
    return;
  }

  // ── 本適用 ──
  if (MODE === "apply") {
    if (!CONFIRM) { console.log(`\n[中止] apply には --yes が必要です。`); return; }
    if (!auto.length) { console.log(`\n[適用なし] 自動補正対象がありません。`); return; }
    const audit = [];
    let batch = db.batch(), n = 0;
    for (const r of auto) {
      batch.update(db.collection("invoices").doc(r.id), { closingPeriod: { start: r.period.start, end: r.period.end } });
      audit.push({ id: r.id, docNo: r.docNo, added_closingPeriod: r.period });
      if (++n % 400 === 0) { await batch.commit(); batch = db.batch(); }
    }
    await batch.commit();
    const auditFile = path.join(BACKUP_DIR, `backfill-audit-${stamp}.json`);
    fs.writeFileSync(auditFile, JSON.stringify(audit, null, 2), "utf8");
    console.log(`\n[適用完了] ${auto.length}件に closingPeriod を追加。監査ログ → ${auditFile}`);
    console.log(`[取消方法] 上記IDの closingPeriod を削除すれば元に戻せます（加算のみのため）。`);
  }
}

main().catch((e) => { console.error("[エラー]", e.message); process.exit(1); });
