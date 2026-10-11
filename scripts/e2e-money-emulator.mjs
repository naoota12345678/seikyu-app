// お金まわり（番号・削除・取り消し・残高の戻し）を、実際の画面を操作して確かめる。
// ★ エミュレータ専用。接続先が localhost でなければ即中止する（顧客本番 sizukaproduct を触らない）。
//
//   1) npx -y firebase-tools@15.26.0 emulators:start --only firestore,auth --project sizukaproduct
//   2) npx vite --mode emulator --port 5173 --strictPort
//   3) node scripts/e2e-money-emulator.mjs
//
// 伝票番号が重複している本番データの形（同じ NO- 番号の納品書が別の取引先に2枚）を入れて、
// 削除・取り消し・発行し直しが別の取引先の書類を巻き込まないか、番号を使い回さないか、
// 期首残高を消さないかを見る。
import { chromium } from "@playwright/test";
import { createRequire } from "module";
const require = createRequire(import.meta.url);

const FIRESTORE_HOST = process.env.FIRESTORE_EMULATOR_HOST || "127.0.0.1:8080";
const AUTH_HOST = process.env.FIREBASE_AUTH_EMULATOR_HOST || "127.0.0.1:9099";
const isLocal = (h) => /^(127\.0\.0\.1|localhost|\[::1\]):\d+$/.test(h);
if (!isLocal(FIRESTORE_HOST) || !isLocal(AUTH_HOST)) {
  console.error("中止: エミュレータ以外を指しています", { FIRESTORE_HOST, AUTH_HOST });
  process.exit(1);
}
process.env.FIRESTORE_EMULATOR_HOST = FIRESTORE_HOST;
process.env.FIREBASE_AUTH_EMULATOR_HOST = AUTH_HOST;

const admin = require("firebase-admin");
admin.initializeApp({ projectId: "sizukaproduct" });
const db = admin.firestore();
const ts = () => admin.firestore.FieldValue.serverTimestamp();
const APP = "http://localhost:5173";

const now = new Date();
const YM = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}`;
const D = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-05`;
const NO = (n) => `NO-${YM}-${String(n).padStart(3, "0")}`;
const INV = (n) => `INV-${YM}-${String(n).padStart(3, "0")}`;
// 今月の番号の流れ（[D] の発行し直し）に影響させないため、[F] の請求書は前月の番号にする
const pm = new Date(now.getFullYear(), now.getMonth() - 1, 1);
const PINV = (n) => `INV-${pm.getFullYear()}${String(pm.getMonth() + 1).padStart(2, "0")}-${String(n).padStart(3, "0")}`;

let passed = 0, failed = 0;
const ok = (name, cond, extra = "") => {
  if (cond) { passed++; console.log(`  OK   ${name}`); }
  else { failed++; console.log(`  FAIL ${name} ${extra}`); }
};
const get = async (col, id) => { const s = await db.collection(col).doc(id).get(); return s.exists ? s.data() : null; };
const bal = (id) => get("clientBalances", id);

async function wipe() {
  for (const c of ["clients", "deliveries", "invoices", "clientBalances", "counters", "pendingBillings", "settings", "sendHistory", "paymentHistory", "recurringBillings", "users"]) {
    const snap = await db.collection(c).get();
    for (const d of snap.docs) await d.ref.delete();
  }
}

async function seed() {
  const auth = admin.auth();
  const email = "test@example.com", password = "test1234";
  let uid;
  try { uid = (await auth.getUserByEmail(email)).uid; } catch { uid = (await auth.createUser({ email, password })).uid; }
  await db.collection("users").doc(uid).set({ email, role: "admin", name: "検証用" });
  await db.collection("settings").doc("company").set({ name: "検証用テスト会社", approvalEnabled: false });

  const cl = (name, extra = {}) => ({ name, kana: name, billingType: "immediate", sendMode: "manual", paymentMonths: 1, divisionId: "", createdAt: ts(), ...extra });
  await db.collection("clients").doc("C1").set(cl("テスト商事"));
  await db.collection("clients").doc("C2").set(cl("別会社サンプル"));
  await db.collection("clients").doc("C3").set(cl("締め株式会社", { billingType: "closing", closingDays: [0] }));
  const b = (clientId, openingBalance, currentBalance) => ({ clientId, openingBalance, openingDate: "2026-01-01", currentBalance, paidAmount: 0, prevBalance: 0 });
  await db.collection("clientBalances").doc("C1").set(b("C1", 50000, 700000));
  await db.collection("clientBalances").doc("C2").set(b("C2", 20000, 300000));
  await db.collection("clientBalances").doc("C3").set(b("C3", 10000, 100000));

  const items = (price) => [{ name: "テスト品", qty: 1, unit: "個", price, taxRate: 10 }];
  const del = (id, docNo, clientId, status, invoiceId, price) => db.collection("deliveries").doc(id).set({
    docNo, clientId, divisionId: "", date: D, items: items(price), subtotal: price, tax: price / 10, total: price * 1.1,
    status, ...(invoiceId ? { invoiceId } : {}), notes: "", createdAt: ts(),
  });
  const inv = (id, docNo, clientId, refs, price, extra = {}) => db.collection("invoices").doc(id).set({
    docNo, clientId, date: D, dueDate: "2099-12-31", billingType: "immediate", items: items(price),
    subtotal: price, tax: price / 10, total: price * 1.1, deliveryRef: refs[0], deliveryRefs: refs,
    status: "unpaid", createdAt: ts(), ...extra,
  });
  // 本番で起きうる形: 同じ伝票番号 NO-001 の納品書が別の取引先に2枚
  await del("D1", NO(1), "C1", "invoiced", "I1", 10000);
  await inv("I1", INV(1), "C1", [NO(1)], 10000);
  await del("D2", NO(1), "C2", "invoiced", "I2", 20000);
  await inv("I2", INV(2), "C2", [NO(1)], 20000);
  // 一括削除: 未請求 D3 と請求済 D4 を選ぶ
  await del("D3", NO(2), "C1", "unissued", null, 5000);
  await del("D4", NO(3), "C1", "invoiced", "I3", 30000);
  await inv("I3", INV(3), "C1", [NO(3)], 30000);
  // 締め請求: D5・D6 を1通にまとめた I4
  await del("D5", NO(4), "C3", "invoiced", "I4", 15000);
  await del("D6", NO(5), "C3", "invoiced", "I4", 25000);
  await inv("I4", INV(4), "C3", [NO(4), NO(5)], 40000, { billingType: "closing", closingDay: 0, deliveryRef: "",
    items: [...items(15000), ...items(25000)], deliveryRefItems: JSON.stringify([items(15000), items(25000)]) });
  // 今月最後の番号の請求書 I5（消して発行し直したとき、同じ番号を付けないこと）
  await del("D7", NO(6), "C2", "invoiced", "I5", 50000);
  await inv("I5", INV(5), "C2", [NO(6)], 50000);
  // [F] 黒崎の再現形: 未請求の納品書（invoiceId なし）と、別の取引先の請求済の納品書が同じ番号
  await del("D8", NO(10), "C1", "unissued", null, 7000);
  await del("D9", NO(10), "C2", "invoiced", "I6", 20000);
  await inv("I6", PINV(1), "C2", [NO(10)], 20000);
  await del("D10", NO(11), "C1", "unissued", null, 3000);
  await del("D11", NO(11), "C2", "invoiced", "I7", 40000);
  await inv("I7", PINV(2), "C2", [NO(11)], 40000);
  // invoiceId を刻む前の古いデータ（請求済・invoiceId なし）は、同じ取引先なら今までどおり番号でつながる
  await del("D12", NO(12), "C1", "invoiced", null, 9000);
  await inv("I8", PINV(3), "C1", [NO(12)], 9000);
}

const dialogs = [];
async function nav(page, label) {
  await page.click(`button:has-text("${label}")`);
  await page.waitForTimeout(800);
}
const row = (page, ...texts) => texts.reduce((loc, t) => loc.filter({ hasText: t }), page.locator("tbody tr"));
const settle = (page) => page.waitForTimeout(2000);

(async () => {
  await wipe();
  await seed();
  const browser = await chromium.launch({ channel: "msedge", headless: true });
  const page = await browser.newPage();
  const pageErrors = [];
  page.on("pageerror", e => pageErrors.push(e.message));
  page.on("dialog", async d => { dialogs.push(d.message()); await d.accept(); });

  await page.goto(APP);
  await page.waitForSelector('input[type="email"]', { timeout: 30000 });
  await page.fill('input[type="email"]', "test@example.com");
  await page.fill('input[type="password"]', "test1234");
  await page.click('button[type="submit"]');
  await page.waitForSelector('button:has-text("📦 納品書一覧")', { timeout: 30000 });
  ok("ログインして画面が表示される", true);

  // ---------- A. 重複番号の納品書を1枚削除 → もう片方の取引先の請求書は残る
  console.log("\n[A] 同じ伝票番号の納品書（取引先が別）を1枚削除");
  await nav(page, "📦 納品書一覧");
  await row(page, NO(1), "テスト商事").getByRole("button", { name: "削除", exact: true }).click();
  await settle(page);
  ok("D1 が消えた", !(await get("deliveries", "D1")));
  ok("D1 の請求書 I1 が消えた", !(await get("invoices", "I1")));
  ok("別の取引先の納品書 D2 は残る", !!(await get("deliveries", "D2")));
  ok("別の取引先の請求書 I2 は残る（旧: 番号で引いて消していた）", !!(await get("invoices", "I2")));
  let b1 = await bal("C1"), b2 = await bal("C2");
  ok("C1 の売掛が I1 の分（11,000）だけ減る", b1.currentBalance === 700000 - 11000, JSON.stringify(b1.currentBalance));
  ok("C1 の期首残高が残る", b1.openingBalance === 50000);
  ok("C2 の売掛は変わらない", b2.currentBalance === 300000, JSON.stringify(b2.currentBalance));
  ok("消した番号を使用済みに記録（NO・INV）", (await get("counters", `NO-${YM}`))?.last >= 1 && (await get("counters", `INV-${YM}`))?.last >= 1);

  // ---------- B. 一括削除は未請求だけ
  console.log("\n[B] 未請求と請求済を選んで一括削除");
  await row(page, NO(2)).locator('input[type="checkbox"]').check();
  await row(page, NO(3)).locator('input[type="checkbox"]').check();
  await page.click('button:has-text("一括削除")');
  await settle(page);
  ok("未請求 D3 は消える", !(await get("deliveries", "D3")));
  ok("請求済 D4 は残る", !!(await get("deliveries", "D4")));
  ok("請求済 D4 の請求書 I3 も残る", !!(await get("invoices", "I3")));
  ok("一括削除の確認に「請求済み…1件は削除しません」と出る", dialogs.some(m => m.includes("1件は削除しません")));

  // ---------- C. 今月最後の請求書を削除 → 納品書が未請求に戻り、売掛が戻る
  console.log("\n[C] 今月最後の番号の請求書を削除");
  await nav(page, "🧾 請求書一覧");
  await row(page, INV(5)).getByRole("button", { name: "削除", exact: true }).click();
  await settle(page);
  ok("I5 が消えた", !(await get("invoices", "I5")));
  const d7 = await get("deliveries", "D7");
  ok("納品書 D7 が未請求に戻る", d7?.status === "unissued" && !d7?.invoiceId, JSON.stringify(d7?.status));
  b2 = await bal("C2");
  ok("C2 の売掛が 55,000 減る", b2.currentBalance === 300000 - 55000, JSON.stringify(b2.currentBalance));
  ok("C2 の期首残高が残る", b2.openingBalance === 20000);

  // ---------- D. 発行し直しても同じ番号を付けない
  console.log("\n[D] 同じ納品書から請求書を発行し直す");
  await nav(page, "📦 納品書一覧");
  await row(page, NO(6)).getByRole("button", { name: "請求書発行", exact: true }).click();
  await settle(page);
  const reissued = (await db.collection("invoices").where("clientId", "==", "C2").get()).docs.map(d => d.data().docNo);
  ok(`新しい請求書は ${INV(6)}（${INV(5)} を使い回さない）`, reissued.includes(INV(6)) && !reissued.includes(INV(5)), JSON.stringify(reissued));
  b2 = await bal("C2");
  ok("C2 の売掛が 55,000 戻る・期首残高は残る", b2.currentBalance === 300000 && b2.openingBalance === 20000, JSON.stringify(b2));

  // ---------- E. 締め請求の取り消し
  console.log("\n[E] 締め請求を取り消す");
  await nav(page, "🧾 請求書一覧");
  await row(page, INV(4)).getByRole("button", { name: "締め取消", exact: true }).click();
  await settle(page);
  ok("I4 が消えた", !(await get("invoices", "I4")));
  const d5 = await get("deliveries", "D5"), d6 = await get("deliveries", "D6");
  ok("まとめた納品書 D5・D6 が未請求に戻る", d5?.status === "unissued" && d6?.status === "unissued");
  const b3 = await bal("C3");
  ok("C3 の売掛が 44,000 減る・期首残高は残る", b3.currentBalance === 100000 - 44000 && b3.openingBalance === 10000, JSON.stringify(b3));
  ok("ほかの取引先の請求書 I2・I3 は残る", !!(await get("invoices", "I2")) && !!(await get("invoices", "I3")));
  ok(`INV の使用済みは ${INV(6)} まで`, (await get("counters", `INV-${YM}`))?.last === 6, JSON.stringify(await get("counters", `INV-${YM}`)));

  // ---------- F. 番号が同じ他社の請求書を巻き込まない（黒崎の差し戻し 指摘1）
  console.log("\n[F] 未請求の納品書と、他社の請求済の納品書が同じ番号");
  await nav(page, "📦 納品書一覧");
  let c1 = (await bal("C1")).currentBalance, c2 = (await bal("C2")).currentBalance;
  await row(page, NO(11), "テスト商事").getByRole("button", { name: "編集", exact: true }).click();
  await page.waitForTimeout(800);
  await page.getByRole("button", { name: "更新", exact: true }).first().click();
  await settle(page);
  const i7 = await get("invoices", "I7");
  ok("未請求の納品書を保存しても、他社の請求書 I7 の金額は変わらない", i7?.total === 44000, JSON.stringify(i7?.total));
  ok("他社の請求書 I7 の明細も変わらない", i7?.items?.[0]?.price === 40000, JSON.stringify(i7?.items?.[0]?.price));
  ok("C1・C2 の売掛は変わらない", (await bal("C1")).currentBalance === c1 && (await bal("C2")).currentBalance === c2);

  await row(page, NO(10), "テスト商事").getByRole("button", { name: "削除", exact: true }).click();
  await settle(page);
  ok("未請求の納品書 D8 は消える", !(await get("deliveries", "D8")));
  ok("他社の請求書 I6 は残る", !!(await get("invoices", "I6")));
  ok("他社の納品書 D9 は請求済のまま", (await get("deliveries", "D9"))?.status === "invoiced");
  ok("C2 の売掛は変わらない", (await bal("C2")).currentBalance === c2, JSON.stringify((await bal("C2")).currentBalance));
  ok("削除の確認に他社の請求書の番号が出ない", !dialogs.some(m => m.includes(PINV(1))));

  await row(page, NO(12)).getByRole("button", { name: "削除", exact: true }).click();
  await settle(page);
  ok("古いデータ（invoiceId なし・同じ取引先）は番号でつながり、請求書 I8 も消える", !(await get("invoices", "I8")));
  ok("C1 の売掛が I8 の分（9,900）減る", (await bal("C1")).currentBalance === c1 - 9900, JSON.stringify((await bal("C1")).currentBalance));

  ok("画面のエラー（pageerror）がゼロ", pageErrors.length === 0, JSON.stringify(pageErrors));
  const failAlerts = dialogs.filter(m => /失敗|エラー/.test(m));
  ok("「失敗」「エラー」の通知がゼロ", failAlerts.length === 0, JSON.stringify(failAlerts));
  await browser.close();
  console.log(`\n結果: ${passed} OK / ${failed} FAIL`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
