// 納品書をコピーして取引先を変えるとき、宛先（取引先）と差出人（事業部）が本当に変わるかを、実際の画面を操作して確かめる。
// ★ エミュレータ専用。接続先が localhost でなければ即中止する（顧客本番 sizukaproduct を触らない）。
//
//   1) npm run emu
//   2) npm run dev:emu
//   3) node scripts/e2e-client-picker-emulator.mjs
//
// 旧: 検索欄に新しい会社名を打つと、リストの表示は「選択してください」になるのに中身はコピー元の取引先のままで、
//     そのまま保存するとコピー元の取引先で保存された。取引先を選び直しても、事業部はコピー元のまま残った。
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

let passed = 0, failed = 0;
const ok = (name, cond, extra = "") => {
  if (cond) { passed++; console.log(`  OK   ${name}`); }
  else { failed++; console.log(`  FAIL ${name} ${extra}`); }
};

async function wipe() {
  for (const c of ["clients", "deliveries", "invoices", "clientBalances", "counters", "pendingBillings", "settings", "divisions", "quotations", "users"]) {
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
  await db.collection("divisions").doc("DV1").set({ name: "第一事業部", createdAt: ts() });
  await db.collection("divisions").doc("DV2").set({ name: "第二事業部", createdAt: ts() });
  // 締め請求にして、保存しても請求書は自動で作らない（納品書だけを見る）
  const cl = (name, divisionId) => ({ name, kana: name, billingType: "closing", closingDays: [0], sendMode: "manual", paymentMonths: 1, divisionId, createdAt: ts() });
  await db.collection("clients").doc("C1").set(cl("コピー元商事", "DV1"));
  await db.collection("clients").doc("C2").set(cl("新しい物産", ""));
  await db.collection("clients").doc("C3").set(cl("三番目工業", "DV2"));
  await db.collection("deliveries").doc("D1").set({
    docNo: "NO-209901-001", clientId: "C1", divisionId: "DV1", date: "2099-01-05",
    items: [{ name: "テスト品", qty: 1, unit: "個", price: 1000, taxRate: 10 }], subtotal: 1000, tax: 100, total: 1100,
    status: "unissued", notes: "", createdAt: ts(),
  });
}

const dialogs = [];
const delsOf = async (clientId) => (await db.collection("deliveries").where("clientId", "==", clientId).get()).docs.map(d => d.data());

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
  await page.click('button:has-text("📦 納品書一覧")');
  await page.waitForTimeout(800);

  const search = page.locator('input[placeholder="取引先を検索..."]');
  const clientSel = page.locator('input[placeholder="取引先を検索..."] + select');
  const divSel = page.locator('select:has(option:has-text("指定なし（設定の自社情報を使用）"))');
  const shown = (sel) => sel.evaluate(s => s.options[s.selectedIndex]?.text ?? "");
  const copy = async () => {
    await page.locator("tbody tr").filter({ hasText: "NO-209901-001" }).getByRole("button", { name: "コピー", exact: true }).click();
    await search.waitFor();
  };
  const save = async (label) => { await page.getByRole("button", { name: label, exact: true }).click(); await page.waitForTimeout(2000); };

  console.log("\n[1] コピーして、検索欄に新しい会社名を打っただけで保存する");
  await copy();
  ok("コピー直後は、コピー元の取引先が選ばれている", (await shown(clientSel)) === "コピー元商事", await shown(clientSel));
  await search.fill("新しい");
  await page.waitForTimeout(300);
  ok("リストの表示は「選択してください」", (await shown(clientSel)) === "選択してください", await shown(clientSel));
  const nD = dialogs.length;
  await save("更新");
  ok("「取引先を選択してください」で止まる（旧: コピー元の取引先で黙って保存）", dialogs.slice(nD).some(m => m.includes("取引先を選択してください")), JSON.stringify(dialogs.slice(nD)));
  ok("コピー元の取引先に納品書が増えていない", (await delsOf("C1")).length === 1, JSON.stringify((await delsOf("C1")).length));

  console.log("\n[2] 続けて、リストで新しい会社を選んで保存する");
  await clientSel.selectOption("C2");
  await page.waitForTimeout(300);
  ok("事業部は「指定なし」に変わる（旧: コピー元の第一事業部のまま）", (await divSel.inputValue()) === "", await shown(divSel));
  await save("更新");
  const c2 = await delsOf("C2");
  ok("新しい会社に納品書が1枚できる", c2.length === 1, JSON.stringify(c2.length));
  ok("その納品書の事業部は空（差出人が設定の自社情報になる）", c2[0]?.divisionId === "", JSON.stringify(c2[0]?.divisionId));
  ok("コピー元の取引先の納品書は1枚のまま", (await delsOf("C1")).length === 1);

  console.log("\n[3] コピーして、検索せずにリストで別の会社を選ぶ");
  await copy();
  await clientSel.selectOption("C3");
  await page.waitForTimeout(300);
  ok("事業部は選んだ会社の第二事業部になる", (await divSel.inputValue()) === "DV2", await shown(divSel));
  await save("更新");
  const c3 = await delsOf("C3");
  ok("三番目工業に納品書が1枚・事業部 DV2", c3.length === 1 && c3[0]?.divisionId === "DV2", JSON.stringify(c3.map(d => d.divisionId)));

  console.log("\n[4] コピーして、今の会社に当てはまる言葉で検索する（選択は外さない）");
  await copy();
  await search.fill("コピー元");
  await page.waitForTimeout(300);
  ok("コピー元の取引先が選ばれたまま", (await shown(clientSel)) === "コピー元商事", await shown(clientSel));
  ok("事業部もそのまま（第一事業部）", (await divSel.inputValue()) === "DV1", await shown(divSel));
  await save("更新");
  const c1 = await delsOf("C1");
  ok("コピー元の取引先に、同じ事業部で納品書が1枚増える", c1.length === 2 && c1.every(d => d.divisionId === "DV1"), JSON.stringify(c1.map(d => d.divisionId)));

  console.log("\n[5] 新しく作る画面で、先に事業部を選んでから、事業部の決まっていない取引先を選ぶ（これまでどおり）");
  await page.getByRole("button", { name: /納品書を作成|新規作成|＋/ }).first().click();
  await search.waitFor();
  await divSel.selectOption("DV2");
  await clientSel.selectOption("C2");
  await page.waitForTimeout(300);
  ok("先に選んだ事業部が残る", (await divSel.inputValue()) === "DV2", await shown(divSel));
  await page.getByRole("button", { name: "キャンセル", exact: true }).click();
  await page.waitForTimeout(500);

  const failAlerts = dialogs.filter(m => /失敗|エラー/.test(m));
  ok("エラーの通知が出ていない", failAlerts.length === 0, JSON.stringify(failAlerts));
  ok("画面のエラー（pageerror）がない", pageErrors.length === 0, JSON.stringify(pageErrors));
  await browser.close();
  console.log(`\n結果: ${passed} OK / ${failed} FAIL`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
