import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

if (!getApps().length) {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}");
  initializeApp({ credential: cert(serviceAccount) });
}
const db = getFirestore();

const API_BASE = "https://api.shop-pro.jp/v1";

async function getColorMeAuth() {
  const snap = await db.collection("settings").limit(1).get();
  if (snap.empty) return null;
  const s = snap.docs[0].data();
  const accessToken = (s.colormeAccessToken || "").trim();
  if (!accessToken) return null;
  return accessToken;
}

// 受注一覧取得（ページネーション対応）
async function fetchSales(accessToken, startDate, endDate) {
  const allSales = [];
  let offset = 0;
  const limit = 100;
  while (true) {
    const params = new URLSearchParams({
      after: startDate,
      before: `${endDate} 23:59:59`,
      limit: String(limit),
      offset: String(offset),
      canceled: "false",
    });
    const res = await fetch(`${API_BASE}/sales?${params}`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); } catch { throw new Error(`ColorMe parse error: ${text.slice(0, 300)}`); }
    if (!res.ok) {
      throw new Error(`ColorMe API ${res.status}: ${text.slice(0, 300)}`);
    }
    const sales = data.sales || [];
    allSales.push(...sales);
    const total = data.meta?.total || 0;
    offset += limit;
    if (offset >= total || sales.length === 0) break;
  }
  return allSales;
}

// 注文を日別に集計
function aggregateSales(sales) {
  const daily = {};
  for (const sale of sales) {
    if (sale.canceled) continue;
    // make_date はUnixタイムスタンプ → JST日付に変換
    const dateObj = new Date((sale.make_date || 0) * 1000 + 9 * 60 * 60 * 1000);
    const dateStr = dateObj.toISOString().split("T")[0];
    if (!daily[dateStr]) daily[dateStr] = { amount: 0, count: 0 };
    daily[dateStr].amount += Number(sale.product_total_price || 0);
    daily[dateStr].count += 1;
  }
  return daily;
}

// Firestoreに保存
async function saveDailyData(daily) {
  const batch = db.batch();
  for (const [date, data] of Object.entries(daily)) {
    const docId = `${date}_colorme`;
    batch.set(db.collection("externalSales").doc(docId), {
      source: "colorme",
      date,
      totalAmount: data.amount,
      orderCount: data.count,
      syncedAt: FieldValue.serverTimestamp(),
    });
  }
  if (Object.keys(daily).length > 0) {
    await batch.commit();
  }
  return daily;
}

export default async function handler(req, res) {
  // cron（GET）は CRON_SECRET で認証
  if (req.method === "GET") {
    const authHeader = req.headers.authorization;
    if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }

  try {
    const accessToken = await getColorMeAuth();
    if (!accessToken) {
      return res.status(200).json({ ok: false, message: "カラーミーAPI認証情報が未設定です" });
    }

    // JST基準で日付を算出
    const nowJST = new Date(Date.now() + 9 * 60 * 60 * 1000);
    const todayJST = nowJST.toISOString().split("T")[0];

    let startDate, endDate;
    if (req.method === "POST") {
      const { mode, start, end } = req.body || {};
      if (mode === "range" && start && end) {
        startDate = start;
        endDate = end;
      } else {
        startDate = todayJST;
        endDate = todayJST;
      }
    } else {
      // cron: 前日分
      const d = new Date(nowJST);
      d.setDate(d.getDate() - 1);
      startDate = d.toISOString().split("T")[0];
      endDate = startDate;
    }

    const sales = await fetchSales(accessToken, startDate, endDate);
    const daily = aggregateSales(sales);
    await saveDailyData(daily);
    const totalOrders = Object.values(daily).reduce((a, d) => a + d.count, 0);
    const totalDays = Object.keys(daily).length;

    return res.status(200).json({
      ok: true,
      source: "colorme",
      period: { start: startDate, end: endDate },
      totalOrders,
      totalDays,
      summary: daily,
    });
  } catch (error) {
    console.error("colorme-sync error:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}
