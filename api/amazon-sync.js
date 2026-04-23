import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

if (!getApps().length) {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}");
  initializeApp({ credential: cert(serviceAccount) });
}
const db = getFirestore();

const MARKETPLACE_ID = "A1VC38T7YXB528"; // 日本
const SP_API_BASE = "https://sellingpartnerapi-fe.amazon.com";

async function getAmazonAuth() {
  const snap = await db.collection("settings").limit(1).get();
  if (snap.empty) return null;
  const s = snap.docs[0].data();
  const clientId = (s.amazonClientId || "").trim();
  const clientSecret = (s.amazonClientSecret || "").trim();
  const refreshToken = (s.amazonRefreshToken || "").trim();
  if (!clientId || !clientSecret || !refreshToken) return null;
  return { clientId, clientSecret, refreshToken };
}

async function getAccessToken({ clientId, clientSecret, refreshToken }) {
  const res = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: refreshToken,
      client_id: clientId,
      client_secret: clientSecret,
    }),
  });
  const data = await res.json();
  if (!res.ok || !data.access_token) {
    throw new Error(`Amazon token error: ${JSON.stringify(data).slice(0, 300)}`);
  }
  return data.access_token;
}

// OrderMetrics API: 日別売上集計を1リクエストで取得
async function getOrderMetrics(accessToken, startDate, endDate) {
  const params = new URLSearchParams({
    marketplaceIds: MARKETPLACE_ID,
    interval: `${startDate}T00:00:00+09:00--${endDate}T23:59:59+09:00`,
    granularity: "Day",
  });

  const url = `${SP_API_BASE}/sales/v1/orderMetrics?${params}`;
  let res, text;
  for (let retry = 0; retry < 5; retry++) {
    res = await fetch(url, {
      headers: { "x-amz-access-token": accessToken },
    });
    text = await res.text();
    if (res.status === 429) {
      const wait = Math.min(2000 * Math.pow(2, retry), 30000);
      await new Promise(r => setTimeout(r, wait));
      continue;
    }
    break;
  }

  let data;
  try { data = JSON.parse(text); } catch { throw new Error(`Amazon parse error: ${text.slice(0, 300)}`); }
  if (!res.ok) {
    throw new Error(`Amazon API ${res.status}: ${text.slice(0, 300)}`);
  }

  return data.payload || [];
}

function aggregateMetrics(metrics) {
  const daily = {};
  for (const m of metrics) {
    // interval: "2026-03-01T00:00:00+09:00--2026-03-02T00:00:00+09:00"
    const dateStr = m.interval.split("T")[0];
    const amount = Number(m.totalSales?.amount || 0);
    const count = m.orderCount || 0;
    if (amount > 0 || count > 0) {
      daily[dateStr] = { amount, count };
    }
  }
  return daily;
}

async function saveDailyData(daily) {
  const batch = db.batch();
  for (const [date, data] of Object.entries(daily)) {
    const docId = `${date}_amazon`;
    batch.set(db.collection("externalSales").doc(docId), {
      source: "amazon",
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
  if (req.method === "GET") {
    const authHeader = req.headers.authorization;
    if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }

  try {
    const auth = await getAmazonAuth();
    if (!auth) {
      return res.status(200).json({ ok: false, message: "Amazon API認証情報が未設定です" });
    }

    const accessToken = await getAccessToken(auth);

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
      // cron: 過去30日分（返品・キャンセル反映のため）
      const d = new Date(nowJST);
      d.setDate(d.getDate() - 30);
      startDate = d.toISOString().split("T")[0];
      endDate = todayJST;
    }

    const metrics = await getOrderMetrics(accessToken, startDate, endDate);
    const daily = aggregateMetrics(metrics);
    await saveDailyData(daily);
    const totalOrders = Object.values(daily).reduce((a, d) => a + d.count, 0);
    const totalDays = Object.keys(daily).length;

    return res.status(200).json({
      ok: true,
      source: "amazon",
      period: { start: startDate, end: endDate },
      totalOrders,
      totalDays,
      summary: daily,
    });
  } catch (error) {
    console.error("amazon-sync error:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}
