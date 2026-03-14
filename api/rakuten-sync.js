import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

if (!getApps().length) {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}");
  initializeApp({ credential: cert(serviceAccount) });
}
const db = getFirestore();

const todayStr = () => new Date().toISOString().split("T")[0];

function toJST(date) {
  const d = new Date(date);
  d.setHours(d.getHours() + 9);
  return d;
}

function formatDateJST(date) {
  const d = toJST(date);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

async function getRakutenAuth() {
  const snap = await db.collection("settings").limit(1).get();
  if (snap.empty) return null;
  const s = snap.docs[0].data();
  const secret = (s.rakutenServiceSecret || "").trim();
  const license = (s.rakutenLicenseKey || "").trim();
  if (!secret || !license) return null;
  const authKey = Buffer.from(`${secret}:${license}`).toString("base64");
  return authKey;
}

// searchOrder: 指定期間の注文番号を取得
async function searchOrders(authKey, startDate, endDate) {
  const allOrderNumbers = [];
  let page = 1;
  while (true) {
    const body = {
      dateType: 1, // 注文日
      startDatetime: `${startDate}T00:00:00+0900`,
      endDatetime: `${endDate}T23:59:59+0900`,
      orderProgressList: [100, 200, 300, 400, 500, 600, 700],
      PaginationRequestModel: {
        requestRecordsAmount: 1000,
        requestPage: page,
      },
    };
    const res = await fetch("https://api.rms.rakuten.co.jp/es/2.0/order/searchOrder/", {
      method: "POST",
      headers: {
        "Authorization": `ESA ${authKey}`,
        "Content-Type": "application/json; charset=utf-8",
      },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); } catch { throw new Error(`searchOrder parse error: ${text.slice(0, 200)}`); }
    if (!res.ok) {
      throw new Error(`searchOrder HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    // 注文が0件の場合もエラーではない
    const msgs = data.MessageModelList || [];
    const errMsg = msgs.find(m => m.messageType === "ERROR");
    if (errMsg) {
      throw new Error(`searchOrder: ${errMsg.messageCode} ${errMsg.message}`);
    }
    const orderNumbers = data.orderNumberList || [];
    allOrderNumbers.push(...orderNumbers);
    const totalPages = data.PaginationResponseModel?.totalPages || 1;
    if (page >= totalPages) break;
    page++;
  }
  return allOrderNumbers;
}

// getOrder: 注文詳細を取得（100件ずつ）
async function getOrders(authKey, orderNumbers) {
  const allOrders = [];
  for (let i = 0; i < orderNumbers.length; i += 100) {
    const chunk = orderNumbers.slice(i, i + 100);
    const res = await fetch("https://api.rms.rakuten.co.jp/es/2.0/order/getOrder/", {
      method: "POST",
      headers: {
        "Authorization": `ESA ${authKey}`,
        "Content-Type": "application/json; charset=utf-8",
      },
      body: JSON.stringify({ orderNumberList: chunk, version: 7 }),
    });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); } catch { throw new Error(`getOrder parse error: ${text.slice(0, 200)}`); }
    if (data.OrderModelList) {
      allOrders.push(...data.OrderModelList);
    }
  }
  return allOrders;
}

// 注文を日別に集計してFirestoreに保存
async function aggregateAndSave(orders) {
  const daily = {}; // { "2026-03-12": { amount, count } }
  for (const order of orders) {
    // キャンセル済み除外
    if (order.orderStatus === 999) continue;
    const dateStr = order.orderDatetime ? order.orderDatetime.split("T")[0] : null;
    if (!dateStr) continue;
    if (!daily[dateStr]) daily[dateStr] = { amount: 0, count: 0 };
    // 商品金額 − クーポン値引き = 楽天売上実績（店舗軸）と一致
    let amount = 0;
    if (order.PackageModelList) {
      for (const pkg of order.PackageModelList) {
        if (pkg.ItemModelList) {
          for (const item of pkg.ItemModelList) {
            amount += Number(item.price || 0) * Number(item.units || 1);
          }
        }
      }
    }
    if (amount === 0) amount = Number(order.goodsPrice || order.totalPrice || 0);
    // クーポン値引き額を差し引く
    const coupon = Number(order.couponAllTotalPrice || 0);
    amount -= coupon;
    daily[dateStr].amount += amount;
    daily[dateStr].count += 1;
  }

  const batch = db.batch();
  for (const [date, data] of Object.entries(daily)) {
    const docId = `${date}_rakuten`;
    batch.set(db.collection("externalSales").doc(docId), {
      source: "rakuten",
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

// 期間を30日以内のチャンクに分割
function splitDateRange(startDate, endDate) {
  const ranges = [];
  let current = new Date(startDate);
  const end = new Date(endDate);
  while (current <= end) {
    const chunkEnd = new Date(current);
    chunkEnd.setDate(chunkEnd.getDate() + 62); // 63日間（楽天API上限）
    const actualEnd = chunkEnd > end ? end : chunkEnd;
    ranges.push({
      start: current.toISOString().split("T")[0],
      end: actualEnd.toISOString().split("T")[0],
    });
    current = new Date(actualEnd);
    current.setDate(current.getDate() + 1);
  }
  return ranges;
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
    const authKey = await getRakutenAuth();
    if (!authKey) {
      return res.status(200).json({ ok: false, message: "楽天API認証情報が未設定です" });
    }

    let startDate, endDate;

    if (req.method === "POST") {
      const { mode, start, end } = req.body || {};
      if (mode === "range" && start && end) {
        // 指定期間（フロントから月ごとに呼ばれる）
        startDate = start;
        endDate = end;
      } else {
        // 通常: 今日分
        startDate = todayStr();
        endDate = todayStr();
      }
    } else {
      // cron: 前日分
      const d = new Date();
      d.setDate(d.getDate() - 1);
      startDate = d.toISOString().split("T")[0];
      endDate = startDate;
    }

    const ranges = splitDateRange(startDate, endDate);
    let totalOrders = 0;
    let totalDays = 0;
    const allDaily = {};
    for (const range of ranges) {
      const orderNumbers = await searchOrders(authKey, range.start, range.end);
      if (orderNumbers.length === 0) continue;
      const orders = await getOrders(authKey, orderNumbers);
      const daily = await aggregateAndSave(orders);
      totalOrders += orderNumbers.length;
      Object.entries(daily).forEach(([date, data]) => {
        allDaily[date] = data;
      });
    }
    totalDays = Object.keys(allDaily).length;

    return res.status(200).json({
      ok: true,
      source: "rakuten",
      period: { start: startDate, end: endDate },
      totalOrders,
      totalDays,
      summary: allDaily,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
}
