import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

// Firebase Admin初期化
if (!getApps().length) {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}");
  initializeApp({ credential: cert(serviceAccount) });
}
const db = getFirestore();

const todayStr = () => new Date().toISOString().split("T")[0];
const currentMonth = () => todayStr().slice(0, 7);
function calcTaxByRate(items) {
  const groups = {};
  (items || []).forEach(i => {
    const rate = i.taxRate !== undefined && i.taxRate !== null && i.taxRate !== "" ? Number(i.taxRate) : 10;
    const amt = Number(i.qty || 0) * Number(i.price || 0);
    if (!groups[rate]) groups[rate] = { sub: 0, tax: 0 };
    groups[rate].sub += amt;
  });
  Object.keys(groups).forEach(r => { groups[r].tax = Math.floor(groups[r].sub * Number(r) / 100); });
  return groups;
}
function totalFromItems(items) {
  const g = calcTaxByRate(items);
  const sub = Object.values(g).reduce((a, v) => a + v.sub, 0);
  const tax = Object.values(g).reduce((a, v) => a + v.tax, 0);
  return { sub, tax, total: sub + tax };
}

function nextMonthEnd(dateStr) {
  const d = new Date(dateStr || todayStr());
  return new Date(d.getFullYear(), d.getMonth() + 2, 0).toISOString().split("T")[0];
}

async function genDocNo(prefix) {
  const now = new Date();
  const ym = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}`;
  const snap = await db.collection("invoices").get();
  const same = snap.docs.filter(d => (d.data().docNo || "").includes(`${prefix}-${ym}`));
  return `${prefix}-${ym}-${String(same.length + 1).padStart(3, "0")}`;
}

function isTodayClosingDay(closingDay) {
  const d = new Date();
  const todayDay = d.getDate();
  const lastDay = new Date(d.getFullYear(), d.getMonth() + 1, 0).getDate();
  if (closingDay === 0) return todayDay === lastDay;
  return todayDay === closingDay;
}

function getClosingPeriod(closingDay) {
  const now = new Date();
  const y = now.getFullYear(), m = now.getMonth() + 1;
  const pad = (n) => String(n).padStart(2, "0");
  const lastDay = new Date(y, m, 0).getDate();
  const endDay = closingDay === 0 ? lastDay : Math.min(closingDay, lastDay);
  const end = `${y}-${pad(m)}-${pad(endDay)}`;
  const start = `${y}-${pad(m)}-01`;
  return { start, end };
}

async function sendEmail(to, subject, html) {
  const RESEND_API_KEY = process.env.RESEND_API_KEY;
  if (!RESEND_API_KEY || !to) return false;
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${RESEND_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      from: "請求管理システム <invoice@romu.ai>",
      to: Array.isArray(to) ? to : [to],
      subject, html,
    }),
  });
  return res.ok;
}

async function createInvoiceAndProcess({ clientId, divisionId, items, deliveryRefs, deliveryRefItems, deliveryIds, type, sendMode, billingDay }) {
  const { sub, tax, total } = totalFromItems(items);
  const docNo = await genDocNo("INV");

  const invData = {
    docNo, clientId, divisionId: divisionId || "",
    date: todayStr(), dueDate: nextMonthEnd(todayStr()),
    billingType: type === "recurring" ? "recurring" : "closing",
    closingDay: billingDay,
    deliveryRefs: deliveryRefs || [],
    deliveryRefItems: deliveryRefItems || [],
    items, subtotal: sub, tax, total,
    status: "unpaid", createdAt: FieldValue.serverTimestamp(),
  };
  const invRef = await db.collection("invoices").add(invData);

  if (deliveryIds && deliveryIds.length) {
    const batch = db.batch();
    deliveryIds.forEach(id => batch.update(db.collection("deliveries").doc(id), { status: "invoiced", invoiceId: invRef.id }));
    await batch.commit();
  }

  const balDoc = await db.collection("clientBalances").doc(clientId).get();
  const bal = balDoc.exists ? balDoc.data() : {};
  await db.collection("clientBalances").doc(clientId).set({
    clientId, prevBalance: bal.currentBalance || 0,
    currentBalance: (bal.currentBalance || 0) + total,
    paidAmount: bal.paidAmount || 0, updatedAt: FieldValue.serverTimestamp(),
  });

  const clientDoc = await db.collection("clients").doc(clientId).get();
  const client = clientDoc.exists ? clientDoc.data() : {};

  const settingsSnap = await db.collection("settings").limit(1).get();
  const company = settingsSnap.empty ? {} : settingsSnap.docs[0].data();
  let co = company;
  if (divisionId) {
    const divDoc = await db.collection("divisions").doc(divisionId).get();
    if (divDoc.exists) {
      const div = divDoc.data();
      co = { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) };
    }
  }

  if (sendMode === "auto" && client.email) {
    const sent = await sendEmail(client.email,
      `【請求書】${docNo} ${co.name || ""}`,
      `<div style="font-family:sans-serif;color:#333;">
        <p>${client.name || ""} 御中</p>
        <p>いつもお世話になっております。<br>${co.name || ""}です。</p>
        <p>請求書（${docNo}）をお送りいたします。</p>
        <p>金額：&yen;${total.toLocaleString()}</p>
        <p>ご確認のほど、よろしくお願いいたします。</p>
        <hr style="border:none;border-top:1px solid #ddd;margin:20px 0">
        <p style="font-size:12px;color:#888">${co.name || ""}<br>${co.address || ""}<br>TEL ${co.tel || ""}</p>
      </div>`
    );
    if (sent) {
      await db.collection("sendHistory").add({
        docNo, invoiceId: invRef.id, clientId, clientName: client.name || "",
        email: client.email, method: "auto", memo: "cron自動送信",
        amount: total, sentAt: FieldValue.serverTimestamp(), sentBy: "auto",
      });
      await invRef.update({ sentStatus: "sent", lastSentAt: FieldValue.serverTimestamp() });
    }
  } else if (sendMode === "confirm") {
    await db.collection("pendingBillings").add({
      invoiceId: invRef.id, invoiceDocNo: docNo,
      clientId, divisionId: divisionId || "",
      total, type, status: "pending",
      createdAt: FieldValue.serverTimestamp(),
    });
  }

  return { docNo, total };
}

export default async function handler(req, res) {
  const authHeader = req.headers.authorization;
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const results = { closing: [], recurring: [], errors: [] };

  try {
    // 設定を読み取り
    const settingsSnap0 = await db.collection("settings").limit(1).get();
    const settings0 = settingsSnap0.empty ? {} : settingsSnap0.docs[0].data();

    // 1. 締日処理
    const clientsSnap = await db.collection("clients").get();
    const closingClients = clientsSnap.docs.filter(d => {
      const data = d.data();
      return (data.billingType === "closing" || data.billingType === "monthly") && data.sendMode !== "manual";
    });

    for (const clientDoc of closingClients) {
      const client = { id: clientDoc.id, ...clientDoc.data() };
      const cDays = client.closingDays && client.closingDays.length ? client.closingDays : [0];

      for (const cd of cDays) {
        if (!isTodayClosingDay(cd)) continue;

        const period = getClosingPeriod(cd);
        const delsSnap = await db.collection("deliveries")
          .where("clientId", "==", client.id)
          .where("status", "==", "unissued")
          .get();
        const dels = delsSnap.docs.filter(d => {
          const date = d.data().date;
          return date >= period.start && date <= period.end;
        });

        if (!dels.length) continue;

        try {
          const allItems = dels.flatMap(d => d.data().items || []);
          const result = await createInvoiceAndProcess({
            clientId: client.id, divisionId: client.divisionId || "",
            items: allItems,
            deliveryRefs: dels.map(d => d.data().docNo),
            deliveryRefItems: dels.map(d => d.data().items || []),
            deliveryIds: dels.map(d => d.id),
            type: "closing", sendMode: client.sendMode || "manual",
            billingDay: cd,
          });
          results.closing.push({ client: client.name, ...result });
        } catch (e) {
          results.errors.push({ client: client.name, error: e.message });
        }
      }
    }

    // 2. 定期請求処理
    const recurSnap = await db.collection("recurringBillings").get();
    const ym = currentMonth();

    for (const recurDoc of recurSnap.docs) {
      const r = { id: recurDoc.id, ...recurDoc.data() };
      if (!r.enabled) continue;
      if (r.startMonth && ym < r.startMonth) continue;
      if (r.endMonth && ym > r.endMonth) continue;
      if (r.lastIssuedMonth === ym) continue;

      if (r.cycle === "bimonthly") {
        const start = new Date(r.startMonth + "-01");
        const now = new Date(ym + "-01");
        const diff = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
        if (diff % 2 !== 0) continue;
      }
      if (r.cycle === "quarterly") {
        const start = new Date(r.startMonth + "-01");
        const now = new Date(ym + "-01");
        const diff = (now.getFullYear() - start.getFullYear()) * 12 + (now.getMonth() - start.getMonth());
        if (diff % 3 !== 0) continue;
      }

      if (!isTodayClosingDay(r.billingDay || 0)) continue;

      try {
        const items = [{ name: r.itemName, qty: r.qty || 1, unit: r.unit || "", price: r.price || 0, taxRate: r.taxRate !== undefined ? r.taxRate : 10 }];
        if (settings0.recurringApproval) {
          // 承認待ちに追加
          const { sub, tax, total } = totalFromItems(items);
          const clientDoc2 = await db.collection("clients").doc(r.clientId).get();
          const clientName = clientDoc2.exists ? clientDoc2.data().name || "" : "";
          await db.collection("pendingBillings").add({
            type: "invoice", clientId: r.clientId, clientName,
            divisionId: r.divisionId || "",
            items, subtotal: sub, tax, total,
            billingType: "recurring", scheduledSendDate: "",
            status: "pending", createdAt: FieldValue.serverTimestamp(),
          });
          await recurDoc.ref.update({ lastIssuedMonth: ym });
          results.recurring.push({ client: r.clientId, item: r.itemName, pending: true });
        } else {
          const result = await createInvoiceAndProcess({
            clientId: r.clientId, divisionId: r.divisionId || "",
            items, deliveryRefs: [], deliveryRefItems: [], deliveryIds: [],
            type: "recurring", sendMode: r.sendMode || "auto",
            billingDay: r.billingDay || 0,
          });
          await recurDoc.ref.update({ lastIssuedMonth: ym });
          results.recurring.push({ client: r.clientId, item: r.itemName, ...result });
        }
      } catch (e) {
        results.errors.push({ recurring: r.itemName, error: e.message });
      }
    }
    // 3. 送信予定日の請求書を自動送信
    results.scheduled = [];
    const invSnap = await db.collection("invoices")
      .where("sentStatus", "==", "scheduled")
      .where("scheduledSendDate", "==", todayStr())
      .get();

    for (const invDoc of invSnap.docs) {
      const inv = { id: invDoc.id, ...invDoc.data() };
      try {
        const clientDoc = await db.collection("clients").doc(inv.clientId).get();
        const client = clientDoc.exists ? clientDoc.data() : {};
        if (!client.email) {
          results.errors.push({ scheduled: inv.docNo, error: "メールアドレス未設定" });
          continue;
        }

        const settingsSnap2 = await db.collection("settings").limit(1).get();
        const company2 = settingsSnap2.empty ? {} : settingsSnap2.docs[0].data();
        let co2 = company2;
        if (inv.divisionId) {
          const divDoc = await db.collection("divisions").doc(inv.divisionId).get();
          if (divDoc.exists) {
            const div = divDoc.data();
            co2 = { ...company2, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) };
          }
        }

        const sent = await sendEmail(client.email,
          `【請求書】${inv.docNo} ${co2.name || ""}`,
          `<div style="font-family:sans-serif;color:#333;">
            <p>${client.name || ""} 御中</p>
            <p>いつもお世話になっております。<br>${co2.name || ""}です。</p>
            <p>請求書（${inv.docNo}）をお送りいたします。</p>
            <p>金額：&yen;${(inv.total || 0).toLocaleString()}</p>
            <p>ご確認のほど、よろしくお願いいたします。</p>
            <hr style="border:none;border-top:1px solid #ddd;margin:20px 0">
            <p style="font-size:12px;color:#888">${co2.name || ""}<br>${co2.address || ""}<br>TEL ${co2.tel || ""}</p>
          </div>`
        );
        if (sent) {
          await invDoc.ref.update({ sentStatus: "sent", lastSentAt: FieldValue.serverTimestamp() });
          await db.collection("sendHistory").add({
            docNo: inv.docNo, invoiceId: inv.id, clientId: inv.clientId,
            clientName: client.name || "", email: client.email,
            method: "auto", memo: "送信予定日による自動送信",
            amount: inv.total || 0, sentAt: FieldValue.serverTimestamp(), sentBy: "scheduled",
          });
          results.scheduled.push({ docNo: inv.docNo, client: client.name });
        } else {
          results.errors.push({ scheduled: inv.docNo, error: "メール送信失敗" });
        }
      } catch (e) {
        results.errors.push({ scheduled: inv.docNo, error: e.message });
      }
    }
  } catch (e) {
    results.errors.push({ fatal: e.message });
  }

  return res.status(200).json({ ok: true, date: todayStr(), ...results });
}
