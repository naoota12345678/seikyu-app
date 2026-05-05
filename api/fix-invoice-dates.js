import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

if (!getApps().length) {
  initializeApp({ credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
}
const db = getFirestore();

export default async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  try {
    const invoicesSnap = await db.collection("invoices").where("billingType", "==", "immediate").get();
    const deliveriesSnap = await db.collection("deliveries").get();
    const delMap = {};
    deliveriesSnap.docs.forEach(d => {
      const data = d.data();
      if (data.docNo) delMap[data.docNo] = data;
    });

    let fixed = 0;
    const batch = db.batch();
    for (const invDoc of invoicesSnap.docs) {
      const inv = invDoc.data();
      const delDocNo = inv.deliveryRef;
      if (!delDocNo || !delMap[delDocNo]) continue;
      const del = delMap[delDocNo];
      if (del.date && inv.date !== del.date) {
        batch.update(invDoc.ref, { date: del.date });
        fixed++;
      }
    }
    if (fixed > 0) await batch.commit();
    res.json({ ok: true, fixed, message: `${fixed}件の請求書日付を納品書日付に修正しました` });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}
