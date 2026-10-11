import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { authorizeUser } from "../lib/apiAuth.js";

if (!getApps().length) {
  initializeApp({ credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}")) });
}

/**
 * 請求書メールの送信。
 *
 * ログイン中の登録ユーザー（users に載っている人）だけが使える。
 * 以前は無認証で、URLを知っている第三者が宛先・件名・本文・差出人を自由に指定して
 * 「請求管理システム」名義のメールを送れた（偽の振込先を書いた請求書のなりすまし・スパム中継。
 * スパム扱いされると送信ドメインの評判が落ち、本物の請求書メールが届かなくなる）。
 * 差出人は呼び出し側に決めさせず固定する（画面はもともと差出人を指定していない）。
 */
const FROM = "請求管理システム <invoice@romu.ai>";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const authz = await authorizeUser(req);
  if (!authz.ok) {
    return res.status(authz.status).json({ error: authz.error });
  }
  // IDトークンが本物でも、事務所が登録していないアカウント（自分で作っただけの人）には使わせない
  const member = await getFirestore().collection("users").doc(authz.uid).get();
  if (!member.exists) {
    return res.status(403).json({ error: "利用者として登録されていません" });
  }

  const { to, subject, html, attachments } = req.body || {};

  if (!to || !subject) {
    return res.status(400).json({ error: "to and subject are required" });
  }

  const RESEND_API_KEY = process.env.RESEND_API_KEY;
  if (!RESEND_API_KEY) {
    return res.status(500).json({ error: "RESEND_API_KEY not configured" });
  }

  try {
    const body = {
      from: FROM,
      to: Array.isArray(to) ? to : [to],
      subject,
      html,
    };

    if (attachments && attachments.length > 0) {
      body.attachments = attachments;
    }

    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    const data = await response.json();

    if (!response.ok) {
      return res.status(response.status).json({ error: data.message || "Failed to send email", details: data });
    }

    return res.status(200).json({ success: true, id: data.id });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
