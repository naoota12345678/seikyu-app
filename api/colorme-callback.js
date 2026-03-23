import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

if (!getApps().length) {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}");
  initializeApp({ credential: cert(serviceAccount) });
}
const db = getFirestore();

export default async function handler(req, res) {
  const { code, error } = req.query;

  if (error) {
    return res.status(400).send(`<html><body><h2>認証エラー</h2><p>${error}</p><script>setTimeout(()=>window.close(),3000)</script></body></html>`);
  }

  if (!code) {
    return res.status(400).send(`<html><body><h2>認証コードがありません</h2><script>setTimeout(()=>window.close(),3000)</script></body></html>`);
  }

  try {
    // Firestoreから認証情報を取得
    const settingsSnap = await db.collection("settings").limit(1).get();
    if (settingsSnap.empty) {
      return res.status(400).send(`<html><body><h2>設定が見つかりません</h2><script>setTimeout(()=>window.close(),3000)</script></body></html>`);
    }
    const settings = settingsSnap.docs[0].data();
    const clientId = (settings.colormeClientId || "").trim();
    const clientSecret = (settings.colormeClientSecret || "").trim();
    if (!clientId || !clientSecret) {
      return res.status(400).send(`<html><body><h2>カラーミーのClient ID/Secretが設定されていません</h2><script>setTimeout(()=>window.close(),5000)</script></body></html>`);
    }

    const redirectUri = (settings.colormeRedirectUri || "").trim()
      || `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}/api/colorme-callback`;

    const tokenRes = await fetch("https://api.shop-pro.jp/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: clientId,
        client_secret: clientSecret,
        code,
        grant_type: "authorization_code",
        redirect_uri: redirectUri,
      }),
    });

    const tokenData = await tokenRes.json();

    if (!tokenRes.ok || !tokenData.access_token) {
      return res.status(400).send(`<html><body><h2>トークン取得失敗</h2><p>${JSON.stringify(tokenData)}</p><script>setTimeout(()=>window.close(),5000)</script></body></html>`);
    }

    // Firestoreのsettingsにアクセストークンを保存
    {
      await settingsSnap.docs[0].ref.update({
        colormeAccessToken: tokenData.access_token,
        colormeTokenCreatedAt: FieldValue.serverTimestamp(),
      });
    }

    return res.status(200).send(`
      <html>
      <body style="font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;background:#f5f5f5;">
        <div style="text-align:center;background:white;padding:40px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,0.1);">
          <h2 style="color:#E95295;">カラーミーショップ認証完了</h2>
          <p>アクセストークンを保存しました。このウィンドウを閉じてください。</p>
          <script>setTimeout(()=>window.close(),3000)</script>
        </div>
      </body>
      </html>
    `);
  } catch (e) {
    return res.status(500).send(`<html><body><h2>エラー</h2><p>${e.message}</p><script>setTimeout(()=>window.close(),5000)</script></body></html>`);
  }
}
