// 同期APIの呼び出し元検証。
//   GET  … Vercel cron からの呼び出し。CRON_SECRET で認証する。
//   POST … 画面（売上管理・設定）からの呼び出し。ログイン中ユーザーの
//          Firebase IDトークンで認証する。
// これが無いと URL を知っているだけで誰でも同期を発火できてしまう。
import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore } from "firebase-admin/firestore";

function bearerToken(req) {
  const header = req.headers?.authorization || "";
  return header.startsWith("Bearer ") ? header.slice(7).trim() : "";
}

// firebase-admin は各APIの先頭で初期化済みだが、
// 単体でも動くように未初期化なら初期化する。
function ensureAdminApp() {
  if (!getApps().length) {
    initializeApp({ credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}")) });
  }
}

/**
 * @returns {Promise<{ok: true, caller: "cron"|"user", uid?: string}
 *                 | {ok: false, status: number, error: string}>}
 */
export async function authorizeSyncRequest(req) {
  const token = bearerToken(req);
  if (!token) {
    return { ok: false, status: 401, error: "認証情報がありません" };
  }

  if (req.method === "GET") {
    const cronSecret = process.env.CRON_SECRET;
    if (cronSecret && token === cronSecret) return { ok: true, caller: "cron" };
    return { ok: false, status: 401, error: "Unauthorized" };
  }

  try {
    ensureAdminApp();
    const decoded = await getAuth().verifyIdToken(token);
    // 本物のIDトークンでも、事務所が登録していないアカウント（自分で作っただけの人）は断る
    const member = await getFirestore().collection("users").doc(decoded.uid).get();
    if (!member.exists) return { ok: false, status: 403, error: "利用者として登録されていません" };
    return { ok: true, caller: "user", uid: decoded.uid };
  } catch {
    return { ok: false, status: 401, error: "ログインが必要です" };
  }
}

/**
 * ログイン中ユーザーのIDトークンだけを検証して uid を返す。
 * body の callerUid はクライアントが自由に詰められるため信用しない。
 * 呼び出し元の身元は必ずこの uid を使うこと。
 * @returns {Promise<{ok: true, uid: string} | {ok: false, status: number, error: string}>}
 */
export async function authorizeUser(req) {
  const token = bearerToken(req);
  if (!token) {
    return { ok: false, status: 401, error: "認証情報がありません" };
  }
  try {
    ensureAdminApp();
    const decoded = await getAuth().verifyIdToken(token);
    return { ok: true, uid: decoded.uid };
  } catch {
    return { ok: false, status: 401, error: "ログインが必要です" };
  }
}
