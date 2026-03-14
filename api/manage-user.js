import { initializeApp, cert, getApps } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore } from "firebase-admin/firestore";

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}");
if (!getApps().length) {
  initializeApp({ credential: cert(serviceAccount) });
}
const adminAuth = getAuth();
const adminDb = getFirestore();

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { action, email, password, displayName, role, uid, callerUid } = req.body;

  // 呼び出し元がadminか確認
  if (callerUid) {
    const callerDoc = await adminDb.collection("users").doc(callerUid).get();
    if (!callerDoc.exists || callerDoc.data().role !== "admin") {
      return res.status(403).json({ error: "管理者権限が必要です" });
    }
  }

  try {
    if (action === "create") {
      if (!email || !password) {
        return res.status(400).json({ error: "メールアドレスとパスワードは必須です" });
      }
      // Firebase Authにユーザー作成
      const userRecord = await adminAuth.createUser({
        email,
        password,
        displayName: displayName || "",
      });
      // Firestoreにユーザー情報保存
      await adminDb.collection("users").doc(userRecord.uid).set({
        email,
        displayName: displayName || "",
        role: role || "staff",
        createdAt: new Date(),
      });
      return res.status(200).json({ success: true, uid: userRecord.uid });

    } else if (action === "delete") {
      if (!uid) {
        return res.status(400).json({ error: "uidは必須です" });
      }
      // 自分自身は削除不可
      if (uid === callerUid) {
        return res.status(400).json({ error: "自分自身は削除できません" });
      }
      await adminAuth.deleteUser(uid);
      await adminDb.collection("users").doc(uid).delete();
      return res.status(200).json({ success: true });

    } else if (action === "updateRole") {
      if (!uid || !role) {
        return res.status(400).json({ error: "uidとroleは必須です" });
      }
      await adminDb.collection("users").doc(uid).update({ role });
      return res.status(200).json({ success: true });

    } else if (action === "list") {
      const snapshot = await adminDb.collection("users").get();
      const users = snapshot.docs.map(d => ({ uid: d.id, ...d.data() }));
      return res.status(200).json({ success: true, users });

    } else if (action === "init") {
      // 初回セットアップ: usersコレクションが空なら呼び出し元をadminとして登録
      const snapshot = await adminDb.collection("users").get();
      if (snapshot.empty && callerUid) {
        const authUser = await adminAuth.getUser(callerUid);
        await adminDb.collection("users").doc(callerUid).set({
          email: authUser.email || email || "",
          displayName: authUser.displayName || "",
          role: "admin",
          createdAt: new Date(),
        });
        return res.status(200).json({ success: true, role: "admin", initialized: true });
      }
      return res.status(200).json({ success: false, message: "既にユーザーが存在します" });

    } else {
      return res.status(400).json({ error: "不明なアクションです" });
    }
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
