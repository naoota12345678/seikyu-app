import { initializeApp } from "firebase/app";
import { getFirestore, connectFirestoreEmulator } from "firebase/firestore";
import { getStorage } from "firebase/storage";
import { getAuth, connectAuthEmulator } from "firebase/auth";

const firebaseConfig = {
  apiKey: "AIzaSyCON2qFps0vvoWvDajUpxH76jQ4SZmdhaA",
  authDomain: "sizukaproduct.firebaseapp.com",
  projectId: "sizukaproduct",
  storageBucket: "sizukaproduct.firebasestorage.app",
  messagingSenderId: "782342436284",
  appId: "1:782342436284:web:16deba45386e9538d0a7b9",
  measurementId: "G-Y18ZQ9T59F"
};

const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
export const storage = getStorage(app);
export const auth = getAuth(app);

// 検証用のローカル接続先。顧客の本番Firestore(sizukaproduct)を触らずに
// 削除・残高まわりを実際に動かして確かめるために使う。
//   npm run emu   … エミュレータ起動＋テストデータ投入
//   npm run dev:emu … このアプリをエミュレータに繋いで起動
// `npm run dev` / `npm run build` では VITE_USE_EMULATOR が無いので本番に繋がる（従来どおり）。
if (import.meta.env.DEV && import.meta.env.VITE_USE_EMULATOR === "1") {
  connectFirestoreEmulator(db, "127.0.0.1", 8080);
  connectAuthEmulator(auth, "http://127.0.0.1:9099", { disableWarnings: true });
  console.log("[emulator] Firestore 127.0.0.1:8080 / Auth 127.0.0.1:9099 に接続しました");
}
