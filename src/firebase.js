import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";

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
