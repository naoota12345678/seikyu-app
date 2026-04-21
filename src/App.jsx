import { useState, useEffect } from "react";
import { db, storage, auth } from "./firebase";
import { onAuthStateChanged, signInWithEmailAndPassword, signOut } from "firebase/auth";
import {
  collection, addDoc, getDocs, getDoc, doc, updateDoc, deleteDoc,
  onSnapshot, query, orderBy, where, serverTimestamp, writeBatch, setDoc
} from "firebase/firestore";
import jsPDF from "jspdf";
import html2canvas from "html2canvas";
import { ref, uploadBytes, getDownloadURL, listAll } from "firebase/storage";
import ManualPage from "./ManualPage";

const C = {
  navy: "#1C2B4A", gold: "#B8905A", cream: "#F4F1EC",
  pale: "#F7EEE0", green: "#2D6A4F", red: "#C53030",
  gray: "#777", light: "#e8e2da", white: "#fff"
};

const fmt = (n) => Number(n || 0).toLocaleString("ja-JP");
const today = () => new Date().toISOString().split("T")[0];
const nextMonthEnd = (dateStr) => {
  const d = new Date(dateStr || today());
  return new Date(d.getFullYear(), d.getMonth() + 2, 0).toISOString().split("T")[0];
};
// 税率別に集計: items=[{qty,price,taxRate},...] → {10:{sub,tax}, 8:{sub,tax}, ...}
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
function parseCSVLine(line) {
  const result = []; let cur = ""; let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"' && line[i+1] === '"') { cur += '"'; i++; }
      else if (ch === '"') { inQuote = false; }
      else { cur += ch; }
    } else {
      if (ch === '"') { inQuote = true; }
      else if (ch === ',') { result.push(cur.trim()); cur = ""; }
      else { cur += ch; }
    }
  }
  result.push(cur.trim());
  return result;
}

function genDocNo(prefix, list) {
  const now = new Date();
  const ym = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}`;
  const same = list.filter(d => (d.docNo || "").includes(`${prefix}-${ym}`));
  return `${prefix}-${ym}-${String(same.length + 1).padStart(3, "0")}`;
}

// 締日ユーティリティ
function closingDayLabel(cd) { return cd === 0 ? "末日" : `${cd}日`; }
function closingDaysLabel(days) {
  if (!days || !days.length) return "即時";
  return days.map(closingDayLabel).join("・") + "締め";
}
// 締日と対象月から請求対象期間を算出。prevClosing=前の締日(ない場合は前月末or月初)
function getClosingPeriod(yearMonth, closingDay, prevClosingDay) {
  const [y, m] = yearMonth.split("-").map(Number);
  const pad = (n) => String(n).padStart(2, "0");
  const lastDay = new Date(y, m, 0).getDate();
  const endDay = closingDay === 0 ? lastDay : Math.min(closingDay, lastDay);
  const end = `${y}-${pad(m)}-${pad(endDay)}`;
  let start;
  if (prevClosingDay === undefined || prevClosingDay === null) {
    // 最初の締日 or 単独締日: 前月の前回締日翌日 or 月初から
    start = `${y}-${pad(m)}-01`;
  } else {
    const prevEnd = prevClosingDay === 0 ? new Date(y, m - 1, 0).getDate() : prevClosingDay;
    const startDay = prevEnd + 1;
    if (prevClosingDay === 0) {
      // 前回が前月末 → 当月1日から
      start = `${y}-${pad(m)}-01`;
    } else if (startDay > lastDay) {
      start = end; // 同日
    } else {
      start = `${y}-${pad(m)}-${pad(startDay)}`;
    }
  }
  const label = closingDay === 0
    ? `${m}月末日締め（${start}～${end}）`
    : `${m}月${endDay}日締め（${start}～${end}）`;
  return { start, end, closingDay, label };
}
// ある月の全締日期間を取得（closingDays配列からソートして順番に期間算出）
function getAllClosingPeriods(yearMonth, closingDays) {
  if (!closingDays || !closingDays.length) return [];
  const sorted = [...closingDays].sort((a, b) => {
    const va = a === 0 ? 32 : a;
    const vb = b === 0 ? 32 : b;
    return va - vb;
  });
  const periods = [];
  for (let i = 0; i < sorted.length; i++) {
    const prev = i === 0 ? null : sorted[i - 1];
    periods.push(getClosingPeriod(yearMonth, sorted[i], prev));
  }
  return periods;
}

// 次回の締日日付を計算（今日以降で最も近い締日）
function getNextClosingDate(closingDays) {
  if (!closingDays || !closingDays.length) return "";
  const now = new Date();
  const todayDay = now.getDate();
  const y = now.getFullYear(), m = now.getMonth();
  const pad = (n) => String(n).padStart(2, "0");
  const sorted = [...closingDays].sort((a, b) => (a === 0 ? 32 : a) - (b === 0 ? 32 : b));
  // 今月の残り締日を探す
  for (const cd of sorted) {
    const lastDay = new Date(y, m + 1, 0).getDate();
    const day = cd === 0 ? lastDay : Math.min(cd, lastDay);
    if (day >= todayDay) return `${y}-${pad(m + 1)}-${pad(day)}`;
  }
  // 今月にない場合は来月の最初の締日
  const nm = m + 1;
  const ny = nm > 11 ? y + 1 : y;
  const nmm = nm > 11 ? 0 : nm;
  const lastDay = new Date(ny, nmm + 1, 0).getDate();
  const cd = sorted[0];
  const day = cd === 0 ? lastDay : Math.min(cd, lastDay);
  return `${ny}-${pad(nmm + 1)}-${pad(day)}`;
}

const s = {
  app: { display: "flex", minHeight: "100vh", fontFamily: "'Noto Sans JP', sans-serif", background: C.cream },
  sidebar: { width: 220, background: C.navy, color: C.white, display: "flex", flexDirection: "column", padding: "0 0 24px 0", flexShrink: 0 },
  sideTitle: { padding: "28px 20px 20px", borderBottom: "1px solid rgba(255,255,255,0.1)", marginBottom: 8 },
  navBtn: (active) => ({
    display: "block", width: "100%", textAlign: "left", padding: "11px 20px",
    background: active ? C.gold : "none", color: active ? C.navy : "rgba(255,255,255,0.8)",
    border: "none", cursor: "pointer", fontSize: 14, fontWeight: active ? 700 : 400,
    borderLeft: active ? `4px solid ${C.cream}` : "4px solid transparent",
  }),
  main: { flex: 1, padding: "32px 36px", overflowY: "auto" },
  pageTitle: { fontSize: 22, fontWeight: 700, color: C.navy, marginBottom: 24 },
  card: { background: C.white, borderRadius: 10, padding: 24, marginBottom: 20, boxShadow: "0 1px 6px rgba(0,0,0,0.07)" },
  row: { display: "flex", gap: 12, alignItems: "flex-start", flexWrap: "wrap" },
  col: { display: "flex", flexDirection: "column", gap: 6 },
  label: { fontSize: 12, color: C.gray, fontWeight: 600 },
  input: { padding: "8px 12px", border: `1px solid ${C.light}`, borderRadius: 6, fontSize: 14, minWidth: 140, fontFamily: "inherit" },
  select: { padding: "8px 12px", border: `1px solid ${C.light}`, borderRadius: 6, fontSize: 14, background: C.white, fontFamily: "inherit" },
  btn: (v = "primary") => ({
    padding: "9px 20px", borderRadius: 6, border: "none", cursor: "pointer", fontSize: 14, fontWeight: 600,
    background: v === "primary" ? C.navy : v === "gold" ? C.gold : v === "red" ? C.red : v === "green" ? C.green : C.light,
    color: v === "light" ? C.navy : C.white,
  }),
  table: { width: "100%", borderCollapse: "collapse", fontSize: 14 },
  th: { padding: "10px 12px", background: C.pale, color: C.navy, fontWeight: 700, textAlign: "left", borderBottom: `2px solid ${C.gold}` },
  td: { padding: "10px 12px", borderBottom: `1px solid ${C.light}`, verticalAlign: "middle" },
  badge: (color) => ({
    display: "inline-block", padding: "2px 10px", borderRadius: 20, fontSize: 12, fontWeight: 700,
    background: color === "green" ? "#d4edda" : color === "gold" ? "#fff3cd" : color === "blue" ? "#d0e4ff" : color === "red" ? "#f8d7da" : "#e2e3e5",
    color: color === "green" ? C.green : color === "gold" ? "#856404" : color === "blue" ? "#004085" : color === "red" ? C.red : "#383d41",
  }),
  modal: { position: "fixed", inset: 0, background: "rgba(0,0,0,0.45)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000, padding: 20 },
  modalBox: { background: C.white, borderRadius: 12, padding: 32, width: "100%", maxWidth: 760, maxHeight: "90vh", overflowY: "auto", boxShadow: "0 8px 40px rgba(0,0,0,0.18)" },
};

// ── Print ──────────────────────────────────────────────────────────────────────
const baseCSS = `body{font-family:'MS PGothic',sans-serif;margin:0;padding:28px;font-size:12px;color:#111}
h1{text-align:center;font-size:20px;letter-spacing:6px;margin:0 0 20px;padding:4px 0;border:2px solid #333}
.hd{display:flex;justify-content:space-between;margin-bottom:16px}
.cn{font-size:15px;font-weight:bold;border-bottom:2px solid #1C2B4A;padding-bottom:3px;display:inline-block}
.co{text-align:right;font-size:11px;line-height:1.9}
.meta{font-size:11px;margin-top:10px;line-height:1.8}
table{width:100%;border-collapse:collapse;margin:14px 0}
.it th{background:#1C2B4A;color:#fff;padding:6px 8px;text-align:center;font-size:11px}
.it td{padding:6px 8px;border:1px solid #ccc;font-size:11px}
.nr{text-align:right}
.tr td{background:#f4f1ec;font-weight:bold}
.tot{text-align:right;margin-top:8px}
.bb{margin-top:16px;padding:10px 14px;background:#f4f1ec;font-size:11px;line-height:1.9}
.pb{page-break-after:always}
.bt th,.bt td{border:1px solid #ccc;padding:6px 8px;text-align:right;font-size:11px}
.bt th{background:#1C2B4A;color:#fff;text-align:center}
@media print{body{padding:10px}}`;

function itemsHTML(items) {
  const groups = calcTaxByRate(items);
  const rates = Object.keys(groups).sort((a,b)=>Number(b)-Number(a));
  return `<table class="it"><thead><tr>
    <th style="width:38%">商品名</th><th style="width:9%">数量</th><th style="width:8%">単位</th>
    <th style="width:18%">単価</th><th style="width:18%">金額</th><th style="width:9%">税率</th>
  </tr></thead><tbody>
  ${items.map(i=>{const a=Number(i.qty||0)*Number(i.price||0);const r=i.taxRate!==undefined&&i.taxRate!==null&&i.taxRate!==""?Number(i.taxRate):10;return`<tr><td>${i.name||""}</td><td class="nr">${i.qty||""}</td><td>${i.unit||""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td><td style="font-size:10px;color:#555">課${r}%</td></tr>`}).join("")}
  ${rates.map(r=>`<tr class="tr"><td colspan="4">【課税${r}% 税抜額】</td><td class="nr" colspan="2">¥${fmt(groups[r].sub)}</td></tr>
  <tr class="tr"><td colspan="4">【課税${r}% 消費税額】</td><td class="nr" colspan="2">¥${fmt(groups[r].tax)}</td></tr>`).join("")}
  </tbody></table>`;
}

function footerHTML(items, bank) {
  const { sub, tax, total } = totalFromItems(items);
  return `<div class="tot"><table style="margin-left:auto;border-collapse:collapse">
  <tr><td style="padding:4px 12px;border:1px solid #ccc">税抜額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(sub)}</td>
  <td style="padding:4px 12px;border:1px solid #ccc">消費税額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(tax)}</td>
  <td style="padding:4px 12px;background:#1C2B4A;color:#fff;font-weight:bold">合計</td>
  <td style="padding:4px 20px;border:2px solid #1C2B4A;text-align:right;font-size:16px;font-weight:bold;color:#1C2B4A">¥${fmt(total)}</td></tr>
  </table></div>
  ${bank?`<div class="bb">振込先口座：${bank.bankName||""}　${bank.bankBranch||""}　${bank.bankType||"普通"}　${bank.bankNo||""}<br>口座名義：${bank.bankHolder||""}<br>※振込手数料はご負担下さいますようお願い致します。</div>`:""}`;
}

function coBlock(c,doc_,showReg){
  return `<div class="co"><strong>${c.name||""}</strong><br>${c.address||""}<br>TEL ${c.tel||""}　FAX ${c.fax||""}<br>${showReg&&c.registrationNo?`登録番号　${c.registrationNo}`:""}</div>`;
}

function openPrint(html){
  const w=window.open("","_blank");w.document.write(html);w.document.close();setTimeout(()=>w.print(),600);
}

async function generatePDF(html, filename) {
  const container = document.createElement("div");
  container.style.cssText = "position:fixed;left:-9999px;top:0;width:794px;background:#fff;z-index:-1;";
  container.innerHTML = `<style>${baseCSS}</style>${html.replace(/<!DOCTYPE[^>]*>|<\/?html[^>]*>|<\/?head[^>]*>|<meta[^>]*>|<style>[^<]*<\/style>/gi, "").replace(/<\/?body[^>]*>/gi, "")}`;
  document.body.appendChild(container);
  await new Promise(r => setTimeout(r, 300));
  const pages = container.querySelectorAll(".pb");
  const pdf = new jsPDF("p", "mm", "a4");
  const pdfW = 210, pdfH = 297, margin = 5;
  if (pages.length > 0) {
    for (let i = 0; i < pages.length; i++) {
      const canvas = await html2canvas(pages[i], { scale: 2, useCORS: true, backgroundColor: "#fff" });
      const imgW = pdfW - margin * 2;
      const imgH = (canvas.height * imgW) / canvas.width;
      if (i > 0) pdf.addPage();
      pdf.addImage(canvas.toDataURL("image/png"), "PNG", margin, margin, imgW, Math.min(imgH, pdfH - margin * 2));
    }
    const remaining = container.cloneNode(true);
    remaining.querySelectorAll(".pb").forEach(el => el.remove());
    if (remaining.textContent.trim()) {
      document.body.appendChild(remaining);
      const canvas = await html2canvas(remaining, { scale: 2, useCORS: true, backgroundColor: "#fff" });
      const imgW = pdfW - margin * 2;
      const imgH = (canvas.height * imgW) / canvas.width;
      pdf.addPage();
      pdf.addImage(canvas.toDataURL("image/png"), "PNG", margin, margin, imgW, Math.min(imgH, pdfH - margin * 2));
      document.body.removeChild(remaining);
    }
  } else {
    const canvas = await html2canvas(container, { scale: 2, useCORS: true, backgroundColor: "#fff" });
    const imgW = pdfW - margin * 2;
    const imgH = (canvas.height * imgW) / canvas.width;
    pdf.addImage(canvas.toDataURL("image/png"), "PNG", margin, margin, imgW, Math.min(imgH, pdfH - margin * 2));
  }
  document.body.removeChild(container);
  return { pdf, blob: pdf.output("blob"), filename };
}

async function savePDFToStorage(blob, path) {
  const storageRef = ref(storage, path);
  await uploadBytes(storageRef, blob, { contentType: "application/pdf" });
  return await getDownloadURL(storageRef);
}

function buildQuotationHTML(q, clients, co) {
  const cl = clients.find(c => c.id === q.clientId) || {};
  return `<h1>見　積　書</h1>
  <div class="hd"><div><div>${cl.address || ""}</div><div class="cn">${cl.name || ""} 御中</div>
  <div class="meta">見積番号：${q.docNo}<br>見積日：${q.date}<br>有効期限：${q.validUntil || ""}</div></div>${coBlock(co, q, false)}</div>
  ${itemsHTML(q.items || [])}${footerHTML(q.items || [], null)}
  ${q.notes ? `<div style="margin-top:10px;font-size:11px;color:#555">備考：${q.notes}</div>` : ""}`;
}

function buildDeliveryHTML(d, clients, co) {
  const cl = clients.find(c => c.id === d.clientId) || {};
  return `<h1>納　品　書</h1>
  <div class="hd"><div><div>${cl.address || ""}</div><div class="cn">${cl.name || ""} 御中</div>
  <div class="meta">伝票番号：${d.docNo}<br>売上日：${d.date}</div></div>${coBlock(co, d, false)}</div>
  ${itemsHTML(d.items || [])}${footerHTML(d.items || [], null)}
  ${d.notes ? `<div style="margin-top:10px;font-size:11px;color:#555">備考：${d.notes}</div>` : ""}`;
}

function buildInvoiceHTML(inv, clients, co) {
  const cl = clients.find(c => c.id === inv.clientId) || {};
  return `<h1>請　求　書</h1>
  <div class="hd"><div><div>${cl.address || ""}</div><div class="cn">${cl.name || ""} 御中</div>
  <div class="meta">請求番号：${inv.docNo}<br>売上日：${inv.date}<br>支払期限：${inv.dueDate || ""}</div></div>${coBlock(co, inv, true)}</div>
  ${itemsHTML(inv.items || [])}${footerHTML(inv.items || [], co)}
  ${inv.deliveryRefs?.length ? `<div style="margin-top:10px;font-size:11px;color:#555">対象納品書：${inv.deliveryRefs.join("、")}</div>` : ""}`;
}

function buildMeisaiHTML(inv, clients, co, bal) {
  const cl = clients.find(c => c.id === inv.clientId) || {};
  const allItems = inv.items || [];
  const { sub, tax, total: invTotal } = totalFromItems(allItems);
  const groups = calcTaxByRate(allItems);
  const rates = Object.keys(groups).sort((a,b)=>Number(b)-Number(a));
  const prev = bal?.prevBalance || 0;
  const paid = bal?.paidAmount || 0;
  const carry = prev - paid;
  const total = carry + invTotal;
  const refs = inv.deliveryRefs || [];
  const refItems = typeof inv.deliveryRefItems === "string" ? JSON.parse(inv.deliveryRefItems) : (inv.deliveryRefItems || []);
  let rows = "";
  if (paid > 0) rows += `<tr><td>${inv.date}</td><td>振込</td><td></td><td></td><td></td><td class="nr" style="color:green">¥${fmt(paid)}</td></tr>`;
  if (refs.length > 0) {
    refs.forEach((ref, ri) => {
      const items = refItems[ri] || [];
      items.forEach(i => {
        const a = Number(i.qty || 0) * Number(i.price || 0);
        const r = i.taxRate !== undefined && i.taxRate !== null && i.taxRate !== "" ? Number(i.taxRate) : 10;
        rows += `<tr><td style="font-size:10px">${ref}</td><td>${i.name || ""}<span style="font-size:9px;float:right;color:#555">課${r}%</span></td><td class="nr">${i.qty}</td><td>${i.unit || ""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td></tr>`;
      });
    });
  } else {
    allItems.forEach(i => {
      const a = Number(i.qty || 0) * Number(i.price || 0);
      const r = i.taxRate !== undefined && i.taxRate !== null && i.taxRate !== "" ? Number(i.taxRate) : 10;
      rows += `<tr><td></td><td>${i.name || ""}<span style="font-size:9px;float:right;color:#555">課${r}%</span></td><td class="nr">${i.qty}</td><td>${i.unit || ""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td></tr>`;
    });
  }
  return `<h1>請　求　明　細　書</h1>
  <div class="hd"><div><div>${cl.address || ""}</div><div class="cn">${cl.name || ""} 御中</div>
  <div class="meta">締切分：${inv.date}　No.${inv.docNo}</div></div>${coBlock(co, inv, true)}</div>
  <table class="bt" style="margin-bottom:16px"><thead><tr>
    <th>前回御請求額</th><th>御入金額</th><th>繰越金額</th><th>今回御買上額</th><th>消費税</th><th>今回御請求額</th>
  </tr></thead><tbody><tr>
    <td>¥${fmt(prev)}</td><td>¥${fmt(paid)}</td><td>¥${fmt(carry)}</td>
    <td>¥${fmt(sub)}</td><td>¥${fmt(tax)}</td>
    <td style="font-weight:bold;font-size:13px;color:#1C2B4A">¥${fmt(total)}</td>
  </tr></tbody></table>
  <p style="font-size:11px;margin:0 0 8px">下記の通り御請求申し上げます</p>
  <table class="it"><thead><tr>
    <th style="width:15%">日付/伝票番号</th><th style="width:35%">商品名</th>
    <th style="width:8%">数量</th><th style="width:8%">単位</th><th style="width:16%">単価</th><th style="width:18%">金額</th>
  </tr></thead><tbody>
    ${rows}
    <tr style="background:#f9f9f9"><td colspan="5">消費税</td><td class="nr">¥${fmt(tax)}</td></tr>
    ${rates.map(r=>`<tr class="tr"><td colspan="5">【課税${r}% 税抜額】</td><td class="nr">¥${fmt(groups[r].sub)}</td></tr>
    <tr class="tr"><td colspan="5">【課税${r}% 消費税額】</td><td class="nr">¥${fmt(groups[r].tax)}</td></tr>`).join("")}
  </tbody></table>
  <div class="tot"><table style="margin-left:auto;border-collapse:collapse">
    <tr><td style="padding:4px 12px;border:1px solid #ccc">税抜額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(sub)}</td>
    <td style="padding:4px 12px;border:1px solid #ccc">消費税額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(tax)}</td>
    <td style="padding:4px 12px;background:#1C2B4A;color:#fff;font-weight:bold">今回御請求額</td>
    <td style="padding:4px 20px;border:2px solid #1C2B4A;text-align:right;font-size:16px;font-weight:bold;color:#1C2B4A">¥${fmt(total)}</td></tr>
  </table></div>
  ${co?.bankName ? `<div class="bb">振込専用口座　${co.bankName}　${co.bankBranch}　${co.bankType || "普通"}　${co.bankNo}　${co.bankHolder || ""}<br>お支払期日：翌月末日　※振込手数料はご負担下さいますようお願い致します。</div>` : ""}`;
}

function printQuotation(q,clients,co){
  const cl=clients.find(c=>c.id===q.clientId)||{};
  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <h1>見　積　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">見積番号：${q.docNo}<br>見積日：${q.date}<br>有効期限：${q.validUntil||""}</div></div>${coBlock(co,q,false)}</div>
  ${itemsHTML(q.items||[])}${footerHTML(q.items||[],null)}
  ${q.notes?`<div style="margin-top:10px;font-size:11px;color:#555">備考：${q.notes}</div>`:""}
  </body></html>`);
}

function printDelivery(d,clients,co){
  const cl=clients.find(c=>c.id===d.clientId)||{};
  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <h1>納　品　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">伝票番号：${d.docNo}<br>売上日：${d.date}</div></div>${coBlock(co,d,false)}</div>
  ${itemsHTML(d.items||[])}${footerHTML(d.items||[],null)}
  ${d.notes?`<div style="margin-top:10px;font-size:11px;color:#555">備考：${d.notes}</div>`:""}
  </body></html>`);
}

function printInvoice(inv,clients,co){
  const cl=clients.find(c=>c.id===inv.clientId)||{};
  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <h1>請　求　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">請求番号：${inv.docNo}<br>売上日：${inv.date}<br>支払期限：${inv.dueDate||""}</div></div>${coBlock(co,inv,true)}</div>
  ${itemsHTML(inv.items||[])}${footerHTML(inv.items||[],co)}
  ${inv.deliveryRefs?.length?`<div style="margin-top:10px;font-size:11px;color:#555">対象納品書：${inv.deliveryRefs.join("、")}</div>`:""}
  </body></html>`);
}

function printCombined(d,inv,clients,co){
  const cl=clients.find(c=>c.id===d.clientId)||{};
  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <div class="pb">
  <h1>納　品　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">伝票番号：${d.docNo}<br>売上日：${d.date}</div></div>${coBlock(co,d,false)}</div>
  ${itemsHTML(d.items||[])}${footerHTML(d.items||[],null)}</div>
  <h1>請　求　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">請求番号：${inv.docNo}<br>売上日：${inv.date}<br>支払期限：${inv.dueDate||""}</div></div>${coBlock(co,inv,true)}</div>
  ${itemsHTML(inv.items||[])}${footerHTML(inv.items||[],co)}
  </body></html>`);
}

function printMeisai(inv,clients,co,bal){
  const cl=clients.find(c=>c.id===inv.clientId)||{};
  const allItems=inv.items||[];
  const {sub,tax,total:invTotal}=totalFromItems(allItems);
  const groups=calcTaxByRate(allItems);
  const rates=Object.keys(groups).sort((a,b)=>Number(b)-Number(a));
  const prev=bal?.prevBalance||0;
  const paid=bal?.paidAmount||0;
  const carry=prev-paid;
  const total=carry+invTotal;
  const refs=inv.deliveryRefs||[];
  const refItems=typeof inv.deliveryRefItems==="string"?JSON.parse(inv.deliveryRefItems):(inv.deliveryRefItems||[]);

  let rows="";
  if(paid>0) rows+=`<tr><td>${inv.date}</td><td>振込</td><td></td><td></td><td></td><td class="nr" style="color:green">¥${fmt(paid)}</td></tr>`;
  if(refs.length>0){
    refs.forEach((ref,ri)=>{
      const items=refItems[ri]||[];
      items.forEach(i=>{
        const a=Number(i.qty||0)*Number(i.price||0);
        const r=i.taxRate!==undefined&&i.taxRate!==null&&i.taxRate!==""?Number(i.taxRate):10;
        rows+=`<tr><td style="font-size:10px">${ref}</td><td>${i.name||""}<span style="font-size:9px;float:right;color:#555">課${r}%</span></td><td class="nr">${i.qty}</td><td>${i.unit||""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td></tr>`;
      });
    });
  } else {
    allItems.forEach(i=>{
      const a=Number(i.qty||0)*Number(i.price||0);
      const r=i.taxRate!==undefined&&i.taxRate!==null&&i.taxRate!==""?Number(i.taxRate):10;
      rows+=`<tr><td></td><td>${i.name||""}<span style="font-size:9px;float:right;color:#555">課${r}%</span></td><td class="nr">${i.qty}</td><td>${i.unit||""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td></tr>`;
    });
  }

  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <h1>請　求　明　細　書</h1>
  <div class="hd">
    <div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
    <div class="meta">締切分：${inv.date}　No.${inv.docNo}</div></div>
    ${coBlock(co,inv,true)}
  </div>
  <table class="bt" style="margin-bottom:16px"><thead><tr>
    <th>前回御請求額</th><th>御入金額</th><th>繰越金額</th><th>今回御買上額</th><th>消費税</th><th>今回御請求額</th>
  </tr></thead><tbody><tr>
    <td>¥${fmt(prev)}</td><td>¥${fmt(paid)}</td><td>¥${fmt(carry)}</td>
    <td>¥${fmt(sub)}</td><td>¥${fmt(tax)}</td>
    <td style="font-weight:bold;font-size:13px;color:#1C2B4A">¥${fmt(total)}</td>
  </tr></tbody></table>
  <p style="font-size:11px;margin:0 0 8px">下記の通り御請求申し上げます</p>
  <table class="it"><thead><tr>
    <th style="width:15%">日付/伝票番号</th><th style="width:35%">商品名</th>
    <th style="width:8%">数量</th><th style="width:8%">単位</th><th style="width:16%">単価</th><th style="width:18%">金額</th>
  </tr></thead><tbody>
    ${rows}
    <tr style="background:#f9f9f9"><td colspan="5">消費税</td><td class="nr">¥${fmt(tax)}</td></tr>
    ${rates.map(r=>`<tr class="tr"><td colspan="5">【課税${r}% 税抜額】</td><td class="nr">¥${fmt(groups[r].sub)}</td></tr>
    <tr class="tr"><td colspan="5">【課税${r}% 消費税額】</td><td class="nr">¥${fmt(groups[r].tax)}</td></tr>`).join("")}
  </tbody></table>
  <div class="tot"><table style="margin-left:auto;border-collapse:collapse">
    <tr><td style="padding:4px 12px;border:1px solid #ccc">税抜額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(sub)}</td>
    <td style="padding:4px 12px;border:1px solid #ccc">消費税額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(tax)}</td>
    <td style="padding:4px 12px;background:#1C2B4A;color:#fff;font-weight:bold">今回御請求額</td>
    <td style="padding:4px 20px;border:2px solid #1C2B4A;text-align:right;font-size:16px;font-weight:bold;color:#1C2B4A">¥${fmt(total)}</td></tr>
  </table></div>
  ${co?.bankName?`<div class="bb">振込専用口座　${co.bankName}　${co.bankBranch}　${co.bankType||"普通"}　${co.bankNo}　${co.bankHolder||""}<br>お支払期日：翌月末日　※振込手数料はご負担下さいますようお願い致します。</div>`:""}
  </body></html>`);
}

// ── Print Mode Modal ──────────────────────────────────────────────────────────
function PrintModeModal({ invoice, delivery, clients, company, balances, divisions, onClose }) {
  const [pdfLoading, setPdfLoading] = useState(false);
  const bal = balances[invoice?.clientId];
  const docData = invoice || delivery || {};
  const div = divisions?.find(d => d.id === docData.divisionId);
  const co = div ? { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) } : company;
  const cl = clients.find(c => c.id === docData.clientId) || {};
  const docNo = invoice?.docNo || delivery?.docNo || "doc";
  const modes = [
    { id: "invoice", label: "🧾 請求書のみ", desc: "請求書を単体で印刷", ok: !!invoice },
    { id: "delivery", label: "📦 納品書のみ", desc: "納品書を単体で印刷", ok: !!delivery },
    { id: "combined", label: "📄 納品書＋請求書同時", desc: "2枚同時印刷（納品書→請求書）", ok: !!delivery && !!invoice },
    { id: "meisai", label: "📋 請求明細書", desc: "残高管理付き（前回残高・繰越・今回請求）", ok: !!invoice },
  ];
  const pdfModes = [
    { id: "pdf-invoice", label: "📥 請求書PDF", desc: "請求書をPDFでダウンロード", ok: !!invoice },
    { id: "pdf-delivery", label: "📥 納品書PDF", desc: "納品書をPDFでダウンロード", ok: !!delivery },
    { id: "pdf-meisai", label: "📥 請求明細書PDF", desc: "請求明細書をPDFでダウンロード", ok: !!invoice },
  ];
  const handle = (id) => {
    if (id === "invoice") printInvoice(invoice, clients, co);
    else if (id === "delivery") printDelivery(delivery, clients, co);
    else if (id === "combined") printCombined(delivery, invoice, clients, co);
    else if (id === "meisai") printMeisai(invoice, clients, co, bal);
    onClose();
  };
  const handlePDF = async (id, download = true) => {
    setPdfLoading(true);
    try {
      const safeName = (cl.name || "").replace(/[\\/:*?"<>|]/g, "_");
      let result, type, storagePath;
      if (id === "pdf-invoice") {
        type = "請求書";
        result = await generatePDF(buildInvoiceHTML(invoice, clients, co), `請求書_${safeName}_${docNo}.pdf`);
        storagePath = `pdfs/invoices/${docNo}.pdf`;
      } else if (id === "pdf-delivery") {
        type = "納品書";
        result = await generatePDF(buildDeliveryHTML(delivery, clients, co), `納品書_${safeName}_${delivery.docNo}.pdf`);
        storagePath = `pdfs/deliveries/${delivery.docNo}.pdf`;
      } else if (id === "pdf-meisai") {
        type = "請求明細書";
        result = await generatePDF(buildMeisaiHTML(invoice, clients, co, bal), `請求明細書_${safeName}_${docNo}.pdf`);
        storagePath = `pdfs/meisai/${docNo}.pdf`;
      }
      if (result) {
        if (download) result.pdf.save(result.filename);
        const url = await savePDFToStorage(result.blob, storagePath);
        await addDoc(collection(db, "pdfHistory"), {
          type, docNo: invoice?.docNo || delivery?.docNo,
          clientId: docData.clientId, clientName: cl.name || "",
          filename: result.filename, storageUrl: url, storagePath,
          createdAt: serverTimestamp(),
        });
      }
    } catch (e) { alert("PDF生成エラー: " + e.message); }
    setPdfLoading(false);
  };
  return (
    <div style={s.modal} onClick={onClose}>
      <div style={{ ...s.modalBox, maxWidth: 460 }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
          <h3 style={{ margin: 0, color: C.navy }}>印刷・PDF出力</h3>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <div style={{ fontSize: 12, color: C.gray, fontWeight: 600, marginBottom: 8 }}>印刷</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 20 }}>
          {modes.map(m => (
            <button key={m.id} disabled={!m.ok}
              style={{ padding: "14px 18px", borderRadius: 8, border: `1px solid ${m.ok ? C.gold : C.light}`, background: m.ok ? C.white : "#f5f5f5", cursor: m.ok ? "pointer" : "not-allowed", textAlign: "left", opacity: m.ok ? 1 : 0.5 }}
              onClick={() => m.ok && handle(m.id)}>
              <div style={{ fontWeight: 700, fontSize: 15, color: C.navy }}>{m.label}</div>
              <div style={{ fontSize: 12, color: C.gray, marginTop: 3 }}>{m.desc}{!m.ok ? " ―（対応データなし）" : ""}</div>
            </button>
          ))}
        </div>
        <div style={{ borderTop: `1px solid ${C.light}`, paddingTop: 16 }}>
          <div style={{ fontSize: 12, color: C.gray, fontWeight: 600, marginBottom: 8 }}>PDF保存</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {pdfModes.map(m => (
              <button key={m.id} disabled={!m.ok || pdfLoading}
                style={{ padding: "14px 18px", borderRadius: 8, border: `1px solid ${m.ok ? C.navy : C.light}`, background: m.ok ? C.pale : "#f5f5f5", cursor: m.ok && !pdfLoading ? "pointer" : "not-allowed", textAlign: "left", opacity: m.ok ? 1 : 0.5 }}
                onClick={() => m.ok && !pdfLoading && handlePDF(m.id)}>
                <div style={{ fontWeight: 700, fontSize: 15, color: C.navy }}>{pdfLoading ? "PDF生成中..." : m.label}</div>
                <div style={{ fontSize: 12, color: C.gray, marginTop: 3 }}>{m.desc}{!m.ok ? " ―（対応データなし）" : ""}</div>
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Balance Modal ────────────────────────────────────────────────────────────
function BalanceModal({ client, balance, onClose }) {
  const [amount, setAmount] = useState("");
  const [date, setDate] = useState(today());
  const save = async () => {
    const n = Number(amount);
    if (!n || n <= 0) return alert("金額を入力してください");
    const prev = balance?.currentBalance || 0;
    const newBalance = prev - n;
    await setDoc(doc(db, "clientBalances", client.id), {
      clientId: client.id, prevBalance: prev,
      currentBalance: newBalance,
      paidAmount: (balance?.paidAmount || 0) + n,
      lastPaidDate: date, lastPaidAmount: n,
      updatedAt: serverTimestamp(),
    });
    // 入金履歴を保存
    await addDoc(collection(db, "paymentHistory"), {
      clientId: client.id, clientName: client.name || "",
      amount: n, date, prevBalance: prev, newBalance,
      createdAt: serverTimestamp(),
    });
    onClose();
  };
  return (
    <div style={s.modal} onClick={onClose}>
      <div style={{ ...s.modalBox, maxWidth: 380 }} onClick={e => e.stopPropagation()}>
        <h3 style={{ margin: "0 0 20px", color: C.navy }}>入金を記録　— {client?.name}</h3>
        <div style={{ background: C.pale, padding: 14, borderRadius: 8, marginBottom: 18, fontSize: 14 }}>
          現在の残高：<strong style={{ color: C.red }}>¥{fmt(balance?.currentBalance || 0)}</strong>
        </div>
        <div style={{ ...s.row, marginBottom: 16 }}>
          <div style={s.col}><span style={s.label}>入金日</span><input style={s.input} type="date" value={date} onChange={e => setDate(e.target.value)} /></div>
          <div style={s.col}><span style={s.label}>入金額</span><input style={{ ...s.input, minWidth: 160 }} type="number" value={amount} onChange={e => setAmount(e.target.value)} /></div>
        </div>
        <div style={{ ...s.row, justifyContent: "flex-end", gap: 8 }}>
          <button style={s.btn("light")} onClick={onClose}>キャンセル</button>
          <button style={s.btn("green")} onClick={save}>保存</button>
        </div>
      </div>
    </div>
  );
}

// ── Product Picker ────────────────────────────────────────────────────────────
function ProductPicker({ products, onSelect, onClose }) {
  const [q, setQ] = useState("");
  const [cat, setCat] = useState("");
  const pickerCats = [...new Set(products.map(p => p.category).filter(Boolean))].sort((a,b) => a.localeCompare(b,"ja"));
  const filtered = products.filter(p => (!cat || p.category === cat) && (p.name?.includes(q) || p.code?.includes(q)));
  return (
    <div style={s.modal} onClick={onClose}>
      <div style={{ ...s.modalBox, maxWidth: 520 }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
          <h3 style={{ margin: 0, color: C.navy }}>商品を選ぶ</h3>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
          <input style={{ ...s.input, flex: 1 }} placeholder="商品名・コードで検索" value={q} onChange={e => setQ(e.target.value)} />
          {pickerCats.length > 0 && (
            <select style={{ ...s.select, width: 140 }} value={cat} onChange={e => setCat(e.target.value)}>
              <option value="">全カテゴリ</option>
              {pickerCats.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          )}
        </div>
        <table style={s.table}>
          <thead><tr><th style={s.th}>商品名</th><th style={s.th}>コード</th><th style={s.th}>単価</th><th style={s.th}></th></tr></thead>
          <tbody>
            {filtered.map(p => (
              <tr key={p.id}>
                <td style={s.td}>{p.name}</td><td style={s.td}>{p.code}</td><td style={s.td}>¥{fmt(p.price)}</td>
                <td style={s.td}><button style={s.btn("gold")} onClick={() => onSelect(p)}>選択</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Item Row ──────────────────────────────────────────────────────────────────
function ItemRow({ item, idx, onChange, onRemove, onPickProduct }) {
  const rate = item.taxRate !== undefined && item.taxRate !== null && item.taxRate !== "" ? Number(item.taxRate) : 10;
  return (
    <tr>
      <td style={s.td}>
        <div style={{ display: "flex", gap: 4 }}>
          <input style={{ ...s.input, flex: 1, minWidth: 160 }} value={item.name} onChange={e => onChange(idx, "name", e.target.value)} placeholder="品名" />
          <button style={{ ...s.btn("light"), padding: "6px 10px", fontSize: 12 }} onClick={() => onPickProduct(idx)}>📦</button>
        </div>
      </td>
      <td style={s.td}><input style={{ ...s.input, width: 70 }} type="number" value={item.qty} onChange={e => onChange(idx, "qty", e.target.value)} /></td>
      <td style={s.td}><input style={{ ...s.input, width: 60 }} value={item.unit} onChange={e => onChange(idx, "unit", e.target.value)} placeholder="袋" /></td>
      <td style={s.td}><input style={{ ...s.input, width: 100 }} type="number" value={item.price} onChange={e => onChange(idx, "price", e.target.value)} /></td>
      <td style={{ ...s.td, textAlign: "center", fontSize: 12, color: C.gray }}>{rate}%</td>
      <td style={{ ...s.td, textAlign: "right" }}>¥{fmt(Number(item.qty||0)*Number(item.price||0))}</td>
      <td style={s.td}><button style={{ ...s.btn("red"), padding: "4px 10px" }} onClick={() => onRemove(idx)}>✕</button></td>
    </tr>
  );
}

// ── Delivery Form ─────────────────────────────────────────────────────────────
function DeliveryForm({ clients, products, deliveries, clientPrices, divisions, company, onSave, onClose, editing }) {
  const defRate = company?.defaultTaxRate !== undefined ? company.defaultTaxRate : 10;
  const [form, setForm] = useState(editing || { clientId: "", divisionId: "", date: today(), notes: "", items: [{ name: "", qty: 1, unit: "", price: 0, taxRate: defRate }] });
  const [pickerIdx, setPickerIdx] = useState(null);
  const setF = (k, v) => setForm(f => ({ ...f, [k]: v }));
  const setItem = (idx, k, v) => setForm(f => ({ ...f, items: f.items.map((it, i) => i === idx ? { ...it, [k]: v } : it) }));
  const addItem = () => setForm(f => ({ ...f, items: [...f.items, { name: "", qty: 1, unit: "", price: 0, taxRate: defRate }] }));
  const remItem = (idx) => setForm(f => ({ ...f, items: f.items.filter((_, i) => i !== idx) }));
  const { sub, tax, total: grandTotal } = totalFromItems(form.items);
  const taxGroups = calcTaxByRate(form.items);
  const save = async () => {
    if (!form.clientId) return alert("取引先を選択してください");
    if (!form.items.some(i => i.name)) return alert("品目を入力してください");
    const data = { ...form, docNo: editing?.docNo || genDocNo("NO", deliveries), status: editing?.status || "unissued", subtotal: sub, tax, total: grandTotal, updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db, "deliveries", editing.id), data);
    else { data.createdAt = serverTimestamp(); await addDoc(collection(db, "deliveries"), data); }
    onSave();
  };
  return (
    <div style={s.modal}>
      <div style={s.modalBox}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
          <h2 style={{ margin: 0, color: C.navy }}>{editing ? "納品書を編集" : "納品書を作成"}</h2>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <div style={{ ...s.row, marginBottom: 16 }}>
          <div style={s.col}><span style={s.label}>取引先 *</span>
            <select style={s.select} value={form.clientId} onChange={e => {
              const cid = e.target.value;
              const cl = clients.find(c => c.id === cid);
              setF("clientId", cid);
              if (cl?.divisionId) setF("divisionId", cl.divisionId);
            }}>
              <option value="">選択してください</option>
              {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
          </div>
          <div style={s.col}><span style={s.label}>事業部</span>
            <select style={s.select} value={form.divisionId||""} onChange={e => setF("divisionId", e.target.value)}>
              <option value="">指定なし（設定の自社情報を使用）</option>
              {divisions.map(d => <option key={d.id} value={d.id}>{d.name}</option>)}
            </select>
          </div>
          <div style={s.col}><span style={s.label}>納品日 *</span><input style={s.input} type="date" value={form.date} onChange={e => setF("date", e.target.value)} /></div>
        </div>
        <table style={s.table}>
          <thead><tr><th style={s.th}>品名</th><th style={s.th}>数量</th><th style={s.th}>単位</th><th style={s.th}>単価</th><th style={s.th}>税率</th><th style={s.th}>金額</th><th style={s.th}></th></tr></thead>
          <tbody>{form.items.map((it, idx) => <ItemRow key={idx} item={it} idx={idx} onChange={setItem} onRemove={remItem} onPickProduct={setPickerIdx} />)}</tbody>
        </table>
        <button style={{ ...s.btn("light"), marginBottom: 16 }} onClick={addItem}>＋ 行を追加</button>
        <div style={{ textAlign: "right", marginBottom: 16 }}>
          {Object.keys(taxGroups).sort((a,b)=>Number(b)-Number(a)).map(r => <div key={r} style={{ fontSize: 12, color: C.gray }}>課税{r}% 小計¥{fmt(taxGroups[r].sub)}　税¥{fmt(taxGroups[r].tax)}</div>)}
          <div style={{ fontSize: 13, color: C.gray, marginTop: 4 }}>小計 ¥{fmt(sub)}　消費税 ¥{fmt(tax)}</div>
          <div style={{ fontSize: 18, fontWeight: 700, color: C.navy }}>合計 ¥{fmt(grandTotal)}</div>
        </div>
        <div style={s.col}><span style={s.label}>備考</span><textarea style={{ ...s.input, minHeight: 60 }} value={form.notes} onChange={e => setF("notes", e.target.value)} /></div>
        <div style={{ ...s.row, justifyContent: "flex-end", marginTop: 20 }}>
          <button style={s.btn("light")} onClick={onClose}>キャンセル</button>
          <button style={s.btn("primary")} onClick={save}>{editing ? "更新" : "保存"}</button>
        </div>
      </div>
      {pickerIdx !== null && <ProductPicker products={products} onClose={() => setPickerIdx(null)}
        onSelect={p => {
          const cp = clientPrices?.find(cp => cp.clientId === form.clientId && cp.productId === p.id);
          const price = cp ? cp.price : (p.price || 0);
          setItem(pickerIdx,"name",p.name); setItem(pickerIdx,"unit",p.unit||""); setItem(pickerIdx,"price",price); setItem(pickerIdx,"taxRate",p.taxRate!==undefined?p.taxRate:10); setPickerIdx(null);
        }} />}
    </div>
  );
}

// ── Quotation Form ───────────────────────────────────────────────────────────
function QuotationForm({ clients, products, quotations, clientPrices, divisions, company, onSave, onClose, editing }) {
  const defRate = company?.defaultTaxRate !== undefined ? company.defaultTaxRate : 10;
  const validDefault = () => { const d = new Date(); d.setMonth(d.getMonth() + 1); return d.toISOString().split("T")[0]; };
  const [form, setForm] = useState(editing || { clientId: "", divisionId: "", date: today(), validUntil: validDefault(), notes: "", items: [{ name: "", qty: 1, unit: "", price: 0, taxRate: defRate }] });
  const [pickerIdx, setPickerIdx] = useState(null);
  const setF = (k, v) => setForm(f => ({ ...f, [k]: v }));
  const setItem = (idx, k, v) => setForm(f => ({ ...f, items: f.items.map((it, i) => i === idx ? { ...it, [k]: v } : it) }));
  const addItem = () => setForm(f => ({ ...f, items: [...f.items, { name: "", qty: 1, unit: "", price: 0, taxRate: defRate }] }));
  const remItem = (idx) => setForm(f => ({ ...f, items: f.items.filter((_, i) => i !== idx) }));
  const { sub, tax, total: grandTotal } = totalFromItems(form.items);
  const taxGroups = calcTaxByRate(form.items);
  const save = async () => {
    if (!form.clientId) return alert("取引先を選択してください");
    if (!form.items.some(i => i.name)) return alert("品目を入力してください");
    const data = { ...form, docNo: editing?.docNo || genDocNo("EST", quotations), status: editing?.status || "draft", subtotal: sub, tax, total: grandTotal, updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db, "quotations", editing.id), data);
    else { data.createdAt = serverTimestamp(); await addDoc(collection(db, "quotations"), data); }
    onSave();
  };
  return (
    <div style={s.modal}>
      <div style={s.modalBox}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
          <h2 style={{ margin: 0, color: C.navy }}>{editing ? "見積書を編集" : "見積書を作成"}</h2>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <div style={{ ...s.row, marginBottom: 16 }}>
          <div style={s.col}><span style={s.label}>取引先 *</span>
            <select style={s.select} value={form.clientId} onChange={e => {
              const cid = e.target.value;
              const cl = clients.find(c => c.id === cid);
              setF("clientId", cid);
              if (cl?.divisionId) setF("divisionId", cl.divisionId);
            }}>
              <option value="">選択してください</option>
              {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
          </div>
          <div style={s.col}><span style={s.label}>事業部</span>
            <select style={s.select} value={form.divisionId||""} onChange={e => setF("divisionId", e.target.value)}>
              <option value="">指定なし（設定の自社情報を使用）</option>
              {divisions.map(d => <option key={d.id} value={d.id}>{d.name}</option>)}
            </select>
          </div>
          <div style={s.col}><span style={s.label}>見積日 *</span><input style={s.input} type="date" value={form.date} onChange={e => setF("date", e.target.value)} /></div>
          <div style={s.col}><span style={s.label}>有効期限</span><input style={s.input} type="date" value={form.validUntil} onChange={e => setF("validUntil", e.target.value)} /></div>
        </div>
        <table style={s.table}>
          <thead><tr><th style={s.th}>品名</th><th style={s.th}>数量</th><th style={s.th}>単位</th><th style={s.th}>単価</th><th style={s.th}>税率</th><th style={s.th}>金額</th><th style={s.th}></th></tr></thead>
          <tbody>{form.items.map((it, idx) => <ItemRow key={idx} item={it} idx={idx} onChange={setItem} onRemove={remItem} onPickProduct={setPickerIdx} />)}</tbody>
        </table>
        <button style={{ ...s.btn("light"), marginBottom: 16 }} onClick={addItem}>＋ 行を追加</button>
        <div style={{ textAlign: "right", marginBottom: 16 }}>
          {Object.keys(taxGroups).sort((a,b)=>Number(b)-Number(a)).map(r => <div key={r} style={{ fontSize: 12, color: C.gray }}>課税{r}% 小計¥{fmt(taxGroups[r].sub)}　税¥{fmt(taxGroups[r].tax)}</div>)}
          <div style={{ fontSize: 13, color: C.gray, marginTop: 4 }}>小計 ¥{fmt(sub)}　消費税 ¥{fmt(tax)}</div>
          <div style={{ fontSize: 18, fontWeight: 700, color: C.navy }}>合計 ¥{fmt(grandTotal)}</div>
        </div>
        <div style={s.col}><span style={s.label}>備考</span><textarea style={{ ...s.input, minHeight: 60 }} value={form.notes} onChange={e => setF("notes", e.target.value)} /></div>
        <div style={{ ...s.row, justifyContent: "flex-end", marginTop: 20 }}>
          <button style={s.btn("light")} onClick={onClose}>キャンセル</button>
          <button style={s.btn("primary")} onClick={save}>{editing ? "更新" : "保存"}</button>
        </div>
      </div>
      {pickerIdx !== null && <ProductPicker products={products} onClose={() => setPickerIdx(null)}
        onSelect={p => {
          const cp = clientPrices?.find(cp => cp.clientId === form.clientId && cp.productId === p.id);
          const price = cp ? cp.price : (p.price || 0);
          setItem(pickerIdx,"name",p.name); setItem(pickerIdx,"unit",p.unit||""); setItem(pickerIdx,"price",price); setItem(pickerIdx,"taxRate",p.taxRate!==undefined?p.taxRate:10); setPickerIdx(null);
        }} />}
    </div>
  );
}

// ── Quotations List ──────────────────────────────────────────────────────────
function QuotationsList({ clients, quotations, products, deliveries, company, clientPrices, divisions, isAdmin }) {
  const [search, setSearch] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [editing, setEditing] = useState(null);
  const [printTarget, setPrintTarget] = useState(null);
  const filtered = quotations.filter(q => {
    const cn = clients.find(c => c.id === q.clientId)?.name || "";
    return cn.includes(search) || (q.docNo || "").includes(search);
  });
  const deleteQ = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db, "quotations", id)); };
  const toDelivery = async (q) => {
    const data = {
      docNo: genDocNo("NO", deliveries), clientId: q.clientId, divisionId: q.divisionId || "",
      date: today(), notes: q.notes || "", items: q.items,
      subtotal: q.subtotal, tax: q.tax, total: q.total,
      status: "unissued", quotationRef: q.docNo, createdAt: serverTimestamp(),
    };
    await addDoc(collection(db, "deliveries"), data);
    await updateDoc(doc(db, "quotations", q.id), { status: "ordered" });
    alert(`納品書 ${data.docNo} を作成しました`);
  };
  const handlePrint = (q) => {
    const div = divisions?.find(d => d.id === q.divisionId);
    const co = div ? { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) } : company;
    printQuotation(q, clients, co);
  };
  const handlePDF = async (q) => {
    const div = divisions?.find(d => d.id === q.divisionId);
    const co = div ? { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) } : company;
    const cl = clients.find(c => c.id === q.clientId) || {};
    const safeName = (cl.name || "").replace(/[\\/:*?"<>|]/g, "_");
    try {
      const result = await generatePDF(buildQuotationHTML(q, clients, co), `見積書_${safeName}_${q.docNo}.pdf`);
      if (result) {
        result.pdf.save(result.filename);
        const storagePath = `pdfs/quotations/${q.docNo}.pdf`;
        const url = await savePDFToStorage(result.blob, storagePath);
        await addDoc(collection(db, "pdfHistory"), {
          type: "見積書", docNo: q.docNo, clientId: q.clientId, clientName: cl.name || "",
          filename: result.filename, storageUrl: url, storagePath, createdAt: serverTimestamp(),
        });
      }
    } catch (e) { alert("PDF生成エラー: " + e.message); }
  };
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={s.pageTitle}>見積書一覧</div>
        <button style={s.btn("primary")} onClick={() => { setEditing(null); setShowForm(true); }}>＋ 新規作成</button>
      </div>
      <div style={{ ...s.card, padding: "12px 20px" }}>
        <input style={s.input} placeholder="取引先名・見積番号で検索" value={search} onChange={e => setSearch(e.target.value)} />
      </div>
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>見積番号</th><th style={s.th}>見積日</th><th style={s.th}>有効期限</th><th style={s.th}>取引先</th><th style={s.th}>金額</th><th style={s.th}>状態</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {filtered.map(q => {
              const client = clients.find(c => c.id === q.clientId);
              const expired = q.validUntil && q.validUntil < today() && q.status === "draft";
              return (
                <tr key={q.id}>
                  <td style={s.td}>{q.docNo}</td><td style={s.td}>{q.date}</td>
                  <td style={{ ...s.td, color: expired ? C.red : "inherit" }}>{q.validUntil || "—"}{expired ? " (期限切れ)" : ""}</td>
                  <td style={s.td}>{client?.name || "—"}</td>
                  <td style={s.td}>¥{fmt(q.total)}</td>
                  <td style={s.td}><span style={s.badge(q.status === "ordered" ? "green" : expired ? "red" : "gold")}>{q.status === "ordered" ? "受注済" : expired ? "期限切れ" : "作成済"}</span></td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => { setEditing(q); setShowForm(true); }}>編集</button>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => handlePrint(q)}>🖨 印刷</button>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => handlePDF(q)}>📥 PDF</button>
                      {q.status !== "ordered" && (
                        <button style={{ ...s.btn("gold"), padding: "4px 8px", fontSize: 12 }} onClick={() => toDelivery(q)}>納品書作成</button>
                      )}
                      {isAdmin && <button style={{ ...s.btn("red"), padding: "4px 8px", fontSize: 12 }} onClick={() => deleteQ(q.id)}>削除</button>}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {showForm && <QuotationForm clients={clients} products={products} quotations={quotations} clientPrices={clientPrices} divisions={divisions} company={company} editing={editing}
        onSave={() => setShowForm(false)} onClose={() => setShowForm(false)} />}
    </div>
  );
}

// ── Home (やることリスト) ─────────────────────────────────────────────────────
function HomePage({ clients, deliveries, invoices, balances, pendings, setPage }) {
  const overdueInv = invoices.filter(i => i.status === "unpaid" && i.dueDate && i.dueDate < today());
  const pendingApprovals = pendings.filter(p => p.status === "pending");
  const unsentInv = invoices.filter(i => i.status === "unpaid" && (!i.sentStatus || i.sentStatus === "scheduled"));
  const unissuedDel = deliveries.filter(d => d.status === "unissued");

  // 次の締日を計算
  const closingSchedule = (() => {
    const now = new Date();
    const y = now.getFullYear(), m = now.getMonth();
    const closingDays = new Set();
    clients.forEach(c => {
      if (c.billingType === "closing" || c.billingType === "monthly") {
        (c.closingDays && c.closingDays.length ? c.closingDays : [0]).forEach(d => closingDays.add(d));
      }
    });
    const upcoming = [];
    closingDays.forEach(cd => {
      const lastDay = new Date(y, m + 1, 0).getDate();
      const day = cd === 0 ? lastDay : Math.min(cd, lastDay);
      const dateStr = `${y}-${String(m + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
      if (dateStr >= today()) {
        const targetClients = clients.filter(c => {
          if (c.billingType !== "closing" && c.billingType !== "monthly") return false;
          const cDays = c.closingDays && c.closingDays.length ? c.closingDays : [0];
          return cDays.includes(cd);
        });
        const targetDels = unissuedDel.filter(d => targetClients.some(c => c.id === d.clientId));
        if (targetClients.length > 0) {
          upcoming.push({ date: dateStr, day: cd === 0 ? "末日" : cd + "日", clientCount: targetClients.length, delCount: targetDels.length });
        }
      }
    });
    return upcoming.sort((a, b) => a.date.localeCompare(b.date));
  })();

  const scheduledSend = invoices.filter(i => i.sentStatus === "scheduled" && i.scheduledSendDate && i.scheduledSendDate >= today()).sort((a, b) => (a.scheduledSendDate || "").localeCompare(b.scheduledSendDate || ""));

  const sections = [
    { label: "承認待ち", count: pendingApprovals.length, color: "#E67E22", icon: "⏳", page: "pending",
      detail: pendingApprovals.slice(0, 3).map(p => ({ text: `${p.clientName || "—"} ¥${fmt(p.total)}`, sub: p.billingType === "recurring" ? "定期請求" : "締日請求" })) },
    { label: "未送信の請求書", count: unsentInv.length, color: "#3498DB", icon: "📨", page: "invoices",
      detail: unsentInv.slice(0, 3).map(i => ({ text: `${i.docNo} ¥${fmt(i.total)}`, sub: clients.find(c => c.id === i.clientId)?.name || "" })) },
    { label: "期限超過の未入金", count: overdueInv.length, color: C.red, icon: "⚠", page: "balance",
      detail: overdueInv.slice(0, 3).map(i => ({ text: `${i.docNo} ¥${fmt(i.total)}`, sub: `期限: ${i.dueDate}` })) },
    { label: "未請求の納品書", count: unissuedDel.length, color: C.gold, icon: "📦", page: "deliveries",
      detail: unissuedDel.slice(0, 3).map(d => ({ text: `${d.docNo} ¥${fmt(d.total)}`, sub: clients.find(c => c.id === d.clientId)?.name || "" })) },
  ];

  const activeSections = sections.filter(sec => sec.count > 0);

  return (
    <div>
      <div style={s.pageTitle}>ホーム</div>
      {activeSections.length === 0 ? (
        <div style={{ ...s.card, textAlign: "center", padding: "40px 20px", color: C.green }}>
          <div style={{ fontSize: 36, marginBottom: 12 }}>✓</div>
          <div style={{ fontSize: 16, fontWeight: 600 }}>対応が必要な項目はありません</div>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 24 }}>
          {activeSections.map(sec => (
            <div key={sec.label} style={{ ...s.card, margin: 0, cursor: "pointer", borderLeft: `4px solid ${sec.color}` }} onClick={() => setPage(sec.page)}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: sec.detail.length > 0 ? 10 : 0 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <span style={{ fontSize: 20 }}>{sec.icon}</span>
                  <span style={{ fontSize: 15, fontWeight: 600, color: C.navy }}>{sec.label}</span>
                </div>
                <span style={{ fontSize: 22, fontWeight: 700, color: sec.color }}>{sec.count}件</span>
              </div>
              {sec.detail.length > 0 && (
                <div style={{ paddingLeft: 30 }}>
                  {sec.detail.map((d, i) => (
                    <div key={i} style={{ fontSize: 13, color: C.gray, padding: "2px 0", display: "flex", gap: 8 }}>
                      <span>{d.text}</span>
                      {d.sub && <span style={{ color: "#aaa" }}>({d.sub})</span>}
                    </div>
                  ))}
                  {sec.count > 3 && <div style={{ fontSize: 12, color: sec.color, marginTop: 4 }}>他 {sec.count - 3}件 →</div>}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {(closingSchedule.length > 0 || scheduledSend.length > 0) && (
        <div style={s.card}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>今後の予定</h3>
          {closingSchedule.length > 0 && (
            <div style={{ marginBottom: scheduledSend.length > 0 ? 16 : 0 }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: C.navy, marginBottom: 8 }}>締日スケジュール</div>
              {closingSchedule.map((cs, i) => (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: "6px 0", borderBottom: i < closingSchedule.length - 1 ? `1px solid ${C.light}` : "none" }}>
                  <span style={{ fontSize: 13, fontWeight: 600, color: C.navy, minWidth: 90 }}>{cs.date}</span>
                  <span style={{ fontSize: 13, color: C.gray }}>（{cs.day}締め）</span>
                  <span style={{ fontSize: 13, color: C.navy }}>{cs.clientCount}社</span>
                  {cs.delCount > 0 && <span style={{ fontSize: 12, color: C.gold }}>未請求 {cs.delCount}件</span>}
                </div>
              ))}
            </div>
          )}
          {scheduledSend.length > 0 && (
            <div>
              <div style={{ fontSize: 13, fontWeight: 600, color: C.navy, marginBottom: 8 }}>送信予定</div>
              {scheduledSend.slice(0, 5).map((inv, i) => (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: "6px 0", borderBottom: i < Math.min(scheduledSend.length, 5) - 1 ? `1px solid ${C.light}` : "none" }}>
                  <span style={{ fontSize: 13, fontWeight: 600, color: C.navy, minWidth: 90 }}>{inv.scheduledSendDate}</span>
                  <span style={{ fontSize: 13, color: C.gray }}>{inv.docNo}</span>
                  <span style={{ fontSize: 13, color: C.navy }}>¥{fmt(inv.total)}</span>
                  <span style={{ fontSize: 12, color: C.gray }}>{clients.find(c => c.id === inv.clientId)?.name || ""}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Dashboard (売上概況) ─────────────────────────────────────────────────────
function Dashboard({ clients, deliveries, invoices, balances }) {
  const totalBalance = Object.values(balances).reduce((a, b) => a + (b.currentBalance || 0), 0);
  const thisMonth = new Date().toISOString().slice(0, 7);
  const monthDel = deliveries.filter(d => (d.date || "").startsWith(thisMonth));
  const monthInv = invoices.filter(i => (i.date || "").startsWith(thisMonth));
  const monthSales = monthInv.reduce((a, i) => a + (i.total || 0), 0);
  const overdue = invoices.filter(i => i.status === "unpaid" && i.dueDate && i.dueDate < today());
  const oneMonthAgo = new Date(); oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
  const overdueAlert = invoices.filter(i => i.status === "unpaid" && i.date && i.date <= oneMonthAgo.toISOString().split("T")[0]);
  return (
    <div>
      <div style={s.pageTitle}>売上概況</div>
      {overdueAlert.length > 0 && (
        <div style={{ background: "#f8d7da", border: `1px solid ${C.red}`, borderRadius: 10, padding: "12px 20px", marginBottom: 16, display: "flex", alignItems: "center", gap: 12 }}>
          <span style={{ fontSize: 22 }}>⚠</span>
          <div style={{ fontSize: 14, color: C.red, fontWeight: 700 }}>未入金アラート：{overdueAlert.length}件の請求書が1ヶ月以上未入金です</div>
        </div>
      )}
      <div style={{ display: "flex", gap: 16, marginBottom: 24, flexWrap: "wrap" }}>
        {[
          { label: "取引先数", value: clients.length + " 社", color: C.navy },
          { label: "今月の納品数", value: monthDel.length + " 件", color: C.gold },
          { label: "今月の売上", value: "¥" + fmt(monthSales), color: C.navy },
          { label: "未収残高合計", value: "¥" + fmt(totalBalance), color: totalBalance > 0 ? C.red : C.green },
          { label: "期限超過", value: overdue.length + " 件", color: overdue.length > 0 ? C.red : C.green },
        ].map(st => (
          <div key={st.label} style={{ ...s.card, flex: "1 1 160px", textAlign: "center", margin: 0 }}>
            <div style={{ fontSize: 13, color: C.gray, marginBottom: 8 }}>{st.label}</div>
            <div style={{ fontSize: 26, fontWeight: 700, color: st.color }}>{st.value}</div>
          </div>
        ))}
      </div>
      <div style={s.card}>
        <h3 style={{ margin: "0 0 16px", color: C.navy }}>最近の納品書</h3>
        <table style={s.table}>
          <thead><tr><th style={s.th}>伝票番号</th><th style={s.th}>日付</th><th style={s.th}>取引先</th><th style={s.th}>金額</th><th style={s.th}>状態</th></tr></thead>
          <tbody>
            {deliveries.slice(0, 8).map(d => (
              <tr key={d.id}>
                <td style={s.td}>{d.docNo}</td><td style={s.td}>{d.date}</td>
                <td style={s.td}>{clients.find(c => c.id === d.clientId)?.name || "—"}</td>
                <td style={s.td}>¥{fmt(d.total)}</td>
                <td style={s.td}><span style={s.badge(d.status === "invoiced" ? "green" : d.status === "pending_approval" ? "blue" : "gold")}>{d.status === "invoiced" ? "請求済" : d.status === "pending_approval" ? "承認待ち" : "未請求"}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Deliveries ────────────────────────────────────────────────────────────────
function DeliveriesList({ clients, deliveries, products, invoices, company, balances, clientPrices, divisions, isAdmin }) {
  const [search, setSearch] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [editing, setEditing] = useState(null);
  const [printTarget, setPrintTarget] = useState(null);
  const filtered = deliveries.filter(d => {
    const cn = clients.find(c => c.id === d.clientId)?.name || "";
    return cn.includes(search) || (d.docNo || "").includes(search);
  });
  const deleteD = async (id) => {
    if (!confirm("削除しますか？")) return;
    await deleteDoc(doc(db, "deliveries", id));
    // 関連する承認待ち（pendingBillings）を削除
    const pbSnap = await getDocs(query(collection(db, "pendingBillings"), where("status", "==", "pending")));
    for (const pbDoc of pbSnap.docs) {
      const d = pbDoc.data();
      const delIds = d.deliveryIds ? (Array.isArray(d.deliveryIds) ? d.deliveryIds : [d.deliveryIds]) : (d.deliveryId ? [d.deliveryId] : []);
      if (delIds.includes(id)) await deleteDoc(pbDoc.ref);
    }
  };
  const issueInvoice = async (d) => {
    const cl = clients.find(c => c.id === d.clientId);
    if (!confirm(`${cl?.name || "—"} の請求書（¥${fmt(d.total)}）を発行しますか？${company?.invoiceApproval ? "\n承認後に発行・送信されます。" : "\n次回のcron実行時に自動送信されます。"}`)) return;
    if (company?.invoiceApproval) {
      // 承認待ちに追加
      await addDoc(collection(db, "pendingBillings"), {
        type: "invoice", clientId: d.clientId, clientName: cl?.name || "",
        divisionId: cl?.divisionId || "", deliveryId: d.id, deliveryDocNo: d.docNo,
        items: d.items, subtotal: d.subtotal, tax: d.tax, total: d.total,
        billingType: "immediate",
        status: "pending", createdAt: serverTimestamp(),
      });
      await updateDoc(doc(db, "deliveries", d.id), { status: "pending_approval" });
      alert("承認待ちに追加しました。承認後に請求書が発行・送信されます。");
      return;
    }
    const inv = {
      docNo: genDocNo("INV", invoices), clientId: d.clientId, date: today(),
      dueDate: nextMonthEnd(d.date), billingType: "immediate",
      deliveryRef: d.docNo, deliveryRefs: [d.docNo],
      items: d.items, subtotal: d.subtotal, tax: d.tax, total: d.total,
      status: "unpaid", createdAt: serverTimestamp(),
    };
    const invRef = await addDoc(collection(db, "invoices"), inv);
    await updateDoc(doc(db, "deliveries", d.id), { status: "invoiced", invoiceId: invRef.id });
    const bal = balances[d.clientId] || {};
    await setDoc(doc(db, "clientBalances", d.clientId), {
      clientId: d.clientId, prevBalance: bal.currentBalance || 0,
      currentBalance: (bal.currentBalance || 0) + d.total,
      paidAmount: bal.paidAmount || 0, updatedAt: serverTimestamp(),
    });
    // 即送信（sendModeがmanual以外かつメールあり）
    if (cl?.email && cl?.sendMode !== "manual") {
      try {
        let coInfo = company || {};
        if (cl.divisionId) {
          const div = divisions.find(dv => dv.id === cl.divisionId);
          if (div) coInfo = { ...coInfo, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) };
        }
        const res = await fetch("/api/send-invoice", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            to: cl.email,
            subject: `【請求書】${inv.docNo} ${coInfo.name || ""}`,
            html: `<div style="font-family:sans-serif;color:#333;">
              <p>${cl.name || ""} 御中</p>
              <p>いつもお世話になっております。<br>${coInfo.name || ""}です。</p>
              <p>請求書（${inv.docNo}）をお送りいたします。</p>
              <p>金額：&yen;${fmt(d.total)}</p>
              <p>ご確認のほど、よろしくお願いいたします。</p>
              <hr style="border:none;border-top:1px solid #ddd;margin:20px 0">
              <p style="font-size:12px;color:#888">${coInfo.name || ""}<br>${coInfo.address || ""}<br>TEL ${coInfo.tel || ""}</p>
            </div>`,
          }),
        });
        if (res.ok) {
          await addDoc(collection(db, "sendHistory"), {
            docNo: inv.docNo, invoiceId: invRef.id, clientId: d.clientId,
            clientName: cl.name || "", email: cl.email, method: "auto",
            memo: "即時発行自動送信", amount: d.total, sentAt: serverTimestamp(), sentBy: "immediate",
          });
          await updateDoc(doc(db, "invoices", invRef.id), { sentStatus: "sent", lastSentAt: serverTimestamp() });
          alert(`請求書を発行し、${cl.name}（${cl.email}）にメール送信しました`);
        } else {
          alert("請求書を発行しました（メール送信に失敗しました。手動で送信してください）");
        }
      } catch (e2) {
        alert(`請求書を発行しました（メール送信エラー: ${e2.message}）`);
      }
    } else {
      alert(`請求書を発行しました${!cl?.email ? "（メールアドレス未設定）" : cl?.sendMode === "manual" ? "（手動送信）" : ""}`);
    }
  };
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={s.pageTitle}>納品書一覧</div>
        <button style={s.btn("primary")} onClick={() => { setEditing(null); setShowForm(true); }}>＋ 新規作成</button>
      </div>
      <div style={{ ...s.card, padding: "12px 20px" }}>
        <input style={s.input} placeholder="取引先名・伝票番号で検索" value={search} onChange={e => setSearch(e.target.value)} />
      </div>
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>伝票番号</th><th style={s.th}>日付</th><th style={s.th}>取引先</th><th style={s.th}>金額</th><th style={s.th}>状態</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {filtered.map(d => {
              const client = clients.find(c => c.id === d.clientId);
              return (
                <tr key={d.id}>
                  <td style={s.td}>{d.docNo}</td><td style={s.td}>{d.date}</td>
                  <td style={s.td}>{client?.name || "—"}<br /><span style={{ fontSize: 11, color: C.gray }}>{(client?.billingType === "closing" || client?.billingType === "monthly") ? closingDaysLabel(client?.closingDays || [0]) : "即時"}</span></td>
                  <td style={s.td}>¥{fmt(d.total)}</td>
                  <td style={s.td}><span style={s.badge(d.status === "invoiced" ? "green" : d.status === "pending_approval" ? "blue" : "gold")}>{d.status === "invoiced" ? "請求済" : d.status === "pending_approval" ? "承認待ち" : "未請求"}</span></td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => { setEditing(d); setShowForm(true); }}>編集</button>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => setPrintTarget({ delivery: d, invoice: invoices.find(i => i.deliveryRef === d.docNo) })}>🖨 印刷</button>
                      {d.status === "unissued" && client?.billingType !== "closing" && client?.billingType !== "monthly" && (
                        <button style={{ ...s.btn("gold"), padding: "4px 8px", fontSize: 12 }} onClick={() => issueInvoice(d)}>請求書発行</button>
                      )}
                      {isAdmin && <button style={{ ...s.btn("red"), padding: "4px 8px", fontSize: 12 }} onClick={() => deleteD(d.id)}>削除</button>}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {showForm && <DeliveryForm clients={clients} products={products} deliveries={deliveries} clientPrices={clientPrices} divisions={divisions} company={company} editing={editing}
        onSave={() => setShowForm(false)} onClose={() => setShowForm(false)} />}
      {printTarget && <PrintModeModal invoice={printTarget.invoice} delivery={printTarget.delivery}
        clients={clients} company={company} balances={balances} divisions={divisions} onClose={() => setPrintTarget(null)} />}
    </div>
  );
}

// ── Send Record Modal ─────────────────────────────────────────────────────────
function SendRecordModal({ invoice, clients, company, divisions, balances, onClose }) {
  const cl = clients.find(c => c.id === invoice?.clientId) || {};
  const [method, setMethod] = useState("mail");
  const [memo, setMemo] = useState("");
  const [email, setEmail] = useState(cl.email || "");
  const [sending, setSending] = useState(false);
  const [result, setResult] = useState(null);

  const docData = invoice || {};
  const div = divisions?.find(d => d.id === docData.divisionId);
  const co = div ? { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) } : company;
  const bal = balances?.[invoice?.clientId];

  const sendEmail = async () => {
    if (!email) return alert("メールアドレスを入力してください");
    setSending(true);
    setResult(null);
    try {
      // PDF生成
      const invoiceHTML = buildInvoiceHTML(invoice, clients, co);
      const safeName = (cl.name||"").replace(/[\\/:*?"<>|]/g,"_");
      const { blob, filename } = await generatePDF(invoiceHTML, `請求書_${safeName}_${invoice.docNo}.pdf`);

      // StorageにPDF保存してダウンロードURL取得
      let pdfUrl = "";
      try {
        const storagePath = `pdfs/invoices/${invoice.docNo}.pdf`;
        pdfUrl = await savePDFToStorage(blob, storagePath);
        await addDoc(collection(db, "pdfHistory"), {
          type: "請求書", docNo: invoice.docNo,
          clientId: invoice.clientId, clientName: cl.name || "",
          filename, storageUrl: pdfUrl, storagePath, createdAt: serverTimestamp(),
        });
      } catch (e) { console.warn("PDF Storage保存スキップ:", e.message); }

      // メール送信（PDFはURLリンクで送付、添付なし）
      const pdfLink = pdfUrl ? `<p style="margin:20px 0"><a href="${pdfUrl}" style="display:inline-block;padding:12px 24px;background:#1C2B4A;color:#fff;text-decoration:none;border-radius:6px;font-weight:bold">📄 請求書PDFをダウンロード</a></p>` : "";
      const res = await fetch("/api/send-invoice", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          to: email,
          subject: `【請求書】${invoice.docNo} ${co.name || ""}`,
          html: `<div style="font-family:sans-serif;color:#333;">
            <p>${cl.name} 御中</p>
            <p>いつもお世話になっております。<br>${co.name || ""}です。</p>
            <p>請求書（${invoice.docNo}）をお送りいたします。</p>
            ${pdfLink}
            <p>ご確認のほど、よろしくお願いいたします。</p>
            <hr style="border:none;border-top:1px solid #ddd;margin:20px 0">
            <p style="font-size:12px;color:#888">${co.name || ""}<br>${co.address || ""}<br>TEL ${co.tel || ""}</p>
          </div>`,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "送信失敗");

      // 送信履歴記録
      await addDoc(collection(db, "sendHistory"), {
        docNo: invoice.docNo, invoiceId: invoice.id,
        clientId: invoice.clientId, clientName: cl.name || "",
        email, method: "auto", memo,
        amount: invoice.total || 0,
        sentAt: serverTimestamp(), sentBy: "auto",
      });
      await updateDoc(doc(db, "invoices", invoice.id), { sentStatus: "sent", lastSentAt: serverTimestamp() });
      setResult("success");
    } catch (e) {
      setResult("error: " + e.message);
    }
    setSending(false);
  };

  const saveManual = async () => {
    setSending(true);
    try {
      await addDoc(collection(db, "sendHistory"), {
        docNo: invoice.docNo, invoiceId: invoice.id,
        clientId: invoice.clientId, clientName: cl.name || "",
        email: email || "", method, memo,
        amount: invoice.total || 0,
        sentAt: serverTimestamp(), sentBy: "manual",
      });
      await updateDoc(doc(db, "invoices", invoice.id), { sentStatus: "sent", lastSentAt: serverTimestamp() });
      onClose();
    } catch (e) { alert("エラー: " + e.message); }
    setSending(false);
  };

  return (
    <div style={s.modal} onClick={onClose}>
      <div style={{ ...s.modalBox, maxWidth: 480 }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
          <h3 style={{ margin: 0, color: C.navy }}>請求書を送信</h3>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <div style={{ background: C.pale, padding: 14, borderRadius: 8, marginBottom: 16, fontSize: 13 }}>
          <strong>{cl.name}</strong> 宛　{invoice.docNo}　¥{fmt(invoice.total)}
        </div>

        {result === "success" ? (
          <div style={{ background: "#d4edda", padding: 16, borderRadius: 8, marginBottom: 16, color: C.green, fontWeight: 700 }}>
            メール送信が完了しました！
            <button style={{ ...s.btn("light"), marginLeft: 12 }} onClick={onClose}>閉じる</button>
          </div>
        ) : (
          <>
            <div style={{ ...s.col, gap: 12, marginBottom: 20 }}>
              <div style={s.col}>
                <span style={s.label}>送信先メールアドレス</span>
                <input style={s.input} type="email" value={email} onChange={e => setEmail(e.target.value)} placeholder="example@company.co.jp" />
              </div>
              <div style={s.col}>
                <span style={s.label}>送信方法</span>
                <select style={s.select} value={method} onChange={e => setMethod(e.target.value)}>
                  <option value="mail">メール（自動送信）</option>
                  <option value="post">郵送（手動記録）</option>
                  <option value="fax">FAX（手動記録）</option>
                  <option value="hand">手渡し（手動記録）</option>
                  <option value="other">その他（手動記録）</option>
                </select>
              </div>
              <div style={s.col}>
                <span style={s.label}>メモ（任意）</span>
                <input style={s.input} value={memo} onChange={e => setMemo(e.target.value)} placeholder="担当者名、備考など" />
              </div>
            </div>
            {result && result.startsWith("error") && (
              <div style={{ background: "#f8d7da", padding: 12, borderRadius: 8, marginBottom: 12, color: C.red, fontSize: 13 }}>{result}</div>
            )}
            <div style={{ ...s.row, justifyContent: "flex-end", gap: 8 }}>
              <button style={s.btn("light")} onClick={onClose}>キャンセル</button>
              {method === "mail" ? (
                <button style={s.btn("primary")} onClick={sendEmail} disabled={sending}>
                  {sending ? "送信中..." : "📧 メール送信"}
                </button>
              ) : (
                <button style={s.btn("gold")} onClick={saveManual} disabled={sending}>
                  {sending ? "記録中..." : "送信済みにする"}
                </button>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ── Resend Modal ──────────────────────────────────────────────────────────────
function ResendModal({ invoice, clients, company, divisions, balances, onClose }) {
  const cl = clients.find(c => c.id === invoice?.clientId) || {};
  const [message, setMessage] = useState("先日お送りいたしました請求書につきまして、改めてお送りいたします。\nご査収のほど、よろしくお願いいたします。");
  const [sending, setSending] = useState(false);
  const [result, setResult] = useState(null);

  const docData = invoice || {};
  const div = divisions?.find(d => d.id === docData.divisionId);
  const co = div ? { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) } : company;

  const needApproval = company?.reRequestApproval !== false;
  const resendEmail = async () => {
    if (!cl.email) return alert("取引先のメールアドレスが設定されていません");
    setSending(true);
    setResult(null);
    try {
      // 既存のPDF URLを取得
      let pdfUrl = "";
      try {
        const pdfSnap = await getDocs(query(collection(db, "pdfHistory"), orderBy("createdAt", "desc")));
        const pdfDoc = pdfSnap.docs.find(d => d.data().docNo === invoice.docNo);
        if (pdfDoc) pdfUrl = pdfDoc.data().storageUrl || "";
      } catch (e) { console.warn("PDF URL取得スキップ:", e.message); }
      // PDFがなければ生成して保存
      if (!pdfUrl) {
        try {
          const invoiceHTML = buildInvoiceHTML(invoice, [cl], co);
          const safeName = (cl.name||"").replace(/[\\/:*?"<>|]/g,"_");
          const { blob, filename } = await generatePDF(invoiceHTML, `請求書_${safeName}_${invoice.docNo}.pdf`);
          const storagePath = `pdfs/invoices/${invoice.docNo}.pdf`;
          pdfUrl = await savePDFToStorage(blob, storagePath);
        } catch (e) { console.warn("PDF生成スキップ:", e.message); }
      }
      const pdfLink = pdfUrl ? `<p style="margin:20px 0"><a href="${pdfUrl}" style="display:inline-block;padding:12px 24px;background:#1C2B4A;color:#fff;text-decoration:none;border-radius:6px;font-weight:bold">📄 請求書PDFをダウンロード</a></p>` : "";

      if (needApproval) {
        await addDoc(collection(db, "pendingBillings"), {
          type: "re-request-email",
          clientId: invoice.clientId, clientName: cl.name || "", email: cl.email,
          invoiceDocNo: invoice.docNo, invoiceIds: [invoice.id],
          total: invoice.total || 0, pdfUrl,
          message: `${cl.name} 御中\n\nいつもお世話になっております。\n${co.name || ""}です。\n\n${message}\n\n請求番号：${invoice.docNo}\n金額：¥${Number(invoice.total||0).toLocaleString()}\n支払期限：${invoice.dueDate || "—"}`,
          status: "pending", createdAt: serverTimestamp(),
        });
        setResult("approval");
      } else {
        const res = await fetch("/api/send-invoice", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            to: cl.email,
            subject: `【再送】請求書 ${invoice.docNo} ${co.name || ""}`,
            html: `<div style="font-family:sans-serif;color:#333;">
              <p>${cl.name} 御中</p>
              <p>いつもお世話になっております。<br>${co.name || ""}です。</p>
              <p style="white-space:pre-line">${message.replace(/</g,"&lt;").replace(/>/g,"&gt;")}</p>
              ${pdfLink}
              <div style="background:#f4f1ec;padding:14px 18px;border-radius:8px;margin:16px 0">
                <strong>請求番号：</strong>${invoice.docNo}<br>
                <strong>金額：</strong>&yen;${Number(invoice.total||0).toLocaleString()}<br>
                <strong>支払期限：</strong>${invoice.dueDate || "—"}
              </div>
              <hr style="border:none;border-top:1px solid #ddd;margin:20px 0">
              <p style="font-size:12px;color:#888">${co.name || ""}<br>${co.address || ""}<br>TEL ${co.tel || ""}</p>
            </div>`,
          }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "送信失敗");
        await addDoc(collection(db, "sendHistory"), {
          docNo: invoice.docNo, invoiceId: invoice.id,
          clientId: invoice.clientId, clientName: cl.name || "",
          email: cl.email, method: "auto", memo: "再送：" + message.slice(0, 50),
          amount: invoice.total || 0,
          sentAt: serverTimestamp(), sentBy: "resend",
        });
        setResult("success");
      }
    } catch (e) {
      setResult("error: " + e.message);
    }
    setSending(false);
  };

  const handlePrint = () => {
    const delivery = null;
    printInvoice(invoice, clients, co);
    addDoc(collection(db, "sendHistory"), {
      docNo: invoice.docNo, invoiceId: invoice.id,
      clientId: invoice.clientId, clientName: cl.name || "",
      email: "", method: "post", memo: "再送（印刷）：" + message.slice(0, 50),
      amount: invoice.total || 0,
      sentAt: serverTimestamp(), sentBy: "resend",
    });
  };

  return (
    <div style={s.modal} onClick={onClose}>
      <div style={{ ...s.modalBox, maxWidth: 500 }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
          <h3 style={{ margin: 0, color: C.navy }}>請求書を再送</h3>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <div style={{ background: C.pale, padding: 14, borderRadius: 8, marginBottom: 16, fontSize: 13 }}>
          <strong>{cl.name}</strong> 宛　{invoice.docNo}　¥{fmt(invoice.total)}
          {cl.email && <div style={{ fontSize: 12, color: C.gray, marginTop: 4 }}>送信先：{cl.email}</div>}
        </div>

        {result === "success" ? (
          <div style={{ background: "#d4edda", padding: 16, borderRadius: 8, marginBottom: 16, color: C.green, fontWeight: 700 }}>
            再送が完了しました
            <button style={{ ...s.btn("light"), marginLeft: 12 }} onClick={onClose}>閉じる</button>
          </div>
        ) : result === "approval" ? (
          <div style={{ background: "#d4edda", padding: 16, borderRadius: 8, marginBottom: 16, color: C.green, fontWeight: 700 }}>
            承認待ちに追加しました。承認待ちページで確認・送信してください。
            <button style={{ ...s.btn("light"), marginLeft: 12 }} onClick={onClose}>閉じる</button>
          </div>
        ) : (
          <>
            <div style={{ ...s.col, gap: 12, marginBottom: 20 }}>
              <div style={s.col}>
                <span style={s.label}>メッセージ（メール本文に反映されます）</span>
                <textarea style={{ ...s.input, minHeight: 100, lineHeight: 1.6 }} value={message} onChange={e => setMessage(e.target.value)} />
              </div>
            </div>
            {result && result.startsWith("error") && (
              <div style={{ background: "#f8d7da", padding: 12, borderRadius: 8, marginBottom: 12, color: C.red, fontSize: 13 }}>{result}</div>
            )}
            <div style={{ ...s.row, justifyContent: "flex-end", gap: 8 }}>
              <button style={s.btn("light")} onClick={onClose}>キャンセル</button>
              <button style={{ ...s.btn("light"), border: `1px solid ${C.navy}` }} onClick={handlePrint}>🖨 印刷して郵送</button>
              <button style={s.btn("primary")} onClick={resendEmail} disabled={sending || !cl.email}>
                {sending ? "処理中..." : needApproval ? "📧 承認待ちに追加" : "📧 メール再送"}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ── Invoices ──────────────────────────────────────────────────────────────────
function InvoicesList({ clients, invoices, deliveries, company, balances, divisions, isAdmin }) {
  const [search, setSearch] = useState("");
  const [printTarget, setPrintTarget] = useState(null);
  const [balTarget, setBalTarget] = useState(null);
  const [sendTarget, setSendTarget] = useState(null);
  const [resendTarget, setResendTarget] = useState(null);
  const [reRequestMenu, setReRequestMenu] = useState(null);
  const [reRequestTarget, setReRequestTarget] = useState(null);
  const [reRequestMsg, setReRequestMsg] = useState("");
  const [emailSending, setEmailSending] = useState(false);
  const [stripeTarget, setStripeTarget] = useState(null);
  const [stripeSending, setStripeSending] = useState(false);
  const filtered = invoices.filter(i => {
    const cn = clients.find(c => c.id === i.clientId)?.name || "";
    return cn.includes(search) || (i.docNo || "").includes(search);
  });
  const totalBal = Object.values(balances).reduce((a, b) => a + (b.currentBalance || 0), 0);
  const del = async (id) => {
    if (!confirm("削除しますか？")) return;
    await deleteDoc(doc(db, "invoices", id));
    // 関連する承認待ち（再請求系）を削除
    const pbSnap = await getDocs(query(collection(db, "pendingBillings"), where("status", "==", "pending")));
    for (const pbDoc of pbSnap.docs) {
      const d = pbDoc.data();
      const invIds = d.invoiceIds || (d.invoiceId ? [d.invoiceId] : []);
      if (invIds.includes(id)) await deleteDoc(pbDoc.ref);
    }
  };
  return (
    <div>
      <div style={s.pageTitle}>請求書一覧</div>
      <div style={{ ...s.card, padding: "12px 20px", display: "flex", gap: 16, alignItems: "center" }}>
        <input style={s.input} placeholder="取引先名・請求番号で検索" value={search} onChange={e => setSearch(e.target.value)} />
        <div style={{ fontSize: 15, fontWeight: 700, color: C.red, marginLeft: "auto" }}>未収残高合計：¥{fmt(totalBal)}</div>
      </div>
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>請求番号</th><th style={s.th}>日付</th><th style={s.th}>取引先</th><th style={s.th}>請求額</th><th style={s.th}>取引先残高</th><th style={s.th}>期限</th><th style={s.th}>状態</th><th style={s.th}>送信</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {filtered.map(inv => {
              const client = clients.find(c => c.id === inv.clientId);
              const bal = balances[inv.clientId] || {};
              const overdue = inv.status === "unpaid" && inv.dueDate && inv.dueDate < today();
              const delivery = deliveries.find(d => d.docNo === inv.deliveryRef);
              return (
                <tr key={inv.id}>
                  <td style={s.td}>{inv.docNo}</td><td style={s.td}>{inv.date}</td>
                  <td style={s.td}>{client?.name || "—"}</td>
                  <td style={s.td}>¥{fmt(inv.total)}</td>
                  <td style={{ ...s.td, fontWeight: 700, color: (bal.currentBalance||0) > 0 ? C.red : C.green }}>¥{fmt(bal.currentBalance||0)}</td>
                  <td style={{ ...s.td, color: overdue ? C.red : "inherit" }}>{inv.dueDate}</td>
                  <td style={s.td}><span style={{ ...s.badge(inv.status === "paid" ? "green" : overdue ? "red" : "gold"), cursor: "pointer" }} onClick={() => {
                    if (inv.status === "paid") { if (confirm(`${inv.docNo} を未収に戻しますか？`)) updateDoc(doc(db, "invoices", inv.id), { status: "unpaid", paidAt: null }); }
                    else { if (confirm(`${inv.docNo} を入金済にしますか？`)) updateDoc(doc(db, "invoices", inv.id), { status: "paid", paidAt: today() }); }
                  }}>{inv.status === "paid" ? "入金済" : overdue ? "期限超過" : "未収"}</span></td>
                  <td style={s.td}>
                    {inv.sentStatus === "sent"
                      ? <span style={s.badge("blue")}>送信済</span>
                      : inv.sentStatus === "scheduled"
                      ? <span style={s.badge("gold")}>{inv.scheduledSendDate} 予約</span>
                      : <span style={s.badge("gray")}>未送信</span>}
                  </td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => setPrintTarget({ invoice: inv, delivery })}>🖨 印刷</button>
                      {inv.sentStatus !== "sent"
                        ? <button style={{ ...s.btn("primary"), padding: "4px 8px", fontSize: 12 }} onClick={() => setSendTarget(inv)}>送信</button>
                        : <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => setResendTarget(inv)}>🔄 再送</button>
                      }
                      {inv.status === "unpaid" && (
                        <span style={{ position: "relative", display: "inline-block" }}>
                          <button style={{ ...s.btn("primary"), padding: "4px 8px", fontSize: 12, background: C.red }} onClick={() => setReRequestMenu(reRequestMenu === inv.id ? null : inv.id)}>再請求 ▼</button>
                          {reRequestMenu === inv.id && (
                            <div style={{ position: "absolute", top: "100%", right: 0, background: "white", border: `1px solid ${C.border}`, borderRadius: 8, boxShadow: "0 4px 12px rgba(0,0,0,0.15)", zIndex: 100, minWidth: 180, marginTop: 4 }}>
                              <button style={{ display: "block", width: "100%", padding: "10px 16px", border: "none", background: "none", textAlign: "left", cursor: "pointer", fontSize: 13, color: C.navy }} onMouseOver={e=>e.target.style.background="#f0f0f0"} onMouseOut={e=>e.target.style.background="none"} onClick={() => { setReRequestMenu(null); setReRequestTarget(inv); }}>
                                ✉ メールで再請求<br/><span style={{ fontSize: 11, color: C.gray }}>督促メール送信</span>
                              </button>
                              {company?.stripeSecretKey && (
                                <button style={{ display: "block", width: "100%", padding: "10px 16px", border: "none", borderTop: `1px solid ${C.border}`, background: "none", textAlign: "left", cursor: "pointer", fontSize: 13, color: "#635BFF" }} onMouseOver={e=>e.target.style.background="#f0f0f0"} onMouseOut={e=>e.target.style.background="none"} onClick={() => { setReRequestMenu(null); setStripeTarget(inv); }}>
                                  💳 Stripeで請求<br/><span style={{ fontSize: 11, color: C.gray }}>オンライン決済・手数料3.6%</span>
                                </button>
                              )}
                            </div>
                          )}
                        </span>
                      )}
                      {inv.status !== "paid" && (
                        <button style={{ ...s.btn("gold"), padding: "4px 8px", fontSize: 12 }} onClick={() => setBalTarget({ client, balance: bal })}>入金記録</button>
                      )}
                      {isAdmin && <button style={{ ...s.btn("red"), padding: "4px 8px", fontSize: 12 }} onClick={() => del(inv.id)}>削除</button>}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {printTarget && <PrintModeModal invoice={printTarget.invoice} delivery={printTarget.delivery}
        clients={clients} company={company} balances={balances} divisions={divisions} onClose={() => setPrintTarget(null)} />}
      {balTarget && <BalanceModal client={balTarget.client} balance={balTarget.balance} onClose={() => setBalTarget(null)} />}
      {sendTarget && <SendRecordModal invoice={sendTarget} clients={clients} company={company} divisions={divisions} balances={balances} onClose={() => setSendTarget(null)} />}
      {resendTarget && <ResendModal invoice={resendTarget} clients={clients} company={company} divisions={divisions} balances={balances} onClose={() => setResendTarget(null)} />}
      {reRequestTarget && (() => {
        const inv = reRequestTarget;
        const cl = clients.find(c => c.id === inv.clientId) || {};
        const defaultMsg = `${cl.name || ""} 御中\n\nいつもお世話になっております。\n下記の請求につきまして、お支払い期日を過ぎておりますのでご確認をお願いいたします。\n\n対象請求: ${inv.docNo}\n未入金額: ¥${fmt(inv.total || 0)}\n\nお忙しいところ恐れ入りますが、ご確認のほどよろしくお願いいたします。`;
        if (!reRequestMsg) setTimeout(() => setReRequestMsg(defaultMsg), 0);
        const msg = reRequestMsg || defaultMsg;
        const needApproval = company?.reRequestApproval !== false;
        const sendEmailReRequest = async () => {
          if (!cl.email) return alert("取引先のメールアドレスが設定されていません");
          setEmailSending(true);
          try {
            if (needApproval) {
              await addDoc(collection(db, "pendingBillings"), {
                type: "re-request-email",
                clientId: inv.clientId, clientName: cl.name || "", email: cl.email,
                invoiceDocNo: inv.docNo, invoiceIds: [inv.id],
                total: inv.total || 0, message: msg,
                status: "pending", createdAt: serverTimestamp(),
              });
              alert("承認待ちに追加しました。承認待ちページで確認・送信してください。");
              setReRequestTarget(null); setReRequestMsg("");
            } else {
              if (!confirm(`${cl.name}（${cl.email}）にお支払いのお願いメールを送信します。`)) { setEmailSending(false); return; }
              const htmlBody = msg.replace(/\n/g, "<br/>");
              const res = await fetch("/api/send-invoice", {
                method: "POST", headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ to: cl.email, subject: `【お支払いのお願い】${inv.docNo}`, html: htmlBody }),
              });
              const data = await res.json();
              if (data.success) {
                await addDoc(collection(db, "sendHistory"), {
                  docNo: inv.docNo, invoiceId: inv.id, clientId: inv.clientId,
                  clientName: cl.name || "", email: cl.email, method: "mail",
                  memo: `再請求メール ¥${fmt(inv.total || 0)}`,
                  amount: inv.total || 0, sentAt: serverTimestamp(), sentBy: "re-request",
                });
                alert("再請求メールを送信しました");
                setReRequestTarget(null); setReRequestMsg("");
              } else { alert("送信エラー: " + (data.error || "不明なエラー")); }
            }
          } catch (e) { alert("エラー: " + e.message); }
          setEmailSending(false);
        };
        return (
          <div style={s.modal} onClick={() => { if (!emailSending) { setReRequestTarget(null); setReRequestMsg(""); } }}>
            <div style={{ ...s.modalBox, maxWidth: 560 }} onClick={e => e.stopPropagation()}>
              <h3 style={{ margin: "0 0 16px", color: C.navy }}>メールで再請求</h3>
              <div style={{ background: "#f8f9fa", borderRadius: 8, padding: 16, marginBottom: 16 }}>
                <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>{cl.name}</div>
                <div style={{ fontSize: 13, color: C.gray, marginBottom: 4 }}>送信先: {cl.email || <span style={{ color: C.red }}>未設定</span>}</div>
                <div style={{ fontSize: 13, color: C.gray }}>対象: {inv.docNo} / ¥{fmt(inv.total || 0)}</div>
              </div>
              <div style={{ marginBottom: 16 }}>
                <span style={{ ...s.label, marginBottom: 6, display: "block" }}>メール本文</span>
                <textarea style={{ ...s.input, width: "100%", minHeight: 160, fontFamily: "inherit", lineHeight: 1.6 }} value={msg} onChange={e => setReRequestMsg(e.target.value)} />
              </div>
              <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, borderTop: `1px solid ${C.border}`, paddingTop: 12 }}>
                <button style={s.btn("light")} onClick={() => { setReRequestTarget(null); setReRequestMsg(""); }} disabled={emailSending}>キャンセル</button>
                <button style={s.btn("primary")} onClick={sendEmailReRequest} disabled={emailSending || !cl.email}>
                  {emailSending ? "処理中..." : needApproval ? "承認待ちに追加" : "再請求メールを送信"}
                </button>
              </div>
            </div>
          </div>
        );
      })()}
      {stripeTarget && (() => {
        const inv = stripeTarget;
        const cl = clients.find(c => c.id === inv.clientId) || {};
        const needApproval = company?.reRequestApproval !== false;
        const sendStripeInvoice = async () => {
          if (!cl.email) return alert("取引先のメールアドレスが設定されていません");
          setStripeSending(true);
          try {
            const items = [{ name: `${inv.docNo} 未入金分`, qty: 1, unitAmount: inv.total || 0 }];
            if (needApproval) {
              await addDoc(collection(db, "pendingBillings"), {
                type: "re-request-stripe",
                clientId: inv.clientId, clientName: cl.name || "", email: cl.email,
                invoiceDocNo: inv.docNo, invoiceIds: [inv.id],
                total: inv.total || 0, invoiceItems: items,
                status: "pending", createdAt: serverTimestamp(),
              });
              alert("承認待ちに追加しました。承認待ちページで確認・送信してください。");
              setStripeTarget(null);
            } else {
              if (!confirm(`${cl.name} に ¥${fmt(inv.total || 0)} のStripe請求書を送信します。\n\n対象: ${inv.docNo}\n送信先: ${cl.email}\n\n※ Stripe手数料（3.6%）が発生します。`)) { setStripeSending(false); return; }
              const res = await fetch("/api/stripe-invoice", {
                method: "POST", headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  stripeSecretKey: company.stripeSecretKey,
                  clientName: cl.name, email: cl.email,
                  amount: inv.total || 0, currency: "jpy",
                  description: `未入金再請求（${inv.docNo}）`,
                  invoiceItems: items, docNos: inv.docNo,
                }),
              });
              const data = await res.json();
              if (data.success) {
                await addDoc(collection(db, "sendHistory"), {
                  docNo: inv.docNo, invoiceId: inv.id, clientId: inv.clientId,
                  clientName: cl.name || "", email: cl.email, method: "stripe",
                  memo: `Stripe再請求 ¥${fmt(inv.total || 0)} / ${data.invoiceUrl}`,
                  amount: inv.total || 0, sentAt: serverTimestamp(), sentBy: "stripe",
                });
                alert(`Stripe請求書を送信しました！\n\n決済URL: ${data.invoiceUrl}`);
                setStripeTarget(null);
              } else { alert("Stripe請求エラー: " + (data.error || "不明なエラー")); }
            }
          } catch (e) { alert("エラー: " + e.message); }
          setStripeSending(false);
        };
        return (
          <div style={s.modal} onClick={() => !stripeSending && setStripeTarget(null)}>
            <div style={{ ...s.modalBox, maxWidth: 520 }} onClick={e => e.stopPropagation()}>
              <h3 style={{ margin: "0 0 16px", color: "#635BFF" }}>Stripe再請求</h3>
              <div style={{ background: "#f8f9fa", borderRadius: 8, padding: 16, marginBottom: 16 }}>
                <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>{cl.name}</div>
                <div style={{ fontSize: 13, color: C.gray, marginBottom: 4 }}>送信先: {cl.email || <span style={{ color: C.red }}>未設定</span>}</div>
                <div style={{ fontSize: 13, color: C.gray }}>対象: {inv.docNo} / ¥{fmt(inv.total || 0)}</div>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderTop: `1px solid ${C.border}`, paddingTop: 12 }}>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: "#635BFF" }}>合計: ¥{fmt(inv.total || 0)}</div>
                  <div style={{ fontSize: 11, color: C.gray }}>Stripe手数料: 約¥{fmt(Math.round((inv.total || 0) * 0.036))}（3.6%）</div>
                </div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button style={s.btn("light")} onClick={() => setStripeTarget(null)} disabled={stripeSending}>キャンセル</button>
                  <button style={{ ...s.btn("primary"), background: "#635BFF" }} onClick={sendStripeInvoice} disabled={stripeSending || !cl.email}>
                    {stripeSending ? "処理中..." : needApproval ? "承認待ちに追加" : "Stripe請求書を送信"}
                  </button>
                </div>
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}

// ── Monthly Billing ───────────────────────────────────────────────────────────
function MonthlyBilling({ clients, deliveries, invoices, company, balances, divisions }) {
  const [month, setMonth] = useState(today().slice(0, 7));
  const [printTarget, setPrintTarget] = useState(null);
  const [openClients, setOpenClients] = useState({});
  // 締日タイプの取引先（旧monthly互換含む）
  const closingClients = clients.filter(c => c.billingType === "closing" || c.billingType === "monthly");

  const issueClosing = async (client, period) => {
    const dels = deliveries.filter(d =>
      d.clientId === client.id && d.status === "unissued" &&
      d.date >= period.start && d.date <= period.end
    );
    if (!dels.length) return alert("対象の未請求納品書がありません");
    const allItems = dels.flatMap(d => d.items || []);
    const { sub, tax, total: grandTotal } = totalFromItems(allItems);
    if (company?.invoiceApproval) {
      await addDoc(collection(db, "pendingBillings"), {
        type: "invoice", clientId: client.id, clientName: client.name || "",
        divisionId: client.divisionId || "",
        deliveryIds: dels.map(d => d.id), deliveryDocNos: dels.map(d => d.docNo),
        items: allItems, subtotal: sub, tax, total: grandTotal,
        billingType: "closing", closingDay: period.closingDay,
        closingPeriod: { start: period.start, end: period.end },
        deliveryRefItems: JSON.stringify(dels.map(d => d.items || [])),
        status: "pending", createdAt: serverTimestamp(),
      });
      const batch2 = writeBatch(db);
      dels.forEach(d => batch2.update(doc(db, "deliveries", d.id), { status: "pending_approval" }));
      await batch2.commit();
      alert(`承認待ちに追加しました（${dels.length}件まとめ、${period.label}）`);
      return;
    }
    const inv = {
      docNo: genDocNo("INV", invoices), clientId: client.id, date: today(),
      dueDate: nextMonthEnd(period.end), billingType: "closing",
      closingDay: period.closingDay, closingPeriod: { start: period.start, end: period.end },
      deliveryRefs: dels.map(d => d.docNo),
      deliveryRefItems: JSON.stringify(dels.map(d => d.items || [])),
      items: allItems, subtotal: sub, tax, total: grandTotal,
      status: "unpaid", createdAt: serverTimestamp(),
    };
    const batch = writeBatch(db);
    const invRef = doc(collection(db, "invoices"));
    batch.set(invRef, inv);
    dels.forEach(d => batch.update(doc(db, "deliveries", d.id), { status: "invoiced", invoiceId: invRef.id }));
    await batch.commit();
    const bal = balances[client.id] || {};
    await setDoc(doc(db, "clientBalances", client.id), {
      clientId: client.id, prevBalance: bal.currentBalance || 0,
      currentBalance: (bal.currentBalance || 0) + sub + tax,
      paidAmount: bal.paidAmount || 0, updatedAt: serverTimestamp(),
    });
    alert(`請求書を発行しました（${dels.length}件まとめ、${period.label}）`);
  };

  return (
    <div>
      <div style={s.pageTitle}>締日・月締め管理</div>
      <div style={{ ...s.card, padding: "12px 20px", display: "flex", alignItems: "center", gap: 12 }}>
        <label style={{ ...s.label, marginRight: 4 }}>対象月：</label>
        <input style={s.input} type="month" value={month} onChange={e => setMonth(e.target.value)} />
        <span style={{ fontSize: 12, color: C.gray }}>※ 締日ごとに対象期間の納品書をまとめて請求できます</span>
      </div>
      {closingClients.map(client => {
        const cDays = client.closingDays && client.closingDays.length ? client.closingDays : [0];
        const periods = getAllClosingPeriods(month, cDays);
        const bal = balances[client.id] || {};
        const allDels = deliveries.filter(d => d.clientId === client.id && periods.some(p => d.date >= p.start && d.date <= p.end));
        const unissuedCount = allDels.filter(d => d.status === "unissued").length;
        const isOpen = openClients[client.id];
        return (
          <div key={client.id} style={s.card}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }} onClick={() => setOpenClients(prev => ({ ...prev, [client.id]: !prev[client.id] }))}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 12, color: C.gray }}>{isOpen ? "▼" : "▶"}</span>
                <span style={{ fontWeight: 700, fontSize: 16, color: C.navy }}>{client.name}</span>
                <span style={s.badge("gold")}>{closingDaysLabel(cDays)}</span>
                {bal.currentBalance > 0 && <span style={{ fontSize: 13, color: C.red }}>残高：¥{fmt(bal.currentBalance)}</span>}
              </div>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <span style={{ fontSize: 13, color: C.gray }}>納品書 {allDels.length}件</span>
                {unissuedCount > 0 && <span style={s.badge("gold")}>未請求 {unissuedCount}件</span>}
              </div>
            </div>
            {isOpen && periods.map((period, pi) => {
              const dels = deliveries.filter(d =>
                d.clientId === client.id && d.date >= period.start && d.date <= period.end
              );
              const unissued = dels.filter(d => d.status === "unissued");
              const unissuedItems = unissued.flatMap(d => d.items || []);
              const { total: unissuedTotal } = totalFromItems(unissuedItems);
              const existInv = invoices.find(i =>
                i.clientId === client.id && (i.billingType === "closing" || i.billingType === "monthly") &&
                i.closingPeriod?.start === period.start && i.closingPeriod?.end === period.end
              );
              return (
                <div key={pi} style={{ marginBottom: pi < periods.length - 1 ? 16 : 0, paddingBottom: pi < periods.length - 1 ? 16 : 0, borderBottom: pi < periods.length - 1 ? `1px solid ${C.light}` : "none" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                    <span style={{ fontSize: 13, fontWeight: 600, color: C.navy }}>{period.label}</span>
                    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                      <span style={{ fontSize: 13, color: C.gray }}>未請求：{unissued.length}件　¥{fmt(unissuedTotal)}</span>
                      {existInv
                        ? <button style={{ ...s.btn("light"), padding: "6px 12px", fontSize: 12 }} onClick={() => setPrintTarget({ invoice: existInv })}>🖨 発行済み請求書</button>
                        : <button style={{ ...s.btn("gold"), padding: "6px 12px", fontSize: 12 }} onClick={() => issueClosing(client, period)} disabled={!unissued.length}>請求書発行</button>
                      }
                    </div>
                  </div>
                  <table style={s.table}>
                    <thead><tr><th style={s.th}>伝票番号</th><th style={s.th}>日付</th><th style={s.th}>金額</th><th style={s.th}>状態</th></tr></thead>
                    <tbody>
                      {dels.map(d => (
                        <tr key={d.id}>
                          <td style={s.td}>{d.docNo}</td><td style={s.td}>{d.date}</td>
                          <td style={s.td}>¥{fmt(d.total)}</td>
                          <td style={s.td}><span style={s.badge(d.status === "invoiced" ? "green" : d.status === "pending_approval" ? "blue" : "gold")}>{d.status === "invoiced" ? "請求済" : d.status === "pending_approval" ? "承認待ち" : "未請求"}</span></td>
                        </tr>
                      ))}
                      {!dels.length && <tr><td colSpan={4} style={{ ...s.td, textAlign: "center", color: C.gray }}>この期間の納品書はありません</td></tr>}
                    </tbody>
                  </table>
                </div>
              );
            })}
          </div>
        );
      })}
      {!closingClients.length && <div style={{ ...s.card, color: C.gray, textAlign: "center" }}>締日設定のある取引先がありません（取引先管理で「締日請求」に変更してください）</div>}
      {printTarget && <PrintModeModal invoice={printTarget.invoice} delivery={null}
        clients={clients} company={company} balances={balances} divisions={divisions} onClose={() => setPrintTarget(null)} />}
    </div>
  );
}

// ── Sales Page ────────────────────────────────────────────────────────────────
function SalesPage({ clients, invoices, divisions, externalSales }) {
  const [viewMonth, setViewMonth] = useState(today().slice(0, 7));
  const [viewMode, setViewMode] = useState("monthly"); // monthly | daily | yearly | division | client
  const [importing, setImporting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [syncMsg, setSyncMsg] = useState("");

  // 楽天同期
  const syncRakuten = async (mode) => {
    if (syncing) return;
    setSyncing(true);
    setSyncMsg("");
    try {
      if (mode === "initial") {
        const now = new Date();
        const months = [];
        for (let i = 11; i >= 0; i--) {
          const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
          const y = d.getFullYear(), m = d.getMonth() + 1;
          const pad = (n) => String(n).padStart(2, "0");
          const start = `${y}-${pad(m)}-01`;
          const lastDay = new Date(y, m, 0).getDate();
          const end = `${y}-${pad(m)}-${pad(lastDay)}`;
          months.push({ start, end, label: `${y}年${m}月` });
        }
        let totalOrders = 0, totalDays = 0;
        for (let i = 0; i < months.length; i++) {
          const mo = months[i];
          setSyncMsg(`${mo.label} を同期中... (${i + 1}/${months.length})`);
          const res = await fetch("/api/rakuten-sync", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ mode: "range", start: mo.start, end: mo.end }),
          });
          const data = await res.json();
          if (data.ok) {
            totalOrders += data.totalOrders || 0;
            totalDays += data.totalDays || 0;
          }
        }
        setSyncMsg("");
        alert(`楽天初期同期完了: ${totalOrders}件の注文、${totalDays}日分`);
      } else {
        const res = await fetch("/api/rakuten-sync", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode: "today" }),
        });
        const data = await res.json();
        if (data.ok) {
          alert(`楽天同期完了: ${data.totalOrders}件の注文、${data.totalDays}日分`);
        } else {
          alert(data.message || data.error || "同期に失敗しました");
        }
      }
    } catch (e) {
      alert("同期エラー: " + e.message);
    }
    setSyncing(false);
    setSyncMsg("");
  };

  // カラーミー同期
  const syncColorMe = async (mode) => {
    if (syncing) return;
    setSyncing(true);
    setSyncMsg("");
    try {
      if (mode === "initial") {
        const now = new Date();
        const months = [];
        for (let i = 11; i >= 0; i--) {
          const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
          const y = d.getFullYear(), m = d.getMonth() + 1;
          const pad = (n) => String(n).padStart(2, "0");
          const start = `${y}-${pad(m)}-01`;
          const lastDay = new Date(y, m, 0).getDate();
          const end = `${y}-${pad(m)}-${pad(lastDay)}`;
          months.push({ start, end, label: `${y}年${m}月` });
        }
        let totalOrders = 0, totalDays = 0, errorCount = 0;
        for (let i = 0; i < months.length; i++) {
          const mo = months[i];
          setSyncMsg(`カラーミー ${mo.label} を同期中... (${i + 1}/${months.length})`);
          try {
            const res = await fetch("/api/colorme-sync", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ mode: "range", start: mo.start, end: mo.end }),
            });
            const data = await res.json();
            if (data.ok) { totalOrders += data.totalOrders || 0; totalDays += data.totalDays || 0; errorCount = 0; }
            else { errorCount++; console.error(`カラーミー同期エラー (${mo.label}):`, data.error || data.message); if (errorCount >= 3) { alert(`カラーミー同期エラーが連続しました。中断します。\nエラー: ${data.error || data.message || "不明"}`); break; } }
          } catch (fetchErr) { errorCount++; console.error(`カラーミー同期fetch失敗 (${mo.label}):`, fetchErr); if (errorCount >= 3) { alert(`カラーミー同期エラーが連続しました。中断します。\nエラー: ${fetchErr.message}`); break; } }
          if (i < months.length - 1) await new Promise(r => setTimeout(r, 2000));
        }
        setSyncMsg("");
        alert(`カラーミー初期同期完了: ${totalOrders}件、${totalDays}日分`);
      } else {
        const res = await fetch("/api/colorme-sync", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode: "today" }),
        });
        const data = await res.json();
        if (data.ok) {
          alert(`カラーミー同期完了: ${data.totalOrders}件、${data.totalDays}日分`);
        } else {
          alert(data.message || data.error || "同期に失敗しました");
        }
      }
    } catch (e) {
      alert("同期エラー: " + e.message);
    }
    setSyncing(false);
    setSyncMsg("");
  };

  // Amazon同期
  const syncAmazon = async (mode) => {
    if (syncing) return;
    setSyncing(true);
    setSyncMsg("");
    try {
      if (mode === "initial") {
        const now = new Date();
        const months = [];
        for (let i = 11; i >= 0; i--) {
          const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
          const y = d.getFullYear(), m = d.getMonth() + 1;
          const pad = (n) => String(n).padStart(2, "0");
          const start = `${y}-${pad(m)}-01`;
          const lastDay = new Date(y, m, 0).getDate();
          const end = `${y}-${pad(m)}-${pad(lastDay)}`;
          months.push({ start, end, label: `${y}年${m}月` });
        }
        let totalOrders = 0, totalDays = 0, errorCount = 0;
        for (let i = 0; i < months.length; i++) {
          const mo = months[i];
          setSyncMsg(`Amazon ${mo.label} を同期中... (${i + 1}/${months.length})`);
          try {
            const res = await fetch("/api/amazon-sync", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ mode: "range", start: mo.start, end: mo.end }),
            });
            const data = await res.json();
            if (data.ok) {
              totalOrders += data.totalOrders || 0;
              totalDays += data.totalDays || 0;
              errorCount = 0;
            } else {
              errorCount++;
              console.error(`Amazon同期エラー (${mo.label}):`, data.error || data.message);
              if (errorCount >= 3) {
                alert(`Amazon同期エラーが連続しました。中断します。\nエラー: ${data.error || data.message || "不明"}`);
                break;
              }
            }
          } catch (fetchErr) {
            errorCount++;
            console.error(`Amazon同期fetch失敗 (${mo.label}):`, fetchErr);
            if (errorCount >= 3) {
              alert(`Amazon同期エラーが連続しました。中断します。\nエラー: ${fetchErr.message}`);
              break;
            }
          }
          // Amazon APIのrate limit対策: 5秒待機
          if (i < months.length - 1) await new Promise(r => setTimeout(r, 5000));
        }
        setSyncMsg("");
        alert(`Amazon初期同期完了: ${totalOrders}件、${totalDays}日分`);
      } else {
        const res = await fetch("/api/amazon-sync", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode: "today" }),
        });
        const data = await res.json();
        if (data.ok) {
          alert(`Amazon同期完了: ${data.totalOrders}件、${data.totalDays}日分`);
        } else {
          alert(data.message || data.error || "同期に失敗しました");
        }
      }
    } catch (e) {
      alert("同期エラー: " + e.message);
    }
    setSyncing(false);
    setSyncMsg("");
  };

  // 外部ソース一覧を自動検出
  const sources = [...new Set(externalSales.map(e => e.source))].sort();

  // ── 日別集計ヘルパー ──
  function buildDailyMap(monthStr) {
    const map = {}; // { "2026-03-12": { invoice: {amount,count}, rakuten: {amount,count}, ... } }
    // 請求書売上
    invoices.filter(i => (i.date || "").startsWith(monthStr)).forEach(inv => {
      if (!map[inv.date]) map[inv.date] = {};
      if (!map[inv.date].invoice) map[inv.date].invoice = { amount: 0, count: 0 };
      map[inv.date].invoice.amount += (inv.total || 0);
      map[inv.date].invoice.count++;
    });
    // 外部売上
    externalSales.filter(e => (e.date || "").startsWith(monthStr)).forEach(e => {
      if (!map[e.date]) map[e.date] = {};
      if (!map[e.date][e.source]) map[e.date][e.source] = { amount: 0, count: 0 };
      map[e.date][e.source].amount += (e.totalAmount || 0);
      map[e.date][e.source].count += (e.orderCount || 0);
    });
    return map;
  }

  // ── 月別集計 ──
  const months = [];
  for (let i = 11; i >= 0; i--) {
    const d = new Date(); d.setMonth(d.getMonth() - i);
    months.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`);
  }

  const monthlyData = months.map(m => {
    const mInvs = invoices.filter(i => (i.date || "").startsWith(m));
    const invTotal = mInvs.reduce((a, i) => a + (i.total || 0), 0);
    const invCount = mInvs.length;
    const ext = {};
    sources.forEach(src => {
      const srcData = externalSales.filter(e => e.source === src && (e.date || "").startsWith(m));
      ext[src] = { amount: srcData.reduce((a, e) => a + (e.totalAmount || 0), 0), count: srcData.reduce((a, e) => a + (e.orderCount || 0), 0) };
    });
    const extTotal = Object.values(ext).reduce((a, v) => a + v.amount, 0);
    const extCount = Object.values(ext).reduce((a, v) => a + v.count, 0);
    return { month: m, invTotal, invCount, ext, grandTotal: invTotal + extTotal, grandCount: invCount + extCount };
  });

  // ── 年別集計 ──
  const years = [...new Set([
    ...invoices.map(i => (i.date || "").slice(0, 4)),
    ...externalSales.map(e => (e.date || "").slice(0, 4)),
  ])].filter(Boolean).sort();
  const yearlyData = years.map(y => {
    const yInvs = invoices.filter(i => (i.date || "").startsWith(y));
    const invTotal = yInvs.reduce((a, i) => a + (i.total || 0), 0);
    const invCount = yInvs.length;
    const ext = {};
    sources.forEach(src => {
      const srcData = externalSales.filter(e => e.source === src && (e.date || "").startsWith(y));
      ext[src] = { amount: srcData.reduce((a, e) => a + (e.totalAmount || 0), 0), count: srcData.reduce((a, e) => a + (e.orderCount || 0), 0) };
    });
    const extTotal = Object.values(ext).reduce((a, v) => a + v.amount, 0);
    const extCount = Object.values(ext).reduce((a, v) => a + v.count, 0);
    return { year: y, invTotal, invCount, ext, grandTotal: invTotal + extTotal, grandCount: invCount + extCount };
  });

  // ── 日別データ ──
  const dailyMap = buildDailyMap(viewMonth);
  const dailyRows = Object.keys(dailyMap).sort().reverse().map(date => {
    const d = dailyMap[date];
    const inv = d.invoice || { amount: 0, count: 0 };
    const ext = {};
    sources.forEach(src => { ext[src] = d[src] || { amount: 0, count: 0 }; });
    const extTotal = Object.values(ext).reduce((a, v) => a + v.amount, 0);
    const extCount = Object.values(ext).reduce((a, v) => a + v.count, 0);
    return { date, inv, ext, grandTotal: inv.amount + extTotal, grandCount: inv.count + extCount };
  });

  const maxSales = Math.max(...monthlyData.map(d => d.grandTotal), 1);
  const currentData = monthlyData.find(d => d.month === viewMonth);
  const curGrand = currentData ? currentData.grandTotal : 0;
  const curCount = currentData ? currentData.grandCount : 0;
  const prevIdx = months.indexOf(viewMonth) - 1;
  const prevData = prevIdx >= 0 ? monthlyData[prevIdx] : null;
  const growth = prevData && prevData.grandTotal > 0 ? ((curGrand - prevData.grandTotal) / prevData.grandTotal * 100).toFixed(1) : null;

  // ── 事業部別集計（請求書＋外部売上） ──
  const divSales = {};
  const mInvs = invoices.filter(i => (i.date || "").startsWith(viewMonth));
  mInvs.forEach(inv => {
    const divId = inv.divisionId || "_none";
    if (!divSales[divId]) divSales[divId] = { total: 0, subtotal: 0, tax: 0, count: 0 };
    divSales[divId].total += (inv.total || 0);
    divSales[divId].subtotal += (inv.subtotal || 0);
    divSales[divId].tax += (inv.tax || 0);
    divSales[divId].count++;
  });
  // 外部売上をソースごとに事業部として追加
  const mExtSales = externalSales.filter(e => (e.date || "").startsWith(viewMonth));
  const extBySource = {};
  mExtSales.forEach(e => {
    if (!extBySource[e.source]) extBySource[e.source] = { total: 0, subtotal: 0, tax: 0, count: 0 };
    extBySource[e.source].total += (e.totalAmount || 0);
    extBySource[e.source].subtotal += (e.totalAmount || 0);
    extBySource[e.source].count += (e.orderCount || 0);
  });
  Object.entries(extBySource).forEach(([src, data]) => {
    divSales[`_ext_${src}`] = data;
  });

  // ── 取引先別集計（請求書＋外部売上） ──
  const clientSales = {};
  mInvs.forEach(inv => {
    const cid = inv.clientId;
    if (!clientSales[cid]) clientSales[cid] = { total: 0, subtotal: 0, count: 0 };
    clientSales[cid].total += (inv.total || 0);
    clientSales[cid].subtotal += (inv.subtotal || 0);
    clientSales[cid].count++;
  });
  const clientRank = Object.entries(clientSales).sort((a, b) => b[1].total - a[1].total);

  // ── CSV同期 ──
  const sourceLabel = (src) => src === "rakuten" ? "楽天" : src === "amazon" ? "Amazon" : src === "invoice" ? "請求書" : src;
  const handleCSVImport = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setImporting(true);
    try {
      const text = await file.text();
      const lines = text.split(/\r?\n/).filter(l => l.trim());
      const header = parseCSVLine(lines[0]);
      const dateIdx = header.findIndex(h => h.includes("日付") || h.toLowerCase() === "date");
      const amountIdx = header.findIndex(h => h.includes("売上") || h.includes("金額") || h.toLowerCase() === "amount");
      const countIdx = header.findIndex(h => h.includes("件数") || h.toLowerCase() === "count");
      const sourceIdx = header.findIndex(h => h.includes("ソース") || h.includes("source") || h.includes("チャネル"));
      if (dateIdx === -1 || amountIdx === -1) { alert("「日付」「売上/金額」列が必要です"); setImporting(false); return; }
      const rows = lines.slice(1);
      const batch = writeBatch(db);
      let count = 0;
      for (const line of rows) {
        const cols = parseCSVLine(line);
        const date = (cols[dateIdx] || "").trim();
        const amount = Number((cols[amountIdx] || "").replace(/[¥￥,]/g, "")) || 0;
        const cnt = countIdx >= 0 ? (Number(cols[countIdx]) || 0) : 1;
        const source = sourceIdx >= 0 ? (cols[sourceIdx] || "").trim().toLowerCase() || "csv" : "csv";
        if (!date || !date.match(/^\d{4}-\d{2}-\d{2}$/)) continue;
        const docId = `${date}_${source}`;
        batch.set(doc(db, "externalSales", docId), { source, date, totalAmount: amount, orderCount: cnt, syncedAt: serverTimestamp() });
        count++;
      }
      await batch.commit();
      alert(`${count}件の売上データをインポートしました`);
    } catch (err) {
      alert("CSV読み込みエラー: " + err.message);
    }
    setImporting(false);
    e.target.value = "";
  };

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div><div style={s.pageTitle}>売上管理</div><div style={{ fontSize: 11, color: C.gray, marginTop: 2 }}>※ 楽天=店舗カルテ（店舗軸）売上実績と同額（商品金額−クーポン値引き、送料除外） / Amazon=SP-API OrderMetrics totalSales</div></div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button style={{ ...s.btn("primary"), opacity: syncing ? 0.6 : 1 }} onClick={() => syncRakuten("today")} disabled={syncing}>{syncing ? "同期中..." : "楽天 今日同期"}</button>
          <button style={{ ...s.btn("primary"), background: "#FF9900", opacity: syncing ? 0.6 : 1 }} onClick={() => syncAmazon("today")} disabled={syncing}>{syncing ? "同期中..." : "Amazon 今日同期"}</button>
          <button style={{ ...s.btn("primary"), background: "#E95295", opacity: syncing ? 0.6 : 1 }} onClick={() => syncColorMe("today")} disabled={syncing}>{syncing ? "同期中..." : "カラーミー 今日同期"}</button>
          <label style={{ ...s.btn("gold"), display: "inline-block", cursor: importing ? "wait" : "pointer", opacity: importing ? 0.6 : 1 }}>
            {importing ? "インポート中..." : "CSV 売上取込"}
            <input type="file" accept=".csv" style={{ display: "none" }} onChange={handleCSVImport} disabled={importing} />
          </label>
        </div>
      </div>
      <div style={{ ...s.card, padding: "12px 20px", display: "flex", alignItems: "center", gap: 16, flexWrap: "wrap" }}>
        <input style={s.input} type="month" value={viewMonth} onChange={e => setViewMonth(e.target.value)} />
        <div style={{ display: "flex", gap: 4 }}>
          {[["daily","日別"],["monthly","月別"],["yearly","年別"],["division","事業部別"],["client","取引先別"]].map(([id,label]) => (
            <button key={id} style={{ ...s.btn(viewMode===id?"primary":"light"), padding: "6px 14px", fontSize: 13 }} onClick={() => setViewMode(id)}>{label}</button>
          ))}
        </div>
      </div>

      {/* サマリーカード */}
      <div style={{ display: "flex", gap: 16, marginBottom: 20, flexWrap: "wrap" }}>
        {[
          { label: `${viewMonth.split("-")[1]}月 合計売上`, value: "¥" + fmt(curGrand), color: C.gold },
          { label: "請求書", value: "¥" + fmt(currentData?.invTotal || 0), color: C.navy },
          ...sources.map(src => ({ label: sourceLabel(src), value: "¥" + fmt(currentData?.ext?.[src]?.amount || 0), color: C.green })),
          { label: "総件数", value: curCount + " 件", color: C.navy },
          { label: "前月比", value: growth !== null ? (growth > 0 ? "+" : "") + growth + "%" : "—", color: growth > 0 ? C.green : growth < 0 ? C.red : C.gray },
        ].map(st => (
          <div key={st.label} style={{ ...s.card, flex: "1 1 120px", textAlign: "center", margin: 0 }}>
            <div style={{ fontSize: 12, color: C.gray, marginBottom: 6 }}>{st.label}</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: st.color }}>{st.value}</div>
          </div>
        ))}
      </div>

      {viewMode === "daily" && (
        <div style={s.card}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>{viewMonth} 日別売上</h3>
          <table style={s.table}>
            <thead><tr><th style={s.th}>日付</th><th style={s.th}>請求書</th>{sources.map(src => <th key={src} style={s.th}>{sourceLabel(src)}</th>)}<th style={s.th}>合計</th><th style={s.th}>件数</th></tr></thead>
            <tbody>
              {dailyRows.map(d => (
                <tr key={d.date}>
                  <td style={s.td}>{d.date}</td>
                  <td style={s.td}>¥{fmt(d.inv.amount)}</td>
                  {sources.map(src => <td key={src} style={s.td}>¥{fmt(d.ext[src]?.amount || 0)}</td>)}
                  <td style={{ ...s.td, fontWeight: 700 }}>¥{fmt(d.grandTotal)}</td>
                  <td style={s.td}>{d.grandCount}件</td>
                </tr>
              ))}
              {!dailyRows.length && <tr><td colSpan={3 + sources.length} style={{ ...s.td, textAlign: "center", color: C.gray }}>この月のデータがありません</td></tr>}
            </tbody>
          </table>
        </div>
      )}

      {viewMode === "monthly" && (
        <div style={s.card}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>月別売上推移（直近12ヶ月）</h3>
          <div style={{ display: "flex", alignItems: "flex-end", gap: 6, height: 200, marginBottom: 16, padding: "0 8px" }}>
            {monthlyData.map(d => {
              const h = Math.max(4, (d.grandTotal / maxSales) * 180);
              const isCurrent = d.month === viewMonth;
              return (
                <div key={d.month} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", cursor: "pointer" }} onClick={() => setViewMonth(d.month)}>
                  <div style={{ fontSize: 10, color: C.navy, marginBottom: 4, fontWeight: d.grandTotal > 0 ? 600 : 400 }}>{d.grandTotal > 0 ? "¥" + fmt(d.grandTotal) : ""}</div>
                  <div style={{ width: "100%", height: h, background: isCurrent ? C.gold : C.navy, borderRadius: "4px 4px 0 0", opacity: isCurrent ? 1 : 0.7, transition: "all 0.2s" }} />
                  <div style={{ fontSize: 10, color: isCurrent ? C.navy : C.gray, marginTop: 4, fontWeight: isCurrent ? 700 : 400 }}>{d.month.split("-")[1]}月</div>
                </div>
              );
            })}
          </div>
          <table style={s.table}>
            <thead><tr><th style={s.th}>月</th><th style={s.th}>請求書</th>{sources.map(src => <th key={src} style={s.th}>{sourceLabel(src)}</th>)}<th style={s.th}>合計</th><th style={s.th}>件数</th></tr></thead>
            <tbody>
              {[...monthlyData].reverse().map(d => (
                <tr key={d.month} style={{ background: d.month === viewMonth ? C.pale : "transparent" }}>
                  <td style={s.td}>{d.month}</td>
                  <td style={s.td}>¥{fmt(d.invTotal)}</td>
                  {sources.map(src => <td key={src} style={s.td}>¥{fmt(d.ext[src]?.amount || 0)}</td>)}
                  <td style={{ ...s.td, fontWeight: 700 }}>¥{fmt(d.grandTotal)}</td>
                  <td style={s.td}>{d.grandCount}件</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {viewMode === "yearly" && (
        <div style={s.card}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>年別売上</h3>
          <table style={s.table}>
            <thead><tr><th style={s.th}>年</th><th style={s.th}>請求書</th>{sources.map(src => <th key={src} style={s.th}>{sourceLabel(src)}</th>)}<th style={s.th}>合計</th><th style={s.th}>件数</th></tr></thead>
            <tbody>
              {[...yearlyData].reverse().map(d => (
                <tr key={d.year}>
                  <td style={s.td}>{d.year}年</td>
                  <td style={s.td}>¥{fmt(d.invTotal)}</td>
                  {sources.map(src => <td key={src} style={s.td}>¥{fmt(d.ext[src]?.amount || 0)}</td>)}
                  <td style={{ ...s.td, fontWeight: 700 }}>¥{fmt(d.grandTotal)}</td>
                  <td style={s.td}>{d.grandCount}件</td>
                </tr>
              ))}
              {!yearlyData.length && <tr><td colSpan={3 + sources.length} style={{ ...s.td, textAlign: "center", color: C.gray }}>データがありません</td></tr>}
            </tbody>
          </table>
        </div>
      )}

      {viewMode === "division" && (() => {
        const months12 = [];
        for (let i = 11; i >= 0; i--) {
          const d = new Date(viewMonth + "-01");
          d.setMonth(d.getMonth() - i);
          months12.push(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`);
        }
        const divMap = {};
        invoices.forEach(inv => {
          const m = (inv.date || "").slice(0, 7);
          if (!months12.includes(m)) return;
          const divId = inv.divisionId || "_none";
          if (!divMap[divId]) divMap[divId] = {};
          if (!divMap[divId][m]) divMap[divId][m] = 0;
          divMap[divId][m] += (inv.total || 0);
        });
        externalSales.forEach(e => {
          const m = (e.date || "").slice(0, 7);
          if (!months12.includes(m)) return;
          const key = `_ext_${e.source}`;
          if (!divMap[key]) divMap[key] = {};
          if (!divMap[key][m]) divMap[key][m] = 0;
          divMap[key][m] += (e.totalAmount || 0);
        });
        const divRows = Object.entries(divMap).sort((a, b) => {
          const ta = Object.values(a[1]).reduce((s, v) => s + v, 0);
          const tb = Object.values(b[1]).reduce((s, v) => s + v, 0);
          return tb - ta;
        });
        return (
        <div style={{ ...s.card, overflowX: "auto" }}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>事業部別売上（12ヶ月）</h3>
          <table style={s.table}>
            <thead><tr><th style={{ ...s.th, position: "sticky", left: 0, background: C.navy, zIndex: 1 }}>事業部</th>{months12.map(m => <th key={m} style={{ ...s.th, minWidth: 90, textAlign: "right" }}>{m.slice(0,4)}/{m.slice(5)}</th>)}<th style={{ ...s.th, textAlign: "right", minWidth: 100 }}>合計</th></tr></thead>
            <tbody>
              {divRows.map(([divId, mData]) => {
                const isExt = divId.startsWith("_ext_");
                const div = isExt ? null : divisions.find(d => d.id === divId);
                const name = isExt ? sourceLabel(divId.replace("_ext_", "")) : (div ? div.name : "未設定");
                const rowTotal = Object.values(mData).reduce((s, v) => s + v, 0);
                return (
                  <tr key={divId} style={isExt ? { background: "#faf8f0" } : {}}>
                    <td style={{ ...s.td, fontWeight: 700, position: "sticky", left: 0, background: isExt ? "#faf8f0" : "#fff", zIndex: 1 }}>{name}</td>
                    {months12.map(m => <td key={m} style={{ ...s.td, textAlign: "right" }}>{mData[m] ? `¥${fmt(mData[m])}` : "—"}</td>)}
                    <td style={{ ...s.td, textAlign: "right", fontWeight: 700, background: "#f8f6f0" }}>¥{fmt(rowTotal)}</td>
                  </tr>
                );
              })}
              <tr style={{ background: C.light }}>
                <td style={{ ...s.td, fontWeight: 700, position: "sticky", left: 0, background: C.light, zIndex: 1 }}>合計</td>
                {months12.map(m => {
                  const mTotal = divRows.reduce((s, [, mData]) => s + (mData[m] || 0), 0);
                  return <td key={m} style={{ ...s.td, textAlign: "right", fontWeight: 700 }}>{mTotal ? `¥${fmt(mTotal)}` : "—"}</td>;
                })}
                <td style={{ ...s.td, textAlign: "right", fontWeight: 700, background: "#f0ede0" }}>¥{fmt(divRows.reduce((s, [, mData]) => s + Object.values(mData).reduce((a, v) => a + v, 0), 0))}</td>
              </tr>
              {!divRows.length && <tr><td colSpan={14} style={{ ...s.td, textAlign: "center", color: C.gray }}>データがありません</td></tr>}
            </tbody>
          </table>
        </div>
        );
      })()}

      {viewMode === "client" && (() => {
        const months12 = [];
        for (let i = 11; i >= 0; i--) {
          const d = new Date(viewMonth + "-01");
          d.setMonth(d.getMonth() - i);
          months12.push(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`);
        }
        const cMap = {};
        invoices.forEach(inv => {
          const m = (inv.date || "").slice(0, 7);
          if (!months12.includes(m)) return;
          const cid = inv.clientId;
          if (!cMap[cid]) cMap[cid] = {};
          if (!cMap[cid][m]) cMap[cid][m] = 0;
          cMap[cid][m] += (inv.total || 0);
        });
        const cRows = Object.entries(cMap).sort((a, b) => {
          const ta = Object.values(a[1]).reduce((s, v) => s + v, 0);
          const tb = Object.values(b[1]).reduce((s, v) => s + v, 0);
          return tb - ta;
        });
        return (
        <div style={{ ...s.card, overflowX: "auto" }}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>取引先別売上（12ヶ月）</h3>
          <table style={s.table}>
            <thead><tr><th style={{ ...s.th, position: "sticky", left: 0, background: C.navy, zIndex: 1 }}>取引先</th>{months12.map(m => <th key={m} style={{ ...s.th, minWidth: 90, textAlign: "right" }}>{m.slice(0,4)}/{m.slice(5)}</th>)}<th style={{ ...s.th, textAlign: "right", minWidth: 100 }}>合計</th></tr></thead>
            <tbody>
              {cRows.map(([cid, mData], idx) => {
                const cl = clients.find(c => c.id === cid);
                const name = cl?.name || "—";
                const rowTotal = Object.values(mData).reduce((s, v) => s + v, 0);
                return (
                  <tr key={cid}>
                    <td style={{ ...s.td, fontWeight: 700, position: "sticky", left: 0, background: "#fff", zIndex: 1 }}>{name}</td>
                    {months12.map(m => <td key={m} style={{ ...s.td, textAlign: "right" }}>{mData[m] ? `¥${fmt(mData[m])}` : "—"}</td>)}
                    <td style={{ ...s.td, textAlign: "right", fontWeight: 700, background: "#f8f6f0" }}>¥{fmt(rowTotal)}</td>
                  </tr>
                );
              })}
              <tr style={{ background: C.light }}>
                <td style={{ ...s.td, fontWeight: 700, position: "sticky", left: 0, background: C.light, zIndex: 1 }}>合計</td>
                {months12.map(m => {
                  const mTotal = cRows.reduce((s, [, mData]) => s + (mData[m] || 0), 0);
                  return <td key={m} style={{ ...s.td, textAlign: "right", fontWeight: 700 }}>{mTotal ? `¥${fmt(mTotal)}` : "—"}</td>;
                })}
                <td style={{ ...s.td, textAlign: "right", fontWeight: 700, background: "#f0ede0" }}>¥{fmt(cRows.reduce((s, [, mData]) => s + Object.values(mData).reduce((a, v) => a + v, 0), 0))}</td>
              </tr>
              {!cRows.length && <tr><td colSpan={14} style={{ ...s.td, textAlign: "center", color: C.gray }}>データがありません</td></tr>}
            </tbody>
          </table>
        </div>
        );
      })()}
    </div>
  );
}

// ── Balance Page ──────────────────────────────────────────────────────────────
function BalancePage({ clients, invoices, balances, company, paymentHistory }) {
  const [balTarget, setBalTarget] = useState(null);
  const [filter, setFilter] = useState("all"); // all | overdue | hasBalance
  const [stripeTarget, setStripeTarget] = useState(null);
  const [stripeSending, setStripeSending] = useState(false);
  const [reRequestTarget, setReRequestTarget] = useState(null);
  const [reRequestMenu, setReRequestMenu] = useState(null);
  const [emailSending, setEmailSending] = useState(false);
  const [reRequestMsg, setReRequestMsg] = useState("");

  const [openingTarget, setOpeningTarget] = useState(null);
  const [openingAmount, setOpeningAmount] = useState("");
  const [openingDate, setOpeningDate] = useState(today().slice(0, 7) + "-01");
  const [openBalClients, setOpenBalClients] = useState({});
  const total = Object.values(balances).reduce((a, b) => a + (b.currentBalance || 0), 0);

  const cancelPayment = async (ph) => {
    if (!confirm(`${ph.clientName} の入金 ¥${fmt(ph.amount)}（${ph.date}）を取り消しますか？\n\n残高が元に戻ります。`)) return;
    // 残高を戻す
    const bal = balances[ph.clientId] || {};
    const prev = bal.currentBalance || 0;
    await setDoc(doc(db, "clientBalances", ph.clientId), {
      clientId: ph.clientId, prevBalance: prev,
      currentBalance: prev + ph.amount,
      paidAmount: Math.max(0, (bal.paidAmount || 0) - ph.amount),
      updatedAt: serverTimestamp(),
    });
    // 入金履歴を削除
    await deleteDoc(doc(db, "paymentHistory", ph.id));
  };

  // 残高再計算
  const recalcBalance = async (clientId, overrideOpening, overrideOpeningDate) => {
    // Firestoreから最新のbalanceを取得
    const balSnap = await getDoc(doc(db, "clientBalances", clientId));
    const bal = balSnap.exists() ? balSnap.data() : {};
    const opening = overrideOpening !== undefined ? overrideOpening : (bal.openingBalance || 0);
    // 全請求書（paid/unpaid問わず）− 全入金 = 元帳と同じ計算
    const invoiceTotal = invoices.filter(i => i.clientId === clientId).reduce((a, i) => a + (i.total || 0), 0);
    const paymentTotal = (paymentHistory || []).filter(p => p.clientId === clientId).reduce((a, p) => a + (p.amount || 0), 0);
    const correct = opening + invoiceTotal - paymentTotal;
    const update = { ...bal, clientId, openingBalance: opening, currentBalance: correct, updatedAt: serverTimestamp() };
    if (overrideOpeningDate) update.openingDate = overrideOpeningDate;
    await setDoc(doc(db, "clientBalances", clientId), update);
    return correct;
  };

  const recalcAll = async () => {
    if (!confirm("全取引先の残高を再計算しますか？\n\n期首残高＋未収請求書合計−入金合計で再計算されます。")) return;
    let count = 0;
    for (const cl of clients) {
      await recalcBalance(cl.id);
      count++;
    }
    alert(`${count}社の残高を再計算しました`);
  };

  // 1ヶ月以上未入金の請求書を検出
  const oneMonthAgo = new Date();
  oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
  const oneMonthAgoStr = oneMonthAgo.toISOString().split("T")[0];

  const overdueInvoices = invoices.filter(i => i.status === "unpaid" && i.date && i.date <= oneMonthAgoStr);
  const overdueClientIds = [...new Set(overdueInvoices.map(i => i.clientId))];

  const clientList = clients.map(client => {
    const bal = balances[client.id] || {};
    const clientOverdue = overdueInvoices.filter(i => i.clientId === client.id);
    const oldestOverdue = clientOverdue.sort((a,b) => (a.date||"").localeCompare(b.date||""))[0];
    const daysSinceOldest = oldestOverdue ? Math.floor((new Date() - new Date(oldestOverdue.date)) / 86400000) : 0;
    return { ...client, bal, clientOverdue, oldestOverdue, daysSinceOldest, hasBalance: (bal.currentBalance || 0) > 0, isOverdue: clientOverdue.length > 0 };
  });

  const filtered = clientList.filter(c => {
    if (filter === "overdue") return c.isOverdue;
    if (filter === "hasBalance") return c.hasBalance;
    return true;
  }).sort((a, b) => (b.bal.currentBalance || 0) - (a.bal.currentBalance || 0));

  const overdueTotal = overdueInvoices.reduce((a, i) => a + (i.total || 0), 0);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={s.pageTitle}>残高管理</div>
        <button style={{ ...s.btn("light"), fontSize: 12 }} onClick={recalcAll}>残高再計算</button>
      </div>
      {/* アラートバナー */}
      {overdueClientIds.length > 0 && (
        <div style={{ background: "#f8d7da", border: `1px solid ${C.red}`, borderRadius: 10, padding: "14px 20px", marginBottom: 20, display: "flex", alignItems: "center", gap: 12 }}>
          <span style={{ fontSize: 24 }}>⚠</span>
          <div>
            <div style={{ fontWeight: 700, color: C.red, fontSize: 15 }}>未入金アラート：{overdueClientIds.length}社・{overdueInvoices.length}件（¥{fmt(overdueTotal)}）</div>
            <div style={{ fontSize: 12, color: C.red, marginTop: 2 }}>請求から1ヶ月以上経過した未入金の請求書があります</div>
          </div>
        </div>
      )}
      {/* サマリー */}
      <div style={{ display: "flex", gap: 16, marginBottom: 20, flexWrap: "wrap" }}>
        {[
          { label: "未収残高合計", value: "¥" + fmt(total), color: total > 0 ? C.red : C.green },
          { label: "未入金アラート", value: overdueInvoices.length + " 件", color: overdueInvoices.length > 0 ? C.red : C.green },
          { label: "対象取引先", value: overdueClientIds.length + " 社", color: overdueClientIds.length > 0 ? C.red : C.green },
          { label: "残高あり取引先", value: clientList.filter(c => c.hasBalance).length + " 社", color: C.navy },
        ].map(st => (
          <div key={st.label} style={{ ...s.card, flex: "1 1 140px", textAlign: "center", margin: 0 }}>
            <div style={{ fontSize: 12, color: C.gray, marginBottom: 6 }}>{st.label}</div>
            <div style={{ fontSize: 22, fontWeight: 700, color: st.color }}>{st.value}</div>
          </div>
        ))}
      </div>
      {/* フィルタ */}
      <div style={{ ...s.card, padding: "10px 20px", display: "flex", gap: 8, marginBottom: 0 }}>
        {[["all","すべて"],["overdue","未入金アラートのみ"],["hasBalance","残高ありのみ"]].map(([id,label]) => (
          <button key={id} style={{ ...s.btn(filter===id?"primary":"light"), padding: "5px 14px", fontSize: 13 }} onClick={() => setFilter(id)}>{label}</button>
        ))}
      </div>
      {filtered.map(client => {
        const isOpenBal = openBalClients[client.id];
        const clientInvoices = invoices.filter(i => i.clientId === client.id).sort((a,b) => (b.date||"").localeCompare(a.date||""));
        return (
          <div key={client.id} style={{ ...s.card, background: client.isOverdue ? "#fff5f5" : "white" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }} onClick={() => setOpenBalClients(prev => ({ ...prev, [client.id]: !prev[client.id] }))}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 12, color: C.gray }}>{isOpenBal ? "▼" : "▶"}</span>
                <strong style={{ fontSize: 15, color: C.navy }}>{client.name}</strong>
                <span style={s.badge((client.billingType==="closing"||client.billingType==="monthly")?"gold":"blue")}>{(client.billingType==="closing"||client.billingType==="monthly")?closingDaysLabel(client.closingDays||[0]):"即時"}</span>
                {client.isOverdue && <span style={{ ...s.badge("red"), fontSize: 10 }}>要確認</span>}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
                <span style={{ fontWeight: 700, fontSize: 16, color: client.hasBalance ? C.red : C.green }}>¥{fmt(client.bal.currentBalance||0)}</span>
                {client.clientOverdue.length > 0 && <span style={{ fontSize: 12, color: C.red }}>未入金 {client.clientOverdue.length}件</span>}
              </div>
            </div>
            {isOpenBal && (
              <div style={{ marginTop: 16, paddingTop: 16, borderTop: `1px solid ${C.border}` }}>
                <div style={{ display: "flex", gap: 16, marginBottom: 12, flexWrap: "wrap", fontSize: 13 }}>
                  <div><span style={{ color: C.gray }}>直近入金：</span>{client.bal.lastPaidDate ? `${client.bal.lastPaidDate}　¥${fmt(client.bal.lastPaidAmount)}` : "—"}</div>
                  {client.daysSinceOldest > 0 && <div><span style={{ color: C.gray }}>最長経過：</span><span style={{ color: client.daysSinceOldest > 60 ? C.red : client.daysSinceOldest > 30 ? "#856404" : C.gray, fontWeight: 700 }}>{client.daysSinceOldest}日</span></div>}
                </div>
                <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 12 }}>
                  <button style={{ ...s.btn("gold"), padding: "5px 12px", fontSize: 12 }} onClick={e => { e.stopPropagation(); setBalTarget({ client, balance: client.bal }); }}>入金記録</button>
                  <button style={{ ...s.btn("light"), padding: "5px 12px", fontSize: 12 }} onClick={e => { e.stopPropagation(); setOpeningTarget(client); setOpeningAmount(String(client.bal.openingBalance || 0)); setOpeningDate(client.bal.openingDate || today().slice(0,7)+"-01"); }}>期首残高</button>
                  {client.isOverdue && (
                    <span style={{ position: "relative", display: "inline-block" }}>
                      <button style={{ ...s.btn("primary"), padding: "5px 12px", fontSize: 12, background: C.red }} onClick={e => { e.stopPropagation(); setReRequestMenu(reRequestMenu === client.id ? null : client.id); }}>再請求 ▼</button>
                      {reRequestMenu === client.id && (
                        <div style={{ position: "absolute", top: "100%", right: 0, background: "white", border: `1px solid ${C.border}`, borderRadius: 8, boxShadow: "0 4px 12px rgba(0,0,0,0.15)", zIndex: 100, minWidth: 180, marginTop: 4 }}>
                          <button style={{ display: "block", width: "100%", padding: "10px 16px", border: "none", background: "none", textAlign: "left", cursor: "pointer", fontSize: 13, color: C.navy }} onMouseOver={e=>e.target.style.background="#f0f0f0"} onMouseOut={e=>e.target.style.background="none"} onClick={() => { setReRequestMenu(null); setReRequestTarget(client); }}>
                            ✉ メールで再請求<br/><span style={{ fontSize: 11, color: C.gray }}>手数料なし・督促メール送信</span>
                          </button>
                          {company?.stripeSecretKey && (
                            <button style={{ display: "block", width: "100%", padding: "10px 16px", border: "none", borderTop: `1px solid ${C.border}`, background: "none", textAlign: "left", cursor: "pointer", fontSize: 13, color: "#635BFF" }} onMouseOver={e=>e.target.style.background="#f0f0f0"} onMouseOut={e=>e.target.style.background="none"} onClick={() => { setReRequestMenu(null); setStripeTarget(client); }}>
                              💳 Stripeで請求<br/><span style={{ fontSize: 11, color: C.gray }}>オンライン決済リンク・手数料3.6%</span>
                            </button>
                          )}
                        </div>
                      )}
                    </span>
                  )}
                </div>
                {(() => {
                  // この取引先の元帳
                  const bal = client.bal || {};
                  const entries = [];
                  if (bal.openingBalance && bal.openingBalance > 0) {
                    entries.push({ date: bal.openingDate || "0000-00-00", type: "opening", description: "期首残高（繰越）", debit: bal.openingBalance, credit: 0 });
                  }
                  clientInvoices.forEach(inv => {
                    entries.push({ date: inv.date || "", type: "invoice", description: `請求書 ${inv.docNo}`, debit: inv.total || 0, credit: 0, status: inv.status, dueDate: inv.dueDate || "" });
                  });
                  (paymentHistory || []).filter(p => p.clientId === client.id).forEach(ph => {
                    entries.push({ date: ph.date || "", type: "payment", description: "入金", debit: 0, credit: ph.amount || 0, phId: ph.id });
                  });
                  entries.sort((a, b) => (a.date || "").localeCompare(b.date || ""));
                  let running = 0;
                  const withBal = entries.map(e => { running += e.debit - e.credit; return { ...e, balance: running }; });
                  if (!withBal.length) return null;
                  return (
                    <table style={{ ...s.table, marginTop: 8 }}>
                      <thead><tr>
                        <th style={s.th}>日付</th><th style={s.th}>摘要</th>
                        <th style={{ ...s.th, textAlign: "right" }}>借方</th>
                        <th style={{ ...s.th, textAlign: "right" }}>貸方</th>
                        <th style={{ ...s.th, textAlign: "right" }}>残高</th>
                        <th style={s.th}></th>
                      </tr></thead>
                      <tbody>
                        {withBal.slice(-20).map((e, i) => (
                          <tr key={i} style={{ background: e.type === "payment" ? "#f0fff0" : e.type === "opening" ? "#f0f4ff" : "transparent" }}>
                            <td style={s.td}>{e.date}</td>
                            <td style={s.td}>
                              {e.type === "opening" && <span style={s.badge("navy")}>繰越</span>}
                              {e.type === "invoice" && (() => { const ov = e.status === "unpaid" && e.dueDate && e.dueDate < today(); return <span style={s.badge(e.status === "paid" ? "green" : ov ? "red" : "gold")}>{e.status === "paid" ? "入金済" : ov ? "期限超過" : "未収"}</span>; })()}
                              {" "}{e.description}
                            </td>
                            <td style={{ ...s.td, textAlign: "right", color: e.debit ? C.red : "transparent" }}>{e.debit ? `¥${fmt(e.debit)}` : ""}</td>
                            <td style={{ ...s.td, textAlign: "right", color: e.credit ? C.green : "transparent" }}>{e.credit ? `¥${fmt(e.credit)}` : ""}</td>
                            <td style={{ ...s.td, textAlign: "right", fontWeight: 700, color: e.balance > 0 ? C.red : C.green }}>¥{fmt(e.balance)}</td>
                            <td style={s.td}>{e.type === "payment" && e.phId && (
                              <button style={{ ...s.btn("red"), padding: "3px 8px", fontSize: 11 }} onClick={() => cancelPayment(paymentHistory.find(p => p.id === e.phId))}>取消</button>
                            )}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  );
                })()}
              </div>
            )}
          </div>
        );
      })}
      {balTarget && <BalanceModal client={balTarget.client} balance={balTarget.balance} onClose={() => setBalTarget(null)} />}
      {openingTarget && (
        <div style={s.modal} onClick={() => setOpeningTarget(null)}>
          <div style={{ ...s.modalBox, maxWidth: 400 }} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: "0 0 16px", color: C.navy }}>期首残高の設定 — {openingTarget.name}</h3>
            <div style={{ fontSize: 12, color: C.gray, marginBottom: 16 }}>システム導入前の未収残高を設定します。元帳の開始残高に反映されます。</div>
            <div style={{ ...s.row, marginBottom: 16 }}>
              <div style={s.col}><span style={s.label}>基準日</span><input style={s.input} type="date" value={openingDate} onChange={e => setOpeningDate(e.target.value)} /></div>
              <div style={s.col}><span style={s.label}>期首残高（円）</span><input style={{ ...s.input, minWidth: 140 }} type="number" value={openingAmount} onChange={e => setOpeningAmount(e.target.value)} /></div>
            </div>
            <div style={{ ...s.row, justifyContent: "flex-end", gap: 8 }}>
              <button style={s.btn("light")} onClick={() => setOpeningTarget(null)}>キャンセル</button>
              <button style={s.btn("primary")} onClick={async () => {
                const n = Number(openingAmount);
                const bal = balances[openingTarget.id] || {};
                // 期首残高を設定して残高を再計算（元帳と一致する計算）
                await recalcBalance(openingTarget.id, n, openingDate);
                setOpeningTarget(null);
                alert(`${openingTarget.name} の期首残高を ¥${fmt(n)} に設定しました`);
              }}>保存</button>
            </div>
          </div>
        </div>
      )}
      {reRequestTarget && (() => {
        const cl = reRequestTarget;
        const clOverdue = overdueInvoices.filter(i => i.clientId === cl.id);
        const overdueTotal = clOverdue.reduce((a, i) => a + (i.total || 0), 0);
        const docNos = clOverdue.map(i => i.docNo).join(", ");
        const defaultMsg = `${cl.name} 御中\n\nいつもお世話になっております。\n下記の請求につきまして、お支払い期日を過ぎておりますのでご確認をお願いいたします。\n\n対象請求: ${docNos}\n未入金額: ¥${fmt(overdueTotal)}\n\nお忙しいところ恐れ入りますが、ご確認のほどよろしくお願いいたします。`;
        if (!reRequestMsg) setTimeout(() => setReRequestMsg(defaultMsg), 0);
        const msg = reRequestMsg || defaultMsg;
        const needApproval = company?.reRequestApproval !== false;
        const sendEmailReRequest = async () => {
          if (!cl.email) return alert("取引先のメールアドレスが設定されていません");
          setEmailSending(true);
          try {
            if (needApproval) {
              await addDoc(collection(db, "pendingBillings"), {
                type: "re-request-email",
                clientId: cl.id, clientName: cl.name, email: cl.email,
                invoiceDocNo: docNos, invoiceIds: clOverdue.map(i => i.id),
                total: overdueTotal, message: msg,
                status: "pending", createdAt: serverTimestamp(),
              });
              alert("承認待ちに追加しました。承認待ちページで確認・送信してください。");
              { setReRequestTarget(null); setReRequestMsg(""); };
            } else {
              if (!confirm(`${cl.name}（${cl.email}）にお支払いのお願いメールを送信します。`)) { setEmailSending(false); return; }
              const htmlBody = msg.replace(/\n/g, "<br/>");
              const res = await fetch("/api/send-invoice", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  to: cl.email,
                  subject: `【お支払いのお願い】${docNos}`,
                  html: htmlBody,
                }),
              });
              const data = await res.json();
              if (data.success) {
                await addDoc(collection(db, "sendHistory"), {
                  docNo: docNos, invoiceId: clOverdue.map(i => i.id).join(","), clientId: cl.id,
                  clientName: cl.name, email: cl.email, method: "mail",
                  memo: `再請求メール ¥${fmt(overdueTotal)}`,
                  amount: overdueTotal, sentAt: serverTimestamp(), sentBy: "re-request",
                });
                alert("再請求メールを送信しました");
                { setReRequestTarget(null); setReRequestMsg(""); };
              } else {
                alert("送信エラー: " + (data.error || "不明なエラー"));
              }
            }
          } catch (e) { alert("エラー: " + e.message); }
          setEmailSending(false);
        };
        return (
          <div style={s.modal} onClick={() => { if (!emailSending) { setReRequestTarget(null); setReRequestMsg(""); } }}>
            <div style={{ ...s.modalBox, maxWidth: 560 }} onClick={e => e.stopPropagation()}>
              <h3 style={{ margin: "0 0 16px", color: C.navy }}>メールで再請求</h3>
              <div style={{ background: "#f8f9fa", borderRadius: 8, padding: 16, marginBottom: 16 }}>
                <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>{cl.name}</div>
                <div style={{ fontSize: 13, color: C.gray, marginBottom: 4 }}>送信先: {cl.email || <span style={{ color: C.red }}>未設定</span>}</div>
                <div style={{ fontSize: 13, color: C.gray }}>未入金: {clOverdue.length}件 / ¥{fmt(overdueTotal)}</div>
              </div>
              <table style={{ ...s.table, marginBottom: 16 }}>
                <thead><tr><th style={s.th}>請求番号</th><th style={s.th}>日付</th><th style={s.th}>金額</th></tr></thead>
                <tbody>
                  {clOverdue.map(inv => (
                    <tr key={inv.id}>
                      <td style={s.td}>{inv.docNo}</td>
                      <td style={s.td}>{inv.date}</td>
                      <td style={{ ...s.td, textAlign: "right", fontWeight: 700 }}>¥{fmt(inv.total || 0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div style={{ marginBottom: 16 }}>
                <span style={{ ...s.label, marginBottom: 6, display: "block" }}>メール本文</span>
                <textarea style={{ ...s.input, width: "100%", minHeight: 160, fontFamily: "inherit", lineHeight: 1.6 }} value={msg} onChange={e => setReRequestMsg(e.target.value)} />
              </div>
              <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, borderTop: `1px solid ${C.border}`, paddingTop: 12 }}>
                <button style={s.btn("light")} onClick={() => { setReRequestTarget(null); setReRequestMsg(""); }} disabled={emailSending}>キャンセル</button>
                <button style={s.btn("primary")} onClick={sendEmailReRequest} disabled={emailSending || !cl.email}>
                  {emailSending ? "処理中..." : needApproval ? "承認待ちに追加" : "再請求メールを送信"}
                </button>
              </div>
            </div>
          </div>
        );
      })()}
      {stripeTarget && (() => {
        const cl = stripeTarget;
        const clOverdue = overdueInvoices.filter(i => i.clientId === cl.id);
        const overdueTotal = clOverdue.reduce((a, i) => a + (i.total || 0), 0);
        const docNos = clOverdue.map(i => i.docNo).join(", ");
        const needApproval = company?.reRequestApproval !== false;
        const sendStripeInvoice = async () => {
          if (!cl.email) return alert("取引先のメールアドレスが設定されていません");
          setStripeSending(true);
          try {
            if (needApproval) {
              const items = clOverdue.map(inv => ({ name: `${inv.docNo} 未入金分`, qty: 1, unitAmount: inv.total || 0 }));
              await addDoc(collection(db, "pendingBillings"), {
                type: "re-request-stripe",
                clientId: cl.id, clientName: cl.name, email: cl.email,
                invoiceDocNo: docNos, invoiceIds: clOverdue.map(i => i.id),
                total: overdueTotal, invoiceItems: items,
                status: "pending", createdAt: serverTimestamp(),
              });
              alert("承認待ちに追加しました。承認待ちページで確認・送信してください。");
              setStripeTarget(null);
            } else {
              if (!confirm(`${cl.name} に ¥${fmt(overdueTotal)} のStripe請求書を送信します。\n\n対象: ${docNos}\n送信先: ${cl.email}\n\n※ Stripe手数料（3.6%）が発生します。`)) { setStripeSending(false); return; }
              const items = clOverdue.map(inv => ({ name: `${inv.docNo} 未入金分`, qty: 1, unitAmount: inv.total || 0 }));
              const res = await fetch("/api/stripe-invoice", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  stripeSecretKey: company.stripeSecretKey,
                  clientName: cl.name, email: cl.email,
                  amount: overdueTotal, currency: "jpy",
                  description: `未入金再請求（${docNos}）`,
                  invoiceItems: items, docNos,
                }),
              });
              const data = await res.json();
              if (data.success) {
                await addDoc(collection(db, "sendHistory"), {
                  docNo: docNos, invoiceId: clOverdue.map(i => i.id).join(","), clientId: cl.id,
                  clientName: cl.name, email: cl.email, method: "stripe",
                  memo: `Stripe再請求 ¥${fmt(overdueTotal)} / ${data.invoiceUrl}`,
                  amount: overdueTotal, sentAt: serverTimestamp(), sentBy: "stripe",
                });
                alert(`Stripe請求書を送信しました！\n\n決済URL: ${data.invoiceUrl}`);
                setStripeTarget(null);
              } else {
                alert("Stripe請求エラー: " + (data.error || "不明なエラー"));
              }
            }
          } catch (e) { alert("エラー: " + e.message); }
          setStripeSending(false);
        };
        return (
          <div style={s.modal} onClick={() => !stripeSending && setStripeTarget(null)}>
            <div style={{ ...s.modalBox, maxWidth: 520 }} onClick={e => e.stopPropagation()}>
              <h3 style={{ margin: "0 0 16px", color: "#635BFF" }}>Stripe再請求</h3>
              <div style={{ background: "#f8f9fa", borderRadius: 8, padding: 16, marginBottom: 16 }}>
                <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>{cl.name}</div>
                <div style={{ fontSize: 13, color: C.gray, marginBottom: 4 }}>送信先: {cl.email || <span style={{ color: C.red }}>未設定</span>}</div>
                <div style={{ fontSize: 13, color: C.gray }}>未入金: {clOverdue.length}件</div>
              </div>
              <table style={{ ...s.table, marginBottom: 16 }}>
                <thead><tr><th style={s.th}>請求番号</th><th style={s.th}>日付</th><th style={s.th}>金額</th></tr></thead>
                <tbody>
                  {clOverdue.map(inv => (
                    <tr key={inv.id}>
                      <td style={s.td}>{inv.docNo}</td>
                      <td style={s.td}>{inv.date}</td>
                      <td style={{ ...s.td, textAlign: "right", fontWeight: 700 }}>¥{fmt(inv.total || 0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderTop: `1px solid ${C.border}`, paddingTop: 12 }}>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: "#635BFF" }}>合計: ¥{fmt(overdueTotal)}</div>
                  <div style={{ fontSize: 11, color: C.gray }}>Stripe手数料: 約¥{fmt(Math.round(overdueTotal * 0.036))}（3.6%）</div>
                </div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button style={s.btn("light")} onClick={() => setStripeTarget(null)} disabled={stripeSending}>キャンセル</button>
                  <button style={{ ...s.btn("primary"), background: "#635BFF" }} onClick={sendStripeInvoice} disabled={stripeSending || !cl.email}>
                    {stripeSending ? "処理中..." : needApproval ? "承認待ちに追加" : "Stripe請求書を送信"}
                  </button>
                </div>
              </div>
            </div>
          </div>
        );
      })()}
      {/* 元帳は各取引先カード内に統合 */}
    </div>
  );
}

// ── Clients ───────────────────────────────────────────────────────────────────
function ClientsPage({ clients, divisions, isAdmin }) {
  const [form, setForm] = useState({ name: "", kana: "", address: "", tel: "", email: "", billingType: "immediate", closingDays: [], sendMode: "auto", isOneTime: false, divisionId: "" });
  const [editing, setEditing] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [importing, setImporting] = useState(false);
  const setF = (k, v) => setForm(f => ({ ...f, [k]: v }));
  const save = async () => {
    if (!form.name) return alert("取引先名を入力してください");
    const data = { ...form, closingDays: Array.isArray(form.closingDays) ? form.closingDays.map(Number) : [], updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db, "clients", editing.id), data);
    else { data.createdAt = serverTimestamp(); await addDoc(collection(db, "clients"), data); }
    setShowForm(false); setEditing(null);
    setForm({ name: "", kana: "", address: "", tel: "", email: "", billingType: "immediate", closingDays: [], sendMode: "auto", isOneTime: false, divisionId: "" });
  };
  const edit = (c) => {
    const cd = c.closingDays;
    const normalizedDays = Array.isArray(cd) ? cd.map(Number) : (cd !== undefined && cd !== null && cd !== "" ? [Number(cd)] : []);
    setForm({ ...c, closingDays: normalizedDays });
    setEditing(c); setShowForm(true);
  };
  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db, "clients", id)); };
  const handleCSV = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setImporting(true);
    try {
      const text = await file.text();
      const lines = text.split(/\r?\n/).filter(l => l.trim());
      const header = parseCSVLine(lines[0]);
      const nameIdx = header.findIndex(h => h.includes("会社名") || h.includes("取引先") || h.includes("名前") || h.toLowerCase() === "name");
      const kanaIdx = header.findIndex(h => h.includes("フリガナ") || h.includes("カナ") || h.toLowerCase() === "kana");
      const addrIdx = header.findIndex(h => h.includes("住所") || h.toLowerCase() === "address");
      const telIdx = header.findIndex(h => h.includes("電話") || h.includes("TEL") || h.toLowerCase() === "tel");
      const emailIdx = header.findIndex(h => h.includes("メール") || h.includes("mail") || h.toLowerCase() === "email");
      const typeIdx = header.findIndex(h => h.includes("請求タイプ") || h.includes("billingType"));
      const closingIdx = header.findIndex(h => h.includes("締日") || h.includes("closingDays"));
      const oneTimeIdx = header.findIndex(h => h.includes("単発") || h.includes("isOneTime"));
      if (nameIdx === -1) { alert("「会社名」または「取引先名」列が見つかりません"); setImporting(false); return; }
      const rows = lines.slice(1);
      const existingByName = {};
      clients.forEach(c => { if (c.name) existingByName[c.name] = c; });
      const batch = writeBatch(db);
      let addCount = 0, updateCount = 0;
      for (const line of rows) {
        const cols = parseCSVLine(line);
        const name = cols[nameIdx];
        if (!name) continue;
        const billingRaw = typeIdx >= 0 ? (cols[typeIdx] || "") : "";
        let billingType = "immediate";
        let closingDays = [];
        if (billingRaw === "月締め" || billingRaw === "monthly") { billingType = "closing"; closingDays = [0]; }
        else if (billingRaw.includes("締") || billingRaw === "closing") { billingType = "closing"; closingDays = [0]; }
        if (billingType !== "immediate" && closingIdx >= 0 && cols[closingIdx]) {
          closingDays = cols[closingIdx].split(/[・,、]/).map(s => { const n = parseInt(s); return isNaN(n) ? 0 : n; }).filter((v,i,a) => a.indexOf(v) === i).slice(0, 4);
        }
        const oneTimeRaw = oneTimeIdx >= 0 ? (cols[oneTimeIdx] || "") : "";
        const isOneTime = oneTimeRaw === "true" || oneTimeRaw === "1" || oneTimeRaw === "はい" || oneTimeRaw === "○";
        const data = {
          name,
          kana: kanaIdx >= 0 ? (cols[kanaIdx] || "") : "",
          address: addrIdx >= 0 ? (cols[addrIdx] || "") : "",
          tel: telIdx >= 0 ? (cols[telIdx] || "") : "",
          email: emailIdx >= 0 ? (cols[emailIdx] || "") : "",
          billingType, closingDays,
          isOneTime,
          updatedAt: serverTimestamp(),
        };
        const existing = existingByName[name];
        if (existing) {
          batch.update(doc(db, "clients", existing.id), data);
          updateCount++;
        } else {
          const ref = doc(collection(db, "clients"));
          batch.set(ref, { ...data, createdAt: serverTimestamp() });
          addCount++;
        }
      }
      await batch.commit();
      alert(`新規 ${addCount}件、上書き ${updateCount}件 インポートしました`);
    } catch (err) {
      alert("CSVの読み込みに失敗しました: " + err.message);
    }
    setImporting(false);
    e.target.value = "";
  };
  const downloadCSV = () => {
    const headers = ["会社名","フリガナ","住所","電話","メール","請求タイプ","締日","単発"];
    const rows = clients.map(c => [
      c.name || "", c.kana || "", c.address || "", c.tel || "", c.email || "",
      (c.billingType === "closing" || c.billingType === "monthly") ? "締日" : "即時",
      (c.closingDays || (c.billingType === "monthly" ? [0] : [])).map(d => d === 0 ? "末日" : `${d}`).join("・"),
      c.isOneTime ? "○" : ""
    ]);
    const csv = [headers, ...rows].map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(",")).join("\n");
    const bom = "\uFEFF";
    const blob = new Blob([bom + csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = `取引先_${today()}.csv`; a.click();
    URL.revokeObjectURL(url);
  };
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={s.pageTitle}>取引先管理</div>
        <div style={{display:"flex",gap:8}}>
          <button style={s.btn("light")} onClick={downloadCSV}>CSV ダウンロード</button>
          <label style={{...s.btn("gold"),display:"inline-block",cursor:importing?"wait":"pointer",opacity:importing?0.6:1}}>
            {importing ? "インポート中..." : "CSV インポート"}
            <input type="file" accept=".csv" style={{display:"none"}} onChange={handleCSV} disabled={importing} />
          </label>
          <button style={s.btn("primary")} onClick={() => { setEditing(null); setForm({ name:"",kana:"",address:"",tel:"",email:"",billingType:"immediate",closingDays:[],sendMode:"manual",isOneTime:false,divisionId:"" }); setShowForm(true); }}>＋ 追加</button>
        </div>
      </div>
      {showForm && (
        <div style={s.card}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>{editing ? "取引先を編集" : "取引先を追加"}</h3>
          <div style={{ ...s.row, marginBottom: 12 }}>
            <div style={s.col}><span style={s.label}>会社名 *</span><input style={s.input} value={form.name} onChange={e => setF("name",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>フリガナ</span><input style={s.input} value={form.kana} onChange={e => setF("kana",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>電話番号</span><input style={s.input} value={form.tel} onChange={e => setF("tel",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>メール</span><input style={s.input} value={form.email} onChange={e => setF("email",e.target.value)} /></div>
          </div>
          <div style={{ ...s.row, marginBottom: 12 }}>
            <div style={s.col}><span style={s.label}>住所</span><input style={{ ...s.input, minWidth: 300 }} value={form.address} onChange={e => setF("address",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>請求タイプ</span>
              <select style={s.select} value={form.billingType} onChange={e => { setF("billingType",e.target.value); if(e.target.value==="immediate") setF("closingDays",[]); if(e.target.value==="closing" && (!form.closingDays||!form.closingDays.length)) setF("closingDays",[0]); }}>
                <option value="immediate">即時請求</option><option value="closing">締日請求</option>
              </select>
            </div>
            {form.billingType === "closing" && (
              <div style={s.col}><span style={s.label}>締日（最大4つ）</span>
                <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
                  {(form.closingDays || []).map((d, i) => (
                    <div key={i} style={{ display: "flex", gap: 4, alignItems: "center" }}>
                      <select style={{ ...s.select, width: 80 }} value={d} onChange={e => { const nv = [...(form.closingDays||[])]; nv[i] = Number(e.target.value); setF("closingDays", nv); }}>
                        {Array.from({ length: 28 }, (_, k) => k + 1).map(n => <option key={n} value={n}>{n}日</option>)}
                        <option value={0}>末日</option>
                      </select>
                      <button style={{ ...s.btn("red"), padding: "2px 8px", fontSize: 11 }} onClick={() => { const nv = (form.closingDays||[]).filter((_,j)=>j!==i); setF("closingDays", nv.length ? nv : [0]); }}>✕</button>
                    </div>
                  ))}
                  {(form.closingDays || []).length < 4 && (
                    <button style={{ ...s.btn("light"), padding: "4px 10px", fontSize: 12 }} onClick={() => setF("closingDays", [...(form.closingDays||[]), 0])}>＋</button>
                  )}
                </div>
              </div>
            )}
            <div style={s.col}><span style={s.label}>送信方法</span>
              <select style={s.select} value={form.sendMode||"manual"} onChange={e => setF("sendMode",e.target.value)}>
                <option value="auto">自動送信</option>
                <option value="confirm">確認後送信</option>
                <option value="manual">手動（紙請求等）</option>
              </select>
            </div>
            <div style={s.col}><span style={s.label}>事業部</span>
              <select style={s.select} value={form.divisionId||""} onChange={e => setF("divisionId",e.target.value)}>
                <option value="">指定なし</option>
                {divisions.map(d => <option key={d.id} value={d.id}>{d.name}</option>)}
              </select>
            </div>
            <div style={s.col}><span style={s.label}>単発取引先</span>
              <label style={{ display:"flex",alignItems:"center",gap:6,padding:"8px 0" }}>
                <input type="checkbox" checked={form.isOneTime} onChange={e => setF("isOneTime",e.target.checked)} />単発フラグ
              </label>
            </div>
          </div>
          <div style={{ ...s.row, justifyContent: "flex-end", gap: 8 }}>
            <button style={s.btn("light")} onClick={() => setShowForm(false)}>キャンセル</button>
            <button style={s.btn("primary")} onClick={save}>{editing ? "更新" : "保存"}</button>
          </div>
        </div>
      )}
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>会社名</th><th style={s.th}>事業部</th><th style={s.th}>電話</th><th style={s.th}>請求タイプ</th><th style={s.th}>区分</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {clients.map(c => {
              const div = divisions.find(d => d.id === c.divisionId);
              return (
              <tr key={c.id}>
                <td style={s.td}>{c.name}</td>
                <td style={s.td}>{div ? <span style={s.badge("blue")}>{div.name}</span> : "—"}</td>
                <td style={s.td}>{c.tel}</td>
                <td style={s.td}><span style={s.badge(c.billingType==="closing"||c.billingType==="monthly"?"gold":"blue")}>{c.billingType==="closing"?closingDaysLabel(c.closingDays):c.billingType==="monthly"?"月末締め":"即時"}</span></td>
                <td style={s.td}>{c.isOneTime&&<span style={s.badge("light")}>単発</span>}</td>
                <td style={s.td}>
                  <div style={{display:"flex",gap:6}}>
                    <button style={{...s.btn("light"),padding:"4px 10px",fontSize:12}} onClick={()=>edit(c)}>編集</button>
                    {isAdmin && <button style={{...s.btn("red"),padding:"4px 10px",fontSize:12}} onClick={()=>del(c.id)}>削除</button>}
                  </div>
                </td>
              </tr>
            );})}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Products ──────────────────────────────────────────────────────────────────
function ProductsPage({ products, company, isAdmin }) {
  const defRate = company?.defaultTaxRate !== undefined ? company.defaultTaxRate : 10;
  const [form, setForm] = useState({ name:"",code:"",jan:"",unit:"",price:"",taxRate:defRate,notes:"",category:"" });
  const [editing, setEditing] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [importing, setImporting] = useState(false);
  const [catFilter, setCatFilter] = useState("");
  const categories = [...new Set(products.map(p => p.category).filter(Boolean))].sort((a,b) => a.localeCompare(b,"ja"));
  const setF = (k,v) => setForm(f => ({...f,[k]:v}));
  const save = async () => {
    if (!form.name) return alert("商品名を入力してください");
    if (!form.code) return alert("商品コードを入力してください");
    if (!editing && products.some(p => p.code === form.code)) return alert(`商品コード「${form.code}」は既に使われています`);
    if (editing && products.some(p => p.code === form.code && p.id !== editing.id)) return alert(`商品コード「${form.code}」は既に使われています`);
    const data = { ...form, price: Number(form.price)||0, taxRate: Number(form.taxRate), updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db,"products",editing.id),data);
    else { data.createdAt = serverTimestamp(); await addDoc(collection(db,"products"),data); }
    setShowForm(false); setEditing(null); setForm({name:"",code:"",jan:"",unit:"",price:"",taxRate:defRate,notes:"",category:""});
  };
  const edit = (p) => { setForm({...p,price:String(p.price),taxRate:p.taxRate!==undefined?p.taxRate:10,category:p.category||""}); setEditing(p); setShowForm(true); };
  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db,"products",id)); };
  const handleCSV = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setImporting(true);
    try {
      const text = await file.text();
      const lines = text.split(/\r?\n/).filter(l => l.trim());
      const header = parseCSVLine(lines[0]);
      const nameIdx = header.findIndex(h => h.includes("商品名") || h.toLowerCase() === "name");
      const codeIdx = header.findIndex(h => h === "商品コード" || h === "コード" || h.toLowerCase() === "code");
      const janIdx = header.findIndex(h => h.includes("JAN") || h === "JANコード" || h.toLowerCase() === "jan");
      const priceIdx = header.findIndex(h => h.includes("単価") || h.includes("価格") || h.toLowerCase() === "price");
      const notesIdx = header.findIndex(h => h.includes("備考") || h.toLowerCase() === "notes");
      const catIdx = header.findIndex(h => h === "カテゴリ" || h.toLowerCase() === "category");
      if (nameIdx === -1) { alert("「商品名」列が見つかりません"); setImporting(false); return; }
      if (codeIdx === -1) { alert("「コード」列が見つかりません"); setImporting(false); return; }
      const rows = lines.slice(1);
      const existingByCode = {};
      products.forEach(p => { if (p.code) existingByCode[p.code] = p; });
      const clean = (v) => { let s = (v||"").trim(); if(s.startsWith('=')) s=s.slice(1); s=s.replace(/^"+|"+$/g,""); return s.trim(); };
      const parsePrice = (v) => Number(clean(v).replace(/[¥￥,]/g,"")) || 0;
      const batch = writeBatch(db);
      let addCount = 0, updateCount = 0;
      for (const line of rows) {
        const cols = parseCSVLine(line);
        const name = clean(cols[nameIdx]);
        const code = codeIdx >= 0 ? clean(cols[codeIdx]) : "";
        if (!name || !code) continue;
        const data = {
          name,
          code,
          jan: janIdx >= 0 ? clean(cols[janIdx]) : "",
          unit: "",
          price: priceIdx >= 0 ? parsePrice(cols[priceIdx]) : 0,
          notes: notesIdx >= 0 ? clean(cols[notesIdx]) : "",
          category: catIdx >= 0 ? clean(cols[catIdx]) : "",
          updatedAt: serverTimestamp(),
        };
        const existing = existingByCode[code];
        if (existing) {
          batch.update(doc(db, "products", existing.id), data);
          updateCount++;
        } else {
          const ref = doc(collection(db, "products"));
          batch.set(ref, { ...data, createdAt: serverTimestamp() });
          addCount++;
        }
      }
      await batch.commit();
      alert(`新規 ${addCount}件、上書き ${updateCount}件 インポートしました`);
    } catch (err) {
      alert("CSVの読み込みに失敗しました: " + err.message);
    }
    setImporting(false);
    e.target.value = "";
  };
  return (
    <div>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:20}}>
        <div style={s.pageTitle}>商品マスタ</div>
        <div style={{display:"flex",gap:8}}>
          <label style={{...s.btn("gold"),display:"inline-block",cursor:importing?"wait":"pointer",opacity:importing?0.6:1}}>
            {importing ? "インポート中..." : "CSV インポート"}
            <input type="file" accept=".csv" style={{display:"none"}} onChange={handleCSV} disabled={importing} />
          </label>
          <button style={s.btn("primary")} onClick={()=>{setEditing(null);setForm({name:"",code:"",jan:"",unit:"",price:"",taxRate:defRate,notes:"",category:""});setShowForm(true);}}>＋ 追加</button>
        </div>
      </div>
      {showForm && (
        <div style={s.card}>
          <h3 style={{margin:"0 0 16px",color:C.navy}}>{editing?"商品を編集":"商品を追加"}</h3>
          <div style={{...s.row,marginBottom:12}}>
            <div style={s.col}><span style={s.label}>商品名 *</span><input style={s.input} value={form.name} onChange={e=>setF("name",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>商品コード *</span><input style={s.input} value={form.code} onChange={e=>setF("code",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>JANコード</span><input style={s.input} value={form.jan||""} onChange={e=>setF("jan",e.target.value)} placeholder="任意" /></div>
            <div style={s.col}><span style={s.label}>単位</span><input style={{...s.input,width:80}} value={form.unit} onChange={e=>setF("unit",e.target.value)} placeholder="袋" /></div>
            <div style={s.col}><span style={s.label}>標準単価</span><input style={{...s.input,width:120}} type="number" value={form.price} onChange={e=>setF("price",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>税率</span>
              <select style={{...s.select,width:80}} value={(() => { const r = form.taxRate !== undefined && form.taxRate !== null && form.taxRate !== "" ? Number(form.taxRate) : 10; return r === 10 || r === 8 || r === 0 ? String(r) : "custom"; })()} onChange={e => setF("taxRate", e.target.value === "custom" ? form.taxRate : Number(e.target.value))}>
                <option value="10">10%</option><option value="8">8%</option><option value="0">0%</option><option value="custom">自由</option>
              </select>
              {(() => { const r = form.taxRate !== undefined && form.taxRate !== null && form.taxRate !== "" ? Number(form.taxRate) : 10; return r !== 10 && r !== 8 && r !== 0 ? <input style={{...s.input,width:50,marginLeft:4}} type="number" min={0} max={100} value={r} onChange={e => setF("taxRate", Number(e.target.value))} /> : null; })()}
            </div>
            <div style={s.col}><span style={s.label}>カテゴリ</span>
              <div style={{display:"flex",gap:4}}>
                <input style={{...s.input,minWidth:120}} value={form.category} onChange={e=>setF("category",e.target.value)} placeholder="例: 食品" list="cat-list" />
                <datalist id="cat-list">{categories.map(c => <option key={c} value={c} />)}</datalist>
              </div>
            </div>
            <div style={s.col}><span style={s.label}>備考</span><input style={{...s.input,minWidth:200}} value={form.notes} onChange={e=>setF("notes",e.target.value)} /></div>
          </div>
          <div style={{...s.row,justifyContent:"flex-end",gap:8}}>
            <button style={s.btn("light")} onClick={()=>setShowForm(false)}>キャンセル</button>
            <button style={s.btn("primary")} onClick={save}>{editing?"更新":"保存"}</button>
          </div>
        </div>
      )}
      <div style={s.card}>
        {categories.length > 0 && (
          <div style={{marginBottom:12,display:"flex",gap:8,alignItems:"center"}}>
            <span style={{fontSize:13,color:C.gray}}>カテゴリ:</span>
            <select style={{...s.select,width:160}} value={catFilter} onChange={e=>setCatFilter(e.target.value)}>
              <option value="">すべて</option>
              {categories.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        )}
        <table style={s.table}>
          <thead><tr><th style={s.th}>商品名</th><th style={s.th}>コード</th><th style={s.th}>カテゴリ</th><th style={s.th}>JAN</th><th style={s.th}>単位</th><th style={s.th}>標準単価</th><th style={s.th}>税率</th><th style={s.th}>備考</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {[...products].filter(p => !catFilter || p.category === catFilter).sort((a,b)=>(a.code||"").localeCompare(b.code||"","ja",{numeric:true})).map(p => (
              <tr key={p.id}>
                <td style={s.td}>{p.name}</td><td style={s.td}>{p.code}</td><td style={s.td}>{p.category||""}</td><td style={s.td}>{p.jan||""}</td>
                <td style={s.td}>{p.unit}</td><td style={s.td}>¥{fmt(p.price)}</td><td style={s.td}>{p.taxRate !== undefined ? `${p.taxRate}%` : "10%"}</td><td style={s.td}>{p.notes}</td>
                <td style={s.td}>
                  <div style={{display:"flex",gap:6}}>
                    <button style={{...s.btn("light"),padding:"4px 10px",fontSize:12}} onClick={()=>edit(p)}>編集</button>
                    {isAdmin && <button style={{...s.btn("red"),padding:"4px 10px",fontSize:12}} onClick={()=>del(p.id)}>削除</button>}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── ClientPrices ──────────────────────────────────────────────────────────────
function ClientPricesPage({ clients, products, clientPrices, isAdmin }) {
  const [selClient, setSelClient] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [formProduct, setFormProduct] = useState("");
  const [formPrice, setFormPrice] = useState("");
  const [editing, setEditing] = useState(null);
  const [importing, setImporting] = useState(false);

  const filtered = selClient ? clientPrices.filter(cp => cp.clientId === selClient) : clientPrices;

  const save = async () => {
    if (!selClient) return alert("取引先を選択してください");
    if (!formProduct) return alert("商品を選択してください");
    const price = Number(formPrice) || 0;
    const data = { clientId: selClient, productId: formProduct, price, updatedAt: serverTimestamp() };
    if (editing) {
      await updateDoc(doc(db, "clientPrices", editing.id), data);
    } else {
      const dup = clientPrices.find(cp => cp.clientId === selClient && cp.productId === formProduct);
      if (dup) {
        await updateDoc(doc(db, "clientPrices", dup.id), data);
      } else {
        data.createdAt = serverTimestamp();
        await addDoc(collection(db, "clientPrices"), data);
      }
    }
    setShowForm(false); setEditing(null); setFormProduct(""); setFormPrice("");
  };

  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db, "clientPrices", id)); };

  const edit = (cp) => {
    setSelClient(cp.clientId); setFormProduct(cp.productId); setFormPrice(String(cp.price)); setEditing(cp); setShowForm(true);
  };

  const handleCSV = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setImporting(true);
    try {
      const text = await file.text();
      const lines = text.split(/\r?\n/).filter(l => l.trim());
      const header = parseCSVLine(lines[0]);
      const clientIdx = header.findIndex(h => h.includes("取引先") || h.includes("会社名"));
      const productIdx = header.findIndex(h => h.includes("商品名") || h.includes("品名"));
      const codeIdx = header.findIndex(h => h === "商品コード" || h === "コード" || h.toLowerCase() === "code");
      const janIdx = header.findIndex(h => h.includes("JAN") || h.toLowerCase() === "jan");
      const priceIdx = header.findIndex(h => h.includes("単価") || h.includes("価格") || h.toLowerCase() === "price");
      if (clientIdx === -1) { alert("「取引先名」列が見つかりません"); setImporting(false); return; }
      if (priceIdx === -1) { alert("「単価」列が見つかりません"); setImporting(false); return; }
      if (productIdx === -1 && codeIdx === -1 && janIdx === -1) { alert("「商品名」「商品コード」「JANコード」のいずれかの列が必要です"); setImporting(false); return; }
      const clean = (v) => { let s = (v||"").trim(); if(s.startsWith('=')) s=s.slice(1); s=s.replace(/^"+|"+$/g,""); return s.trim(); };
      const parsePrice = (v) => Number(clean(v).replace(/[¥￥,]/g,"")) || 0;
      const rows = lines.slice(1);
      const batch = writeBatch(db);
      let count = 0, prodCount = 0;
      for (const line of rows) {
        const cols = parseCSVLine(line);
        const clientName = clean(cols[clientIdx]);
        const productName = productIdx >= 0 ? clean(cols[productIdx]) : "";
        const productCode = codeIdx >= 0 ? clean(cols[codeIdx]) : "";
        const janCode = janIdx >= 0 ? clean(cols[janIdx]) : "";
        const price = parsePrice(cols[priceIdx]);
        const client = clients.find(c => c.name === clientName);
        if (!client) continue;
        let product = (productCode && products.find(p => p.code === productCode))
          || (janCode && products.find(p => p.jan === janCode))
          || (productName && products.find(p => p.name === productName));
        if (!product && (productCode || productName)) {
          const ref = doc(collection(db, "products"));
          const newProd = { name: productName || productCode, code: productCode, jan: janCode, unit: "", price, notes: "", createdAt: serverTimestamp(), updatedAt: serverTimestamp() };
          batch.set(ref, newProd);
          product = { id: ref.id, ...newProd };
          products.push(product);
          prodCount++;
        }
        if (!product) continue;
        const dup = clientPrices.find(cp => cp.clientId === client.id && cp.productId === product.id);
        const data = { clientId: client.id, productId: product.id, price, updatedAt: serverTimestamp() };
        if (dup) {
          batch.update(doc(db, "clientPrices", dup.id), data);
        } else {
          const ref = doc(collection(db, "clientPrices"));
          batch.set(ref, { ...data, createdAt: serverTimestamp() });
        }
        count++;
      }
      await batch.commit();
      alert(`単価 ${count}件 インポート${prodCount ? `、商品 ${prodCount}件 新規登録` : ""}しました`);
    } catch (err) {
      alert("CSVの読み込みに失敗しました: " + err.message);
    }
    setImporting(false);
    e.target.value = "";
  };

  const downloadCSV = () => {
    const headers = ["取引先名","商品名","商品コード","JANコード","単価"];
    const rows = filtered.map(cp => {
      const cl = clients.find(c => c.id === cp.clientId);
      const pr = products.find(p => p.id === cp.productId);
      return [cl?.name || "", pr?.name || "", pr?.code || "", pr?.jan || "", cp.price || 0];
    });
    const csv = [headers, ...rows].map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(",")).join("\n");
    const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = `取引先別単価_${today()}.csv`; a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={s.pageTitle}>取引先別単価</div>
        <div style={{ display: "flex", gap: 8 }}>
          <button style={s.btn("light")} onClick={downloadCSV}>CSV ダウンロード</button>
          <label style={{...s.btn("gold"),display:"inline-block",cursor:importing?"wait":"pointer",opacity:importing?0.6:1}}>
            {importing ? "インポート中..." : "CSV インポート"}
            <input type="file" accept=".csv" style={{display:"none"}} onChange={handleCSV} disabled={importing} />
          </label>
          <button style={s.btn("primary")} onClick={() => { setEditing(null); setFormProduct(""); setFormPrice(""); setShowForm(true); }}>＋ 追加</button>
        </div>
      </div>
      <div style={{ ...s.card, marginBottom: 16 }}>
        <div style={s.row}>
          <div style={s.col}>
            <span style={s.label}>取引先で絞り込み</span>
            <select style={s.select} value={selClient} onChange={e => setSelClient(e.target.value)}>
              <option value="">すべて</option>
              {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
          </div>
        </div>
      </div>
      {showForm && (
        <div style={s.card}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>{editing ? "単価を編集" : "取引先別単価を追加"}</h3>
          <div style={{ ...s.row, marginBottom: 12 }}>
            <div style={s.col}><span style={s.label}>取引先 *</span>
              <select style={s.select} value={selClient} onChange={e => setSelClient(e.target.value)}>
                <option value="">選択してください</option>
                {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
            <div style={s.col}><span style={s.label}>商品 *</span>
              <select style={s.select} value={formProduct} onChange={e => setFormProduct(e.target.value)}>
                <option value="">選択してください</option>
                {products.map(p => <option key={p.id} value={p.id}>{p.name}（{p.code}）標準¥{fmt(p.price)}</option>)}
              </select>
            </div>
            <div style={s.col}><span style={s.label}>この取引先への単価 *</span>
              <input style={{ ...s.input, width: 140 }} type="number" value={formPrice} onChange={e => setFormPrice(e.target.value)} />
            </div>
          </div>
          <div style={{ ...s.row, justifyContent: "flex-end", gap: 8 }}>
            <button style={s.btn("light")} onClick={() => setShowForm(false)}>キャンセル</button>
            <button style={s.btn("primary")} onClick={save}>{editing ? "更新" : "保存"}</button>
          </div>
        </div>
      )}
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>取引先</th><th style={s.th}>商品名</th><th style={s.th}>コード</th><th style={s.th}>標準単価</th><th style={s.th}>取引先単価</th><th style={s.th}>差額</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {filtered.map(cp => {
              const cl = clients.find(c => c.id === cp.clientId);
              const pr = products.find(p => p.id === cp.productId);
              const diff = (cp.price || 0) - (pr?.price || 0);
              return (
                <tr key={cp.id}>
                  <td style={s.td}>{cl?.name || "—"}</td>
                  <td style={s.td}>{pr?.name || "—"}</td>
                  <td style={s.td}>{pr?.code || ""}</td>
                  <td style={s.td}>¥{fmt(pr?.price)}</td>
                  <td style={{ ...s.td, fontWeight: 700, color: C.navy }}>¥{fmt(cp.price)}</td>
                  <td style={{ ...s.td, color: diff > 0 ? C.green : diff < 0 ? C.red : C.gray }}>{diff > 0 ? "+" : ""}{fmt(diff)}</td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 6 }}>
                      <button style={{ ...s.btn("light"), padding: "4px 10px", fontSize: 12 }} onClick={() => edit(cp)}>編集</button>
                      {isAdmin && <button style={{ ...s.btn("red"), padding: "4px 10px", fontSize: 12 }} onClick={() => del(cp.id)}>削除</button>}
                    </div>
                  </td>
                </tr>
              );
            })}
            {!filtered.length && <tr><td colSpan={7} style={{ ...s.td, textAlign: "center", color: C.gray }}>データがありません</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Send History ──────────────────────────────────────────────────────────────
function SendHistoryPage({ isAdmin }) {
  const [history, setHistory] = useState([]);
  const [search, setSearch] = useState("");
  useEffect(() => {
    const unsub = onSnapshot(query(collection(db, "sendHistory"), orderBy("sentAt", "desc")), snap => {
      setHistory(snap.docs.map(d => ({ id: d.id, ...d.data() })));
    });
    return unsub;
  }, []);
  const filtered = history.filter(h =>
    (h.clientName || "").includes(search) || (h.docNo || "").includes(search)
  );
  const methodLabel = { mail: "メール", post: "郵送", fax: "FAX", hand: "手渡し", other: "その他", auto: "自動送信" };
  const deleteH = async (h) => {
    if (!window.confirm("この送信記録を削除しますか？")) return;
    await deleteDoc(doc(db, "sendHistory", h.id));
  };
  return (
    <div>
      <div style={{ ...s.row, justifyContent: "space-between", marginBottom: 16 }}>
        <h2 style={s.pageTitle}>📨 送信履歴</h2>
        <input style={{ ...s.input, minWidth: 240 }} placeholder="取引先名・伝票番号で検索" value={search} onChange={e => setSearch(e.target.value)} />
      </div>
      <div style={{ ...s.card, padding: "12px 20px", marginBottom: 16, display: "flex", gap: 24 }}>
        <div style={{ fontSize: 14 }}>総送信件数：<strong>{history.length}</strong></div>
        <div style={{ fontSize: 14 }}>今月：<strong>{history.filter(h => h.sentAt?.toDate?.()?.toISOString?.()?.slice(0,7) === today().slice(0,7)).length}</strong></div>
      </div>
      <div style={s.card}>
        {filtered.length === 0 ? <p style={{ color: C.gray }}>送信履歴がありません。請求書一覧の「送信記録」ボタンから記録できます。</p> : (
          <table style={s.table}>
            <thead><tr>
              <th style={s.th}>送信日時</th><th style={s.th}>請求番号</th><th style={s.th}>取引先</th>
              <th style={s.th}>金額</th><th style={s.th}>送信方法</th><th style={s.th}>メモ</th><th style={s.th}>操作</th>
            </tr></thead>
            <tbody>
              {filtered.map(h => (
                <tr key={h.id}>
                  <td style={s.td}>{h.sentAt?.toDate?.()?.toLocaleString("ja-JP") || ""}</td>
                  <td style={s.td}>{h.docNo}</td>
                  <td style={s.td}>{h.clientName}</td>
                  <td style={s.td}>¥{fmt(h.amount)}</td>
                  <td style={s.td}><span style={s.badge(h.method === "mail" || h.method === "auto" ? "blue" : "gray")}>{methodLabel[h.method] || h.method}</span></td>
                  <td style={s.td}>{h.memo || ""}</td>
                  {isAdmin && <td style={s.td}>
                    <button style={{ ...s.btn("red"), padding: "4px 12px", fontSize: 12 }} onClick={() => deleteH(h)}>削除</button>
                  </td>}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ── PDF History ───────────────────────────────────────────────────────────────
function PDFHistoryPage({ isAdmin }) {
  const [history, setHistory] = useState([]);
  const [search, setSearch] = useState("");
  useEffect(() => {
    const unsub = onSnapshot(query(collection(db, "pdfHistory"), orderBy("createdAt", "desc")), snap => {
      setHistory(snap.docs.map(d => ({ id: d.id, ...d.data() })));
    });
    return unsub;
  }, []);
  const filtered = history.filter(h =>
    (h.clientName || "").includes(search) || (h.docNo || "").includes(search) || (h.type || "").includes(search)
  );
  const deleteH = async (h) => {
    if (!window.confirm(`${h.filename} を削除しますか？`)) return;
    try {
      const { deleteObject } = await import("firebase/storage");
      await deleteObject(ref(storage, h.storagePath));
    } catch (e) { /* storage file may not exist */ }
    await deleteDoc(doc(db, "pdfHistory", h.id));
  };
  return (
    <div>
      <div style={{ ...s.row, justifyContent: "space-between", marginBottom: 16 }}>
        <h2 style={s.pageTitle}>📁 PDF履歴</h2>
        <input style={{ ...s.input, minWidth: 240 }} placeholder="取引先名・伝票番号で検索" value={search} onChange={e => setSearch(e.target.value)} />
      </div>
      <div style={s.card}>
        {filtered.length === 0 ? <p style={{ color: C.gray }}>PDF履歴がありません。印刷・PDF出力モーダルからPDFを保存すると、ここに表示されます。</p> : (
          <table style={s.table}>
            <thead><tr>
              <th style={s.th}>種類</th><th style={s.th}>伝票番号</th><th style={s.th}>取引先</th>
              <th style={s.th}>ファイル名</th><th style={s.th}>作成日時</th><th style={s.th}>操作</th>
            </tr></thead>
            <tbody>
              {filtered.map(h => (
                <tr key={h.id}>
                  <td style={s.td}><span style={s.badge(h.type === "請求書" ? "blue" : h.type === "納品書" ? "green" : "gold")}>{h.type}</span></td>
                  <td style={s.td}>{h.docNo}</td>
                  <td style={s.td}>{h.clientName}</td>
                  <td style={s.td} title={h.filename}>{h.filename}</td>
                  <td style={s.td}>{h.createdAt?.toDate?.()?.toLocaleString("ja-JP") || ""}</td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 6 }}>
                      <a href={h.storageUrl} target="_blank" rel="noopener noreferrer" style={{ ...s.btn("primary"), textDecoration: "none", padding: "4px 12px", fontSize: 12 }}>表示</a>
                      <button style={{ ...s.btn("gold"), padding: "4px 12px", fontSize: 12 }} onClick={() => { const a = document.createElement("a"); a.href = h.storageUrl; a.download = h.filename; a.click(); }}>DL</button>
                      {isAdmin && <button style={{ ...s.btn("red"), padding: "4px 12px", fontSize: 12 }} onClick={() => deleteH(h)}>削除</button>}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ── Divisions ─────────────────────────────────────────────────────────────────
function DivisionsPage({ divisions, isAdmin }) {
  const empty = { name:"",prefix:"",address:"",tel:"",fax:"",registrationNo:"",bankName:"",bankBranch:"",bankType:"普通",bankNo:"",bankHolder:"" };
  const [form, setForm] = useState(empty);
  const [editing, setEditing] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const setF = (k,v) => setForm(f => ({...f,[k]:v}));
  const save = async () => {
    if (!form.name) return alert("事業部名を入力してください");
    if (!form.prefix) return alert("接頭辞を入力してください");
    if (!editing && divisions.some(d => d.prefix === form.prefix)) return alert(`接頭辞「${form.prefix}」は既に使われています`);
    if (editing && divisions.some(d => d.prefix === form.prefix && d.id !== editing.id)) return alert(`接頭辞「${form.prefix}」は既に使われています`);
    const data = { ...form, updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db,"divisions",editing.id),data);
    else { data.createdAt = serverTimestamp(); await addDoc(collection(db,"divisions"),data); }
    setShowForm(false); setEditing(null); setForm(empty);
  };
  const edit = (d) => { setForm(d); setEditing(d); setShowForm(true); };
  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db,"divisions",id)); };
  return (
    <div>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:20}}>
        <div style={s.pageTitle}>事業部管理</div>
        <button style={s.btn("primary")} onClick={()=>{setEditing(null);setForm(empty);setShowForm(true);}}>＋ 追加</button>
      </div>
      {showForm && (
        <div style={s.card}>
          <h3 style={{margin:"0 0 16px",color:C.navy}}>{editing?"事業部を編集":"事業部を追加"}</h3>
          <div style={{...s.row,marginBottom:12}}>
            <div style={s.col}><span style={s.label}>事業部名 *</span><input style={s.input} value={form.name} onChange={e=>setF("name",e.target.value)} placeholder="例：自社製品事業部" /></div>
            <div style={s.col}><span style={s.label}>接頭辞 *</span><input style={{...s.input,width:100}} value={form.prefix} onChange={e=>setF("prefix",e.target.value)} placeholder="例：A" /></div>
            <div style={s.col}><span style={s.label}>電話番号</span><input style={s.input} value={form.tel} onChange={e=>setF("tel",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>FAX</span><input style={s.input} value={form.fax} onChange={e=>setF("fax",e.target.value)} /></div>
          </div>
          <div style={{...s.row,marginBottom:12}}>
            <div style={s.col}><span style={s.label}>住所</span><input style={{...s.input,minWidth:400}} value={form.address} onChange={e=>setF("address",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>インボイス登録番号</span><input style={s.input} value={form.registrationNo} onChange={e=>setF("registrationNo",e.target.value)} placeholder="T6430001064243" /></div>
          </div>
          <h4 style={{margin:"12px 0 8px",color:C.navy,fontSize:14}}>振込先口座</h4>
          <div style={{...s.row,marginBottom:12}}>
            <div style={s.col}><span style={s.label}>銀行名</span><input style={s.input} value={form.bankName} onChange={e=>setF("bankName",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>支店名</span><input style={s.input} value={form.bankBranch} onChange={e=>setF("bankBranch",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>種別</span>
              <select style={s.select} value={form.bankType||"普通"} onChange={e=>setF("bankType",e.target.value)}>
                <option>普通</option><option>当座</option>
              </select>
            </div>
            <div style={s.col}><span style={s.label}>口座番号</span><input style={s.input} value={form.bankNo} onChange={e=>setF("bankNo",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>口座名義</span><input style={s.input} value={form.bankHolder} onChange={e=>setF("bankHolder",e.target.value)} /></div>
          </div>
          <div style={{...s.row,justifyContent:"flex-end",gap:8}}>
            <button style={s.btn("light")} onClick={()=>setShowForm(false)}>キャンセル</button>
            <button style={s.btn("primary")} onClick={save}>{editing?"更新":"保存"}</button>
          </div>
        </div>
      )}
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>事業部名</th><th style={s.th}>接頭辞</th><th style={s.th}>住所</th><th style={s.th}>電話</th><th style={s.th}>口座</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {divisions.map(d => (
              <tr key={d.id}>
                <td style={s.td}><strong>{d.name}</strong></td>
                <td style={s.td}><span style={s.badge("blue")}>{d.prefix}</span></td>
                <td style={s.td}>{d.address||"—"}</td>
                <td style={s.td}>{d.tel||"—"}</td>
                <td style={s.td}>{d.bankName ? `${d.bankName} ${d.bankBranch}` : "—"}</td>
                <td style={s.td}>
                  <div style={{display:"flex",gap:6}}>
                    <button style={{...s.btn("light"),padding:"4px 10px",fontSize:12}} onClick={()=>edit(d)}>編集</button>
                    {isAdmin && <button style={{...s.btn("red"),padding:"4px 10px",fontSize:12}} onClick={()=>del(d.id)}>削除</button>}
                  </div>
                </td>
              </tr>
            ))}
            {!divisions.length && <tr><td colSpan={6} style={{...s.td,textAlign:"center",color:C.gray}}>事業部が登録されていません</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Recurring Billings ────────────────────────────────────────────────────────
function RecurringPage({ clients, divisions, invoices, company, balances, isAdmin }) {
  const [items, setItems] = useState([]);
  const [showForm, setShowForm] = useState(false);
  const [editing, setEditing] = useState(null);
  const [printTarget, setPrintTarget] = useState(null);
  const [form, setForm] = useState({ clientId: "", divisionId: "", itemName: "", qty: 1, unit: "", price: 0, taxRate: 10, cycle: "monthly", billingDay: 0, sendMode: "auto", startMonth: today().slice(0,7), endMonth: "", enabled: true });
  const setF = (k, v) => setForm(f => ({ ...f, [k]: v }));

  useEffect(() => {
    const unsub = onSnapshot(collection(db, "recurringBillings"), snap => {
      setItems(snap.docs.map(d => ({ id: d.id, ...d.data() })));
    });
    return () => unsub();
  }, []);

  const save = async () => {
    if (!form.clientId) return alert("取引先を選択してください");
    if (!form.itemName) return alert("品名を入力してください");
    const data = { ...form, price: Number(form.price), qty: Number(form.qty), billingDay: Number(form.billingDay), updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db, "recurringBillings", editing.id), data);
    else { data.createdAt = serverTimestamp(); data.lastIssuedMonth = ""; await addDoc(collection(db, "recurringBillings"), data); }
    setShowForm(false); setEditing(null);
    setForm({ clientId: "", divisionId: "", itemName: "", qty: 1, unit: "", price: 0, taxRate: 10, cycle: "monthly", billingDay: 0, sendMode: "auto", startMonth: today().slice(0,7), endMonth: "", enabled: true });
  };
  const edit = (r) => { setForm(r); setEditing(r); setShowForm(true); };
  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db, "recurringBillings", id)); };
  const toggle = async (r) => { await updateDoc(doc(db, "recurringBillings", r.id), { enabled: !r.enabled }); };
  const cycleLabel = (c) => c === "quarterly" ? "四半期" : c === "bimonthly" ? "隔月" : "毎月";

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={s.pageTitle}>定期請求</div>
        <button style={s.btn("primary")} onClick={() => { setEditing(null); setForm({ clientId:"",divisionId:"",itemName:"",qty:1,unit:"",price:0,taxRate:10,cycle:"monthly",billingDay:0,sendMode:"auto",startMonth:today().slice(0,7),endMonth:"",enabled:true }); setShowForm(true); }}>＋ 追加</button>
      </div>
      {showForm && (
        <div style={s.card}>
          <h3 style={{ margin: "0 0 16px", color: C.navy }}>{editing ? "定期請求を編集" : "定期請求を追加"}</h3>
          <div style={{ ...s.row, marginBottom: 12 }}>
            <div style={s.col}><span style={s.label}>取引先 *</span>
              <select style={s.select} value={form.clientId} onChange={e => { const cid = e.target.value; setF("clientId", cid); const cl = clients.find(c=>c.id===cid); if(cl?.divisionId) setF("divisionId",cl.divisionId); }}>
                <option value="">選択してください</option>
                {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
            <div style={s.col}><span style={s.label}>事業部</span>
              <select style={s.select} value={form.divisionId||""} onChange={e => setF("divisionId",e.target.value)}>
                <option value="">指定なし</option>
                {divisions.map(d => <option key={d.id} value={d.id}>{d.name}</option>)}
              </select>
            </div>
          </div>
          <div style={{ ...s.row, marginBottom: 12 }}>
            <div style={s.col}><span style={s.label}>品名 *</span><input style={{ ...s.input, minWidth: 200 }} value={form.itemName} onChange={e => setF("itemName",e.target.value)} placeholder="顧問料、保守費用など" /></div>
            <div style={s.col}><span style={s.label}>数量</span><input style={{ ...s.input, width: 60 }} type="number" value={form.qty} onChange={e => setF("qty",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>単位</span><input style={{ ...s.input, width: 60 }} value={form.unit} onChange={e => setF("unit",e.target.value)} placeholder="式" /></div>
            <div style={s.col}><span style={s.label}>単価 *</span><input style={{ ...s.input, width: 120 }} type="number" value={form.price} onChange={e => setF("price",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>税率</span>
              <select style={{ ...s.select, width: 80 }} value={(() => { const r = form.taxRate !== undefined && form.taxRate !== null && form.taxRate !== "" ? Number(form.taxRate) : 10; return r === 10 || r === 8 || r === 0 ? String(r) : "custom"; })()} onChange={e => setF("taxRate", e.target.value === "custom" ? form.taxRate : Number(e.target.value))}>
                <option value="10">10%</option><option value="8">8%</option><option value="0">0%</option><option value="custom">自由</option>
              </select>
              {(() => { const r = form.taxRate !== undefined && form.taxRate !== null && form.taxRate !== "" ? Number(form.taxRate) : 10; return r !== 10 && r !== 8 && r !== 0 ? <input style={{ ...s.input, width: 50, marginLeft: 4 }} type="number" min={0} max={100} value={r} onChange={e => setF("taxRate", Number(e.target.value))} /> : null; })()}
            </div>
          </div>
          <div style={{ ...s.row, marginBottom: 12 }}>
            <div style={s.col}><span style={s.label}>サイクル</span>
              <select style={s.select} value={form.cycle} onChange={e => setF("cycle",e.target.value)}>
                <option value="monthly">毎月</option><option value="bimonthly">隔月</option><option value="quarterly">四半期</option>
              </select>
            </div>
            <div style={s.col}><span style={s.label}>請求日</span>
              <select style={s.select} value={form.billingDay} onChange={e => setF("billingDay",Number(e.target.value))}>
                <option value={0}>末日</option>
                {Array.from({length:28},(_,i)=>i+1).map(v => <option key={v} value={v}>{v}日</option>)}
              </select>
            </div>
            <div style={s.col}><span style={s.label}>送信設定</span>
              <select style={s.select} value={form.sendMode} onChange={e => setF("sendMode",e.target.value)}>
                <option value="auto">自動送信</option><option value="confirm">確認後送信</option><option value="manual">手動</option>
              </select>
            </div>
          </div>
          <div style={{ ...s.row, marginBottom: 12 }}>
            <div style={s.col}><span style={s.label}>開始年月 *</span><input style={s.input} type="month" value={form.startMonth} onChange={e => setF("startMonth",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>終了年月（空欄=無期限）</span><input style={s.input} type="month" value={form.endMonth||""} onChange={e => setF("endMonth",e.target.value)} /></div>
          </div>
          <div style={{ ...s.row, justifyContent: "flex-end", gap: 8 }}>
            <button style={s.btn("light")} onClick={() => setShowForm(false)}>キャンセル</button>
            <button style={s.btn("primary")} onClick={save}>{editing ? "更新" : "保存"}</button>
          </div>
        </div>
      )}
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>取引先</th><th style={s.th}>品名</th><th style={s.th}>金額</th><th style={s.th}>サイクル</th><th style={s.th}>請求日</th><th style={s.th}>送信</th><th style={s.th}>期間</th><th style={s.th}>最終発行</th><th style={s.th}>状態</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {items.map(r => {
              const cl = clients.find(c => c.id === r.clientId);
              const amt = Number(r.qty||1) * Number(r.price||0);
              return (
                <tr key={r.id} style={{ opacity: r.enabled ? 1 : 0.5 }}>
                  <td style={s.td}>{cl?.name || "—"}</td>
                  <td style={s.td}>{r.itemName}</td>
                  <td style={s.td}>¥{fmt(amt)}</td>
                  <td style={s.td}>{cycleLabel(r.cycle)}</td>
                  <td style={s.td}>{r.billingDay === 0 ? "末日" : `${r.billingDay}日`}</td>
                  <td style={s.td}><span style={s.badge(r.sendMode==="auto"?"green":r.sendMode==="confirm"?"gold":"gray")}>{r.sendMode==="auto"?"自動":r.sendMode==="confirm"?"確認":"手動"}</span></td>
                  <td style={s.td}><span style={{ fontSize: 12 }}>{r.startMonth}～{r.endMonth || "無期限"}</span></td>
                  <td style={s.td}>{r.lastIssuedMonth || "—"}</td>
                  <td style={s.td}><span style={s.badge(r.enabled?"green":"gray")}>{r.enabled?"有効":"停止"}</span></td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => {
                        const items = [{ name: r.itemName, qty: r.qty || 1, unit: r.unit || "", price: r.price || 0, taxRate: r.taxRate !== undefined ? r.taxRate : 10 }];
                        const { sub, tax, total } = totalFromItems(items);
                        const preview = { docNo: "（プレビュー）", clientId: r.clientId, divisionId: r.divisionId || "", date: today(), dueDate: nextMonthEnd(today()), billingType: "recurring", items, subtotal: sub, tax, total };
                        setPrintTarget({ invoice: preview });
                      }}>👁 プレビュー</button>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => edit(r)}>編集</button>
                      <button style={{ ...s.btn(r.enabled?"gold":"green"), padding: "4px 8px", fontSize: 12 }} onClick={() => toggle(r)}>{r.enabled?"停止":"有効化"}</button>
                      {isAdmin && <button style={{ ...s.btn("red"), padding: "4px 8px", fontSize: 12 }} onClick={() => del(r.id)}>削除</button>}
                    </div>
                  </td>
                </tr>
              );
            })}
            {!items.length && <tr><td colSpan={10} style={{ ...s.td, textAlign: "center", color: C.gray }}>定期請求はまだ登録されていません</td></tr>}
          </tbody>
        </table>
      </div>
      {printTarget && <PrintModeModal invoice={printTarget.invoice} delivery={null}
        clients={clients} company={company} balances={balances} divisions={divisions} onClose={() => setPrintTarget(null)} />}
    </div>
  );
}

// ── Pending Approvals ─────────────────────────────────────────────────────────
function PendingPage({ clients, company, divisions, balances, isAdmin, invoices }) {
  const [pendings, setPendings] = useState([]);
  const [sending, setSending] = useState(null);
  const [previewTarget, setPreviewTarget] = useState(null);

  useEffect(() => {
    const unsub = onSnapshot(query(collection(db, "pendingBillings"), orderBy("createdAt", "desc")), snap => {
      setPendings(snap.docs.map(d => ({ id: d.id, ...d.data() })));
    });
    return () => unsub();
  }, []);

  const approveInvoice = async (p) => {
    const cl = clients.find(c => c.id === p.clientId) || {};
    if (!confirm(`${cl.name || p.clientName} の請求書（¥${fmt(p.total)}）を発行しますか？`)) return;
    setSending(p.id);
    try {
      const inv = {
        docNo: genDocNo("INV", invoices), clientId: p.clientId, date: today(),
        dueDate: nextMonthEnd(today()), billingType: p.billingType || "immediate",
        items: p.items, subtotal: p.subtotal, tax: p.tax, total: p.total,
        status: "unpaid", createdAt: serverTimestamp(),
      };
      if (p.billingType === "closing") {
        inv.closingDay = p.closingDay;
        inv.closingPeriod = p.closingPeriod;
        inv.deliveryRefs = p.deliveryDocNos || [];
        inv.deliveryRefItems = typeof p.deliveryRefItems === "string" ? p.deliveryRefItems : JSON.stringify(p.deliveryRefItems || []);
      } else {
        inv.deliveryRef = p.deliveryDocNo || "";
        inv.deliveryRefs = p.deliveryDocNo ? [p.deliveryDocNo] : [];
      }
      // scheduledSendDateは使用しない（承認後は即送信）
      const invRef = await addDoc(collection(db, "invoices"), inv);
      // 納品書をinvoiced状態に
      const delIds = p.deliveryIds ? (Array.isArray(p.deliveryIds) ? p.deliveryIds : [p.deliveryIds]) : (p.deliveryId ? [p.deliveryId] : []);
      for (const did of delIds) {
        await updateDoc(doc(db, "deliveries", did), { status: "invoiced", invoiceId: invRef.id });
      }
      // 残高更新
      const bal = balances[p.clientId] || {};
      await setDoc(doc(db, "clientBalances", p.clientId), {
        clientId: p.clientId, prevBalance: bal.currentBalance || 0,
        currentBalance: (bal.currentBalance || 0) + p.total,
        paidAmount: bal.paidAmount || 0, updatedAt: serverTimestamp(),
      });
      await updateDoc(doc(db, "pendingBillings", p.id), { status: "approved", approvedAt: serverTimestamp(), invoiceDocNo: inv.docNo });

      // 承認後メール自動送信
      const email = cl.email;
      if (email && cl.sendMode !== "manual") {
        try {
          const co = company || {};
          let coInfo = co;
          if (p.divisionId) {
            const div = divisions.find(d => d.id === p.divisionId);
            if (div) coInfo = { ...co, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) };
          }
          const res = await fetch("/api/send-invoice", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              to: email,
              subject: `【請求書】${inv.docNo} ${coInfo.name || ""}`,
              html: `<div style="font-family:sans-serif;color:#333;">
                <p>${cl.name || ""} 御中</p>
                <p>いつもお世話になっております。<br>${coInfo.name || ""}です。</p>
                <p>請求書（${inv.docNo}）をお送りいたします。</p>
                <p>金額：&yen;${fmt(p.total)}</p>
                <p>ご確認のほど、よろしくお願いいたします。</p>
                <hr style="border:none;border-top:1px solid #ddd;margin:20px 0">
                <p style="font-size:12px;color:#888">${coInfo.name || ""}<br>${coInfo.address || ""}<br>TEL ${coInfo.tel || ""}</p>
              </div>`,
            }),
          });
          if (res.ok) {
            await addDoc(collection(db, "sendHistory"), {
              docNo: inv.docNo, invoiceId: invRef.id,
              clientId: p.clientId, clientName: cl.name || "",
              email, method: "auto", memo: "承認後自動送信",
              amount: p.total, sentAt: serverTimestamp(), sentBy: "approval",
            });
            await updateDoc(doc(db, "invoices", invRef.id), { sentStatus: "sent", lastSentAt: serverTimestamp() });
            alert(`請求書 ${inv.docNo} を発行し、${cl.name}（${email}）にメール送信しました`);
          } else {
            alert(`請求書 ${inv.docNo} を発行しました（メール送信に失敗しました。手動で送信してください）`);
          }
        } catch (e2) {
          console.warn("承認後メール送信エラー:", e2.message);
          alert(`請求書 ${inv.docNo} を発行しました（メール送信エラー: ${e2.message}）`);
        }
      } else {
        alert(`請求書 ${inv.docNo} を発行しました${!email ? "（メールアドレス未設定）" : cl.sendMode === "manual" ? "（手動送信）" : ""}`);
      }
    } catch (e) { alert("エラー: " + e.message); }
    setSending(null);
  };

  const approve = async (p, sendMethod) => {
    if (p.type === "invoice") return approveInvoice(p);
    const cl = clients.find(c => c.id === p.clientId) || {};
    const email = p.email || cl.email;
    if (!email) return alert("取引先のメールアドレスが設定されていません");
    const methodLabel = sendMethod === "stripe" ? "Stripe請求書" : "メール";
    if (!confirm(`${cl.name || p.clientName}（${email}）に${methodLabel}で送信します。よろしいですか？${sendMethod === "stripe" ? "\n\n※ Stripe手数料（3.6%）が発生します。" : ""}`)) return;
    setSending(p.id);
    try {
      if (sendMethod === "stripe") {
        // Stripe送信
        const items = p.invoiceItems || [{ name: `${p.invoiceDocNo} 未入金分`, qty: 1, unitAmount: p.total || 0 }];
        const res = await fetch("/api/stripe-invoice", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            stripeSecretKey: company.stripeSecretKey,
            clientName: cl.name, email,
            amount: p.total, currency: "jpy",
            description: `未入金再請求（${p.invoiceDocNo}）`,
            invoiceItems: items, docNos: p.invoiceDocNo,
          }),
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || "Stripe送信エラー");
        await addDoc(collection(db, "sendHistory"), {
          docNo: p.invoiceDocNo, invoiceId: (p.invoiceIds || [p.invoiceId]).join(","),
          clientId: p.clientId, clientName: cl.name || "",
          email, method: "stripe",
          memo: `Stripe再請求（承認後送信） ¥${fmt(p.total)} / ${data.invoiceUrl}`,
          amount: p.total || 0, sentAt: serverTimestamp(), sentBy: "stripe",
        });
        alert(`Stripe請求書を送信しました！\n\n決済URL: ${data.invoiceUrl}`);
      } else {
        // メール送信
        if (p.type === "re-request-email" || p.type === "re-request-stripe") {
          // 再請求系（PDFリンク付き）
          const pdfLink = p.pdfUrl ? `<p style="margin:20px 0"><a href="${p.pdfUrl}" style="display:inline-block;padding:12px 24px;background:#1C2B4A;color:#fff;text-decoration:none;border-radius:6px;font-weight:bold">📄 請求書PDFをダウンロード</a></p>` : "";
          const msgHtml = (p.message || `${cl.name} 御中\n\nお支払い期日を過ぎておりますのでご確認をお願いいたします。\n\n対象: ${p.invoiceDocNo}\n金額: ¥${fmt(p.total)}`).replace(/\n/g, "<br/>");
          const htmlBody = msgHtml + pdfLink;
          await fetch("/api/send-invoice", {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ to: email, subject: `【お支払いのお願い】${p.invoiceDocNo}`, html: htmlBody }),
          });
          await addDoc(collection(db, "sendHistory"), {
            docNo: p.invoiceDocNo, invoiceId: (p.invoiceIds || [p.invoiceId]).join(","),
            clientId: p.clientId, clientName: cl.name || "",
            email, method: "mail",
            memo: `再請求メール（承認後送信） ¥${fmt(p.total)}`,
            amount: p.total || 0, sentAt: serverTimestamp(), sentBy: "re-request",
          });
        } else {
          // 定期請求/締日請求
          const div = divisions?.find(d => d.id === p.divisionId);
          const co = div ? { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) } : company;
          await fetch("/api/send-invoice", {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              to: email,
              subject: `【請求書】${p.invoiceDocNo} ${co.name || ""}`,
              html: `<div style="font-family:sans-serif;color:#333;">
                <p>${cl.name} 御中</p>
                <p>いつもお世話になっております。<br>${co.name || ""}です。</p>
                <p>請求書（${p.invoiceDocNo}）をお送りいたします。</p>
                <p>金額：¥${fmt(p.total)}</p>
                <p>ご確認のほど、よろしくお願いいたします。</p>
                <hr style="border:none;border-top:1px solid #ddd;margin:20px 0">
                <p style="font-size:12px;color:#888">${co.name || ""}<br>${co.address || ""}<br>TEL ${co.tel || ""}</p>
              </div>`,
            }),
          });
          await addDoc(collection(db, "sendHistory"), {
            docNo: p.invoiceDocNo, invoiceId: p.invoiceId,
            clientId: p.clientId, clientName: cl.name || "",
            email, method: "auto", memo: "承認後自動送信",
            amount: p.total || 0, sentAt: serverTimestamp(), sentBy: "auto",
          });
          if (p.invoiceId) await updateDoc(doc(db, "invoices", p.invoiceId), { sentStatus: "sent", lastSentAt: serverTimestamp() });
        }
        alert("メールを送信しました");
      }
      await updateDoc(doc(db, "pendingBillings", p.id), { status: "approved", approvedAt: serverTimestamp(), approvedMethod: sendMethod });
    } catch (e) { alert("送信エラー: " + e.message); }
    setSending(null);
  };
  const reject = async (p) => {
    if (!confirm("この承認待ちを却下しますか？")) return;
    await updateDoc(doc(db, "pendingBillings", p.id), { status: "rejected", rejectedAt: serverTimestamp() });
    // 納品書をunissuedに戻す
    const delIds = p.deliveryIds ? (Array.isArray(p.deliveryIds) ? p.deliveryIds : [p.deliveryIds]) : (p.deliveryId ? [p.deliveryId] : []);
    for (const did of delIds) {
      await updateDoc(doc(db, "deliveries", did), { status: "unissued", invoiceId: "" });
    }
  };

  const pending = pendings.filter(p => p.status === "pending");
  const done = pendings.filter(p => p.status !== "pending");

  return (
    <div>
      <div style={s.pageTitle}>承認待ち</div>
      {pending.length > 0 ? (
        <div style={s.card}>
          <div style={{ fontSize: 14, fontWeight: 700, color: C.navy, marginBottom: 12 }}>未承認（{pending.length}件）</div>
          <table style={s.table}>
            <thead><tr><th style={s.th}>請求番号</th><th style={s.th}>取引先</th><th style={s.th}>金額</th><th style={s.th}>種別</th><th style={s.th}>作成日</th><th style={s.th}>操作</th></tr></thead>
            <tbody>
              {pending.map(p => {
                const cl = clients.find(c => c.id === p.clientId);
                return (
                  <tr key={p.id}>
                    <td style={s.td}>{p.invoiceDocNo || (p.type === "invoice" ? <span style={{ color: C.gray }}>（承認後発行）</span> : "—")}</td>
                    <td style={s.td}>{cl?.name || "—"}{cl?.email ? "" : <span style={{ fontSize: 11, color: C.red, marginLeft: 6 }}>※メール未設定</span>}</td>
                    <td style={s.td}>¥{fmt(p.total)}</td>
                    <td style={s.td}><span style={s.badge(p.type==="invoice"?"navy":p.type==="recurring"?"blue":p.type==="re-request-email"?"red":p.type==="re-request-stripe"?"red":"gold")}>{p.type==="invoice"?"請求書発行":p.type==="recurring"?"定期":p.type==="re-request-email"?"✉再請求":p.type==="re-request-stripe"?"💳Stripe":"締日"}</span></td>
                    <td style={s.td}>{p.createdAt?.toDate?.()?.toLocaleDateString?.() || "—"}</td>
                    <td style={s.td}>
                      {isAdmin ? (
                        <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                          {p.type === "invoice" ? (
                            <button style={{ ...s.btn("green"), padding: "4px 10px", fontSize: 12 }} onClick={() => approveInvoice(p)} disabled={!!sending}>{sending===p.id ? "発行中..." : "承認・発行"}</button>
                          ) : <>
                            <button style={{ ...s.btn("green"), padding: "4px 10px", fontSize: 12 }} onClick={() => approve(p, "email")} disabled={!!sending}>{sending===p.id ? "送信中..." : "✉ メール送信"}</button>
                            {company?.stripeSecretKey && <button style={{ ...s.btn("primary"), padding: "4px 10px", fontSize: 12, background: "#635BFF" }} onClick={() => approve(p, "stripe")} disabled={!!sending}>{sending===p.id ? "処理中..." : "💳 Stripe送信"}</button>}
                          </>}
                          <button style={{ ...s.btn("red"), padding: "4px 10px", fontSize: 12 }} onClick={() => reject(p)}>却下</button>
                          <button style={{ ...s.btn("light"), padding: "4px 10px", fontSize: 12 }} onClick={() => setPreviewTarget(p)}>👁 内容</button>
                        </div>
                      ) : (
                        <span style={{ fontSize: 12, color: C.gray }}>管理者のみ</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <div style={{ ...s.card, color: C.gray, textAlign: "center" }}>承認待ちの請求はありません</div>
      )}
      {done.length > 0 && (
        <div style={s.card}>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.gray, marginBottom: 12 }}>処理済み（直近）</div>
          <table style={s.table}>
            <thead><tr><th style={s.th}>請求番号</th><th style={s.th}>取引先</th><th style={s.th}>金額</th><th style={s.th}>種別</th><th style={s.th}>結果</th></tr></thead>
            <tbody>
              {done.slice(0, 20).map(p => {
                const cl = clients.find(c => c.id === p.clientId);
                return (
                  <tr key={p.id}>
                    <td style={s.td}>{p.invoiceDocNo || (p.type === "invoice" ? <span style={{ color: C.gray }}>（承認後発行）</span> : "—")}</td>
                    <td style={s.td}>{cl?.name || "—"}</td>
                    <td style={s.td}>¥{fmt(p.total)}</td>
                    <td style={s.td}><span style={s.badge(p.type==="invoice"?"navy":p.type==="recurring"?"blue":p.type==="re-request-email"?"red":p.type==="re-request-stripe"?"red":"gold")}>{p.type==="invoice"?"請求書発行":p.type==="recurring"?"定期":p.type==="re-request-email"?"✉再請求":p.type==="re-request-stripe"?"💳Stripe":"締日"}</span></td>
                    <td style={s.td}><span style={s.badge(p.status==="approved"?"green":"red")}>{p.status==="approved"?"承認済":"却下"}</span></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {previewTarget && (() => {
        const p = previewTarget;
        const cl = clients.find(c => c.id === p.clientId) || {};
        const div = divisions?.find(d => d.id === p.divisionId);
        const co = div ? { ...company, ...Object.fromEntries(Object.entries(div).filter(([,v]) => v)) } : company;
        const items = p.items || [];
        return (
          <div style={s.modal} onClick={() => setPreviewTarget(null)}>
            <div style={{ ...s.modalBox, maxWidth: 640 }} onClick={e => e.stopPropagation()}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
                <h3 style={{ margin: 0, color: C.navy }}>承認待ち内容確認</h3>
                <button style={s.btn("light")} onClick={() => setPreviewTarget(null)}>✕</button>
              </div>
              <div style={{ background: C.pale, padding: 16, borderRadius: 8, marginBottom: 16 }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
                  <div>
                    <div style={{ fontSize: 16, fontWeight: 700 }}>{cl.name || p.clientName}</div>
                    <div style={{ fontSize: 12, color: C.gray, marginTop: 4 }}>{cl.email || p.email || "メール未設定"}</div>
                  </div>
                  <div style={{ textAlign: "right" }}>
                    <span style={s.badge(p.type==="invoice"?"navy":p.type==="re-request-email"?"red":"gold")}>{p.type==="invoice"?"請求書発行":p.type==="re-request-email"?"✉再請求":p.type==="re-request-stripe"?"💳Stripe":"締日/定期"}</span>
                  </div>
                </div>
                {p.billingType === "closing" && p.closingPeriod && (
                  <div style={{ fontSize: 12, color: C.navy }}>対象期間: {p.closingPeriod.start} ～ {p.closingPeriod.end}</div>
                )}
                {p.scheduledSendDate && <div style={{ fontSize: 12, color: C.navy, marginTop: 4 }}>送信予定日: {p.scheduledSendDate}</div>}
              </div>
              {items.length > 0 && (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>明細</div>
                  <table style={s.table}>
                    <thead><tr><th style={s.th}>品名</th><th style={s.th}>数量</th><th style={s.th}>単価</th><th style={{ ...s.th, textAlign: "right" }}>金額</th></tr></thead>
                    <tbody>
                      {items.map((item, i) => (
                        <tr key={i}>
                          <td style={s.td}>{item.name}</td>
                          <td style={s.td}>{item.qty} {item.unit}</td>
                          <td style={s.td}>¥{fmt(item.price)}</td>
                          <td style={{ ...s.td, textAlign: "right" }}>¥{fmt((item.qty || 0) * (item.price || 0))}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {p.message && (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>メール本文</div>
                  <div style={{ background: "#f8f9fa", padding: 12, borderRadius: 8, fontSize: 13, whiteSpace: "pre-wrap", lineHeight: 1.6 }}>{p.message}</div>
                </div>
              )}
              <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 12, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div style={{ fontSize: 20, fontWeight: 700, color: C.navy }}>合計: ¥{fmt(p.total)}</div>
                <div style={{ display: "flex", gap: 8 }}>
                  {isAdmin && <>
                    {p.status === "pending" && <>
                      {p.type === "invoice" ? (
                        <button style={{ ...s.btn("green"), fontSize: 13 }} onClick={() => { setPreviewTarget(null); approveInvoice(p); }} disabled={!!sending}>承認・発行</button>
                      ) : <>
                        <button style={{ ...s.btn("green"), fontSize: 13 }} onClick={() => { setPreviewTarget(null); approve(p, "email"); }} disabled={!!sending}>✉ メール送信</button>
                        {company?.stripeSecretKey && <button style={{ ...s.btn("primary"), fontSize: 13, background: "#635BFF" }} onClick={() => { setPreviewTarget(null); approve(p, "stripe"); }} disabled={!!sending}>💳 Stripe</button>}
                      </>}
                      <button style={{ ...s.btn("red"), fontSize: 13 }} onClick={() => { setPreviewTarget(null); reject(p); }}>却下</button>
                    </>}
                  </>}
                  <button style={s.btn("light")} onClick={() => setPreviewTarget(null)}>閉じる</button>
                </div>
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}

// ── Settings ──────────────────────────────────────────────────────────────────
function SettingsPage({ company, setCompany, isAdmin, currentUser }) {
  const [form, setForm] = useState(company || {});
  const setF = (k,v) => setForm(f => ({...f,[k]:v}));
  const [syncing, setSyncing] = useState(false);
  const [syncMsg, setSyncMsg] = useState("");
  const runInitialSync = async (source) => {
    if (syncing) return;
    setSyncing(true); setSyncMsg("");
    const endpoints = { rakuten: "/api/rakuten-sync", amazon: "/api/amazon-sync", colorme: "/api/colorme-sync" };
    const labels = { rakuten: "楽天", amazon: "Amazon", colorme: "カラーミー" };
    const endpoint = endpoints[source];
    const label = labels[source];
    try {
      const now = new Date();
      const months = [];
      for (let i = 11; i >= 0; i--) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        const y = d.getFullYear(), m = d.getMonth() + 1;
        const pad = (n) => String(n).padStart(2, "0");
        months.push({ start: `${y}-${pad(m)}-01`, end: `${y}-${pad(m)}-${pad(new Date(y, m, 0).getDate())}`, label: `${y}年${m}月` });
      }
      let totalOrders = 0, totalDays = 0, errorCount = 0;
      for (let i = 0; i < months.length; i++) {
        const mo = months[i];
        setSyncMsg(`${label} ${mo.label} を同期中... (${i + 1}/${months.length})`);
        try {
          const res = await fetch(endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ mode: "range", start: mo.start, end: mo.end }) });
          const data = await res.json();
          if (data.ok) { totalOrders += data.totalOrders || 0; totalDays += data.totalDays || 0; errorCount = 0; }
          else { errorCount++; if (errorCount >= 3) { alert(`${label}同期エラーが連続しました。中断します。\nエラー: ${data.error || data.message || "不明"}`); break; } }
        } catch (e) { errorCount++; if (errorCount >= 3) { alert(`${label}同期エラーが連続しました。中断します。\nエラー: ${e.message}`); break; } }
        if (i < months.length - 1) await new Promise(r => setTimeout(r, 5000));
      }
      alert(`${label}初期同期完了: ${totalOrders}件、${totalDays}日分`);
    } catch (e) { alert("同期エラー: " + e.message); }
    setSyncing(false); setSyncMsg("");
  };
  const save = async () => {
    // reRequestApprovalが未設定の場合は明示的にtrueを設定
    const saveData = { ...form };
    if (saveData.reRequestApproval === undefined) saveData.reRequestApproval = true;
    const existing = await getDocs(collection(db,"settings"));
    if (existing.empty) await addDoc(collection(db,"settings"),{...saveData,updatedAt:serverTimestamp()});
    else await updateDoc(doc(db,"settings",existing.docs[0].id),{...saveData,updatedAt:serverTimestamp()});
    setCompany(saveData); alert("保存しました");
  };
  return (
    <div>
      <div style={s.pageTitle}>設定</div>
      <div style={s.card}>
        <h3 style={{margin:"0 0 16px",color:C.navy}}>自社情報</h3>
        <div style={{...s.row,flexDirection:"column",gap:12}}>
          <div style={s.row}>
            <div style={s.col}><span style={s.label}>会社名</span><input style={{...s.input,minWidth:280}} value={form.name||""} onChange={e=>setF("name",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>電話番号</span><input style={s.input} value={form.tel||""} onChange={e=>setF("tel",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>FAX</span><input style={s.input} value={form.fax||""} onChange={e=>setF("fax",e.target.value)} /></div>
          </div>
          <div style={s.col}><span style={s.label}>住所</span><input style={{...s.input,minWidth:400}} value={form.address||""} onChange={e=>setF("address",e.target.value)} /></div>
          <div style={s.col}><span style={s.label}>インボイス登録番号</span><input style={{...s.input,minWidth:220}} value={form.registrationNo||""} onChange={e=>setF("registrationNo",e.target.value)} placeholder="T6430001064243" /></div>
          <div style={s.col}><span style={s.label}>デフォルト税率</span>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <select style={{...s.select,width:100}} value={form.defaultTaxRate!==undefined&&[10,8,0].includes(form.defaultTaxRate)?String(form.defaultTaxRate):"custom"} onChange={e=>{if(e.target.value==="custom")setF("defaultTaxRate",5);else setF("defaultTaxRate",Number(e.target.value));}}>
                <option value="10">10%</option><option value="8">8%</option><option value="0">0%</option><option value="custom">自由設定</option>
              </select>
              {form.defaultTaxRate!==undefined&&![10,8,0].includes(form.defaultTaxRate)&&(
                <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <input style={{...s.input,width:60,textAlign:"right"}} type="number" min="0" max="100" step="0.1" value={form.defaultTaxRate} onChange={e=>setF("defaultTaxRate",Number(e.target.value))} />
                  <span style={{ fontSize: 13 }}>%</span>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
      <div style={s.card}>
        <h3 style={{margin:"0 0 16px",color:C.navy}}>振込先口座</h3>
        <div style={s.row}>
          <div style={s.col}><span style={s.label}>銀行名</span><input style={s.input} value={form.bankName||""} onChange={e=>setF("bankName",e.target.value)} /></div>
          <div style={s.col}><span style={s.label}>支店名</span><input style={s.input} value={form.bankBranch||""} onChange={e=>setF("bankBranch",e.target.value)} /></div>
          <div style={s.col}><span style={s.label}>種別</span>
            <select style={s.select} value={form.bankType||"普通"} onChange={e=>setF("bankType",e.target.value)}>
              <option>普通</option><option>当座</option>
            </select>
          </div>
          <div style={s.col}><span style={s.label}>口座番号</span><input style={s.input} value={form.bankNo||""} onChange={e=>setF("bankNo",e.target.value)} /></div>
          <div style={s.col}><span style={s.label}>口座名義</span><input style={s.input} value={form.bankHolder||""} onChange={e=>setF("bankHolder",e.target.value)} /></div>
        </div>
      </div>
      <div style={s.card}>
        <h3 style={{margin:"0 0 16px",color:C.navy}}>外部売上連携</h3>
        <p style={{fontSize:12,color:C.gray,marginBottom:12}}>API認証情報を設定すると、売上管理ページで外部チャネルの売上を自動同期できます。</p>
        <div style={{...s.row,flexDirection:"column",gap:12}}>
          <div>
            <div style={{fontWeight:600,fontSize:14,color:C.navy,marginBottom:8}}>楽天RMS</div>
            <div style={s.row}>
              <div style={s.col}><span style={s.label}>serviceSecret</span><input style={{...s.input,minWidth:220}} type="password" value={form.rakutenServiceSecret||""} onChange={e=>setF("rakutenServiceSecret",e.target.value)} placeholder="未設定" /></div>
              <div style={s.col}><span style={s.label}>licenseKey</span><input style={{...s.input,minWidth:220}} type="password" value={form.rakutenLicenseKey||""} onChange={e=>setF("rakutenLicenseKey",e.target.value)} placeholder="未設定" /></div>
            </div>
            <button style={{...s.btn("light"),marginTop:8,fontSize:12}} onClick={() => { if(confirm("楽天の過去1年分を取得します。数分かかります。"))runInitialSync("rakuten"); }} disabled={syncing}>{syncMsg && syncMsg.includes("楽天") ? syncMsg : "楽天 初期同期（過去1年）"}</button>
          </div>
          <div>
            <div style={{fontWeight:600,fontSize:14,color:C.navy,marginBottom:8}}>Amazon SP-API</div>
            <div style={s.row}>
              <div style={s.col}><span style={s.label}>Client ID (LWA)</span><input style={{...s.input,minWidth:220}} value={form.amazonClientId||""} onChange={e=>setF("amazonClientId",e.target.value)} placeholder="未設定" /></div>
              <div style={s.col}><span style={s.label}>Client Secret (LWA)</span><input style={{...s.input,minWidth:220}} type="password" value={form.amazonClientSecret||""} onChange={e=>setF("amazonClientSecret",e.target.value)} placeholder="未設定" /></div>
              <div style={s.col}><span style={s.label}>Refresh Token</span><input style={{...s.input,minWidth:220}} type="password" value={form.amazonRefreshToken||""} onChange={e=>setF("amazonRefreshToken",e.target.value)} placeholder="未設定" /></div>
            </div>
            <button style={{...s.btn("light"),marginTop:8,fontSize:12}} onClick={() => { if(confirm("Amazonの過去1年分を取得します。"))runInitialSync("amazon"); }} disabled={syncing}>{syncMsg && syncMsg.includes("Amazon") ? syncMsg : "Amazon 初期同期（過去1年）"}</button>
          </div>
          <div>
            <div style={{fontWeight:600,fontSize:14,color:C.navy,marginBottom:8}}>カラーミーショップ</div>
            <div style={s.row}>
              <div style={s.col}><span style={s.label}>Client ID</span><input style={{...s.input,minWidth:220}} value={form.colormeClientId||""} onChange={e=>setF("colormeClientId",e.target.value)} placeholder="未設定" /></div>
              <div style={s.col}><span style={s.label}>Client Secret</span><input style={{...s.input,minWidth:220}} type="password" value={form.colormeClientSecret||""} onChange={e=>setF("colormeClientSecret",e.target.value)} placeholder="未設定" /></div>
            </div>
            <div style={{background:"#FFF5F9",border:"1px solid #E95295",borderRadius:6,padding:"8px 12px",marginTop:8,fontSize:12,color:"#333"}}>
              カラーミー開発者設定の「リダイレクトURL」に以下を登録してください：<br/><code style={{background:"#fff",padding:"2px 6px",borderRadius:3,userSelect:"all"}}>{location.origin}/api/colorme-callback</code><br/>※ Client ID・Client Secretを入力し、<b>保存してから</b>認証ボタンを押してください
            </div>
            <div style={{display:"flex",gap:8,alignItems:"center",marginTop:8}}>
              <button style={{...s.btn("primary"),background:"#E95295",fontSize:12}} onClick={()=>{if(!form.colormeClientId){alert("Client IDとClient Secretを入力し、保存してから認証ボタンを押してください");return;}window.open(`https://api.shop-pro.jp/oauth/authorize?client_id=${form.colormeClientId}&redirect_uri=${encodeURIComponent(location.origin+"/api/colorme-callback")}&response_type=code&scope=read_sales`,"_blank","width=600,height=700")}}>カラーミー認証</button>
              {form.colormeAccessToken && <span style={{fontSize:12,color:C.green}}>認証済み</span>}
            </div>
            <button style={{...s.btn("light"),marginTop:8,fontSize:12}} onClick={() => { if(confirm("カラーミーの過去1年分を取得します。"))runInitialSync("colorme"); }} disabled={syncing}>{syncMsg && syncMsg.includes("カラーミー") ? syncMsg : "カラーミー 初期同期（過去1年）"}</button>
          </div>
        </div>
      </div>
      <div style={s.card}>
        <h3 style={{margin:"0 0 16px",color:C.navy}}>承認設定</h3>
        <div style={{ ...s.col, gap: 12 }}>
          <div style={s.col}>
            <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
              <input type="checkbox" checked={form.invoiceApproval === true} onChange={e => setF("invoiceApproval", e.target.checked)} />
              <span style={{ fontSize: 13 }}>請求書発行時に承認を必要とする</span>
            </label>
            <div style={{ fontSize: 11, color: C.gray, marginTop: 4 }}>ONにすると、手動の請求書発行が承認待ちに入ります</div>
          </div>
          <div style={s.col}>
            <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
              <input type="checkbox" checked={form.recurringApproval === true} onChange={e => setF("recurringApproval", e.target.checked)} />
              <span style={{ fontSize: 13 }}>定期請求の発行時に承認を必要とする</span>
            </label>
            <div style={{ fontSize: 11, color: C.gray, marginTop: 4 }}>ONにすると、cron自動発行の定期請求も承認待ちに入ります</div>
          </div>
          <div style={s.col}>
            <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
              <input type="checkbox" checked={form.reRequestApproval !== false} onChange={e => setF("reRequestApproval", e.target.checked)} />
              <span style={{ fontSize: 13 }}>再請求時に承認を必要とする</span>
            </label>
            <div style={{ fontSize: 11, color: C.gray, marginTop: 4 }}>OFFにすると、残高管理から直接メール/Stripe請求を送信します</div>
          </div>
        </div>
      </div>
      <div style={s.card}>
        <h3 style={{margin:"0 0 16px",color:C.navy}}>Stripe決済連携</h3>
        <p style={{fontSize:12,color:C.gray,marginBottom:12}}>Stripe APIキーを設定すると、未入金の取引先にオンライン決済リンク付きの再請求を送信できます。手数料は決済時のみ（国内カード3.6%）。</p>
        <div style={s.row}>
          <div style={s.col}><span style={s.label}>Stripe Secret Key</span><input style={{...s.input,minWidth:320}} type="password" value={form.stripeSecretKey||""} onChange={e=>setF("stripeSecretKey",e.target.value)} placeholder="sk_live_..." /></div>
        </div>
      </div>
      <button style={s.btn("primary")} onClick={save}>保存</button>
      {isAdmin && <UserManagement currentUser={currentUser} />}
    </div>
  );
}

// ── ユーザー管理 ──────────────────────────────────────────────────────────────
function UserManagement({ currentUser }) {
  const [users, setUsers] = useState([]);
  const [newEmail, setNewEmail] = useState("");
  const [newPass, setNewPass] = useState("");
  const [newName, setNewName] = useState("");
  const [newRole, setNewRole] = useState("staff");
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState("");

  const load = async () => {
    try {
      const res = await fetch("/api/manage-user", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: "list", callerUid: currentUser.uid }) });
      const data = await res.json();
      if (data.success) setUsers(data.users);
    } catch (e) { console.error(e); }
  };
  useEffect(() => { load(); }, []);

  const addUser = async () => {
    if (!newEmail || !newPass) { alert("メールアドレスとパスワードは必須です"); return; }
    if (newPass.length < 6) { alert("パスワードは6文字以上にしてください"); return; }
    setLoading(true); setMsg("");
    try {
      const res = await fetch("/api/manage-user", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: "create", email: newEmail, password: newPass, displayName: newName, role: newRole, callerUid: currentUser.uid }) });
      const data = await res.json();
      if (data.success) { setMsg("ユーザーを追加しました"); setNewEmail(""); setNewPass(""); setNewName(""); setNewRole("staff"); load(); }
      else setMsg("エラー: " + (data.error || "不明"));
    } catch (e) { setMsg("エラー: " + e.message); }
    setLoading(false);
  };

  const delUser = async (uid) => {
    if (!confirm("このユーザーを削除しますか？")) return;
    try {
      const res = await fetch("/api/manage-user", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: "delete", uid, callerUid: currentUser.uid }) });
      const data = await res.json();
      if (data.success) load();
      else alert("エラー: " + (data.error || "不明"));
    } catch (e) { alert("エラー: " + e.message); }
  };

  const changeRole = async (uid, role) => {
    try {
      const res = await fetch("/api/manage-user", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: "updateRole", uid, role, callerUid: currentUser.uid }) });
      const data = await res.json();
      if (data.success) load();
      else alert("エラー: " + (data.error || "不明"));
    } catch (e) { alert("エラー: " + e.message); }
  };

  return (
    <div style={{ ...({background:"white",borderRadius:12,padding:"24px 28px",marginBottom:20,border:`1px solid #e8e2da`}), marginTop: 20 }}>
      <h3 style={{ margin: "0 0 16px", color: C.navy }}>ユーザー管理</h3>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, marginBottom: 16 }}>
        <thead><tr>
          <th style={s.th}>メール</th><th style={s.th}>名前</th><th style={s.th}>権限</th><th style={s.th}>操作</th>
        </tr></thead>
        <tbody>
          {users.map(u => (
            <tr key={u.uid}>
              <td style={s.td}>{u.email}{u.uid === currentUser.uid ? " (自分)" : ""}</td>
              <td style={s.td}>{u.displayName || "—"}</td>
              <td style={s.td}>
                <select style={{ ...({border:"1px solid #ccc",borderRadius:6,padding:"4px 8px",fontSize:13}), width: 100 }} value={u.role || "staff"} onChange={e => changeRole(u.uid, e.target.value)} disabled={u.uid === currentUser.uid}>
                  <option value="admin">管理者</option>
                  <option value="staff">スタッフ</option>
                </select>
              </td>
              <td style={s.td}>
                {u.uid !== currentUser.uid && (
                  <button style={{ ...({background:C.red,color:"white",border:"none",borderRadius:6,padding:"4px 10px",fontSize:12,cursor:"pointer"}) }} onClick={() => delUser(u.uid)}>削除</button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ borderTop: `1px solid ${C.light}`, paddingTop: 16 }}>
        <div style={{ fontWeight: 600, fontSize: 13, color: C.navy, marginBottom: 10 }}>ユーザー追加</div>
        <div style={{ display: "flex", gap: 8, alignItems: "flex-end", flexWrap: "wrap" }}>
          <div><span style={{ fontSize: 11, color: C.gray, display: "block", marginBottom: 4 }}>メール *</span><input style={{ ...({border:"1px solid #ccc",borderRadius:6,padding:"6px 10px",fontSize:13}), width: 200 }} type="email" value={newEmail} onChange={e => setNewEmail(e.target.value)} placeholder="user@example.com" /></div>
          <div><span style={{ fontSize: 11, color: C.gray, display: "block", marginBottom: 4 }}>パスワード *</span><input style={{ ...({border:"1px solid #ccc",borderRadius:6,padding:"6px 10px",fontSize:13}), width: 140 }} type="password" value={newPass} onChange={e => setNewPass(e.target.value)} placeholder="6文字以上" /></div>
          <div><span style={{ fontSize: 11, color: C.gray, display: "block", marginBottom: 4 }}>名前</span><input style={{ ...({border:"1px solid #ccc",borderRadius:6,padding:"6px 10px",fontSize:13}), width: 120 }} value={newName} onChange={e => setNewName(e.target.value)} placeholder="任意" /></div>
          <div><span style={{ fontSize: 11, color: C.gray, display: "block", marginBottom: 4 }}>権限</span>
            <select style={{ ...({border:"1px solid #ccc",borderRadius:6,padding:"6px 10px",fontSize:13}), width: 100 }} value={newRole} onChange={e => setNewRole(e.target.value)}>
              <option value="staff">スタッフ</option>
              <option value="admin">管理者</option>
            </select>
          </div>
          <button style={{ background: C.navy, color: "white", border: "none", borderRadius: 6, padding: "8px 16px", fontSize: 13, cursor: "pointer" }} onClick={addUser} disabled={loading}>{loading ? "追加中..." : "追加"}</button>
        </div>
        {msg && <div style={{ marginTop: 8, fontSize: 12, color: msg.includes("エラー") ? C.red : C.green }}>{msg}</div>}
      </div>
    </div>
  );
}

// ── Login ─────────────────────────────────────────────────────────────────────
function LoginPage({ onLogin }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const handleLogin = async (e) => {
    e.preventDefault();
    setError(""); setLoading(true);
    try {
      await signInWithEmailAndPassword(auth, email, password);
      onLogin?.();
    } catch (err) {
      const msgs = { "auth/invalid-credential": "メールアドレスまたはパスワードが正しくありません", "auth/too-many-requests": "ログイン試行回数が多すぎます。しばらくしてからお試しください", "auth/user-not-found": "アカウントが見つかりません", "auth/wrong-password": "パスワードが正しくありません" };
      setError(msgs[err.code] || "ログインエラー: " + err.message);
    }
    setLoading(false);
  };
  return (
    <div style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", background: `linear-gradient(135deg, ${C.navy} 0%, #2a3f6a 100%)` }}>
      <form onSubmit={handleLogin} style={{ background: "white", borderRadius: 16, padding: "48px 40px", width: 380, boxShadow: "0 8px 32px rgba(0,0,0,0.2)" }}>
        <div style={{ textAlign: "center", marginBottom: 32 }}>
          <div style={{ fontSize: 32, marginBottom: 8 }}>🧾</div>
          <div style={{ fontFamily: "'Shippori Mincho',serif", fontSize: 22, fontWeight: 600, color: C.navy }}>請求管理システム</div>
          <div style={{ fontSize: 12, color: C.gray, marginTop: 4 }}>ログインしてください</div>
        </div>
        {error && <div style={{ background: "#f8d7da", color: C.red, padding: "10px 14px", borderRadius: 8, fontSize: 13, marginBottom: 16 }}>{error}</div>}
        <div style={{ marginBottom: 16 }}>
          <label style={{ fontSize: 12, fontWeight: 500, color: C.navy, display: "block", marginBottom: 6 }}>メールアドレス</label>
          <input type="email" value={email} onChange={e => setEmail(e.target.value)} required style={{ ...s.input, width: "100%", padding: "10px 14px", fontSize: 14 }} placeholder="user@example.com" />
        </div>
        <div style={{ marginBottom: 24 }}>
          <label style={{ fontSize: 12, fontWeight: 500, color: C.navy, display: "block", marginBottom: 6 }}>パスワード</label>
          <input type="password" value={password} onChange={e => setPassword(e.target.value)} required style={{ ...s.input, width: "100%", padding: "10px 14px", fontSize: 14 }} placeholder="••••••••" />
        </div>
        <button type="submit" disabled={loading} style={{ ...s.btn("primary"), width: "100%", padding: "12px", fontSize: 15, fontWeight: 600 }}>
          {loading ? "ログイン中..." : "ログイン"}
        </button>
      </form>
    </div>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────
export default function App() {
  const [user, setUser] = useState(undefined); // undefined=checking, null=not logged in, object=logged in
  const [userRole, setUserRole] = useState(null); // "admin" | "staff"
  const [page, setPage] = useState("home");
  const [openGroups, setOpenGroups] = useState({});
  const [clients, setClients] = useState([]);
  const [deliveries, setDeliveries] = useState([]);
  const [invoices, setInvoices] = useState([]);
  const [products, setProducts] = useState([]);
  const [balances, setBalances] = useState({});
  const [clientPrices, setClientPrices] = useState([]);
  const [divisions, setDivisions] = useState([]);
  const [company, setCompany] = useState({});
  const [quotations, setQuotations] = useState([]);
  const [externalSales, setExternalSales] = useState([]);
  const [paymentHistory, setPaymentHistory] = useState([]);
  const [pendings, setPendings] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsubAuth = onAuthStateChanged(auth, (u) => setUser(u || null));
    return () => unsubAuth();
  }, []);

  // ユーザー権限取得
  useEffect(() => {
    if (!user) { setUserRole(null); return; }
    const unsub = onSnapshot(doc(db, "users", user.uid), async (snap) => {
      if (snap.exists()) {
        setUserRole(snap.data().role || "staff");
      } else {
        // usersコレクションにまだ登録されていない → 初回セットアップ
        try {
          const res = await fetch("/api/manage-user", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: "init", callerUid: user.uid, email: user.email }) });
          const data = await res.json();
          if (data.success && data.initialized) setUserRole("admin");
          else setUserRole("staff");
        } catch { setUserRole("staff"); }
      }
    });
    return () => unsub();
  }, [user]);

  useEffect(() => {
    if (!user) return;
    const unsubs = [];
    unsubs.push(onSnapshot(query(collection(db,"clients"),orderBy("createdAt","desc")),snap=>setClients(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"deliveries"),orderBy("createdAt","desc")),snap=>setDeliveries(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"invoices"),orderBy("createdAt","desc")),snap=>setInvoices(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"products"),orderBy("createdAt","desc")),snap=>setProducts(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"quotations"),orderBy("createdAt","desc")),snap=>setQuotations(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(collection(db,"clientBalances"),snap=>{
      const map={};
      snap.docs.forEach(d=>{map[d.data().clientId]={id:d.id,...d.data()};});
      setBalances(map);
    }));
    unsubs.push(onSnapshot(collection(db,"clientPrices"),snap=>setClientPrices(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(collection(db,"divisions"),snap=>setDivisions(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(collection(db,"externalSales"),snap=>setExternalSales(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"paymentHistory"),orderBy("createdAt","desc")),snap=>setPaymentHistory(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"pendingBillings"),orderBy("createdAt","desc")),snap=>setPendings(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(collection(db,"settings"),snap=>{
      if(!snap.empty) setCompany({id:snap.docs[0].id,...snap.docs[0].data()});
      setLoading(false);
    }));
    return ()=>unsubs.forEach(u=>u());
  }, [user]);

  // Auth loading / login screen
  if (user === undefined) return <div style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", background: C.navy }}><div style={{ color: "white", fontSize: 16 }}>読み込み中...</div></div>;
  if (user === null) return <LoginPage />;

  const isAdmin = userRole === "admin";

  const toggleGroup = (g) => setOpenGroups(prev => ({ ...prev, [g]: !prev[g] }));
  const nav = [
    { id: "home", label: "🏠 ホーム" },
    { id: "dashboard", label: "📊 売上概況" },
    { id: "sales", label: "📈 売上管理" },
    { id: "quotations", label: "📝 見積書一覧" },
    { id: "deliveries", label: "📦 納品書一覧" },
    { id: "invoices", label: "🧾 請求書一覧" },
    { id: "monthly", label: "📅 月締め管理" },
    { id: "balance", label: "💰 残高管理" },
    { id: "pending", label: "⏳ 承認待ち" },
    { id: "recurring", label: "🔄 定期請求" },
    { type: "group", label: "マスタ", children: [
      { id: "clients", label: "🏢 取引先管理" },
      { id: "products", label: "🗂 商品マスタ" },
      { id: "clientPrices", label: "💲 取引先別単価" },
      { id: "divisions", label: "🏭 事業部管理" },
    ]},
    { type: "group", label: "履歴", children: [
      { id: "sendHistory", label: "📨 送信履歴" },
      { id: "pdfHistory", label: "📁 PDF履歴" },
    ]},
    { id: "settings", label: "⚙ 設定" },
    { id: "manual", label: "📖 取扱説明書" },
  ];

  if (loading) return <div style={{display:"flex",alignItems:"center",justifyContent:"center",height:"100vh",fontSize:18,color:C.navy}}>読み込み中...</div>;

  return (
    <div style={s.app}>
      <div style={s.sidebar}>
        <div style={s.sideTitle}>
          <div style={{fontSize:16,fontWeight:700}}>📋 請求管理</div>
          <div style={{fontSize:11,color:"rgba(255,255,255,0.5)",marginTop:4}}>{company?.name||"自社名未設定"}</div>
        </div>
        {nav.map((n,i)=> n.type === "group" ? (
          <div key={"g"+i}>
            <div style={{ fontSize: 12, color: "rgba(255,255,255,0.6)", padding: "10px 20px 6px", fontWeight: 700, cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center" }} onClick={() => toggleGroup(n.label)}>
              <span>{n.label}</span>
              <span style={{ fontSize: 10 }}>{openGroups[n.label] ? "▲" : "▼"}</span>
            </div>
            {openGroups[n.label] && n.children.map(c => (
              <button key={c.id} style={{ ...s.navBtn(page===c.id), paddingLeft: 36 }} onClick={()=>setPage(c.id)}>{c.label}</button>
            ))}
          </div>
        ) : (
          <button key={n.id} style={s.navBtn(page===n.id)} onClick={()=>setPage(n.id)}>{n.label}</button>
        ))}
        <div style={{ marginTop: "auto", padding: "16px 20px", borderTop: "1px solid rgba(255,255,255,0.1)" }}>
          <div style={{ fontSize: 11, color: "rgba(255,255,255,0.5)", marginBottom: 4, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{user?.email}</div>
          <div style={{ fontSize: 10, color: isAdmin ? C.gold : "rgba(255,255,255,0.4)", marginBottom: 8 }}>{isAdmin ? "管理者" : "スタッフ"}</div>
          <button style={{ background: "rgba(255,255,255,0.1)", color: "rgba(255,255,255,0.7)", border: "none", borderRadius: 6, padding: "6px 12px", fontSize: 12, cursor: "pointer", width: "100%" }} onClick={() => signOut(auth)}>ログアウト</button>
        </div>
      </div>
      <div style={s.main}>
        {page==="home"&&<HomePage clients={clients} deliveries={deliveries} invoices={invoices} balances={balances} pendings={pendings} setPage={setPage}/>}
        {page==="dashboard"&&<Dashboard clients={clients} deliveries={deliveries} invoices={invoices} balances={balances}/>}
        {page==="quotations"&&<QuotationsList clients={clients} quotations={quotations} products={products} deliveries={deliveries} company={company} clientPrices={clientPrices} divisions={divisions} isAdmin={isAdmin}/>}
        {page==="deliveries"&&<DeliveriesList clients={clients} deliveries={deliveries} products={products} invoices={invoices} company={company} balances={balances} clientPrices={clientPrices} divisions={divisions} isAdmin={isAdmin}/>}
        {page==="invoices"&&<InvoicesList clients={clients} invoices={invoices} deliveries={deliveries} company={company} balances={balances} divisions={divisions} isAdmin={isAdmin}/>}
        {page==="monthly"&&<MonthlyBilling clients={clients} deliveries={deliveries} invoices={invoices} company={company} balances={balances} divisions={divisions}/>}
        {page==="sales"&&<SalesPage clients={clients} invoices={invoices} divisions={divisions} externalSales={externalSales}/>}
        {page==="balance"&&<BalancePage clients={clients} invoices={invoices} balances={balances} company={company} paymentHistory={paymentHistory}/>}
        {page==="clients"&&<ClientsPage clients={clients} divisions={divisions} isAdmin={isAdmin}/>}
        {page==="products"&&<ProductsPage products={products} company={company} isAdmin={isAdmin}/>}
        {page==="clientPrices"&&<ClientPricesPage clients={clients} products={products} clientPrices={clientPrices} isAdmin={isAdmin}/>}
        {page==="recurring"&&<RecurringPage clients={clients} divisions={divisions} invoices={invoices} company={company} balances={balances} isAdmin={isAdmin}/>}
        {page==="pending"&&<PendingPage clients={clients} company={company} divisions={divisions} balances={balances} isAdmin={isAdmin} invoices={invoices}/>}
        {page==="divisions"&&<DivisionsPage divisions={divisions} isAdmin={isAdmin}/>}
        {page==="sendHistory"&&<SendHistoryPage isAdmin={isAdmin}/>}
        {page==="pdfHistory"&&<PDFHistoryPage isAdmin={isAdmin}/>}
        {page==="settings"&&<SettingsPage company={company} setCompany={setCompany} isAdmin={isAdmin} currentUser={user}/>}
        {page==="manual"&&<ManualPage/>}
      </div>
    </div>
  );
}
