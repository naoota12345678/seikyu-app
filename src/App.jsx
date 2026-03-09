import { useState, useEffect } from "react";
import { db } from "./firebase";
import {
  collection, addDoc, getDocs, doc, updateDoc, deleteDoc,
  onSnapshot, query, orderBy, serverTimestamp, writeBatch, setDoc
} from "firebase/firestore";

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
const calcTax = (sub) => Math.floor(sub * 0.1);

function genDocNo(prefix, list) {
  const now = new Date();
  const ym = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}`;
  const same = list.filter(d => (d.docNo || "").includes(`${prefix}-${ym}`));
  return `${prefix}-${ym}-${String(same.length + 1).padStart(3, "0")}`;
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
  const sub = items.reduce((a,i)=>a+Number(i.qty||0)*Number(i.price||0),0);
  return `<table class="it"><thead><tr>
    <th style="width:38%">商品名</th><th style="width:9%">数量</th><th style="width:8%">単位</th>
    <th style="width:18%">単価</th><th style="width:18%">金額</th><th style="width:9%">備考</th>
  </tr></thead><tbody>
  ${items.map(i=>{const a=Number(i.qty||0)*Number(i.price||0);return`<tr><td>${i.name||""}</td><td class="nr">${i.qty||""}</td><td>${i.unit||""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td><td style="font-size:10px;color:#555">課10%</td></tr>`}).join("")}
  <tr class="tr"><td colspan="4">【合計 課税10.0% 税抜額】</td><td class="nr" colspan="2">¥${fmt(sub)}</td></tr>
  <tr class="tr"><td colspan="4">【合計 課税10.0% 消費税額】</td><td class="nr" colspan="2">¥${fmt(calcTax(sub))}</td></tr>
  </tbody></table>`;
}

function footerHTML(sub, bank) {
  const tax=calcTax(sub);
  return `<div class="tot"><table style="margin-left:auto;border-collapse:collapse">
  <tr><td style="padding:4px 12px;border:1px solid #ccc">税抜額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(sub)}</td>
  <td style="padding:4px 12px;border:1px solid #ccc">消費税額</td><td style="padding:4px 16px;border:1px solid #ccc;text-align:right;font-weight:bold">¥${fmt(tax)}</td>
  <td style="padding:4px 12px;background:#1C2B4A;color:#fff;font-weight:bold">合計</td>
  <td style="padding:4px 20px;border:2px solid #1C2B4A;text-align:right;font-size:16px;font-weight:bold;color:#1C2B4A">¥${fmt(sub+tax)}</td></tr>
  </table></div>
  ${bank?`<div class="bb">振込先口座：${bank.bankName||""}　${bank.bankBranch||""}　${bank.bankType||"普通"}　${bank.bankNo||""}<br>口座名義：${bank.bankHolder||""}<br>※振込手数料はご負担下さいますようお願い致します。</div>`:""}`;
}

function coBlock(c,doc_,showReg){
  return `<div class="co"><strong>${c.name||""}</strong><br>${c.address||""}<br>TEL ${c.tel||""}　FAX ${c.fax||""}<br>${showReg&&c.registrationNo?`登録番号　${c.registrationNo}`:""}</div>`;
}

function openPrint(html){
  const w=window.open("","_blank");w.document.write(html);w.document.close();setTimeout(()=>w.print(),600);
}

function printDelivery(d,clients,co){
  const cl=clients.find(c=>c.id===d.clientId)||{};
  const sub=d.items?.reduce((a,i)=>a+Number(i.qty||0)*Number(i.price||0),0)||0;
  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <h1>納　品　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">伝票番号：${d.docNo}<br>売上日：${d.date}</div></div>${coBlock(co,d,false)}</div>
  ${itemsHTML(d.items||[])}${footerHTML(sub,null)}
  ${d.notes?`<div style="margin-top:10px;font-size:11px;color:#555">備考：${d.notes}</div>`:""}
  </body></html>`);
}

function printInvoice(inv,clients,co){
  const cl=clients.find(c=>c.id===inv.clientId)||{};
  const sub=inv.items?.reduce((a,i)=>a+Number(i.qty||0)*Number(i.price||0),0)||0;
  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <h1>請　求　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">請求番号：${inv.docNo}<br>売上日：${inv.date}<br>支払期限：${inv.dueDate||""}</div></div>${coBlock(co,inv,true)}</div>
  ${itemsHTML(inv.items||[])}${footerHTML(sub,co)}
  ${inv.deliveryRefs?.length?`<div style="margin-top:10px;font-size:11px;color:#555">対象納品書：${inv.deliveryRefs.join("、")}</div>`:""}
  </body></html>`);
}

function printCombined(d,inv,clients,co){
  const cl=clients.find(c=>c.id===d.clientId)||{};
  const sub=d.items?.reduce((a,i)=>a+Number(i.qty||0)*Number(i.price||0),0)||0;
  openPrint(`<!DOCTYPE html><html><head><meta charset="utf-8"><style>${baseCSS}</style></head><body>
  <div class="pb">
  <h1>納　品　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">伝票番号：${d.docNo}<br>売上日：${d.date}</div></div>${coBlock(co,d,false)}</div>
  ${itemsHTML(d.items||[])}${footerHTML(sub,null)}</div>
  <h1>請　求　書</h1>
  <div class="hd"><div><div>${cl.address||""}</div><div class="cn">${cl.name||""} 御中</div>
  <div class="meta">請求番号：${inv.docNo}<br>売上日：${inv.date}<br>支払期限：${inv.dueDate||""}</div></div>${coBlock(co,inv,true)}</div>
  ${itemsHTML(inv.items||[])}${footerHTML(sub,co)}
  </body></html>`);
}

function printMeisai(inv,clients,co,bal){
  const cl=clients.find(c=>c.id===inv.clientId)||{};
  const sub=inv.items?.reduce((a,i)=>a+Number(i.qty||0)*Number(i.price||0),0)||0;
  const tax=calcTax(sub);
  const prev=bal?.prevBalance||0;
  const paid=bal?.paidAmount||0;
  const carry=prev-paid;
  const total=carry+sub+tax;
  const refs=inv.deliveryRefs||[];
  const refItems=inv.deliveryRefItems||[];

  let rows="";
  if(paid>0) rows+=`<tr><td>${inv.date}</td><td>振込</td><td></td><td></td><td></td><td class="nr" style="color:green">¥${fmt(paid)}</td></tr>`;
  if(refs.length>0){
    refs.forEach((ref,ri)=>{
      const items=refItems[ri]||[];
      items.forEach(i=>{
        const a=Number(i.qty||0)*Number(i.price||0);
        rows+=`<tr><td style="font-size:10px">${ref}</td><td>${i.name||""}<span style="font-size:9px;float:right;color:#555">課10%</span></td><td class="nr">${i.qty}</td><td>${i.unit||""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td></tr>`;
      });
    });
  } else {
    (inv.items||[]).forEach(i=>{
      const a=Number(i.qty||0)*Number(i.price||0);
      rows+=`<tr><td></td><td>${i.name||""}<span style="font-size:9px;float:right;color:#555">課10%</span></td><td class="nr">${i.qty}</td><td>${i.unit||""}</td><td class="nr">¥${fmt(i.price)}</td><td class="nr">¥${fmt(a)}</td></tr>`;
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
    <tr class="tr"><td colspan="5">【合計 課税10.0% 税抜額】</td><td class="nr">¥${fmt(sub)}</td></tr>
    <tr class="tr"><td colspan="5">【合計 課税10.0% 消費税額】</td><td class="nr">¥${fmt(tax)}</td></tr>
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
function PrintModeModal({ invoice, delivery, clients, company, balances, onClose }) {
  const bal = balances[invoice?.clientId];
  const modes = [
    { id: "invoice", label: "🧾 請求書のみ", desc: "請求書を単体で印刷", ok: !!invoice },
    { id: "delivery", label: "📦 納品書のみ", desc: "納品書を単体で印刷", ok: !!delivery },
    { id: "combined", label: "📄 納品書＋請求書同時", desc: "2枚同時印刷（納品書→請求書）", ok: !!delivery && !!invoice },
    { id: "meisai", label: "📋 請求明細書", desc: "残高管理付き（前回残高・繰越・今回請求）", ok: !!invoice },
  ];
  const handle = (id) => {
    if (id === "invoice") printInvoice(invoice, clients, company);
    else if (id === "delivery") printDelivery(delivery, clients, company);
    else if (id === "combined") printCombined(delivery, invoice, clients, company);
    else if (id === "meisai") printMeisai(invoice, clients, company, bal);
    onClose();
  };
  return (
    <div style={s.modal} onClick={onClose}>
      <div style={{ ...s.modalBox, maxWidth: 420 }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
          <h3 style={{ margin: 0, color: C.navy }}>印刷形式を選ぶ</h3>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {modes.map(m => (
            <button key={m.id} disabled={!m.ok}
              style={{ padding: "14px 18px", borderRadius: 8, border: `1px solid ${m.ok ? C.gold : C.light}`, background: m.ok ? C.white : "#f5f5f5", cursor: m.ok ? "pointer" : "not-allowed", textAlign: "left", opacity: m.ok ? 1 : 0.5 }}
              onClick={() => m.ok && handle(m.id)}>
              <div style={{ fontWeight: 700, fontSize: 15, color: C.navy }}>{m.label}</div>
              <div style={{ fontSize: 12, color: C.gray, marginTop: 3 }}>{m.desc}{!m.ok ? " ―（対応データなし）" : ""}</div>
            </button>
          ))}
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
    await setDoc(doc(db, "clientBalances", client.id), {
      clientId: client.id, prevBalance: prev,
      currentBalance: Math.max(0, prev - n),
      paidAmount: (balance?.paidAmount || 0) + n,
      lastPaidDate: date, lastPaidAmount: n,
      updatedAt: serverTimestamp(),
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
  const filtered = products.filter(p => p.name?.includes(q) || p.code?.includes(q));
  return (
    <div style={s.modal} onClick={onClose}>
      <div style={{ ...s.modalBox, maxWidth: 480 }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
          <h3 style={{ margin: 0, color: C.navy }}>商品を選ぶ</h3>
          <button style={s.btn("light")} onClick={onClose}>✕</button>
        </div>
        <input style={{ ...s.input, width: "100%", marginBottom: 12 }} placeholder="商品名・コードで検索" value={q} onChange={e => setQ(e.target.value)} />
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
      <td style={{ ...s.td, textAlign: "right" }}>¥{fmt(Number(item.qty||0)*Number(item.price||0))}</td>
      <td style={s.td}><button style={{ ...s.btn("red"), padding: "4px 10px" }} onClick={() => onRemove(idx)}>✕</button></td>
    </tr>
  );
}

// ── Delivery Form ─────────────────────────────────────────────────────────────
function DeliveryForm({ clients, products, deliveries, onSave, onClose, editing }) {
  const [form, setForm] = useState(editing || { clientId: "", date: today(), notes: "", items: [{ name: "", qty: 1, unit: "", price: 0 }] });
  const [pickerIdx, setPickerIdx] = useState(null);
  const setF = (k, v) => setForm(f => ({ ...f, [k]: v }));
  const setItem = (idx, k, v) => setForm(f => ({ ...f, items: f.items.map((it, i) => i === idx ? { ...it, [k]: v } : it) }));
  const addItem = () => setForm(f => ({ ...f, items: [...f.items, { name: "", qty: 1, unit: "", price: 0 }] }));
  const remItem = (idx) => setForm(f => ({ ...f, items: f.items.filter((_, i) => i !== idx) }));
  const sub = form.items.reduce((a, i) => a + Number(i.qty||0)*Number(i.price||0), 0);
  const tax = calcTax(sub);
  const save = async () => {
    if (!form.clientId) return alert("取引先を選択してください");
    if (!form.items.some(i => i.name)) return alert("品目を入力してください");
    const data = { ...form, docNo: editing?.docNo || genDocNo("NO", deliveries), status: editing?.status || "unissued", subtotal: sub, tax, total: sub + tax, updatedAt: serverTimestamp() };
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
            <select style={s.select} value={form.clientId} onChange={e => setF("clientId", e.target.value)}>
              <option value="">選択してください</option>
              {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
          </div>
          <div style={s.col}><span style={s.label}>納品日 *</span><input style={s.input} type="date" value={form.date} onChange={e => setF("date", e.target.value)} /></div>
        </div>
        <table style={s.table}>
          <thead><tr><th style={s.th}>品名</th><th style={s.th}>数量</th><th style={s.th}>単位</th><th style={s.th}>単価</th><th style={s.th}>金額</th><th style={s.th}></th></tr></thead>
          <tbody>{form.items.map((it, idx) => <ItemRow key={idx} item={it} idx={idx} onChange={setItem} onRemove={remItem} onPickProduct={setPickerIdx} />)}</tbody>
        </table>
        <button style={{ ...s.btn("light"), marginBottom: 16 }} onClick={addItem}>＋ 行を追加</button>
        <div style={{ textAlign: "right", marginBottom: 16 }}>
          <div style={{ fontSize: 13, color: C.gray }}>小計 ¥{fmt(sub)}　消費税 ¥{fmt(tax)}</div>
          <div style={{ fontSize: 18, fontWeight: 700, color: C.navy }}>合計 ¥{fmt(sub + tax)}</div>
        </div>
        <div style={s.col}><span style={s.label}>備考</span><textarea style={{ ...s.input, minHeight: 60 }} value={form.notes} onChange={e => setF("notes", e.target.value)} /></div>
        <div style={{ ...s.row, justifyContent: "flex-end", marginTop: 20 }}>
          <button style={s.btn("light")} onClick={onClose}>キャンセル</button>
          <button style={s.btn("primary")} onClick={save}>{editing ? "更新" : "保存"}</button>
        </div>
      </div>
      {pickerIdx !== null && <ProductPicker products={products} onClose={() => setPickerIdx(null)}
        onSelect={p => { setItem(pickerIdx,"name",p.name); setItem(pickerIdx,"unit",p.unit||""); setItem(pickerIdx,"price",p.price||0); setPickerIdx(null); }} />}
    </div>
  );
}

// ── Dashboard ────────────────────────────────────────────────────────────────
function Dashboard({ clients, deliveries, invoices, balances }) {
  const totalBalance = Object.values(balances).reduce((a, b) => a + (b.currentBalance || 0), 0);
  const thisMonth = new Date().toISOString().slice(0, 7);
  const monthDel = deliveries.filter(d => (d.date || "").startsWith(thisMonth));
  const overdue = invoices.filter(i => i.status === "unpaid" && i.dueDate && i.dueDate < today());
  return (
    <div>
      <div style={s.pageTitle}>ダッシュボード</div>
      <div style={{ display: "flex", gap: 16, marginBottom: 24, flexWrap: "wrap" }}>
        {[
          { label: "取引先数", value: clients.length + " 社", color: C.navy },
          { label: "今月の納品数", value: monthDel.length + " 件", color: C.gold },
          { label: "未収残高合計", value: "¥" + fmt(totalBalance), color: totalBalance > 0 ? C.red : C.green },
          { label: "期限超過", value: overdue.length + " 件", color: overdue.length > 0 ? C.red : C.green },
        ].map(st => (
          <div key={st.label} style={{ ...s.card, flex: "1 1 180px", textAlign: "center", margin: 0 }}>
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
                <td style={s.td}><span style={s.badge(d.status === "invoiced" ? "green" : "gold")}>{d.status === "invoiced" ? "請求済" : "未請求"}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Deliveries ────────────────────────────────────────────────────────────────
function DeliveriesList({ clients, deliveries, products, invoices, company, balances }) {
  const [search, setSearch] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [editing, setEditing] = useState(null);
  const [printTarget, setPrintTarget] = useState(null);
  const filtered = deliveries.filter(d => {
    const cn = clients.find(c => c.id === d.clientId)?.name || "";
    return cn.includes(search) || (d.docNo || "").includes(search);
  });
  const deleteD = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db, "deliveries", id)); };
  const issueInvoice = async (d) => {
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
                  <td style={s.td}>{client?.name || "—"}<br /><span style={{ fontSize: 11, color: C.gray }}>{client?.billingType === "monthly" ? "月締め" : "即時"}</span></td>
                  <td style={s.td}>¥{fmt(d.total)}</td>
                  <td style={s.td}><span style={s.badge(d.status === "invoiced" ? "green" : "gold")}>{d.status === "invoiced" ? "請求済" : "未請求"}</span></td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => { setEditing(d); setShowForm(true); }}>編集</button>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => setPrintTarget({ delivery: d, invoice: invoices.find(i => i.deliveryRef === d.docNo) })}>🖨 印刷</button>
                      {d.status !== "invoiced" && client?.billingType !== "monthly" && (
                        <button style={{ ...s.btn("gold"), padding: "4px 8px", fontSize: 12 }} onClick={() => issueInvoice(d)}>請求書発行</button>
                      )}
                      <button style={{ ...s.btn("red"), padding: "4px 8px", fontSize: 12 }} onClick={() => deleteD(d.id)}>削除</button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {showForm && <DeliveryForm clients={clients} products={products} deliveries={deliveries} editing={editing}
        onSave={() => setShowForm(false)} onClose={() => setShowForm(false)} />}
      {printTarget && <PrintModeModal invoice={printTarget.invoice} delivery={printTarget.delivery}
        clients={clients} company={company} balances={balances} onClose={() => setPrintTarget(null)} />}
    </div>
  );
}

// ── Invoices ──────────────────────────────────────────────────────────────────
function InvoicesList({ clients, invoices, deliveries, company, balances }) {
  const [search, setSearch] = useState("");
  const [printTarget, setPrintTarget] = useState(null);
  const [balTarget, setBalTarget] = useState(null);
  const filtered = invoices.filter(i => {
    const cn = clients.find(c => c.id === i.clientId)?.name || "";
    return cn.includes(search) || (i.docNo || "").includes(search);
  });
  const totalBal = Object.values(balances).reduce((a, b) => a + (b.currentBalance || 0), 0);
  const markPaid = async (inv) => {
    await updateDoc(doc(db, "invoices", inv.id), { status: "paid", paidAt: today() });
    const bal = balances[inv.clientId] || {};
    await setDoc(doc(db, "clientBalances", inv.clientId), {
      clientId: inv.clientId, prevBalance: bal.currentBalance || 0,
      currentBalance: Math.max(0, (bal.currentBalance || 0) - inv.total),
      paidAmount: (bal.paidAmount || 0) + inv.total,
      lastPaidDate: today(), lastPaidAmount: inv.total, updatedAt: serverTimestamp(),
    });
  };
  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db, "invoices", id)); };
  return (
    <div>
      <div style={s.pageTitle}>請求書一覧</div>
      <div style={{ ...s.card, padding: "12px 20px", display: "flex", gap: 16, alignItems: "center" }}>
        <input style={s.input} placeholder="取引先名・請求番号で検索" value={search} onChange={e => setSearch(e.target.value)} />
        <div style={{ fontSize: 15, fontWeight: 700, color: C.red, marginLeft: "auto" }}>未収残高合計：¥{fmt(totalBal)}</div>
      </div>
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>請求番号</th><th style={s.th}>日付</th><th style={s.th}>取引先</th><th style={s.th}>今回請求額</th><th style={s.th}>残高</th><th style={s.th}>期限</th><th style={s.th}>状態</th><th style={s.th}>操作</th></tr></thead>
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
                  <td style={s.td}><span style={s.badge(inv.status === "paid" ? "green" : overdue ? "red" : "gold")}>{inv.status === "paid" ? "入金済" : overdue ? "期限超過" : "未収"}</span></td>
                  <td style={s.td}>
                    <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                      <button style={{ ...s.btn("light"), padding: "4px 8px", fontSize: 12 }} onClick={() => setPrintTarget({ invoice: inv, delivery })}>🖨 印刷</button>
                      {inv.status !== "paid" && <>
                        <button style={{ ...s.btn("green"), padding: "4px 8px", fontSize: 12 }} onClick={() => markPaid(inv)}>入金済</button>
                        <button style={{ ...s.btn("gold"), padding: "4px 8px", fontSize: 12 }} onClick={() => setBalTarget({ client, balance: bal })}>入金記録</button>
                      </>}
                      <button style={{ ...s.btn("red"), padding: "4px 8px", fontSize: 12 }} onClick={() => del(inv.id)}>削除</button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {printTarget && <PrintModeModal invoice={printTarget.invoice} delivery={printTarget.delivery}
        clients={clients} company={company} balances={balances} onClose={() => setPrintTarget(null)} />}
      {balTarget && <BalanceModal client={balTarget.client} balance={balTarget.balance} onClose={() => setBalTarget(null)} />}
    </div>
  );
}

// ── Monthly Billing ───────────────────────────────────────────────────────────
function MonthlyBilling({ clients, deliveries, invoices, company, balances }) {
  const [month, setMonth] = useState(today().slice(0, 7));
  const [printTarget, setPrintTarget] = useState(null);
  const monthlyClients = clients.filter(c => c.billingType === "monthly");
  const issueMonthly = async (client) => {
    const dels = deliveries.filter(d => d.clientId === client.id && d.date?.startsWith(month) && d.status !== "invoiced");
    if (!dels.length) return alert("対象の未請求納品書がありません");
    const allItems = dels.flatMap(d => d.items || []);
    const sub = allItems.reduce((a, i) => a + Number(i.qty||0)*Number(i.price||0), 0);
    const tax = calcTax(sub);
    const inv = {
      docNo: genDocNo("INV", invoices), clientId: client.id, date: today(),
      dueDate: nextMonthEnd(month + "-01"), billingType: "monthly",
      deliveryRefs: dels.map(d => d.docNo),
      deliveryRefItems: dels.map(d => d.items || []),
      items: allItems, subtotal: sub, tax, total: sub + tax,
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
    alert(`請求書を発行しました（${dels.length}件まとめ）`);
  };
  return (
    <div>
      <div style={s.pageTitle}>掛け・月締め管理</div>
      <div style={{ ...s.card, padding: "12px 20px" }}>
        <label style={{ ...s.label, marginRight: 8 }}>対象月：</label>
        <input style={s.input} type="month" value={month} onChange={e => setMonth(e.target.value)} />
      </div>
      {monthlyClients.map(client => {
        const dels = deliveries.filter(d => d.clientId === client.id && d.date?.startsWith(month));
        const unissued = dels.filter(d => d.status !== "invoiced");
        const sub = unissued.reduce((a, d) => a + (d.subtotal || 0), 0);
        const bal = balances[client.id] || {};
        const monthInv = invoices.find(i => i.clientId === client.id && i.date?.startsWith(month) && i.billingType === "monthly");
        return (
          <div key={client.id} style={s.card}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
              <div>
                <span style={{ fontWeight: 700, fontSize: 16, color: C.navy }}>{client.name}</span>
                <span style={{ ...s.badge("gold"), marginLeft: 8 }}>月締め</span>
                {bal.currentBalance > 0 && <span style={{ marginLeft: 12, fontSize: 13, color: C.red }}>残高：¥{fmt(bal.currentBalance)}</span>}
              </div>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <span style={{ fontSize: 14, color: C.gray }}>未請求：{unissued.length}件　¥{fmt(sub + calcTax(sub))}</span>
                <button style={s.btn("gold")} onClick={() => issueMonthly(client)} disabled={!unissued.length}>一括請求書発行</button>
                {monthInv && <button style={{ ...s.btn("light"), padding: "8px 14px" }} onClick={() => setPrintTarget({ invoice: monthInv })}>🖨 印刷</button>}
              </div>
            </div>
            <table style={s.table}>
              <thead><tr><th style={s.th}>伝票番号</th><th style={s.th}>日付</th><th style={s.th}>金額</th><th style={s.th}>状態</th></tr></thead>
              <tbody>
                {dels.map(d => (
                  <tr key={d.id}>
                    <td style={s.td}>{d.docNo}</td><td style={s.td}>{d.date}</td>
                    <td style={s.td}>¥{fmt(d.total)}</td>
                    <td style={s.td}><span style={s.badge(d.status === "invoiced" ? "green" : "gold")}>{d.status === "invoiced" ? "請求済" : "未請求"}</span></td>
                  </tr>
                ))}
                {!dels.length && <tr><td colSpan={4} style={{ ...s.td, textAlign: "center", color: C.gray }}>この月の納品書はありません</td></tr>}
              </tbody>
            </table>
          </div>
        );
      })}
      {!monthlyClients.length && <div style={{ ...s.card, color: C.gray, textAlign: "center" }}>月締め取引先がありません</div>}
      {printTarget && <PrintModeModal invoice={printTarget.invoice} delivery={null}
        clients={clients} company={company} balances={balances} onClose={() => setPrintTarget(null)} />}
    </div>
  );
}

// ── Balance Page ──────────────────────────────────────────────────────────────
function BalancePage({ clients, balances }) {
  const [balTarget, setBalTarget] = useState(null);
  const total = Object.values(balances).reduce((a, b) => a + (b.currentBalance || 0), 0);
  return (
    <div>
      <div style={s.pageTitle}>残高管理</div>
      <div style={{ ...s.card, padding: "12px 20px", display: "flex", alignItems: "center" }}>
        <div style={{ fontSize: 15, fontWeight: 700, color: C.red }}>未収残高合計：¥{fmt(total)}</div>
      </div>
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>取引先</th><th style={s.th}>請求タイプ</th><th style={s.th}>前回残高</th><th style={s.th}>直近入金日</th><th style={s.th}>直近入金額</th><th style={s.th}>現在残高</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {clients.map(client => {
              const bal = balances[client.id] || {};
              return (
                <tr key={client.id}>
                  <td style={s.td}><strong>{client.name}</strong></td>
                  <td style={s.td}><span style={s.badge(client.billingType === "monthly" ? "gold" : "blue")}>{client.billingType === "monthly" ? "月締め" : "即時"}</span></td>
                  <td style={s.td}>¥{fmt(bal.prevBalance||0)}</td>
                  <td style={s.td}>{bal.lastPaidDate || "—"}</td>
                  <td style={s.td}>{bal.lastPaidAmount ? "¥" + fmt(bal.lastPaidAmount) : "—"}</td>
                  <td style={{ ...s.td, fontWeight: 700, color: (bal.currentBalance||0) > 0 ? C.red : C.green }}>¥{fmt(bal.currentBalance||0)}</td>
                  <td style={s.td}><button style={{ ...s.btn("gold"), padding: "4px 10px", fontSize: 12 }} onClick={() => setBalTarget({ client, balance: bal })}>入金記録</button></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {balTarget && <BalanceModal client={balTarget.client} balance={balTarget.balance} onClose={() => setBalTarget(null)} />}
    </div>
  );
}

// ── Clients ───────────────────────────────────────────────────────────────────
function ClientsPage({ clients }) {
  const [form, setForm] = useState({ name: "", kana: "", address: "", tel: "", email: "", billingType: "immediate", isOneTime: false });
  const [editing, setEditing] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const setF = (k, v) => setForm(f => ({ ...f, [k]: v }));
  const save = async () => {
    if (!form.name) return alert("取引先名を入力してください");
    const data = { ...form, updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db, "clients", editing.id), data);
    else { data.createdAt = serverTimestamp(); await addDoc(collection(db, "clients"), data); }
    setShowForm(false); setEditing(null);
    setForm({ name: "", kana: "", address: "", tel: "", email: "", billingType: "immediate", isOneTime: false });
  };
  const edit = (c) => { setForm(c); setEditing(c); setShowForm(true); };
  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db, "clients", id)); };
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={s.pageTitle}>取引先管理</div>
        <button style={s.btn("primary")} onClick={() => { setEditing(null); setForm({ name:"",kana:"",address:"",tel:"",email:"",billingType:"immediate",isOneTime:false }); setShowForm(true); }}>＋ 追加</button>
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
              <select style={s.select} value={form.billingType} onChange={e => setF("billingType",e.target.value)}>
                <option value="immediate">即時請求</option><option value="monthly">月締め請求</option>
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
          <thead><tr><th style={s.th}>会社名</th><th style={s.th}>電話</th><th style={s.th}>請求タイプ</th><th style={s.th}>区分</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {clients.map(c => (
              <tr key={c.id}>
                <td style={s.td}>{c.name}</td><td style={s.td}>{c.tel}</td>
                <td style={s.td}><span style={s.badge(c.billingType==="monthly"?"gold":"blue")}>{c.billingType==="monthly"?"月締め":"即時"}</span></td>
                <td style={s.td}>{c.isOneTime&&<span style={s.badge("light")}>単発</span>}</td>
                <td style={s.td}>
                  <div style={{display:"flex",gap:6}}>
                    <button style={{...s.btn("light"),padding:"4px 10px",fontSize:12}} onClick={()=>edit(c)}>編集</button>
                    <button style={{...s.btn("red"),padding:"4px 10px",fontSize:12}} onClick={()=>del(c.id)}>削除</button>
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

// ── Products ──────────────────────────────────────────────────────────────────
function ProductsPage({ products }) {
  const [form, setForm] = useState({ name:"",code:"",jan:"",unit:"",price:"",notes:"" });
  const [editing, setEditing] = useState(null);
  const [showForm, setShowForm] = useState(false);
  const [importing, setImporting] = useState(false);
  const setF = (k,v) => setForm(f => ({...f,[k]:v}));
  const save = async () => {
    if (!form.name) return alert("商品名を入力してください");
    const data = { ...form, price: Number(form.price)||0, updatedAt: serverTimestamp() };
    if (editing) await updateDoc(doc(db,"products",editing.id),data);
    else { data.createdAt = serverTimestamp(); await addDoc(collection(db,"products"),data); }
    setShowForm(false); setEditing(null); setForm({name:"",code:"",jan:"",unit:"",price:"",notes:""});
  };
  const edit = (p) => { setForm({...p,price:String(p.price)}); setEditing(p); setShowForm(true); };
  const del = async (id) => { if (confirm("削除しますか？")) await deleteDoc(doc(db,"products",id)); };
  const handleCSV = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setImporting(true);
    try {
      const text = await file.text();
      const lines = text.split(/\r?\n/).filter(l => l.trim());
      const header = lines[0].split(",").map(h => h.trim().replace(/^["']|["']$/g, ""));
      const nameIdx = header.findIndex(h => h.includes("商品名") || h.toLowerCase() === "name");
      const codeIdx = header.findIndex(h => h.includes("コード") || h.toLowerCase() === "code");
      const janIdx = header.findIndex(h => h.includes("JAN") || h.toLowerCase() === "jan");
      const priceIdx = header.findIndex(h => h.includes("単価") || h.includes("価格") || h.toLowerCase() === "price");
      const notesIdx = header.findIndex(h => h.includes("備考") || h.toLowerCase() === "notes");
      if (nameIdx === -1) { alert("「商品名」列が見つかりません"); setImporting(false); return; }
      const rows = lines.slice(1);
      const batch = writeBatch(db);
      let count = 0;
      for (const line of rows) {
        const cols = line.match(/(".*?"|[^,]*),?/g)?.map(c => c.replace(/,$/,"").replace(/^["']|["']$/g,"").trim()) || [];
        const name = cols[nameIdx];
        if (!name) continue;
        const ref = doc(collection(db, "products"));
        batch.set(ref, {
          name,
          code: codeIdx >= 0 ? (cols[codeIdx] || "") : "",
          jan: janIdx >= 0 ? (cols[janIdx] || "") : "",
          unit: "",
          price: priceIdx >= 0 ? (Number(cols[priceIdx]) || 0) : 0,
          notes: notesIdx >= 0 ? (cols[notesIdx] || "") : "",
          createdAt: serverTimestamp(),
          updatedAt: serverTimestamp(),
        });
        count++;
      }
      await batch.commit();
      alert(`${count}件の商品をインポートしました`);
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
          <button style={s.btn("primary")} onClick={()=>{setEditing(null);setForm({name:"",code:"",jan:"",unit:"",price:"",notes:""});setShowForm(true);}}>＋ 追加</button>
        </div>
      </div>
      {showForm && (
        <div style={s.card}>
          <h3 style={{margin:"0 0 16px",color:C.navy}}>{editing?"商品を編集":"商品を追加"}</h3>
          <div style={{...s.row,marginBottom:12}}>
            <div style={s.col}><span style={s.label}>商品名 *</span><input style={s.input} value={form.name} onChange={e=>setF("name",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>コード</span><input style={s.input} value={form.code} onChange={e=>setF("code",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>JAN</span><input style={s.input} value={form.jan||""} onChange={e=>setF("jan",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>単位</span><input style={{...s.input,width:80}} value={form.unit} onChange={e=>setF("unit",e.target.value)} placeholder="袋" /></div>
            <div style={s.col}><span style={s.label}>標準単価</span><input style={{...s.input,width:120}} type="number" value={form.price} onChange={e=>setF("price",e.target.value)} /></div>
            <div style={s.col}><span style={s.label}>備考</span><input style={{...s.input,minWidth:200}} value={form.notes} onChange={e=>setF("notes",e.target.value)} /></div>
          </div>
          <div style={{...s.row,justifyContent:"flex-end",gap:8}}>
            <button style={s.btn("light")} onClick={()=>setShowForm(false)}>キャンセル</button>
            <button style={s.btn("primary")} onClick={save}>{editing?"更新":"保存"}</button>
          </div>
        </div>
      )}
      <div style={s.card}>
        <table style={s.table}>
          <thead><tr><th style={s.th}>商品名</th><th style={s.th}>コード</th><th style={s.th}>JAN</th><th style={s.th}>単位</th><th style={s.th}>標準単価</th><th style={s.th}>備考</th><th style={s.th}>操作</th></tr></thead>
          <tbody>
            {products.map(p => (
              <tr key={p.id}>
                <td style={s.td}>{p.name}</td><td style={s.td}>{p.code}</td><td style={s.td}>{p.jan||""}</td>
                <td style={s.td}>{p.unit}</td><td style={s.td}>¥{fmt(p.price)}</td><td style={s.td}>{p.notes}</td>
                <td style={s.td}>
                  <div style={{display:"flex",gap:6}}>
                    <button style={{...s.btn("light"),padding:"4px 10px",fontSize:12}} onClick={()=>edit(p)}>編集</button>
                    <button style={{...s.btn("red"),padding:"4px 10px",fontSize:12}} onClick={()=>del(p.id)}>削除</button>
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

// ── Settings ──────────────────────────────────────────────────────────────────
function SettingsPage({ company, setCompany }) {
  const [form, setForm] = useState(company || {});
  const setF = (k,v) => setForm(f => ({...f,[k]:v}));
  const save = async () => {
    const existing = await getDocs(collection(db,"settings"));
    if (existing.empty) await addDoc(collection(db,"settings"),{...form,updatedAt:serverTimestamp()});
    else await updateDoc(doc(db,"settings",existing.docs[0].id),{...form,updatedAt:serverTimestamp()});
    setCompany(form); alert("保存しました");
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
      <button style={s.btn("primary")} onClick={save}>保存</button>
    </div>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────
export default function App() {
  const [page, setPage] = useState("dashboard");
  const [clients, setClients] = useState([]);
  const [deliveries, setDeliveries] = useState([]);
  const [invoices, setInvoices] = useState([]);
  const [products, setProducts] = useState([]);
  const [balances, setBalances] = useState({});
  const [company, setCompany] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsubs = [];
    unsubs.push(onSnapshot(query(collection(db,"clients"),orderBy("createdAt","desc")),snap=>setClients(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"deliveries"),orderBy("createdAt","desc")),snap=>setDeliveries(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"invoices"),orderBy("createdAt","desc")),snap=>setInvoices(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(query(collection(db,"products"),orderBy("createdAt","desc")),snap=>setProducts(snap.docs.map(d=>({id:d.id,...d.data()})))));
    unsubs.push(onSnapshot(collection(db,"clientBalances"),snap=>{
      const map={};
      snap.docs.forEach(d=>{map[d.data().clientId]={id:d.id,...d.data()};});
      setBalances(map);
    }));
    unsubs.push(onSnapshot(collection(db,"settings"),snap=>{
      if(!snap.empty) setCompany({id:snap.docs[0].id,...snap.docs[0].data()});
      setLoading(false);
    }));
    return ()=>unsubs.forEach(u=>u());
  }, []);

  const nav = [
    { id: "dashboard", label: "📊 ダッシュボード" },
    { id: "deliveries", label: "📦 納品書一覧" },
    { id: "invoices", label: "🧾 請求書一覧" },
    { id: "monthly", label: "📅 月締め管理" },
    { id: "balance", label: "💰 残高管理" },
    { id: "clients", label: "🏢 取引先管理" },
    { id: "products", label: "🗂 商品マスタ" },
    { id: "settings", label: "⚙ 設定" },
  ];

  if (loading) return <div style={{display:"flex",alignItems:"center",justifyContent:"center",height:"100vh",fontSize:18,color:C.navy}}>読み込み中...</div>;

  return (
    <div style={s.app}>
      <div style={s.sidebar}>
        <div style={s.sideTitle}>
          <div style={{fontSize:16,fontWeight:700}}>📋 請求管理</div>
          <div style={{fontSize:11,color:"rgba(255,255,255,0.5)",marginTop:4}}>{company?.name||"自社名未設定"}</div>
        </div>
        {nav.map(n=><button key={n.id} style={s.navBtn(page===n.id)} onClick={()=>setPage(n.id)}>{n.label}</button>)}
      </div>
      <div style={s.main}>
        {page==="dashboard"&&<Dashboard clients={clients} deliveries={deliveries} invoices={invoices} balances={balances}/>}
        {page==="deliveries"&&<DeliveriesList clients={clients} deliveries={deliveries} products={products} invoices={invoices} company={company} balances={balances}/>}
        {page==="invoices"&&<InvoicesList clients={clients} invoices={invoices} deliveries={deliveries} company={company} balances={balances}/>}
        {page==="monthly"&&<MonthlyBilling clients={clients} deliveries={deliveries} invoices={invoices} company={company} balances={balances}/>}
        {page==="balance"&&<BalancePage clients={clients} balances={balances}/>}
        {page==="clients"&&<ClientsPage clients={clients}/>}
        {page==="products"&&<ProductsPage products={products}/>}
        {page==="settings"&&<SettingsPage company={company} setCompany={setCompany}/>}
      </div>
    </div>
  );
}
