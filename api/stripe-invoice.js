export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { stripeSecretKey, clientName, email, amount, currency, description, invoiceItems, docNos } = req.body;

  if (!stripeSecretKey) {
    return res.status(400).json({ error: "Stripe APIキーが設定されていません" });
  }
  if (!email || !amount) {
    return res.status(400).json({ error: "メールアドレスと金額は必須です" });
  }

  const headers = {
    "Authorization": `Bearer ${stripeSecretKey}`,
    "Content-Type": "application/x-www-form-urlencoded",
  };

  const encode = (params) => new URLSearchParams(params).toString();

  try {
    // 1. 顧客を検索 or 作成
    const searchRes = await fetch(
      `https://api.stripe.com/v1/customers/search?query=${encodeURIComponent(`email:"${email}"`)}`,
      { headers }
    );
    const searchData = await searchRes.json();

    let customerId;
    if (searchData.data && searchData.data.length > 0) {
      customerId = searchData.data[0].id;
    } else {
      const createRes = await fetch("https://api.stripe.com/v1/customers", {
        method: "POST",
        headers,
        body: encode({ email, name: clientName || "" }),
      });
      const createData = await createRes.json();
      if (!createRes.ok) {
        return res.status(400).json({ error: "Stripe顧客作成エラー", details: createData });
      }
      customerId = createData.id;
    }

    // 2. 請求書を作成
    const invParams = {
      customer: customerId,
      collection_method: "send_invoice",
      days_until_due: "14",
      auto_advance: "true",
    };
    if (description) {
      invParams.description = description;
    }
    // メタデータに請求書番号を保存
    if (docNos) {
      invParams["metadata[docNos]"] = docNos;
    }

    const invRes = await fetch("https://api.stripe.com/v1/invoices", {
      method: "POST",
      headers,
      body: encode(invParams),
    });
    const invData = await invRes.json();
    if (!invRes.ok) {
      return res.status(400).json({ error: "Stripe請求書作成エラー", details: invData });
    }

    // 3. 明細行を追加
    if (invoiceItems && invoiceItems.length > 0) {
      for (const item of invoiceItems) {
        const lineParams = {
          invoice: invData.id,
          quantity: String(item.qty || 1),
          "price_data[currency]": currency || "jpy",
          "price_data[product_data][name]": item.name || "請求",
          "price_data[unit_amount]": String(item.unitAmount || 0),
        };
        await fetch("https://api.stripe.com/v1/invoiceitems", {
          method: "POST",
          headers,
          body: encode(lineParams),
        });
      }
    } else {
      // 明細なし：合計金額で1行
      const lineParams = {
        invoice: invData.id,
        quantity: "1",
        "price_data[currency]": currency || "jpy",
        "price_data[product_data][name]": description || "未入金請求",
        "price_data[unit_amount]": String(amount),
      };
      await fetch("https://api.stripe.com/v1/invoiceitems", {
        method: "POST",
        headers,
        body: encode(lineParams),
      });
    }

    // 4. 請求書を確定して送信
    const finalizeRes = await fetch(`https://api.stripe.com/v1/invoices/${invData.id}/finalize`, {
      method: "POST",
      headers,
    });
    const finalizeData = await finalizeRes.json();
    if (!finalizeRes.ok) {
      return res.status(400).json({ error: "Stripe請求書確定エラー", details: finalizeData });
    }

    const sendRes = await fetch(`https://api.stripe.com/v1/invoices/${invData.id}/send`, {
      method: "POST",
      headers,
    });
    const sendData = await sendRes.json();
    if (!sendRes.ok) {
      return res.status(400).json({ error: "Stripe請求書送信エラー", details: sendData });
    }

    return res.status(200).json({
      success: true,
      invoiceId: sendData.id,
      invoiceUrl: sendData.hosted_invoice_url,
      invoicePdf: sendData.invoice_pdf,
      amount: sendData.amount_due,
      status: sendData.status,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
