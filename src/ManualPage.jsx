const C = {
  navy: "#1C2B4A", gold: "#B8905A", cream: "#F4F1EC",
  pale: "#F7EEE0", green: "#2D6A4F", red: "#C53030",
  gray: "#777", light: "#e8e2da", white: "#fff"
};

const ms = {
  wrap: { maxWidth: 860, margin: "0 auto" },
  title: { fontSize: 26, fontWeight: 700, color: C.navy, marginBottom: 8, borderBottom: `3px solid ${C.gold}`, paddingBottom: 12 },
  toc: { background: C.pale, borderRadius: 10, padding: "20px 28px", marginBottom: 28 },
  tocTitle: { fontSize: 15, fontWeight: 700, color: C.navy, marginBottom: 10 },
  tocLink: { display: "block", padding: "3px 0", color: C.navy, textDecoration: "none", fontSize: 14, cursor: "pointer" },
  section: { background: C.white, borderRadius: 10, padding: "24px 28px", marginBottom: 20, boxShadow: "0 1px 6px rgba(0,0,0,0.07)" },
  h2: { fontSize: 19, fontWeight: 700, color: C.navy, marginBottom: 14, paddingBottom: 8, borderBottom: `2px solid ${C.gold}` },
  h3: { fontSize: 15, fontWeight: 700, color: C.navy, marginTop: 18, marginBottom: 8 },
  p: { fontSize: 14, lineHeight: 1.8, color: "#333", marginBottom: 10 },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13, marginBottom: 14 },
  th: { padding: "8px 12px", background: C.pale, color: C.navy, fontWeight: 700, textAlign: "left", borderBottom: `2px solid ${C.gold}` },
  td: { padding: "8px 12px", borderBottom: `1px solid ${C.light}`, verticalAlign: "top", lineHeight: 1.6 },
  note: { background: "#fff8e1", border: "1px solid #ffe082", borderRadius: 6, padding: "10px 14px", fontSize: 13, lineHeight: 1.7, marginBottom: 12, color: "#5d4037" },
  code: { background: C.pale, borderRadius: 6, padding: "10px 14px", fontSize: 12, fontFamily: "monospace", whiteSpace: "pre-wrap", marginBottom: 12, display: "block", lineHeight: 1.6 },
  badge: (color) => ({
    display: "inline-block", padding: "1px 8px", borderRadius: 12, fontSize: 11, fontWeight: 700, marginRight: 4,
    background: color === "green" ? "#d4edda" : color === "gold" ? "#fff3cd" : color === "blue" ? "#d0e4ff" : color === "red" ? "#f8d7da" : "#e2e3e5",
    color: color === "green" ? C.green : color === "gold" ? "#856404" : color === "blue" ? "#004085" : color === "red" ? C.red : "#383d41",
  }),
};

const T = ({ children }) => (
  <table style={ms.table}><tbody>{children}</tbody></table>
);
const TH = ({ cols }) => (
  <tr>{cols.map((c, i) => <th key={i} style={ms.th}>{c}</th>)}</tr>
);
const TR = ({ cols }) => (
  <tr>{cols.map((c, i) => <td key={i} style={ms.td}>{c}</td>)}</tr>
);

export default function ManualPage() {
  const scrollTo = (id) => {
    const el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: "smooth" });
  };

  const tocItems = [
    ["sec-intro", "1. はじめに"],
    ["sec-login", "2. ログイン"],
    ["sec-layout", "3. 画面構成"],
    ["sec-dashboard", "4. ダッシュボード"],
    ["sec-quotations", "5. 見積書一覧"],
    ["sec-deliveries", "6. 納品書一覧"],
    ["sec-invoices", "7. 請求書一覧"],
    ["sec-monthly", "8. 月締め管理"],
    ["sec-sales", "9. 売上管理"],
    ["sec-balance", "10. 残高管理"],
    ["sec-pending", "11. 承認待ち"],
    ["sec-recurring", "12. 定期請求"],
    ["sec-clients", "13. 取引先管理"],
    ["sec-products", "14. 商品マスタ"],
    ["sec-clientprices", "15. 取引先別単価"],
    ["sec-divisions", "16. 事業部管理"],
    ["sec-sendhistory", "17. 送信履歴"],
    ["sec-pdfhistory", "18. PDF履歴"],
    ["sec-settings", "19. 設定"],
    ["sec-cron", "20. 自動処理"],
    ["sec-appendix", "付録"],
  ];

  return (
    <div style={ms.wrap}>
      <div style={ms.title}>📖 取扱説明書</div>
      <p style={{ ...ms.p, color: C.gray, marginBottom: 20 }}>請求管理アプリの操作ガイドです。各機能の使い方を説明しています。</p>

      {/* 目次 */}
      <div style={ms.toc}>
        <div style={ms.tocTitle}>目次</div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2px 24px" }}>
          {tocItems.map(([id, label]) => (
            <span key={id} style={ms.tocLink} onClick={() => scrollTo(id)}>{label}</span>
          ))}
        </div>
      </div>

      {/* 1. はじめに */}
      <div id="sec-intro" style={ms.section}>
        <div style={ms.h2}>1. はじめに</div>
        <p style={ms.p}>本アプリは、見積書・納品書・請求書の作成から送信、入金管理までを一元管理する請求管理システムです。</p>
        <div style={ms.h3}>主な特徴</div>
        <ul style={{ ...ms.p, paddingLeft: 20 }}>
          <li>見積書 → 納品書 → 請求書のワンクリック変換</li>
          <li>締日請求・即時請求・定期請求に対応</li>
          <li>税率別（10%/8%/軽減税率）の自動計算</li>
          <li>PDF生成・メール送信・送信予約</li>
          <li>楽天・Amazon売上の自動連携</li>
          <li>Stripe決済による再請求</li>
          <li>承認フロー（管理者承認）</li>
          <li>CSV入出力対応</li>
        </ul>
        <div style={ms.h3}>権限について</div>
        <T>
          <TH cols={["権限", "できること"]} />
          <TR cols={["管理者（admin）", "全機能の利用、ユーザー管理、データ削除、承認・却下"]} />
          <TR cols={["スタッフ（staff）", "書類の作成・編集・送信（削除・承認は不可）"]} />
        </T>
      </div>

      {/* 2. ログイン */}
      <div id="sec-login" style={ms.section}>
        <div style={ms.h2}>2. ログイン</div>
        <p style={ms.p}>アプリにアクセスすると、ログイン画面が表示されます。管理者から通知されたメールアドレスとパスワードを入力し、「ログイン」ボタンをクリックしてください。</p>
        <div style={ms.note}>ログインに失敗した場合、エラーメッセージが表示されます。パスワードを何度も間違えるとアカウントが一時ロックされます。</div>
      </div>

      {/* 3. 画面構成 */}
      <div id="sec-layout" style={ms.section}>
        <div style={ms.h2}>3. 画面構成</div>
        <p style={ms.p}>画面左側のネイビーのサイドバーから各機能にアクセスします。「マスタ」「履歴」グループはクリックで開閉できます。</p>
        <p style={ms.p}>サイドバー下部にログイン中のメールアドレスと権限が表示されます。「ログアウト」ボタンでログアウトできます。</p>
        <T>
          <TH cols={["メニュー", "説明"]} />
          <TR cols={["📊 ダッシュボード", "全体概況・アラート"]} />
          <TR cols={["📈 売上管理", "売上集計・EC連携"]} />
          <TR cols={["📝 見積書一覧", "見積書の作成・管理"]} />
          <TR cols={["📦 納品書一覧", "納品書の作成・管理"]} />
          <TR cols={["🧾 請求書一覧", "請求書の管理・送信"]} />
          <TR cols={["📅 月締め管理", "締日ごとの一括請求"]} />
          <TR cols={["💰 残高管理", "未収残高・入金管理"]} />
          <TR cols={["⏳ 承認待ち", "承認フロー管理"]} />
          <TR cols={["🔄 定期請求", "定期請求の設定"]} />
          <TR cols={["🏢 取引先管理", "取引先情報の管理"]} />
          <TR cols={["🗂 商品マスタ", "商品情報の管理"]} />
          <TR cols={["💲 取引先別単価", "取引先ごとの単価設定"]} />
          <TR cols={["🏭 事業部管理", "事業部情報の管理"]} />
          <TR cols={["📨 送信履歴", "メール送信の履歴"]} />
          <TR cols={["📁 PDF履歴", "PDF保存の履歴"]} />
          <TR cols={["⚙ 設定", "自社情報・API連携"]} />
        </T>
      </div>

      {/* 4. ダッシュボード */}
      <div id="sec-dashboard" style={ms.section}>
        <div style={ms.h2}>4. ダッシュボード</div>
        <p style={ms.p}>ログイン後に最初に表示される画面です。業務の全体状況を把握できます。</p>
        <div style={ms.h3}>表示内容</div>
        <ul style={{ ...ms.p, paddingLeft: 20 }}>
          <li><strong>未入金アラート</strong> — 支払期限を1ヶ月以上超過した請求書がある場合、画面上部に警告が表示されます</li>
          <li><strong>サマリーカード</strong> — 取引先数、今月の納品件数、今月の売上合計、未収残高合計、期限超過件数</li>
          <li><strong>最近の納品書</strong> — 直近8件の納品書（番号・日付・取引先・金額・ステータス）</li>
        </ul>
      </div>

      {/* 5. 見積書一覧 */}
      <div id="sec-quotations" style={ms.section}>
        <div style={ms.h2}>5. 見積書一覧</div>
        <p style={ms.p}>見積書の作成・管理・納品書への変換を行います。</p>
        <div style={ms.h3}>見積書の作成</div>
        <p style={ms.p}>「+ 新規作成」ボタンをクリックし、以下の項目を入力します。</p>
        <T>
          <TH cols={["項目", "説明"]} />
          <TR cols={["取引先", "ドロップダウンから選択"]} />
          <TR cols={["事業部", "ドロップダウンから選択（任意）"]} />
          <TR cols={["見積日", "日付を入力"]} />
          <TR cols={["有効期限", "日付を入力"]} />
          <TR cols={["明細行", "商品を選択し、数量・単価を入力。「+ 行追加」で追加、「×」で削除"]} />
          <TR cols={["備考", "自由記述（任意）"]} />
        </T>
        <p style={ms.p}>商品を選択すると、単価と税率が自動セットされます（取引先別単価がある場合はそちらを優先）。小計・消費税・合計は税率別に自動計算されます。</p>
        <div style={ms.h3}>ステータス</div>
        <T>
          <TH cols={["ステータス", "意味"]} />
          <TR cols={[<span style={ms.badge("gold")}>作成済</span>, "作成直後の状態"]} />
          <TR cols={[<span style={ms.badge("green")}>受注済</span>, "納品書に変換済み"]} />
          <TR cols={[<span style={ms.badge("red")}>期限切れ</span>, "有効期限を過ぎた見積書"]} />
        </T>
        <div style={ms.h3}>操作ボタン</div>
        <T>
          <TH cols={["ボタン", "説明"]} />
          <TR cols={["編集", "見積書の内容を修正"]} />
          <TR cols={["🖨 印刷", "印刷プレビューを表示"]} />
          <TR cols={["📥 PDF", "PDFを生成してクラウドに保存"]} />
          <TR cols={["納品書作成", "見積書の内容をもとに納品書を自動作成（ワンクリック変換）"]} />
          <TR cols={["削除", "見積書を削除（管理者のみ）"]} />
        </T>
      </div>

      {/* 6. 納品書一覧 */}
      <div id="sec-deliveries" style={ms.section}>
        <div style={ms.h2}>6. 納品書一覧</div>
        <p style={ms.p}>納品書の作成・管理・請求書発行を行います。</p>
        <div style={ms.h3}>納品書の作成</div>
        <p style={ms.p}>「+ 新規作成」ボタンをクリックし、取引先・事業部・納品日・明細行を入力して「保存」をクリックします。見積書一覧から「納品書作成」で変換した場合は、見積書の内容がコピーされます。</p>
        <div style={ms.h3}>ステータス</div>
        <T>
          <TH cols={["ステータス", "意味"]} />
          <TR cols={[<span style={ms.badge("gold")}>未請求</span>, "請求書が未発行"]} />
          <TR cols={[<span style={ms.badge("blue")}>承認待ち</span>, "承認フロー待ち"]} />
          <TR cols={[<span style={ms.badge("green")}>請求済</span>, "請求書を発行済み"]} />
        </T>
        <div style={ms.h3}>請求書発行（即時請求の取引先）</div>
        <p style={ms.p}>「請求書発行」ボタンをクリックすると確認モーダルが表示されます。送信予定日を任意で設定し、「発行」をクリックすると請求書が自動作成されます。</p>
        <div style={ms.note}>締日請求の取引先の場合、ここからは発行できません。「月締め管理」画面から一括発行してください。</div>
      </div>

      {/* 7. 請求書一覧 */}
      <div id="sec-invoices" style={ms.section}>
        <div style={ms.h2}>7. 請求書一覧</div>
        <p style={ms.p}>請求書の管理・送信・入金記録を行います。上部に未収残高合計が表示されます。</p>
        <div style={ms.h3}>ステータス</div>
        <T>
          <TH cols={["ステータス", "意味"]} />
          <TR cols={[<span style={ms.badge("green")}>入金済</span>, "入金確認済み"]} />
          <TR cols={[<span style={ms.badge("gold")}>未収</span>, "未入金"]} />
          <TR cols={[<span style={ms.badge("red")}>期限超過</span>, "支払期限を過ぎた未入金"]} />
        </T>
        <p style={ms.p}>ステータスバッジをクリックすると、入金済⇔未収を切り替えられます。</p>
        <div style={ms.h3}>送信状態</div>
        <T>
          <TH cols={["状態", "意味"]} />
          <TR cols={[<span style={ms.badge("green")}>送信済</span>, "メール送信完了"]} />
          <TR cols={[<span style={ms.badge("blue")}>予約</span>, "送信予定日を設定済み"]} />
          <TR cols={[<span style={ms.badge()}>未送信</span>, "未送信"]} />
        </T>
        <div style={ms.h3}>操作ボタン</div>
        <T>
          <TH cols={["ボタン", "説明"]} />
          <TR cols={["🖨 印刷", "印刷プレビューを表示"]} />
          <TR cols={["送信 / 再送", "メールで請求書を送信（PDFリンク付き）"]} />
          <TR cols={["再請求 ▼", "メール督促 or Stripe決済で再請求"]} />
          <TR cols={["入金記録", "入金があった場合に記録"]} />
          <TR cols={["削除", "請求書を削除（管理者のみ）"]} />
        </T>
        <div style={ms.h3}>メール送信</div>
        <p style={ms.p}>「送信」ボタンをクリックすると、取引先のメールアドレスにPDFダウンロードリンク付きのメールが送信されます。送信履歴に自動記録されます。</p>
        <div style={ms.h3}>再請求（未入金の場合）</div>
        <p style={ms.p}><strong>メール再請求：</strong>「再請求 ▼」→「✉ メールで再請求」を選択。督促メッセージのテンプレートが表示されます（編集可能）。承認設定が有効な場合は「承認待ち」に登録されます。</p>
        <p style={ms.p}><strong>Stripe再請求：</strong>「再請求 ▼」→「💳 Stripeで請求」を選択。手数料（3.6%）が加算された金額が表示され、確認後にStripeの決済リンクが作成されます。</p>
      </div>

      {/* 8. 月締め管理 */}
      <div id="sec-monthly" style={ms.section}>
        <div style={ms.h2}>8. 月締め管理</div>
        <p style={ms.p}>締日請求の取引先に対して、期間内の納品書をまとめて請求書を発行する画面です。</p>
        <div style={ms.h3}>使い方</div>
        <ol style={{ ...ms.p, paddingLeft: 20 }}>
          <li>画面上部で対象月を選択します（例: 2026-03）</li>
          <li>締日請求の取引先がカード形式で一覧表示されます</li>
          <li>カードをクリックして展開すると、その期間の納品書が表示されます</li>
          <li>「請求書発行」ボタンで対象期間の納品書をまとめて1枚の請求書を作成します</li>
        </ol>
        <div style={ms.h3}>締日の仕組み</div>
        <p style={ms.p}>取引先ごとに最大4つの締日を設定できます（例: 15日・末日）。締日が複数ある場合、各期間ごとに請求書を分けて発行します。</p>
        <div style={ms.note}>例: 15日・末日の場合 → 1日〜15日分は15日締め請求書、16日〜末日分は末日締め請求書として発行</div>
      </div>

      {/* 9. 売上管理 */}
      <div id="sec-sales" style={ms.section}>
        <div style={ms.h2}>9. 売上管理</div>
        <p style={ms.p}>売上の集計・分析を行います。自社請求分に加え、楽天・Amazonの売上も統合表示できます。</p>
        <div style={ms.h3}>表示切替</div>
        <T>
          <TH cols={["ボタン", "説明"]} />
          <TR cols={["日別", "選択月の日別売上"]} />
          <TR cols={["月別", "直近12ヶ月の月別売上（棒グラフ付き）"]} />
          <TR cols={["年別", "年単位の売上集計"]} />
          <TR cols={["事業部別", "事業部ごとの12ヶ月推移"]} />
          <TR cols={["取引先別", "取引先ごとの12ヶ月推移"]} />
        </T>
        <div style={ms.h3}>EC売上連携</div>
        <T>
          <TH cols={["ボタン", "説明"]} />
          <TR cols={["楽天 今日同期", "楽天RMSから当日の売上を取得"]} />
          <TR cols={["Amazon 今日同期", "Amazon SP-APIから当日の売上を取得"]} />
          <TR cols={["CSV 売上取込", "外部売上データをCSVで取り込み"]} />
        </T>
        <div style={ms.h3}>CSV取込フォーマット</div>
        <div style={ms.code}>日付,売上,件数,ソース{"\n"}2026-03-01,50000,3,楽天{"\n"}2026-03-01,30000,2,Amazon</div>
        <p style={ms.p}>「日付」「売上/金額」列は必須。「件数」「ソース」列は任意です。</p>
      </div>

      {/* 10. 残高管理 */}
      <div id="sec-balance" style={ms.section}>
        <div style={ms.h2}>10. 残高管理</div>
        <p style={ms.p}>取引先ごとの未収残高の確認、入金記録、取引先元帳の表示を行います。</p>
        <div style={ms.h3}>画面上部</div>
        <ul style={{ ...ms.p, paddingLeft: 20 }}>
          <li><strong>未入金アラート</strong> — 支払期限を1ヶ月以上超過した取引先の一覧</li>
          <li><strong>サマリーカード</strong> — 未収残高合計、期限超過件数、対象取引先数、残高あり取引先数</li>
        </ul>
        <div style={ms.h3}>フィルター</div>
        <p style={ms.p}>「すべて」「未入金アラートのみ」「残高ありのみ」のボタンで表示を絞り込めます。</p>
        <div style={ms.h3}>操作ボタン</div>
        <T>
          <TH cols={["ボタン", "説明"]} />
          <TR cols={["入金記録", "入金日・入金額を入力して記録"]} />
          <TR cols={["期首残高", "基準日と期首残高を設定（導入時の繰越残高）"]} />
          <TR cols={["再請求 ▼", "メール督促 or Stripe決済で再請求"]} />
          <TR cols={["残高再計算", "残高を再計算（期首残高 + 請求合計 − 入金合計）"]} />
        </T>
        <div style={ms.h3}>取引先元帳</div>
        <p style={ms.p}>カードを展開すると、直近20件の取引履歴が表示されます。</p>
        <T>
          <TH cols={["列", "説明"]} />
          <TR cols={["日付", "取引日"]} />
          <TR cols={["摘要", "請求書番号 or 入金内容"]} />
          <TR cols={["借方", "請求額（赤字）"]} />
          <TR cols={["貸方", "入金額（緑字）"]} />
          <TR cols={["残高", "その時点の累計残高"]} />
        </T>
        <p style={ms.p}>入金行には「取消」ボタンがあり、誤入金を取り消せます（管理者のみ）。</p>
        <div style={ms.h3}>期首残高の設定</div>
        <p style={ms.p}>アプリ導入時に、それ以前の未収残高を登録する機能です。「期首残高」ボタンをクリックし、基準日と金額を入力して「保存」をクリックします。</p>
        <div style={ms.note}>期首残高を設定すると、元帳の最初の行に表示され、以降の残高計算の起点となります。</div>
      </div>

      {/* 11. 承認待ち */}
      <div id="sec-pending" style={ms.section}>
        <div style={ms.h2}>11. 承認待ち</div>
        <p style={ms.p}>承認フローが有効な場合、請求書発行・再請求などが「承認待ち」に登録されます。</p>
        <div style={ms.h3}>承認が必要なケース（設定による）</div>
        <ul style={{ ...ms.p, paddingLeft: 20 }}>
          <li>請求書の手動発行</li>
          <li>Cron（自動処理）による定期請求の発行</li>
          <li>再請求（メール・Stripe）</li>
        </ul>
        <div style={ms.h3}>タイプバッジ</div>
        <T>
          <TH cols={["バッジ", "意味"]} />
          <TR cols={[<span style={ms.badge("blue")}>請求書発行</span>, "通常の請求書発行"]} />
          <TR cols={[<span style={ms.badge("blue")}>定期</span>, "定期請求による発行"]} />
          <TR cols={[<span style={ms.badge("gold")}>締日</span>, "締日請求による発行"]} />
          <TR cols={[<span style={ms.badge("red")}>✉再請求</span>, "メール再請求"]} />
          <TR cols={[<span style={ms.badge("red")}>💳Stripe</span>, "Stripe再請求"]} />
        </T>
        <div style={ms.h3}>操作（管理者のみ）</div>
        <p style={ms.p}>「承認・発行」で請求書を発行（設定に応じてメールを自動送信）。「却下」で承認を拒否。処理済みタブで過去の承認・却下履歴を確認できます（直近20件）。</p>
      </div>

      {/* 12. 定期請求 */}
      <div id="sec-recurring" style={ms.section}>
        <div style={ms.h2}>12. 定期請求</div>
        <p style={ms.p}>毎月・隔月・四半期で自動的に請求書を発行する設定を行います。</p>
        <div style={ms.h3}>定期請求の登録</div>
        <T>
          <TH cols={["項目", "説明"]} />
          <TR cols={["取引先", "ドロップダウンから選択"]} />
          <TR cols={["事業部", "ドロップダウンから選択（任意）"]} />
          <TR cols={["品名", "請求する商品・サービス名"]} />
          <TR cols={["数量 / 単位 / 単価", "数量、単位（式・個など）、単価"]} />
          <TR cols={["税率", "10% / 8% / 0% / カスタム"]} />
          <TR cols={["サイクル", "毎月 / 隔月 / 四半期"]} />
          <TR cols={["請求日", "末日 or 1〜28日"]} />
          <TR cols={["送信モード", "自動送信 / 確認後送信 / 手動"]} />
          <TR cols={["開始月 / 終了月", "請求の開始月と終了月（空欄=無期限）"]} />
        </T>
        <div style={ms.h3}>操作ボタン</div>
        <T>
          <TH cols={["ボタン", "説明"]} />
          <TR cols={["👁 プレビュー", "請求書のプレビュー表示"]} />
          <TR cols={["編集", "設定を修正"]} />
          <TR cols={["有効 / 停止", "定期請求のON/OFF切替"]} />
          <TR cols={["削除", "定期請求を削除（管理者のみ）"]} />
        </T>
        <div style={ms.note}>定期請求はCron（自動処理）により、設定された請求日に自動発行されます。承認設定が有効な場合は「承認待ち」に登録されます。</div>
      </div>

      {/* 13. 取引先管理 */}
      <div id="sec-clients" style={ms.section}>
        <div style={ms.h2}>13. 取引先管理</div>
        <p style={ms.p}>取引先（お客様・仕入先）の情報を管理します。</p>
        <div style={ms.h3}>取引先の登録</div>
        <T>
          <TH cols={["項目", "説明"]} />
          <TR cols={["会社名 *", "取引先の名前（必須）"]} />
          <TR cols={["フリガナ", "カタカナ表記"]} />
          <TR cols={["住所", "所在地"]} />
          <TR cols={["電話番号", "連絡先電話番号"]} />
          <TR cols={["メール", "請求書の送信先メールアドレス"]} />
          <TR cols={["請求タイプ", "即時請求 or 締日請求"]} />
          <TR cols={["締日", "締日請求の場合、最大4つの締日を設定（末日 or 1〜28日）"]} />
          <TR cols={["締日処理", "自動送信 / 確認後送信 / 手動"]} />
          <TR cols={["事業部", "紐づける事業部"]} />
          <TR cols={["単発フラグ", "一度きりの取引先の場合にON"]} />
        </T>
        <div style={ms.h3}>請求タイプの違い</div>
        <T>
          <TH cols={["タイプ", "説明"]} />
          <TR cols={["即時請求", "納品書ごとに個別に請求書を発行"]} />
          <TR cols={["締日請求", "設定した締日ごとに、期間内の納品書をまとめて請求書を発行"]} />
        </T>
        <div style={ms.h3}>CSV入出力</div>
        <p style={ms.p}><strong>CSVインポート：</strong>「CSV取込」ボタンからCSVファイルを選択。既存の取引先は会社名で照合して上書き更新、新規は追加されます。</p>
        <p style={ms.p}><strong>CSV出力：</strong>「CSV出力」ボタンで全取引先をCSVダウンロード（BOM付きUTF-8）。</p>
      </div>

      {/* 14. 商品マスタ */}
      <div id="sec-products" style={ms.section}>
        <div style={ms.h2}>14. 商品マスタ</div>
        <p style={ms.p}>商品・サービスの情報を管理します。納品書・請求書の明細行で使用されます。</p>
        <div style={ms.h3}>商品の登録</div>
        <T>
          <TH cols={["項目", "説明"]} />
          <TR cols={["商品名 *", "商品の名前（必須）"]} />
          <TR cols={["商品コード *", "一意のコード（必須）"]} />
          <TR cols={["JANコード", "バーコード番号（任意）"]} />
          <TR cols={["単位", "個、袋、箱など"]} />
          <TR cols={["標準単価", "通常の販売単価"]} />
          <TR cols={["税率", "10% / 8% / 0% / カスタム"]} />
          <TR cols={["備考", "自由記述"]} />
        </T>
        <div style={ms.h3}>CSV取込</div>
        <p style={ms.p}>「CSV取込」ボタンからCSVファイルを選択。商品コードで照合して既存商品は上書き更新、新規は追加されます。</p>
        <div style={ms.note}>CSV内の単価に「¥」やカンマが含まれていても自動で除去されます。</div>
      </div>

      {/* 15. 取引先別単価 */}
      <div id="sec-clientprices" style={ms.section}>
        <div style={ms.h2}>15. 取引先別単価</div>
        <p style={ms.p}>取引先ごとに商品の特別単価を設定できます。設定された単価は、納品書作成時に標準単価より優先されます。</p>
        <div style={ms.h3}>単価の登録</div>
        <p style={ms.p}>「+ 新規作成」ボタンをクリックし、取引先・商品を選択して特別単価を入力します。</p>
        <p style={ms.p}>一覧では標準単価との差額が色分け表示されます（緑: 高い / 赤: 安い）。</p>
        <div style={ms.h3}>CSV入出力</div>
        <p style={ms.p}>CSVインポート時、商品のマッチングは商品コード → JANコード → 商品名の優先順で行われます。</p>
      </div>

      {/* 16. 事業部管理 */}
      <div id="sec-divisions" style={ms.section}>
        <div style={ms.h2}>16. 事業部管理</div>
        <p style={ms.p}>複数の事業部がある場合に、事業部ごとの情報を管理します。請求書の印刷時に、取引先に紐づいた事業部の情報が使用されます。</p>
        <div style={ms.h3}>事業部の登録</div>
        <T>
          <TH cols={["項目", "説明"]} />
          <TR cols={["事業部名 *", "事業部の名前（必須）"]} />
          <TR cols={["接頭辞 *", "書類番号に使用（一意、必須）"]} />
          <TR cols={["住所 / 電話 / FAX", "事業部の連絡先"]} />
          <TR cols={["インボイス登録番号", "適格請求書発行事業者番号"]} />
          <TR cols={["振込先", "銀行名・支店名・口座種別・口座番号・口座名義"]} />
        </T>
        <div style={ms.note}>事業部の振込先情報は、請求書のPDF・印刷に自動反映されます。</div>
      </div>

      {/* 17. 送信履歴 */}
      <div id="sec-sendhistory" style={ms.section}>
        <div style={ms.h2}>17. 送信履歴</div>
        <p style={ms.p}>メールで送信した請求書の履歴を確認できます。上部に送信件数の合計と今月の送信件数が表示されます。</p>
        <T>
          <TH cols={["列", "説明"]} />
          <TR cols={["送信日時", "メールを送信した日時"]} />
          <TR cols={["請求書番号", "対象の請求書番号"]} />
          <TR cols={["取引先", "送信先の取引先"]} />
          <TR cols={["金額", "請求金額"]} />
          <TR cols={["送信方法", "メール / 郵送 / FAX / 手渡し / その他"]} />
          <TR cols={["メモ", "送信時のメモ"]} />
        </T>
      </div>

      {/* 18. PDF履歴 */}
      <div id="sec-pdfhistory" style={ms.section}>
        <div style={ms.h2}>18. PDF履歴</div>
        <p style={ms.p}>PDFとして保存した書類の履歴を確認・ダウンロードできます。</p>
        <T>
          <TH cols={["ボタン", "説明"]} />
          <TR cols={["表示", "PDFを新しいタブで開く"]} />
          <TR cols={["DL", "PDFをダウンロード"]} />
          <TR cols={["削除", "PDFを削除（管理者のみ）"]} />
        </T>
      </div>

      {/* 19. 設定 */}
      <div id="sec-settings" style={ms.section}>
        <div style={ms.h2}>19. 設定</div>
        <p style={ms.p}>自社情報、外部連携、承認設定、ユーザー管理を行います。</p>
        <div style={ms.h3}>自社情報</div>
        <p style={ms.p}>会社名、電話番号、FAX、住所、インボイス登録番号、デフォルト税率を設定します。振込先の銀行情報も設定でき、請求書のPDF・印刷に反映されます。</p>
        <div style={ms.h3}>外部売上連携</div>
        <p style={ms.p}><strong>楽天RMS：</strong>serviceSecretとlicenseKeyを入力。「初期同期」ボタンで過去12ヶ月分の売上を一括取得できます。</p>
        <p style={ms.p}><strong>Amazon SP-API：</strong>Client ID、Client Secret、Refresh Tokenを入力。「初期同期」ボタンで過去12ヶ月分の売上を一括取得できます。</p>
        <div style={ms.h3}>承認設定</div>
        <T>
          <TH cols={["設定", "説明"]} />
          <TR cols={["請求書発行時に承認を必要とする", "ONにすると、手動での請求書発行が承認待ちに"]} />
          <TR cols={["定期請求の自動発行時に承認を必要とする", "ONにすると、Cronによる定期請求が承認待ちに"]} />
          <TR cols={["再請求時に承認を必要とする", "ONにすると、再請求が承認待ちに（デフォルトON）"]} />
        </T>
        <div style={ms.h3}>Stripe連携</div>
        <p style={ms.p}>Stripe Secret Keyを入力すると、Stripe経由の再請求機能が有効になります。</p>
        <div style={ms.h3}>ユーザー管理（管理者のみ）</div>
        <p style={ms.p}>ユーザーの一覧表示、権限変更（admin/staff）、削除、新規ユーザー追加ができます。新規追加時はメールアドレスとパスワード（6文字以上）が必要です。</p>
      </div>

      {/* 20. 自動処理 */}
      <div id="sec-cron" style={ms.section}>
        <div style={ms.h2}>20. 自動処理（Cron）</div>
        <p style={ms.p}>毎日 JST 3:00 に以下の処理が自動実行されます。</p>
        <T>
          <TH cols={["処理", "説明"]} />
          <TR cols={["締日請求", "前日が締日の取引先に対して、請求書を自動発行"]} />
          <TR cols={["定期請求", "対象月の定期請求を自動発行"]} />
          <TR cols={["送信予定日送信", "送信予定日が当日以前の請求書を自動メール送信"]} />
          <TR cols={["楽天同期", "楽天RMSの前日売上を自動取得"]} />
          <TR cols={["Amazon同期", "Amazon SP-APIの前日売上を自動取得"]} />
        </T>
        <div style={ms.note}>承認設定が有効な場合、締日請求・定期請求は「承認待ち」に登録され、管理者の承認後に発行されます。</div>
      </div>

      {/* 付録 */}
      <div id="sec-appendix" style={ms.section}>
        <div style={ms.h2}>付録</div>

        <div style={ms.h3}>書類番号の体系</div>
        <p style={ms.p}>書類番号は以下のルールで自動採番されます。事業部に接頭辞が設定されている場合、その接頭辞が使用されます。</p>
        <T>
          <TH cols={["書類", "形式", "例"]} />
          <TR cols={["見積書", "QT-YYYYMM-NNN", "QT-202603-001"]} />
          <TR cols={["納品書", "DL-YYYYMM-NNN", "DL-202603-001"]} />
          <TR cols={["請求書", "INV-YYYYMM-NNN", "INV-202603-001"]} />
        </T>

        <div style={ms.h3}>消費税の計算方法</div>
        <ul style={{ ...ms.p, paddingLeft: 20 }}>
          <li>明細行ごとに税率（10%/8%/0%/カスタム）が設定されます</li>
          <li>同じ税率の商品はグループ化され、税率別に消費税を計算します</li>
          <li>消費税は切り捨て（端数処理）で計算されます</li>
          <li>合計 = 税率別小計の合計 + 税率別消費税の合計</li>
        </ul>
        <div style={ms.h3}>計算例</div>
        <div style={ms.code}>商品A: 1,000円 x 3個 = 3,000円（税率10%）→ 税300円{"\n"}商品B: 500円 x 2個 = 1,000円（税率8%）→ 税80円{"\n"}────────────────{"\n"}小計: 4,000円 / 消費税: 380円 / 合計: 4,380円</div>

        <div style={ms.h3}>メール送信について</div>
        <ul style={{ ...ms.p, paddingLeft: 20 }}>
          <li>送信元: invoice@romu.ai</li>
          <li>PDFは添付ではなく、ダウンロードリンクとして送信されます</li>
          <li>リンクはクラウドに保存されたPDFへのURLです</li>
        </ul>
      </div>

      <div style={{ textAlign: "center", padding: "20px 0 40px", color: C.gray, fontSize: 12 }}>
        — 取扱説明書 ここまで —
      </div>
    </div>
  );
}
