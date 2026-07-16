const { chromium } = require("@playwright/test");
(async () => {
  const url = process.argv[2] || "http://localhost:4173";
  const browser = await chromium.launch({ channel: "msedge" });
  const page = await browser.newPage();
  const errors = [];
  page.on("console", msg => { if (msg.type() === "error") errors.push("CONSOLE: " + msg.text().slice(0, 200)); });
  page.on("pageerror", err => errors.push("PAGEERROR: " + err.message));
  await page.goto(url, { waitUntil: "load", timeout: 30000 }).catch(e => errors.push("GOTO: " + e.message));
  await page.waitForTimeout(3000);
  const bodyText = (await page.textContent("body").catch(() => "")) || "";
  console.log(url, "| BODY:", bodyText.trim().length, "| ERRORS:", errors.length);
  errors.slice(0, 5).forEach(e => console.log(e));
  await browser.close();
  process.exit(errors.length ? 1 : 0);
})();
