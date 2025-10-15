const symbols = [
  "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
  "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
];

const attrs = [
  "data_price", "data_open_price", "data_high_price", "data_low_price",
  "data_best_ask_price", "data_best_bid_price", "data_volume_quote", "data_volume_token",
  "data_price_change", "data_price_change_pct"
];

const cards = {};

function createCard(symbol) {
  const card = document.createElement("div");
  card.className = "crypto-card";
  card.id = symbol;

  const title = document.createElement("h2");
  title.textContent = symbol.toUpperCase();
  card.appendChild(title);

  const details = document.createElement("div");
  details.className = "crypto-details";
  attrs.forEach(attr => {
    const p = document.createElement("p");
    p.id = `${symbol}_${attr}`;
    p.textContent = `${attr.replace("data_", "").replace(/_/g, " ")}: ?`;
    details.appendChild(p);
  });
  card.appendChild(details);

  const pred = document.createElement("div");
  pred.className = "prediction";
  pred.id = `${symbol}_prediction`;
  pred.textContent = `Dự đoán (next): ?`;
  card.appendChild(pred);

  document.getElementById("crypto-container").appendChild(card);
  cards[symbol] = card;
}

// Hàm cập nhật dữ liệu thực tế (2s/lần)
async function updateLive(symbol) {
  try {
    const res = await fetch(`/api/crypto/${symbol}`);
    const data = await res.json();
    const latest = Array.isArray(data) ? data.at(-1) : null;

    if (latest) {
      attrs.forEach(attr => {
        const el = document.getElementById(`${symbol}_${attr}`);
        if (el) el.textContent = `${attr.replace("data_", "").replace(/_/g, " ")}: ${latest[attr]}`;
      });
    }
  } catch (err) {
    console.error(`Live update error for ${symbol}:`, err);
  }
}

// Hàm cập nhật dự đoán (10s/lần)
async function updatePrediction(symbol) {
  try {
    const res = await fetch(`/api/crypto/predictions/${symbol}`);
    const pred = await res.json();
    const predEl = document.getElementById(`${symbol}_prediction`);

    if (predEl && pred && pred.predicted_price !== undefined) {
      predEl.textContent = `Dự đoán (next): ${pred.predicted_price}`;
    }
  } catch (err) {
    console.error(`Prediction update error for ${symbol}:`, err);
  }
}

function main() {
  symbols.forEach(symbol => {
    createCard(symbol);
    updateLive(symbol);
    updatePrediction(symbol);

    setInterval(() => updateLive(symbol), 1500);
    setInterval(() => updatePrediction(symbol), 5000);
  });
}

main();
