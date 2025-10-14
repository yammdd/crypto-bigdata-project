const symbols = [
  "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
  "adausdt", "dogeusdt", "linkusdt", "dotusdt", "maticusdt"
];

function createCard(data, prediction, symbol) {
  const card = document.createElement("div");
  card.className = "crypto-card";

  const title = document.createElement("h2");
  title.textContent = symbol.toUpperCase();

  const details = document.createElement("div");
  details.className = "crypto-details";

  const attrs = [
    "data_price", "data_open_price", "data_high_price", "data_low_price",
    "data_best_ask_price", "data_best_bid_price", "data_volume_quote", "data_volume_token",
    "data_price_change", "data_price_change_pct"
  ];

  attrs.forEach(attr => {
    if (data && data[attr]) {
      const p = document.createElement("p");
      p.textContent = `${attr.replace("data_", "").replace(/_/g, " ")}: ${data[attr]}`;
      details.appendChild(p);
    }
  });

  const pred = document.createElement("div");
  pred.className = "prediction";
  pred.textContent = `Dự đoán (next): ${prediction?.predicted_price ?? "?"}`;

  card.appendChild(title);
  card.appendChild(details);
  card.appendChild(pred);
  return card;
}

async function main() {
  const container = document.getElementById("crypto-container");

  for (let symbol of symbols) {
    try {
      const res1 = await fetch(`/api/crypto/${symbol}`);
      const data = await res1.json();
      const latest = Array.isArray(data) ? data.at(-1) : null;

      const res2 = await fetch(`/api/crypto/predictions/${symbol}`);
      const pred = await res2.json();

      if (latest) {
        const card = createCard(latest, pred, symbol);
        container.appendChild(card);
      }
    } catch (err) {
      console.error(`Error for ${symbol}:`, err);
    }
  }
}

main();
setInterval(main, 1000);