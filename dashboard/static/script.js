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

const previousData = {};

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
    const label = attr.replace("data_", "").replace(/_/g, " ");
    p.innerHTML = `${label}: <span class="value">?</span>`;
    details.appendChild(p);
  });
  card.appendChild(details);

  const pred = document.createElement("div");
  pred.className = "prediction";
  pred.id = `${symbol}_prediction`;
  pred.innerHTML = `Dự đoán (next): <span class="value">?</span>`;
  card.appendChild(pred);

  document.getElementById("crypto-container").appendChild(card);
  cards[symbol] = card;
}

function updateColor(element, newValue, oldValue) {
  if (element && !isNaN(newValue) && oldValue !== undefined && !isNaN(oldValue)) {
    element.classList.remove("price-up", "price-down");
    if (newValue > oldValue) {
      element.classList.add("price-up");
    } else if (newValue < oldValue) {
      element.classList.add("price-down");
    }
  }
}

async function updateLive(symbol) {
  try {
    const res = await fetch(`/api/crypto/${symbol}`);
    const data = await res.json();
    const latest = Array.isArray(data) ? data.at(-1) : null;

    if (latest) {
      if (!previousData[symbol]) {
        previousData[symbol] = {};
      }

      attrs.forEach(attr => {
        const p_element = document.getElementById(`${symbol}_${attr}`);
        const value_span = p_element ? p_element.querySelector('.value') : null;

        if (value_span) {
          const newValue = parseFloat(latest[attr]);
          const oldValue = previousData[symbol][attr];

          updateColor(value_span, newValue, oldValue);

          value_span.textContent = newValue;
          previousData[symbol][attr] = newValue;
        }
      });
    }
  } catch (err) {
    console.error(`Live update error for ${symbol}:`, err);
  }
}

async function updatePrediction(symbol) {
  try {
    const res = await fetch(`/api/crypto/predictions/${symbol}`);
    const pred = await res.json();
    const predElContainer = document.getElementById(`${symbol}_prediction`);
    const value_span = predElContainer ? predElContainer.querySelector('.value') : null;

    if (value_span && pred && pred.predicted_price !== undefined) {
      const newPredValue = parseFloat(pred.predicted_price);
      const oldPredValue = previousData[symbol] ? previousData[symbol].prediction : undefined;

      updateColor(value_span, newPredValue, oldPredValue);

      value_span.textContent = newPredValue;

      if (!previousData[symbol]) previousData[symbol] = {};
      previousData[symbol].prediction = newPredValue;
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