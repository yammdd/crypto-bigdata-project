const symbols = [
  "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
  "adausdt", "dogeusdt", "linkusdt", "dotusdt", "ltcusdt"
];

const attrs = [
  "data_price", "data_best_ask_price", "data_best_bid_price", "data_price_change",
  "data_price_change_pct", "data_volume_quote", "data_volume_token"
];

const cards = {};
const previousData = {};

const charts = {};

// Hàm hỗ trợ để định dạng timestamp thành chuỗi thời gian dễ đọc
function formatTimestampToTime(timestamp) {
  const date = new Date(timestamp * 1000); // timestamp từ HBase thường là giây
  return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
}

function createCard(symbol) {
  const baseAsset = symbol.replace('usdt', '');

  const container = document.createElement("div");
  container.className = "crypto-card-container";

  const cardInfo = document.createElement("div");
  cardInfo.className = "crypto-card";
  cardInfo.id = symbol;

  const header = document.createElement("div");
  header.className = "card-header";
  
  const icon = document.createElement("img");
  icon.className = "crypto-icon";
  icon.src = `/static/icons/${baseAsset}.png`;
  icon.alt = `${baseAsset}`;

  const title = document.createElement("h2");
  title.textContent = `${baseAsset.toUpperCase()}/USDT`;
  header.appendChild(icon);
  header.appendChild(title);
  
  const body = document.createElement("div");
  body.className = "card-body";

  const priceWrapper = document.createElement("div");
  priceWrapper.className = 'price-wrapper';
  
  const priceEl = document.createElement("p");
  priceEl.id = `${symbol}_data_price`;
  priceEl.className = "price-large";
  priceEl.innerHTML = `<span class="value">?</span>`;

  const priceChangePctEl = document.createElement("p");
  priceChangePctEl.id = `${symbol}_data_price_change_pct`;
  priceChangePctEl.className = "price-change-pct";
  priceChangePctEl.innerHTML = ` &nbsp; 24h: <span class="value">?</span>`;
  
  priceWrapper.appendChild(priceEl);
  priceWrapper.appendChild(priceChangePctEl);

  const priceChangeEl = document.createElement("p");
  priceChangeEl.id = `${symbol}_data_price_change`;
  priceChangeEl.className = "info-line";
  priceChangeEl.innerHTML = `Price Change (24h): <span class="value">?</span>`;

  const bidAskEl = document.createElement("p");
  bidAskEl.id = `${symbol}_bid_ask`;
  bidAskEl.className = "info-line";
  bidAskEl.innerHTML = `Bid/Ask: <span class="value">? / ?</span>`;

  const spreadEl = document.createElement("p");
  spreadEl.id = `${symbol}_spread`;
  spreadEl.className = "info-line";
  spreadEl.innerHTML = `Spread: <span class="value">?</span>`;

  const volumeQuoteEl = document.createElement("p");
  volumeQuoteEl.id = `${symbol}_data_volume_quote`;
  volumeQuoteEl.className = "info-line";
  volumeQuoteEl.innerHTML = `Volume (USDT): <span class="value">?</span>`;

  const volumeTokenEl = document.createElement("p");
  volumeTokenEl.id = `${symbol}_data_volume_token`;
  volumeTokenEl.className = "info-line";
  volumeTokenEl.innerHTML = `Volume (${baseAsset.toUpperCase()}): <span class="value">?</span>`;

  body.appendChild(priceWrapper);
  body.appendChild(priceChangeEl);
  body.appendChild(bidAskEl);
  body.appendChild(spreadEl);
  body.appendChild(volumeQuoteEl);
  body.appendChild(volumeTokenEl);
  
  const pred = document.createElement("div");
  pred.className = "prediction";
  pred.id = `${symbol}_prediction`;
  pred.innerHTML = `Predicted Price: <span class="value">?</span>`;

  cardInfo.appendChild(header);
  cardInfo.appendChild(body);
  cardInfo.appendChild(pred);

  const cardChart = document.createElement("div");
  cardChart.className = "crypto-card crypto-card-chart hidden";
  
  const chartCanvas = document.createElement("canvas");
  chartCanvas.id = `${symbol}-chart`;
  cardChart.appendChild(chartCanvas);

  container.appendChild(cardInfo);
  container.appendChild(cardChart);

  container.addEventListener('click', () => {
    cardInfo.classList.toggle('hidden');
    cardChart.classList.toggle('hidden');
  });

  document.getElementById("crypto-container").appendChild(container);
  
  drawChart(symbol);
}

// Sửa đổi hàm drawChart để khởi tạo và lưu trữ biểu đồ
function drawChart(symbol) {
    const ctx = document.getElementById(`${symbol}-chart`).getContext('2d');
    charts[symbol] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [], // Sẽ được điền bởi dữ liệu lịch sử
            datasets: [{
                label: `${symbol.toUpperCase()} Price`,
                data: [], // Sẽ được điền bởi dữ liệu lịch sử
                fill: false,
                borderColor: 'rgb(75, 192, 192)',
                tension: 0.1,
                pointRadius: 0 // Ẩn các điểm dữ liệu
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true, // Hiển thị legend
                    labels: {
                        color: 'white' // Màu chữ legend
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (context.parsed.y !== null) {
                                label += formatPrice(context.parsed.y);
                            }
                            return label;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { color: 'grey' },
                    grid: { color: 'rgba(200, 200, 200, 0.1)' }
                },
                y: {
                    ticks: {
                        color: 'grey',
                        callback: function(value, index, values) {
                            return formatPrice(value); // Định dạng giá trên trục y
                        }
                    },
                    grid: { color: 'rgba(200, 200, 200, 0.1)' }
                }
            }
        }
    });
}

// Hàm mới để cập nhật biểu đồ với dữ liệu lịch sử và real-time
async function updateChartData(symbol) {
    try {
        const res = await fetch(`/api/crypto/history/${symbol}`);
        const historicalData = await res.json();

        if (historicalData && historicalData.length > 0) {
            const labels = historicalData.map(d => formatTimestampToTime(d.timestamp));
            const dataPoints = historicalData.map(d => d.price);

            if (charts[symbol]) {
                charts[symbol].data.labels = labels;
                charts[symbol].data.datasets[0].data = dataPoints;
                charts[symbol].update();
            }
        }
    } catch (err) {
        console.error(`Error updating chart for ${symbol}:`, err);
    }
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

function formatPrice(price) {
  if (price >= 100) {
    return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  } else if (price >= 10) {
    return price.toLocaleString('en-US', { minimumFractionDigits: 3, maximumFractionDigits: 3 });
  } else {
    return price.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 4 });
  }
}

async function updateLive(symbol) {
  try {
    const res = await fetch(`/api/crypto/${symbol}`);
    const data = await res.json();
    const latest = Array.isArray(data) ? data.at(-1) : null;

    if (latest) {
      if (!previousData[symbol]) previousData[symbol] = {};
      
      const price = parseFloat(latest.data_price);
      const bid = parseFloat(latest.data_best_bid_price);
      const ask = parseFloat(latest.data_best_ask_price);
      const priceChange = parseFloat(latest.data_price_change);
      const pctChange = parseFloat(latest.data_price_change_pct);
      const spread = ask - bid;
      const volumeQuote = parseFloat(latest.data_volume_quote);
      const volumeToken = parseFloat(latest.data_volume_token);
      
      const priceEl = document.querySelector(`#${symbol}_data_price .value`);
      const pctChangeEl = document.querySelector(`#${symbol}_data_price_change_pct .value`);
      const priceChangeEl = document.querySelector(`#${symbol}_data_price_change .value`);
      const bidAskEl = document.querySelector(`#${symbol}_bid_ask .value`);
      const spreadEl = document.querySelector(`#${symbol}_spread .value`);
      const volumeQuoteEl = document.querySelector(`#${symbol}_data_volume_quote .value`);
      const volumeTokenEl = document.querySelector(`#${symbol}_data_volume_token .value`);
      
      updateColor(priceEl, price, previousData[symbol].price);
      
      if (pctChangeEl) {
          pctChangeEl.classList.remove("price-up", "price-down");
          if (pctChange > 0) {
            pctChangeEl.classList.add("price-up");
          } else if (pctChange < 0) {
            pctChangeEl.classList.add("price-down");
          }
      }

      updateColor(priceChangeEl, priceChange, previousData[symbol].priceChange);
      updateColor(bidAskEl, bid, previousData[symbol].bid);
      updateColor(spreadEl, spread, previousData[symbol].spread);
      updateColor(volumeQuoteEl, volumeQuote, previousData[symbol].volumeQuote);
      updateColor(volumeTokenEl, volumeToken, previousData[symbol].volumeToken);

      if(priceEl) priceEl.textContent = formatPrice(price);
      
      if(pctChangeEl) pctChangeEl.textContent = `${pctChange.toFixed(3)}%`;
      if(priceChangeEl) priceChangeEl.textContent = priceChange.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 });
      if(bidAskEl) bidAskEl.textContent = `${bid.toLocaleString('en-US')} / ${ask.toLocaleString('en-US')}`;
      if(spreadEl) spreadEl.textContent = spread.toFixed(Math.max(2, (spread.toString().split('.')[1] || []).length));
      if(volumeQuoteEl) volumeQuoteEl.textContent = Math.round(volumeQuote).toLocaleString('en-US');
      if(volumeTokenEl) volumeTokenEl.textContent = Math.round(volumeToken).toLocaleString('en-US');

      previousData[symbol].price = price;
      previousData[symbol].pctChange = pctChange;
      previousData[symbol].priceChange = priceChange;
      previousData[symbol].bid = bid;
      previousData[symbol].ask = ask;
      previousData[symbol].spread = spread;
      previousData[symbol].volumeQuote = volumeQuote;
      previousData[symbol].volumeToken = volumeToken;
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
      value_span.textContent = newPredValue.toLocaleString('en-US', { minimumFractionDigits: 4 });

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
    updateChartData(symbol); // Gọi hàm này để tải dữ liệu ban đầu cho biểu đồ

    setInterval(() => updateLive(symbol), 3000);
    setInterval(() => updatePrediction(symbol), 10000);
    // Cập nhật biểu đồ thường xuyên hơn để giữ cho nó gần real-time
    setInterval(() => updateChartData(symbol), 3000); // Cập nhật mỗi 3 giây
  });
}

main();

document.addEventListener('DOMContentLoaded', (event) => {
  const chatForm = document.getElementById('chat-form');
  const userInput = document.getElementById('user-input');
  const chatMessages = document.getElementById('chat-messages');
  const chatContainer = document.querySelector('.chat-container');
  const toggleChatBtn = document.getElementById('toggle-chat-btn');

  if (chatForm && userInput && chatMessages && chatContainer && toggleChatBtn) {
    chatContainer.classList.add('minimized');
    toggleChatBtn.textContent = '';
    
    toggleChatBtn.addEventListener('click', () => {
      chatContainer.classList.toggle('minimized');
      toggleChatBtn.textContent = chatContainer.classList.contains('minimized') ? '' : '-';
    });
  
    function addMessage(text, sender) {
      const loading = document.querySelector('.loading-message');
      if (loading) loading.remove();
      const messageElement = document.createElement('div');
      messageElement.classList.add('message', `${sender}-message`);
      if (sender === 'bot') {
        messageElement.innerHTML = typeof marked !== 'undefined' ? marked.parse(text) : text;
      } else {
        const p = document.createElement('p');
        p.textContent = text;
        messageElement.appendChild(p);
      }
      chatMessages.appendChild(messageElement);
      chatMessages.scrollTop = chatMessages.scrollHeight;
    }
  
    function showLoadingIndicator() {
      const loadingElement = document.createElement('div');
      loadingElement.classList.add('message', 'bot-message', 'loading-message');
      const flashingDot = document.createElement('div');
      flashingDot.classList.add('dot-flashing');
      loadingElement.appendChild(flashingDot);
      chatMessages.appendChild(loadingElement);
      chatMessages.scrollTop = chatMessages.scrollHeight;
    }
  
    async function handleFormSubmit(e) {
      e.preventDefault();
      const userText = userInput.value.trim();
      if (!userText) return;
      addMessage(userText, 'user');
      userInput.value = '';
      showLoadingIndicator();
      try {
        const res = await fetch('/api/chatbot/ask', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question: userText }),
        });
        if (!res.ok) throw new Error(`Server responded with status: ${res.status}`);
        const data = await res.json();
        addMessage(data.answer || data.error || "Sorry, I couldn't get a response.", 'bot');
      } catch (error) {
        addMessage("Rất tiếc, tôi đang gặp sự cố kết nối. Vui lòng thử lại sau.", 'bot');
      }
    }
  
    chatForm.addEventListener('submit', handleFormSubmit);
  }

  const themeToggleBtn = document.getElementById('theme-toggle-btn');
  const body = document.body;
  
  function applyTheme(theme) {
    if (theme === 'light') {
        body.classList.add('light-mode');
        body.classList.remove('dark-mode');
    } else {
        body.classList.add('dark-mode');
        body.classList.remove('light-mode');
    }
  }

  const savedTheme = localStorage.getItem('theme') || 'dark';
  applyTheme(savedTheme);

  if (themeToggleBtn) {
    themeToggleBtn.addEventListener('click', () => {
      const newTheme = body.classList.contains('dark-mode') ? 'light' : 'dark';
      localStorage.setItem('theme', newTheme);
      applyTheme(newTheme);
    });
  }
});