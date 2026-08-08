const form = document.querySelector('#order-form');
const items = document.querySelector('#items');
const result = document.querySelector('#result');
const eventEmpty = document.querySelector('#event-empty');
const eventJson = document.querySelector('#event-json');
const eventPanel = document.querySelector('.event-panel');

const setStatus = (id, state, detail) => {
  const pill = document.querySelector(`#${id}-status`);
  const value = document.querySelector(`#${id}-status-detail`);
  pill.textContent = state;
  pill.classList.toggle('checking', state === 'CHECKING');
  value.textContent = detail;
};

const setFlow = (id, state) => {
  const step = document.querySelector(`#${id}-step`);
  if (!step) return;
  step.classList.toggle('active', state === 'ACTIVE');
  const label = step.querySelector('.flow-state');
  label.textContent = state;
};

async function refreshStatus() {
  try {
    const health = await fetch('/api/health');
    if (!health.ok) throw new Error('Order service unavailable');
    setStatus('order', 'ONLINE', 'Connected');
  } catch (error) {
    setStatus('order', 'OFFLINE', 'Unavailable');
  }

  try {
    const response = await fetch('/connectors/platform-postgres/status');
    const status = await response.json();
    const running = status.connector?.state === 'RUNNING' && status.tasks?.every(task => task.state === 'RUNNING');
    setStatus('cdc', running ? 'ONLINE' : 'CHECKING', running ? 'Connector running' : 'Starting connector');
  } catch (error) {
    setStatus('cdc', 'OFFLINE', 'Unavailable');
  }
}

function addItem() {
  const row = document.createElement('div');
  row.className = 'item-row';
  row.innerHTML = '<input name="product_id" value="prod-2" placeholder="Product ID" required><input name="quantity" value="1" type="number" min="1" placeholder="Qty" required><input name="unit_price_cents" value="2500" type="number" min="0" placeholder="Price (cents)" required><button type="button" class="remove-item" aria-label="Remove item">×</button>';
  items.append(row);
}

document.querySelector('#add-item').addEventListener('click', addItem);
items.addEventListener('click', (event) => {
  if (event.target.classList.contains('remove-item') && items.children.length > 1) event.target.parentElement.remove();
});

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const button = document.querySelector('#submit-order');
  button.disabled = true;
  button.querySelector('span').textContent = 'Sending...';
  result.hidden = true;
  setFlow('postgres', 'ACTIVE');

  const data = new FormData(form);
  const productIds = data.getAll('product_id');
  const quantities = data.getAll('quantity');
  const prices = data.getAll('unit_price_cents');
  const payload = {
    customer_id: data.get('customer_id'),
    items: productIds.map((productId, index) => ({
      product_id: productId,
      quantity: Number(quantities[index]),
      unit_price_cents: Number(prices[index])
    }))
  };

  try {
    const response = await fetch('/api/orders', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
    const body = await response.json();
    if (!response.ok) throw new Error(body.message || 'Order could not be created');
    result.className = 'result';
    result.innerHTML = `Created <strong>${body.id}</strong> · $${(body.total_cents / 100).toFixed(2)} · ${body.status}`;
    result.hidden = false;
    eventEmpty.hidden = true;
    eventJson.hidden = false;
    eventJson.textContent = JSON.stringify({ after: body, op: 'c', table: 'orders', note: 'Waiting for the actual Kafka CDC envelope...' }, null, 2);
    eventPanel.classList.add('success');
    setFlow('postgres', 'DONE');
    setFlow('debezium', 'ACTIVE');
    setFlow('kafka', 'ACTIVE');
    setTimeout(() => { setFlow('debezium', 'DONE'); setFlow('kafka', 'DONE'); }, 1200);
    waitForActualEvent();
  } catch (error) {
    result.className = 'result error';
    result.textContent = error.message;
    result.hidden = false;
    setFlow('postgres', 'WAITING');
  } finally {
    button.disabled = false;
    button.querySelector('span').textContent = 'Create order';
  }
});

async function waitForActualEvent(attempt = 0) {
  if (attempt > 12) return;
  try {
    const response = await fetch('/api/events/latest');
    if (!response.ok) throw new Error('not ready');
    eventJson.textContent = JSON.stringify(await response.json(), null, 2);
  } catch (error) {
    setTimeout(() => waitForActualEvent(attempt + 1), 500);
  }
}

document.querySelector('#refresh').addEventListener('click', refreshStatus);
setInterval(() => { document.querySelector('#clock').textContent = new Date().toLocaleTimeString([], {hour12:false}); }, 1000);
refreshStatus();
