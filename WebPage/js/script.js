(() => {
  const button = document.getElementById("cta");
  const status = document.getElementById("status");

  button.addEventListener("click", () => {
    const now = new Date();
    status.textContent = `Page loaded successfully at ${now.toLocaleString()}`;
  });
})();

(() => {
  const PRODUCT_URL = "https://activitylearning1-383161404298.s3.amazonaws.com/products/web/sales_by_country.json";

  const money = (v) =>
    v.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
  const num = (v) => v.toLocaleString("en-US");

  const render = (product, panel) => {
    const rows = product.datos
      .map(
        (r) => `<tr>
          <td>${r.country}</td>
          <td class="num">${num(r.ventas)}</td>
          <td class="num">${money(r.ingresos)}</td>
          <td class="num">${money(r.ticket_promedio)}</td>
        </tr>`
      )
      .join("");

    panel.innerHTML = `
      <h2>Sales by country</h2>
      <table>
        <thead>
          <tr>
            <th>Country</th>
            <th class="num">Sales</th>
            <th class="num">Revenue</th>
            <th class="num">Avg. ticket</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
      <p class="muted provenance">
        ${num(product.ventas_totales)} sales ·
        ${product.cobertura.desde} to ${product.cobertura.hasta} ·
        generated ${new Date(product.generado_en).toLocaleString()}
      </p>`;
  };

  const panel = document.getElementById("sales-by-country");
  if (!panel) return;

  fetch(`${PRODUCT_URL}?v=${Date.now()}`)
    .then((response) => {
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    })
    .then((product) => render(product, panel))
    .catch((error) => {
      panel.innerHTML = `<h2>Sales by country</h2>
        <p class="error">Could not load the data product: ${error.message}</p>`;
    });
})();
