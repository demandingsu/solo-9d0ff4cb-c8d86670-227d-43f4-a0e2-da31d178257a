/* 称量记录查询页：与采集页共用同一后端状态（顶栏可看到实时阶段/重量）。 */

let autoTimer = null;

function $(id) {
  return document.getElementById(id);
}

async function loadSamples() {
  const samples = await api("/api/samples");
  const sel = $("filter-sample");
  sel.innerHTML =
    '<option value="">全部样品</option>' +
    samples
      .map((s) => `<option value="${s.code}">${s.code}｜${s.name}</option>`)
      .join("");
}

async function query() {
  const sample = $("filter-sample").value;
  const limit = Number($("filter-limit").value) || 100;
  const params = new URLSearchParams({ limit: String(limit) });
  if (sample) params.set("sample_code", sample);
  try {
    const data = await api(`/api/weighings?${params.toString()}`);
    render(data);
  } catch (err) {
    toast(`查询失败：${err.message}`, "error");
  }
}

function render(data) {
  $("result-title").textContent = `称量记录（共 ${data.count} 条，按时间倒序）`;
  const body = $("records-body");
  if (data.count === 0) {
    body.innerHTML =
      '<tr><td colspan="6" class="empty">暂无记录——去“实时采集”页完成一次称量</td></tr>';
    return;
  }
  body.innerHTML = data.items
    .map(
      (r) => `
      <tr>
        <td>${r.id}</td>
        <td>${r.sample_code}</td>
        <td>${r.sample_name}</td>
        <td>${Number(r.weight_g).toFixed(4)}</td>
        <td>${
          r.stable
            ? '<span class="badge ok"><span class="dot"></span>稳定</span>'
            : '<span class="badge warn"><span class="dot"></span>不稳定</span>'
        }</td>
        <td>${fmtTime(r.recorded_at)}</td>
      </tr>`
    )
    .join("");
}

function toggleAuto() {
  const btn = $("btn-auto");
  if (autoTimer) {
    clearInterval(autoTimer);
    autoTimer = null;
    btn.textContent = "自动刷新：关";
  } else {
    query();
    autoTimer = setInterval(query, 2000);
    btn.textContent = "自动刷新：开（2s）";
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  $("btn-query").addEventListener("click", query);
  $("btn-auto").addEventListener("click", toggleAuto);
  try {
    await loadSamples();
    await query();
  } catch (err) {
    toast(err.message, "error");
  }
});
