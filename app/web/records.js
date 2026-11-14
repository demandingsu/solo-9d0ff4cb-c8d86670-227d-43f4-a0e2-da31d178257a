/* 称量记录查询 + 归档管理页：与采集页共用同一后端状态（顶栏可看到实时阶段/重量）。 */

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
  $("result-title").textContent = `称量记录（共 ${data.count} 条，按 ID 倒序）`;
  const body = $("records-body");
  if (data.count === 0) {
    body.innerHTML =
      '<tr><td colspan="7" class="empty">暂无记录——去“实时采集”页完成一次称量</td></tr>';
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
        <td>${
          r.source === "archive"
            ? '<span class="badge info"><span class="dot"></span>归档</span>'
            : '<span class="badge ok"><span class="dot"></span>在线</span>'
        }</td>
        <td>${fmtTime(r.recorded_at)}</td>
      </tr>`
    )
    .join("");
  (data.warnings || []).forEach((w) => toast(w, "error", 8000));
}

/* ------------------------------------------------------------------ */
/* 归档管理                                                             */
/* ------------------------------------------------------------------ */
const RUN_STATUS_TEXT = {
  running: ["执行中…", "info"],
  archived: ["已完成", "ok"],
  noop: ["无需归档（范围内无在线记录）", "warn"],
  error: ["失败", "bad"],
};

async function loadArchives() {
  try {
    const data = await api("/api/archives");
    renderArchives(data);
  } catch (err) {
    toast(`归档状态加载失败：${err.message}`, "error");
  }
}

function renderArchives(data) {
  $("arch-online-count").textContent = String(data.online_count);
  $("arch-archived-count").textContent = String(data.archived_count);
  $("arch-max-id").textContent = data.max_online_id > 0 ? String(data.max_online_id) : "—";
  $("arch-dir").textContent = data.archive_dir || "—";

  const lr = data.last_run;
  const box = $("archive-lastrun");
  if (!lr) {
    box.textContent = "尚未执行过归档";
    box.className = "archive-lastrun empty";
  } else {
    const [text, cls] = RUN_STATUS_TEXT[lr.status] || [lr.status, "info"];
    const range =
      lr.min_id !== null && lr.min_id !== undefined
        ? `｜范围 ID ${lr.min_id}–${lr.max_id}（${lr.row_count} 条 → ${lr.file_name}）`
        : "";
    const err = lr.error ? `｜错误：${lr.error}` : "";
    box.innerHTML = `最近归档 <span class="badge ${cls}"><span class="dot"></span>${text}</span>` +
      ` 截止 ID ${lr.cutoff_id}${range}${err}｜${fmtTime(lr.finished_at || lr.started_at)}`;
    box.className = "archive-lastrun";
  }

  const body = $("archive-body");
  if (!data.batches || data.batches.length === 0) {
    body.innerHTML = '<tr><td colspan="5" class="empty">暂无归档批次</td></tr>';
  } else {
    body.innerHTML = data.batches
      .map(
        (b) => `
        <tr>
          <td class="mono">${b.file_name}</td>
          <td>${b.min_id} – ${b.max_id}</td>
          <td>${b.row_count}</td>
          <td><span class="badge ok"><span class="dot"></span>已登记</span></td>
          <td>${fmtTime(b.created_at)}</td>
        </tr>`
      )
      .join("");
  }
  (data.warnings || []).forEach((w) => toast(w, "error", 8000));
}

async function doArchive() {
  const cutoff = Number($("archive-cutoff").value);
  if (!Number.isInteger(cutoff) || cutoff < 1) {
    toast("请输入正整数的截止记录 ID", "error");
    return;
  }
  const btn = $("btn-archive");
  btn.disabled = true;
  try {
    const data = await api("/api/archives", {
      method: "POST",
      body: JSON.stringify({ cutoff_id: cutoff }),
    });
    toast(data.message, data.ok ? "success" : "error", 6000);
  } catch (err) {
    toast(err.message, "error", 8000);
  } finally {
    btn.disabled = false;
  }
  // 归档后在线行已搬走：同时刷新记录查询与归档状态
  await Promise.all([query(), loadArchives()]);
}

async function failNextRegister() {
  try {
    const data = await api("/api/dev/fail-next-archive-register", {
      method: "POST",
      body: JSON.stringify({ count: 1 }),
    });
    toast(
      data.fail_remaining > 0
        ? "已注入：下一次归档“登记”将失败（在线行保留，不发布半成品）"
        : "归档登记故障注入已取消"
    );
  } catch (err) {
    toast(err.message, "error");
  }
}

function toggleAuto() {
  const btn = $("btn-auto");
  if (autoTimer) {
    clearInterval(autoTimer);
    autoTimer = null;
    btn.textContent = "自动刷新：关";
  } else {
    query();
    loadArchives();
    autoTimer = setInterval(() => {
      query();
      loadArchives();
    }, 2000);
    btn.textContent = "自动刷新：开（2s）";
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  $("btn-query").addEventListener("click", query);
  $("btn-auto").addEventListener("click", toggleAuto);
  $("btn-archive").addEventListener("click", doArchive);
  $("btn-archive-refresh").addEventListener("click", loadArchives);
  $("btn-archive-fail").addEventListener("click", failNextRegister);
  try {
    await loadSamples();
    await Promise.all([query(), loadArchives()]);
  } catch (err) {
    toast(err.message, "error");
  }
});
