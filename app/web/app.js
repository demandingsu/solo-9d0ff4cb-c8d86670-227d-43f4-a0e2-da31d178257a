/* 实时采集页逻辑：轮询服务端单例状态 + 转发模拟器控制动作。 */

const els = {};
let lastEventCount = 0;
let lastLockedSample = null;

function $(id) {
  return document.getElementById(id);
}

async function loadSamples() {
  try {
    const samples = await api("/api/samples");
    const sel = $("sample-select");
    const current = sel.value;
    sel.innerHTML =
      '<option value="">— 请选择样品 —</option>' +
      samples
        .map((s) => `<option value="${s.code}">${s.code}｜${s.name}</option>`)
        .join("");
    if (current) sel.value = current;
  } catch (err) {
    toast(`样品列表加载失败：${err.message}`, "error");
  }
}

async function chooseSample() {
  const code = $("sample-select").value;
  if (!code) {
    toast("请先选择一个样品", "error");
    return;
  }
  try {
    await api("/api/samples/select", {
      method: "POST",
      body: JSON.stringify({ sample_code: code }),
    });
    toast(`已选择样品 ${code}`);
  } catch (err) {
    toast(err.message, "error", 6000);
  }
}

async function simControl(action, extra = {}) {
  try {
    const data = await api("/api/simulator/control", {
      method: "POST",
      body: JSON.stringify({ action, ...extra }),
    });
    toast(data.message || action);
  } catch (err) {
    toast(`模拟器控制失败：${err.message}`, "error");
  }
}

async function setFailWrites(count) {
  try {
    const data = await api("/api/dev/fail-next-writes", {
      method: "POST",
      body: JSON.stringify({ count }),
    });
    toast(
      data.fail_remaining > 0
        ? `故障注入已开启，剩余 ${data.fail_remaining} 次写入将失败`
        : "故障注入已取消"
    );
  } catch (err) {
    toast(err.message, "error");
  }
}

function renderStatus(s) {
  $("weight-value").textContent = fmtWeight(s.current_weight);
  $("weight-value").style.color = s.stable ? "var(--text)" : "var(--amber)";

  const phaseBadge = $("badge-phase");
  phaseBadge.textContent = PHASE_TEXT[s.phase] || s.phase;
  phaseBadge.className = `badge ${PHASE_BADGE[s.phase] || "info"}`;

  const stableBadge = $("badge-stable");
  stableBadge.textContent = s.stable ? "稳定" : "不稳定";
  stableBadge.className = `badge ${s.stable ? "ok" : "warn"}`;

  const connBadge = $("badge-conn");
  connBadge.textContent = s.connected ? "已连接" : "未连接";
  connBadge.className = `badge ${s.connected ? "ok" : "bad"}`;

  $("kv-selected").textContent = s.selected_sample || "—";
  $("kv-locked").textContent = s.locked_sample
    ? `${s.locked_sample}（锁定）`
    : "—";
  $("kv-recorded").textContent = s.recorded
    ? "✅ 已记录（拒绝重复记）"
    : s.phase === "HOLDING"
      ? "⏳ 等待/重试入库"
      : "—";
  $("kv-attempts").textContent = String(s.record_attempts);
  $("kv-reading-at").textContent = fmtTime(s.last_reading_at);
  $("kv-error").textContent = s.last_error || "—";

  const locked = s.phase === "HOLDING";
  $("locked-note").classList.toggle("hidden", !locked);
  $("select-locked-note").classList.toggle("hidden", !locked);
  $("sample-select").disabled = locked;
  $("btn-select").disabled = locked;

  // 锁定样品变化时同步下拉框显示
  if (locked && s.locked_sample) {
    lastLockedSample = s.locked_sample;
    $("sample-select").value = s.locked_sample;
  } else if (!locked && lastLockedSample) {
    lastLockedSample = null;
  }

  // 事件环增量渲染（新事件在尾部）
  const log = $("event-log");
  const events = s.events || [];
  if (events.length === 0) {
    log.innerHTML = '<div class="empty">暂无事件</div>';
  } else if (events.length !== lastEventCount || log.querySelector(".empty")) {
    log.innerHTML = events
      .map(
        (e) =>
          `<div class="event-line ${e.level}"><span class="t">${fmtTime(
            e.ts
          )}</span><span class="m">${e.message}</span></div>`
      )
      .join("");
    log.scrollTop = log.scrollHeight;
  }
  lastEventCount = events.length;
}

async function poll() {
  try {
    const s = await api("/api/status");
    renderStatus(s);
  } catch (err) {
    $("badge-conn").textContent = "后端不可达";
    $("badge-conn").className = "badge bad";
  }
}

function bindEvents() {
  $("btn-select").addEventListener("click", chooseSample);
  $("btn-place").addEventListener("click", () => {
    const weight = Number($("place-weight").value);
    if (!Number.isFinite(weight) || weight <= 0) {
      toast("放样重量必须是正数", "error");
      return;
    }
    simControl("place", { weight });
  });
  $("btn-jitter-on").addEventListener("click", () => simControl("jitter_on"));
  $("btn-jitter-off").addEventListener("click", () => simControl("jitter_off"));
  $("btn-zero").addEventListener("click", () => simControl("zero"));
  $("btn-frag-on").addEventListener("click", () => simControl("fragment_on"));
  $("btn-frag-off").addEventListener("click", () => simControl("fragment_off"));
  $("btn-disconnect").addEventListener("click", () => simControl("disconnect"));
  $("btn-reconnect").addEventListener("click", () => simControl("reconnect"));
  $("btn-fail-on").addEventListener("click", () => setFailWrites(3));
  $("btn-fail-off").addEventListener("click", () => setFailWrites(0));
}

document.addEventListener("DOMContentLoaded", async () => {
  bindEvents();
  await loadSamples();
  poll();
  setInterval(poll, 500);
});
