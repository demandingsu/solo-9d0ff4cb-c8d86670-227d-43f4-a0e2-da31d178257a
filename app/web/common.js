/* 两个页面共用的工具：请求封装、提示、时间格式化、顶栏状态轮询。 */

async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let data = null;
  try {
    data = await resp.json();
  } catch (_) {
    /* 非 JSON 响应 */
  }
  if (!resp.ok) {
    const detail = (data && (data.detail || data.error)) || `HTTP ${resp.status}`;
    const err = new Error(detail);
    err.status = resp.status;
    throw err;
  }
  return data;
}

function toast(message, level = "success", ttl = 4000) {
  let box = document.querySelector(".toast");
  if (!box) {
    box = document.createElement("div");
    box.className = "toast";
    document.body.appendChild(box);
  }
  const item = document.createElement("div");
  item.className = `toast-item ${level}`;
  item.textContent = message;
  box.appendChild(item);
  setTimeout(() => item.remove(), ttl);
}

function fmtTime(epochSeconds) {
  if (epochSeconds === null || epochSeconds === undefined) return "—";
  const d = new Date(epochSeconds * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(
    d.getHours()
  )}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function fmtWeight(w) {
  if (w === null || w === undefined) return "----";
  return Number(w).toFixed(4);
}

const PHASE_TEXT = {
  WAIT_ZERO: "等待稳定零位",
  READY: "零位就绪",
  HOLDING: "样品锁定中",
};

const PHASE_BADGE = {
  WAIT_ZERO: "warn",
  READY: "ok",
  HOLDING: "purple",
};

/**
 * 在顶栏渲染连接/阶段/当前重量——两个页面打开时各自轮询同一个后端，
 * 看到的是同一份服务端采集状态。
 */
async function pollHeaderStatus() {
  const el = document.getElementById("header-status");
  if (!el) return;
  try {
    const s = await api("/api/status");
    const connClass = s.connected ? "ok" : "bad";
    const connText = s.connected ? "TCP 已连接" : "TCP 未连接";
    const phaseClass = PHASE_BADGE[s.phase] || "info";
    const stableClass = s.stable ? "ok" : "warn";
    const stableText = s.stable ? "稳定" : "不稳定";
    el.innerHTML = `
      <span class="badge ${connClass}"><span class="dot"></span>${connText}</span>
      <span class="badge ${phaseClass}"><span class="dot"></span>${
      PHASE_TEXT[s.phase] || s.phase
    }</span>
      <span class="badge ${stableClass}"><span class="dot"></span>${stableText}</span>
      <span>实时重量：<strong>${fmtWeight(s.current_weight)}</strong> g</span>
    `;
  } catch (err) {
    el.innerHTML = `<span class="badge bad"><span class="dot"></span>后端不可达：${err.message}</span>`;
  }
}

document.addEventListener("DOMContentLoaded", () => {
  pollHeaderStatus();
  setInterval(pollHeaderStatus, 1000);
});
