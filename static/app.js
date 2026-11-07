// 데모 채팅 화면. 빌드 단계 없이 브라우저 기본 기능(EventSource, fetch)만 쓴다.
(() => {
  const $ = (id) => document.getElementById(id);
  const list = $("messages");
  const LEDGER_KEY = "chat.ledger.v1";

  // ---- 이 브라우저가 보낸 메시지의 장부 (유실 검증용) ----
  // client_msg_id -> { body, acked, seen }
  const ledger = new Map(JSON.parse(localStorage.getItem(LEDGER_KEY) || "[]"));
  const saveLedger = () => localStorage.setItem(LEDGER_KEY, JSON.stringify([...ledger]));

  const author = $("author");
  author.value = localStorage.getItem("chat.author") || `guest-${Math.random().toString(36).slice(2, 6)}`;
  author.addEventListener("change", () => localStorage.setItem("chat.author", author.value.trim()));

  // crypto.randomUUID는 HTTPS에서만 쓸 수 있어서, 사설 IP로 접속하는 경우를 위해 직접 만든다
  const newId = () => [...crypto.getRandomValues(new Uint8Array(16))].map((x) => x.toString(16).padStart(2, "0")).join("");

  // ---- 색 ----
  const DB_COLORS = { "pg-a": "var(--pg-a)", "pg-b": "var(--pg-b)" };
  const hue = (s) => [...String(s)].reduce((h, c) => (h * 31 + c.charCodeAt(0)) >>> 0, 7) % 360;
  const dbColor = (db) => DB_COLORS[db] || `hsl(${hue(db)} 60% 60%)`;
  const avatarColor = (name) => `hsl(${hue(name)} 55% 45%)`;

  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  };
  const fmtTime = (iso) => (iso ? new Date(iso).toLocaleTimeString("ko-KR", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }) : "");

  let reconnects = 0;
  let pending = 0;

  function renderCounters() {
    let acked = 0, seen = 0;
    for (const e of ledger.values()) { if (e.acked) acked++; if (e.seen) seen++; }
    $("v-sent").textContent = ledger.size;
    $("v-acked").textContent = acked;
    $("v-seen").textContent = seen;
    $("v-pending").textContent = pending;
    $("v-reconnects").textContent = reconnects;
  }

  // ---- 메시지 표시 ----
  const rendered = new Map(); // client_msg_id -> li
  let lastRoute = null; // 직전에 받은 메시지의 { app_env, db_node }

  function stickToBottom(fn) {
    const atBottom = list.scrollHeight - list.scrollTop - list.clientHeight < 120;
    fn();
    if (atBottom) list.scrollTop = list.scrollHeight;
  }

  // DB나 앱 환경이 바뀐 지점에 구분선을 넣는다. 이전 과정이 대화 기록에 그대로 남는다
  function dividerFor(prev, m) {
    if (!prev) return null;
    const parts = [];
    let kind = "app";
    if (prev.db_node !== m.db_node) { parts.push(["DB 전환", prev.db_node, m.db_node]); kind = "db"; }
    if (prev.app_env !== m.app_env) parts.push(["앱 전환", prev.app_env, m.app_env]);
    if (!parts.length) return null;
    const li = el("li", `divider ${kind}`);
    const tag = el("span", "tag");
    parts.forEach(([label, from, to], i) => {
      if (i) tag.append(" · ");
      tag.append(`${label} `, el("b", "", from), " → ", el("b", "", to));
    });
    li.append(tag);
    return li;
  }

  function renderMessage(m, { isPending = false } = {}) {
    let li = rendered.get(m.client_msg_id);
    const isNew = !li;
    if (isNew) {
      li = el("li");
      rendered.set(m.client_msg_id, li);
    }
    const mine = ledger.get(m.client_msg_id);
    li.className = `msg${mine ? " mine" : ""}${isPending ? " pending" : ""}`;
    li.replaceChildren();

    const avatar = el("div", "avatar", (m.author || "?").slice(0, 1).toUpperCase());
    avatar.style.setProperty("--c", avatarColor(m.author));

    const wrap = el("div", "bubble-wrap");
    const head = el("div", "head");
    const bubble = el("div", "bubble", m.body);
    wrap.append(head, bubble);
    if (m.translation) wrap.append(el("div", "translation", m.translation));

    // 이름 · 시각 · 경로(앱 → DB)를 한 줄에 둔다
    const route = el("span", "route");
    if (isPending) {
      route.append("전송 중…");
    } else {
      const dot = el("span", "dot");
      dot.style.setProperty("--db", dbColor(m.db_node));
      route.append(dot, m.app_env, el("span", "arrow", "→"), m.db_node);
    }
    if (mine) {
      route.append(el("span", `tick${mine.seen ? " seen" : ""}`, mine.seen ? "✓✓" : mine.acked ? "✓" : "…"));
    }
    head.append(el("span", "who", m.author), el("span", "time", fmtTime(m.created_at)), route);
    li.append(avatar, wrap);

    if (isNew) stickToBottom(() => list.append(li));
  }

  function onStreamMessage(m) {
    const divider = dividerFor(lastRoute, m);
    lastRoute = { app_env: m.app_env, db_node: m.db_node };
    const existing = rendered.get(m.client_msg_id); // 내가 보낸 메시지는 이미 "전송 중"으로 그려져 있다
    if (divider) stickToBottom(() => (existing ? existing.before(divider) : list.append(divider)));
    renderMessage(m);
  }

  // ---- 수신: SSE ----
  // EventSource는 네트워크 오류면 Last-Event-ID를 붙여 스스로 재접속하지만,
  // 서버가 200이 아닌 상태(예: 503 draining)를 돌려주면 영구히 닫힌다. 그 경우 직접 다시 연다.
  let lastId = 0;
  let es;
  const conn = $("conn");

  function setConn(text, ok) {
    $("conn-text").textContent = text;
    conn.classList.toggle("down", !ok);
  }

  function onStatus(ev) {
    const s = JSON.parse(ev.data);
    setConn(`앱 ${s.app_env} · DB ${s.db_node}`, true);
  }

  function connect() {
    es = new EventSource(lastId ? `/api/stream?after=${lastId}` : "/api/stream");
    es.addEventListener("hello", onStatus);
    es.addEventListener("status", onStatus);
    es.addEventListener("bye", () => setConn("서버 교체 중… 재접속합니다", false));
    es.addEventListener("message", (ev) => {
      const m = JSON.parse(ev.data);
      if (m.id <= lastId) return;
      lastId = m.id;
      const mine = ledger.get(m.client_msg_id);
      if (mine && !mine.seen) { mine.seen = true; mine.acked = true; saveLedger(); }
      onStreamMessage(m);
      renderCounters();
    });
    es.onerror = () => {
      setConn("연결 끊김 · 재접속 중…", false);
      reconnects++;
      renderCounters();
      if (es.readyState === EventSource.CLOSED) setTimeout(connect, 1000);
    };
  }
  connect();

  // ---- 전송: 같은 client_msg_id로 성공할 때까지 재전송 (서버가 중복을 막는다) ----
  async function send(id, payload) {
    let delay = 300;
    for (;;) {
      try {
        const res = await fetch("/api/messages", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (res.ok) {
          const m = await res.json();
          ledger.get(id).acked = true;
          saveLedger();
          if (!ledger.get(id).seen) renderMessage(m);
          return;
        }
        if (res.status >= 400 && res.status < 500) {
          notice(`전송 거부: ${(await res.json()).error}`);
          return;
        }
      } catch (_) { /* 네트워크 오류: 재시도 */ }
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, 3000);
    }
  }

  function notice(text) {
    stickToBottom(() => list.append(el("li", "notice", text)));
  }

  $("composer").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const body = $("body").value.trim();
    const name = author.value.trim() || "guest";
    if (!body) return;
    $("body").value = "";
    localStorage.setItem("chat.author", name);

    const id = newId();
    ledger.set(id, { body, acked: false, seen: false });
    saveLedger();
    renderMessage({ client_msg_id: id, author: name, body }, { isPending: true });
    pending++;
    renderCounters();
    await send(id, { client_msg_id: id, author: name, body });
    pending--;
    renderCounters();
  });

  // ---- 검증 ----
  $("verify").addEventListener("click", async () => {
    const ids = [...ledger.keys()];
    const out = $("v-result");
    if (!ids.length) { out.className = "result"; out.textContent = "아직 보낸 메시지가 없습니다."; return; }
    try {
      const res = await fetch("/api/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ client_msg_ids: ids }),
      });
      const r = await res.json();
      const lost = r.missing.length;
      out.className = `result ${lost ? "bad" : "ok"}`;
      out.textContent = lost ? `보낸 ${r.checked}건 중 ${lost}건이 DB에 없습니다` : `보낸 ${r.checked}건 모두 DB에 있습니다 · 유실 0`;
    } catch (e) {
      out.className = "result bad";
      out.textContent = `대조 실패: ${e}`;
    }
  });

  function legend(ul, entries, color, total) {
    ul.replaceChildren();
    for (const [k, v] of entries) {
      const li = el("li");
      const sw = el("span", "sw");
      sw.style.background = color(k);
      li.append(sw, el("span", "k", k), el("span", "v", `${v} · ${total ? Math.round((v / total) * 100) : 0}%`));
      ul.append(li);
    }
  }

  async function refreshStats() {
    try {
      const st = await (await fetch("/api/stats")).json();
      const dbs = Object.entries(st.by_db_node).sort();
      const apps = Object.entries(st.by_app_env).sort();
      $("total").textContent = `총 ${st.total}건`;
      const bar = $("dbbar");
      bar.replaceChildren(...dbs.map(([k, v]) => {
        const s = el("span");
        s.style.width = `${st.total ? (v / st.total) * 100 : 0}%`;
        s.style.background = dbColor(k);
        s.title = `${k}: ${v}`;
        return s;
      }));
      legend($("dblegend"), dbs, dbColor, st.total);
      legend($("applegend"), apps, (k) => `hsl(${hue(k)} 45% 55%)`, st.total);
    } catch (_) { /* 전환 중에는 잠시 실패할 수 있다 */ }
  }
  setInterval(refreshStats, 3000);
  refreshStats();
  renderCounters();
})();
