"use strict";

// ═══════════════════════════════════════════════════════════════
//  CONSTANTS
// ═══════════════════════════════════════════════════════════════

const WEEKDAY_LABELS  = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
const MONTH_LABELS    = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const PAGE_SIZE       = 40;   // messages rendered per batch
const SEARCH_DEBOUNCE = 180;  // ms

const INTEREST_PALETTE = {
  software_engineering:        "gold",
  ai_ml_llm:                   "rose",
  web_frontend:                "sky",
  security_privacy:            "rose",
  data_engineering_analytics:  "sky",
  open_source_community:       "sage",
  product_management:          "gold",
  project_delivery:            "gold",
  startup_business:            "rose",
  communication_coordination:  "sage",
  humor_memes:                 "sage",
  philosophy_thinking:         "sky",
  ops_observability:           "gold",
  cloud_platforms:             "sky",
  health_wellness:             "sage",
  gaming:                      "rose",
  media_entertainment:         "sky",
  travel_geography:            "sage",
  finance_crypto:              "gold",
  education_learning:          "sky",
};

// ═══════════════════════════════════════════════════════════════
//  STATE
// ═══════════════════════════════════════════════════════════════

const state = {
  chats:        [],
  users:        [],
  snapshot:     null,
  sourceLabel:  "",
  apiAvailable: true,
  // messages virtual scroll
  allMessages:      [],
  filteredMessages: [],
  renderedCount:    0,
  searchQuery:      "",
  // sentinel observer
  sentinelObserver: null,
};

// ═══════════════════════════════════════════════════════════════
//  DOM CACHE
// ═══════════════════════════════════════════════════════════════

const els = {};

function cacheElements() {
  const ids = [
    "statusBadge","statusText","sourceHint",
    "chatSelect","userSelect","loadBtn","refreshBtn","fileInput",
    "chatUsersCount","bestActivity",
    // hero
    "heroName","heroMeta","heroSummary","heroBadges","heroEyebrow",
    // persona sidebar
    "sidebarPersona","scoreRingSvg","scoreRingFill","scoreRingText",
    "personaRole","personaTone","personaConf","personaTraits",
    "personaInterests","personaTriggers","personaTriggersSection","personaPhrases",
    // kpi
    "kpiGrid","behaviorGrid",
    // charts
    "hourChart","weekdayChart","monthChart","hourPeak",
    "dailyTrend","dateRangeLabel",
    // lexicon
    "topWords","smartWords",
    // relations
    "incomingRelations","outgoingRelations","incomingCount","outgoingCount",
    // topics
    "topicsPanel","topicsCount","topicsList",
    // content
    "contentSection","urlsList","mentionsList","urlsCount","mentionsCount",
    // messages
    "messageFilter","clearSearch","messageCount","messageFeed","feedSentinel","scrollTopBtn",
  ];
  ids.forEach(id => { els[id] = document.getElementById(id); });
  els.emptyTemplate = document.getElementById("emptyTemplate");
}

// ═══════════════════════════════════════════════════════════════
//  INIT
// ═══════════════════════════════════════════════════════════════

document.addEventListener("DOMContentLoaded", () => {
  cacheElements();
  bindEvents();
  renderInitialState();
  refreshSources();
});

function bindEvents() {
  els.refreshBtn.addEventListener("click", refreshSources);
  els.loadBtn.addEventListener("click", loadSelectedSnapshot);
  els.chatSelect.addEventListener("change", () => {
    if (state.apiAvailable) loadUsersForSelectedChat();
  });

  // File upload
  els.fileInput.addEventListener("change", async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    try {
      setStatus("reading json", "busy");
      const text = await file.text();
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== "object") throw new Error("Not a valid snapshot.");
      state.snapshot = parsed;
      state.sourceLabel = `file:${file.name}`;
      renderSnapshot();
      setStatus("manual snapshot loaded", "ok");
      els.sourceHint.textContent = "Manual mode active. Refresh Sources to switch back.";
    } catch (err) {
      console.error(err);
      setStatus("invalid json", "warn");
      els.sourceHint.textContent = "Could not parse JSON. Load a valid user snapshot.";
    } finally {
      e.target.value = "";
    }
  });

  // Search — debounced
  const debouncedSearch = debounce(() => {
    state.searchQuery = els.messageFilter.value.trim().toLowerCase();
    els.clearSearch.hidden = !state.searchQuery;
    rebuildMessageFilter();
    renderMessageBatch(true);
  }, SEARCH_DEBOUNCE);

  els.messageFilter.addEventListener("input", debouncedSearch);

  els.clearSearch.addEventListener("click", () => {
    els.messageFilter.value = "";
    state.searchQuery = "";
    els.clearSearch.hidden = true;
    rebuildMessageFilter();
    renderMessageBatch(true);
    els.messageFilter.focus();
  });

  // Keyboard shortcut: / focuses search
  document.addEventListener("keydown", (e) => {
    if (e.key === "/" && document.activeElement !== els.messageFilter
        && !["INPUT","SELECT","TEXTAREA"].includes(document.activeElement.tagName)) {
      e.preventDefault();
      els.messageFilter.focus();
      els.messageFilter.select();
    }
    if (e.key === "Escape" && document.activeElement === els.messageFilter) {
      els.messageFilter.blur();
    }
  });

  // Scroll-to-top button
  els.messageFeed.addEventListener("scroll", () => {
    els.scrollTopBtn.hidden = els.messageFeed.scrollTop < 300;
  });
  els.scrollTopBtn.addEventListener("click", () => {
    els.messageFeed.scrollTo({ top: 0, behavior: "smooth" });
  });
}

function renderInitialState() {
  setStatus("idle", "idle");
  const lists = [els.kpiGrid, els.behaviorGrid, els.hourChart, els.weekdayChart,
                 els.monthChart, els.topWords, els.smartWords,
                 els.incomingRelations, els.outgoingRelations, els.messageFeed];
  lists.forEach(el => renderEmpty(el, "No data loaded yet."));
  clearSvg(els.dailyTrend);
}

// ═══════════════════════════════════════════════════════════════
//  API
// ═══════════════════════════════════════════════════════════════

async function refreshSources() {
  setStatus("scanning exports", "busy");
  state.apiAvailable = true;
  try {
    const chats = await fetchJSON("/api/chats");
    state.chats = Array.isArray(chats) ? chats : [];
    populateChatSelect();
    if (!state.chats.length) {
      state.users = [];
      populateUserSelect();
      els.chatUsersCount.textContent = "—";
      els.bestActivity.textContent = "—";
      els.sourceHint.textContent = "No snapshots found. Run user-snapshot and refresh.";
      setStatus("no exports", "warn");
      return;
    }
    await loadUsersForSelectedChat();
    setStatus(`${state.chats.length} chat${state.chats.length !== 1 ? "s" : ""}`, "ok");
  } catch (err) {
    console.error(err);
    state.apiAvailable = false;
    state.chats = [];
    state.users = [];
    populateChatSelect();
    populateUserSelect();
    els.chatUsersCount.textContent = "—";
    els.bestActivity.textContent = "—";
    els.sourceHint.textContent = "API offline. Use manual JSON upload.";
    setStatus("api offline", "warn");
  }
}

function populateChatSelect() {
  const prev = els.chatSelect.value;
  els.chatSelect.innerHTML = "";
  if (!state.chats.length) {
    appendOption(els.chatSelect, "", "No source detected");
    els.chatSelect.disabled = true;
    return;
  }
  for (const chat of state.chats) {
    const val = `${chat.account_id}|${chat.chat_id}`;
    appendOption(els.chatSelect, val, `${chat.account_id} / ${chat.chat_id} (${chat.users_count} users)`);
  }
  els.chatSelect.disabled = false;
  const hasPrev = state.chats.some(c => `${c.account_id}|${c.chat_id}` === prev);
  els.chatSelect.value = hasPrev ? prev : `${state.chats[0].account_id}|${state.chats[0].chat_id}`;
}

async function loadUsersForSelectedChat() {
  const src = selectedSource();
  if (!src) { state.users = []; populateUserSelect(); return; }
  setStatus("loading users", "busy");
  const params = new URLSearchParams({ account: src.account, chat: String(src.chat) });
  const users = await fetchJSON(`/api/users?${params}`);
  state.users = Array.isArray(users) ? users : [];
  populateUserSelect();
  els.chatUsersCount.textContent = formatNum(state.users.length);
  const best = state.users.reduce((acc, u) => Math.max(acc, Number(u.activity_score) || 0), 0);
  els.bestActivity.textContent = best.toFixed(2);
  if (!state.users.length) {
    els.sourceHint.textContent = "No user snapshots in this chat. Run user-snapshot first.";
    setStatus("no users", "warn");
  } else {
    const top = state.users[0];
    els.sourceHint.textContent = `Top: ${displayUser(top)} · ${formatNum(top.messages_total)} msgs`;
    setStatus(`${state.users.length} users`, "ok");
  }
}

function populateUserSelect() {
  const prev = els.userSelect.value;
  els.userSelect.innerHTML = "";
  if (!state.users.length) {
    appendOption(els.userSelect, "", "Choose chat first");
    els.userSelect.disabled = true;
    els.loadBtn.disabled = true;
    return;
  }
  for (const u of state.users) {
    appendOption(els.userSelect, String(u.user_id), `${displayUser(u)} · ${formatNum(u.messages_total)} msgs`);
  }
  els.userSelect.disabled = false;
  els.loadBtn.disabled = false;
  const hasPrev = state.users.some(u => String(u.user_id) === prev);
  els.userSelect.value = hasPrev ? prev : String(state.users[0].user_id);
}

async function loadSelectedSnapshot() {
  const src = selectedSource();
  const uid = parseInt(els.userSelect.value, 10);
  if (!src || !isFinite(uid) || uid <= 0) return;
  setStatus("loading snapshot", "busy");
  const params = new URLSearchParams({ account: src.account, chat: String(src.chat), user_id: String(uid) });
  try {
    state.snapshot = await fetchJSON(`/api/user?${params}`);
    state.sourceLabel = `${src.account}/${src.chat}/user_${uid}`;
    renderSnapshot();
    setStatus("snapshot ready", "ok");
  } catch (err) {
    console.error(err);
    setStatus("snapshot failed", "warn");
    els.sourceHint.textContent = "Failed to load snapshot. Check state directory.";
  }
}

// ═══════════════════════════════════════════════════════════════
//  RENDER SNAPSHOT (orchestrator)
// ═══════════════════════════════════════════════════════════════

function renderSnapshot() {
  if (!state.snapshot || typeof state.snapshot !== "object") return;
  const { profile = {}, stats = {}, persona = {} } = state.snapshot;

  const name = nonEmpty(profile.display_name, [
    [profile.first_name, profile.last_name].filter(Boolean).join(" "),
    profile.username ? `@${profile.username}` : "",
    profile.user_id ? `user_${profile.user_id}` : "Unknown",
  ]);

  // Hero
  els.heroName.textContent = name;
  els.heroEyebrow.textContent = profile.username ? `@${profile.username}` : "Operator Card";
  els.heroMeta.textContent =
    `Source: ${state.sourceLabel || "manual"} · ID: ${profile.user_id || "n/a"} · Generated: ${fmtDate(state.snapshot.generated_at)}`;
  els.heroSummary.textContent = nonEmpty(persona.summary, [
    "Snapshot loaded. Explore temporal activity, lexical behavior, social graph, and message stream.",
  ]);
  renderHeroBadges(persona, stats);

  // Sidebar persona
  renderPersonaSidebar(persona, stats);

  // KPIs
  renderKPI(stats);
  renderBehaviorGrid(stats);

  // Charts
  renderBarChart(els.hourChart, numArr(stats.messages_by_hour, 24), Array.from({length:24}, (_,i)=>String(i).padStart(2,"0")));
  const peakHour = argmax(numArr(stats.messages_by_hour, 24));
  els.hourPeak.textContent = `peak ${String(peakHour).padStart(2,"0")}:00 · ${formatNum(stats.messages_by_hour?.[peakHour] ?? 0)}`;

  renderBarChart(els.weekdayChart, numArr(stats.messages_by_weekday, 7), WEEKDAY_LABELS);
  renderBarChart(els.monthChart,   numArr(stats.messages_by_month, 12),  MONTH_LABELS);
  renderDailyTrend(stats.messages_by_date || {});

  // Lexicon
  renderWordList(els.topWords,   (stats.top_words   || []).slice(0, 15), "count",  "");
  renderWordList(els.smartWords, (stats.smart_words || []).slice(0, 15), "score", "smart");

  // Social
  renderRelations(els.incomingRelations, state.snapshot.incoming_relations || []);
  renderRelations(els.outgoingRelations, state.snapshot.outgoing_relations || []);
  els.incomingCount.textContent = formatNum((state.snapshot.incoming_relations || []).length);
  els.outgoingCount.textContent = formatNum((state.snapshot.outgoing_relations || []).length);

  // Topics
  renderTopics(state.snapshot.topics || []);

  // Content signals
  renderContent(state.snapshot.content || {});

  // Messages (virtual scroll)
  setupMessageFeed(state.snapshot.recent_messages || [], profile.user_id);
}

// ═══════════════════════════════════════════════════════════════
//  HERO BADGES
// ═══════════════════════════════════════════════════════════════

function renderHeroBadges(persona, stats) {
  els.heroBadges.innerHTML = "";
  const items = [];
  if (persona.role)  items.push({ text: persona.role, cls: "badge--gold" });
  if (persona.tone)  items.push({ text: persona.tone, cls: "badge--sky" });
  if (Array.isArray(persona.traits)) persona.traits.slice(0, 3).forEach(t => items.push({ text: t, cls: "badge--sage" }));
  if (isFinite(Number(stats.activity_score)))
    items.push({ text: `activity ${Number(stats.activity_score).toFixed(2)}`, cls: "badge--rose" });
  if (!items.length) items.push({ text: "profile loaded", cls: "" });

  items.forEach(({ text, cls }) => {
    const span = document.createElement("span");
    span.className = `badge ${cls}`;
    span.textContent = text;
    els.heroBadges.appendChild(span);
  });
}

// ═══════════════════════════════════════════════════════════════
//  PERSONA SIDEBAR
// ═══════════════════════════════════════════════════════════════

function renderPersonaSidebar(persona, stats) {
  els.sidebarPersona.hidden = false;

  // Score ring
  const score = Math.min(1, Math.max(0, Number(stats.activity_score) || 0));
  const circ = 2 * Math.PI * 26; // r=26
  els.scoreRingFill.style.strokeDashoffset = (circ * (1 - score)).toFixed(2);
  els.scoreRingText.textContent = Math.round(score * 100);

  els.personaRole.textContent = persona.role || "—";
  els.personaTone.textContent = persona.tone || "—";
  els.personaConf.textContent = `confidence: ${isFinite(Number(persona.confidence)) ? Number(persona.confidence).toFixed(2) : "—"}`;

  // Traits
  els.personaTraits.innerHTML = "";
  (persona.traits || []).forEach(t => {
    const b = document.createElement("span");
    b.className = "badge badge--gold";
    b.textContent = t;
    els.personaTraits.appendChild(b);
  });

  // Interests
  els.personaInterests.innerHTML = "";
  (persona.interests || []).slice(0, 12).forEach(interest => {
    const cls = INTEREST_PALETTE[interest] || "sky";
    const b = document.createElement("span");
    b.className = `badge badge--${cls}`;
    b.textContent = interest.replace(/_/g, " ");
    els.personaInterests.appendChild(b);
  });

  // Triggers
  const triggers = persona.triggers || [];
  els.personaTriggersSection.hidden = !triggers.length;
  els.personaTriggers.innerHTML = "";
  triggers.forEach(t => {
    const b = document.createElement("span");
    b.className = "badge badge--rose";
    b.textContent = t.replace(/_/g, " ");
    els.personaTriggers.appendChild(b);
  });

  // Typical phrases
  els.personaPhrases.innerHTML = "";
  (persona.typical_phrases || []).slice(0, 3).forEach(phrase => {
    if (!phrase) return;
    const p = document.createElement("p");
    p.className = "persona-phrase";
    p.textContent = `"${phrase}"`;
    els.personaPhrases.appendChild(p);
  });
}

// ═══════════════════════════════════════════════════════════════
//  KPI CARDS
// ═══════════════════════════════════════════════════════════════

function renderKPI(s) {
  const cards = [
    { label: "Messages",      value: formatNum(s.messages_total),          sub: `Active days: ${formatNum(s.active_days)}`,                     color: "gold" },
    { label: "Chat Share",    value: `${formatPct(s.message_share_pct)}%`, sub: `Avg/day: ${formatFlt(s.avg_messages_per_active_day)}`,          color: "" },
    { label: "Avg Length",    value: formatFlt(s.avg_message_length),      sub: `Words/msg: ${formatFlt(s.avg_words_per_message)}`,              color: "" },
    { label: "Meaningful Wd", value: formatNum(s.meaningful_words_total),  sub: `Rate: ${formatFlt(s.meaningful_word_rate)}`,                    color: "sage" },
    { label: "Media",         value: formatNum(s.media_count),             sub: `Voice: ${formatNum(s.voice_count)} · Emoji: ${formatNum(s.emoji_count)}`, color: "" },
    { label: "Reply Out",     value: formatNum(s.reply_out),               sub: `Reply In: ${formatNum(s.reply_in)}`,                           color: "sky" },
    { label: "Mentions Out",  value: formatNum(s.mention_out),             sub: `Mention In: ${formatNum(s.mention_in)}`,                       color: "" },
    { label: "Toxicity Avg",  value: formatFlt(s.avg_toxicity),            sub: `Toxic msgs: ${formatNum(s.toxic_messages)}`,                   color: "rose" },
    { label: "URLs Shared",   value: formatNum(s.urls_shared),             sub: `Text msgs: ${formatNum(s.text_messages)}`,                     color: "" },
    { label: "Engagement",    value: formatFlt(s.engagement_rate),         sub: `Activity score: ${formatFlt(s.activity_score)}`,               color: "gold" },
  ];
  buildKpiGrid(els.kpiGrid, cards);
}

function renderBehaviorGrid(s) {
  const cards = [
    { label: "Night Msgs",    value: formatNum(s.night_messages),          sub: `Share: ${formatPct(s.night_share_pct)}%`,                       color: "sky" },
    { label: "Weekend Msgs",  value: formatNum(s.weekend_messages),        sub: `Share: ${formatPct(s.weekend_share_pct)}%`,                     color: "" },
    { label: "Questions",     value: formatNum(s.question_messages),       sub: `Exclamations: ${formatNum(s.exclamation_messages)}`,             color: "rose" },
    { label: "Empty Msgs",    value: formatNum(s.empty_messages),          sub: `Smart words/msg: ${formatFlt(s.avg_meaningful_words_per_message)}`, color: "" },
  ];
  buildKpiGrid(els.behaviorGrid, cards);
}

function buildKpiGrid(container, cards) {
  container.innerHTML = "";
  cards.forEach(({ label, value, sub, color }) => {
    const el = document.createElement("article");
    el.className = `kpi-card${color ? ` kpi-card--${color}` : ""}`;
    el.innerHTML = `
      <span class="kpi-card__label">${esc(label)}</span>
      <strong class="kpi-card__value">${esc(value)}</strong>
      <span class="kpi-card__sub">${esc(sub)}</span>
    `;
    container.appendChild(el);
  });
}

// ═══════════════════════════════════════════════════════════════
//  BAR CHART
// ═══════════════════════════════════════════════════════════════

function renderBarChart(container, values, labels) {
  container.innerHTML = "";
  if (!values.length) { renderEmpty(container, "No chart data."); return; }
  const max = Math.max(...values, 1);
  const dominant = argmax(values);
  values.forEach((v, i) => {
    const col = document.createElement("div");
    col.className = "bar-col";
    const bar = document.createElement("div");
    bar.className = "bar";
    bar.style.height = `${Math.max(3, (v / max) * 100)}px`;
    bar.title = `${labels[i] ?? i}: ${formatNum(v)}`;
    if (i === dominant) bar.dataset.dominant = "true";
    const lbl = document.createElement("span");
    lbl.className = "bar-label";
    lbl.textContent = labels[i] ?? String(i);
    col.appendChild(bar);
    col.appendChild(lbl);
    container.appendChild(col);
  });
}

// ═══════════════════════════════════════════════════════════════
//  DAILY TREND SVG
// ═══════════════════════════════════════════════════════════════

function renderDailyTrend(dateMap) {
  clearSvg(els.dailyTrend);
  const entries = Object.entries(dateMap || {})
    .map(([k, v]) => [k, Number(v)])
    .filter(([, v]) => isFinite(v))
    .sort(([a], [b]) => a.localeCompare(b));

  if (!entries.length) { els.dateRangeLabel.textContent = "n/a"; return; }

  const vals  = entries.map(e => e[1]);
  const dates = entries.map(e => e[0]);
  const max   = Math.max(...vals, 1);
  const ns = "http://www.w3.org/2000/svg";
  const W = 1000, H = 220, pad = 20;
  const sx = (W - pad * 2) / Math.max(vals.length - 1, 1);
  const sy = (H - pad * 2);

  const pts = vals.map((v, i) => ({
    x: (pad + i * sx).toFixed(1),
    y: (H - pad - (v / max) * sy).toFixed(1),
  }));

  const lineStr = pts.map(p => `${p.x},${p.y}`).join(" ");
  const areaStr = `${lineStr} ${W - pad},${H - pad} ${pad},${H - pad}`;

  // Defs
  const defs = document.createElementNS(ns, "defs");
  const grad = document.createElementNS(ns, "linearGradient");
  grad.id = "trendGrad";
  grad.setAttribute("x1", "0"); grad.setAttribute("y1", "0");
  grad.setAttribute("x2", "0"); grad.setAttribute("y2", "1");
  const s1 = document.createElementNS(ns, "stop");
  s1.setAttribute("offset", "0%");   s1.setAttribute("stop-color", "rgba(212,150,74,0.4)");
  const s2 = document.createElementNS(ns, "stop");
  s2.setAttribute("offset", "100%"); s2.setAttribute("stop-color", "rgba(212,150,74,0.02)");
  grad.appendChild(s1); grad.appendChild(s2); defs.appendChild(grad);
  els.dailyTrend.appendChild(defs);

  // Area
  const area = document.createElementNS(ns, "polygon");
  area.setAttribute("points", areaStr);
  area.setAttribute("fill", "url(#trendGrad)");
  els.dailyTrend.appendChild(area);

  // Line
  const line = document.createElementNS(ns, "polyline");
  line.setAttribute("points", lineStr);
  line.setAttribute("fill", "none");
  line.setAttribute("stroke", "rgba(212,150,74,0.85)");
  line.setAttribute("stroke-width", "2.5");
  line.setAttribute("stroke-linejoin", "round");
  els.dailyTrend.appendChild(line);

  // Peak marker
  const peakIdx = argmax(vals);
  const pk = pts[peakIdx];
  const c = document.createElementNS(ns, "circle");
  c.setAttribute("cx", pk.x); c.setAttribute("cy", pk.y);
  c.setAttribute("r", "5"); c.setAttribute("fill", "var(--sage)");
  const title = document.createElementNS(ns, "title");
  title.textContent = `${dates[peakIdx]}: ${formatNum(vals[peakIdx])}`;
  c.appendChild(title);
  els.dailyTrend.appendChild(c);

  // Last point marker
  const last = pts[pts.length - 1];
  const lc = document.createElementNS(ns, "circle");
  lc.setAttribute("cx", last.x); lc.setAttribute("cy", last.y);
  lc.setAttribute("r", "4"); lc.setAttribute("fill", "var(--rose)");
  els.dailyTrend.appendChild(lc);

  els.dateRangeLabel.textContent = `${dates[0]} → ${dates[dates.length - 1]}`;
}

// ═══════════════════════════════════════════════════════════════
//  WORD LISTS
// ═══════════════════════════════════════════════════════════════

function renderWordList(container, words, valueKey, variant) {
  container.innerHTML = "";
  if (!Array.isArray(words) || !words.length) { renderEmpty(container, "No word data."); return; }
  const max = words.reduce((m, w) => Math.max(m, Number(w[valueKey]) || 0), 1);
  words.forEach(w => {
    const val = Number(w[valueKey]) || 0;
    const pct = Math.max(4, (val / max) * 100);
    const li = document.createElement("li");
    li.className = "word-item";
    li.innerHTML = `
      <span class="word-label">${esc(String(w.word ?? ""))}</span>
      <span class="word-track"><span class="word-fill${variant ? " word-fill--"+variant : ""}" style="width:${pct.toFixed(1)}%"></span></span>
      <span class="word-count">${valueKey === "score" ? Number(val).toFixed(2) : formatNum(val)}</span>
    `;
    container.appendChild(li);
  });
}

// ═══════════════════════════════════════════════════════════════
//  RELATIONS
// ═══════════════════════════════════════════════════════════════

function renderRelations(container, edges) {
  container.innerHTML = "";
  const sorted = [...(edges || [])]
    .sort((a, b) => (Number(b.weight) || 0) - (Number(a.weight) || 0))
    .slice(0, 15);
  if (!sorted.length) { renderEmpty(container, "No relation edges."); return; }

  sorted.forEach(edge => {
    const li = document.createElement("li");
    li.className = "relation-item";
    li.innerHTML = `
      <div class="relation-top">
        <span class="relation-id">${esc(String(edge.from_user_id ?? "?"))} → ${esc(String(edge.to_user_id ?? "?"))}</span>
        <span class="relation-weight">${formatFlt(edge.weight)}</span>
      </div>
      <div class="relation-meta">
        <span class="relation-chip">reply <span>${formatNum(edge.replies)}</span></span>
        <span class="relation-chip">mention <span>${formatNum(edge.mentions)}</span></span>
        <span class="relation-chip">adj <span>${formatNum(edge.temporal_adjacency)}</span></span>
        <span class="relation-chip">overlap <span>${formatNum(edge.context_overlap)}</span></span>
      </div>
    `;
    container.appendChild(li);
  });
}

// ═══════════════════════════════════════════════════════════════
//  TOPICS
// ═══════════════════════════════════════════════════════════════

function renderTopics(topics) {
  if (!topics.length) { els.topicsPanel.hidden = true; return; }
  els.topicsPanel.hidden = false;
  els.topicsCount.textContent = String(topics.length);
  els.topicsList.innerHTML = "";

  topics.slice(0, 20).forEach(t => {
    const div = document.createElement("div");
    div.className = "topic-card";
    const keywords = (t.keywords || []).slice(0, 6).map(k => `<span class="topic-kw">${esc(k)}</span>`).join("");
    div.innerHTML = `
      <span class="topic-id">${esc(t.topic_id || "")}</span>
      ${t.summary ? `<p class="topic-summary">${esc(t.summary)}</p>` : ""}
      ${keywords ? `<div class="topic-keywords">${keywords}</div>` : ""}
      <span class="topic-confidence">conf: ${isFinite(Number(t.confidence)) ? Number(t.confidence).toFixed(2) : "—"} · ${formatNum((t.message_ids || []).length)} msgs</span>
    `;
    els.topicsList.appendChild(div);
  });
}

// ═══════════════════════════════════════════════════════════════
//  CONTENT SIGNALS
// ═══════════════════════════════════════════════════════════════

function renderContent(content) {
  const urls     = content.urls     || [];
  const mentions = content.mentions || [];
  if (!urls.length && !mentions.length) { els.contentSection.hidden = true; return; }
  els.contentSection.hidden = false;

  els.urlsCount.textContent     = String(urls.length);
  els.mentionsCount.textContent = String(mentions.length);

  renderContentList(els.urlsList, urls.slice(0, 20), item => ({
    value: item.value || "—",
    count: `×${item.count}`,
  }));
  renderContentList(els.mentionsList, mentions.slice(0, 20), item => ({
    value: `user_${item.user_id}`,
    count: `×${item.count}`,
  }));
}

function renderContentList(container, items, mapper) {
  container.innerHTML = "";
  if (!items.length) { renderEmpty(container, "No data."); return; }
  items.forEach(item => {
    const { value, count } = mapper(item);
    const li = document.createElement("li");
    li.className = "content-item";
    li.innerHTML = `
      <span class="content-value" title="${esc(value)}">${esc(value)}</span>
      <span class="content-count">${esc(count)}</span>
    `;
    container.appendChild(li);
  });
}

// ═══════════════════════════════════════════════════════════════
//  MESSAGES — VIRTUAL SCROLL
// ═══════════════════════════════════════════════════════════════

function setupMessageFeed(messages, ownUserId) {
  // Sort newest first
  state.allMessages = [...messages].sort((a, b) =>
    new Date(b.date || 0) - new Date(a.date || 0)
  ).map(m => ({ ...m, _isOwn: m.from_user_id === ownUserId }));

  state.searchQuery = els.messageFilter.value.trim().toLowerCase();
  els.clearSearch.hidden = !state.searchQuery;

  rebuildMessageFilter();
  renderMessageBatch(true);
  setupSentinelObserver();
}

function rebuildMessageFilter() {
  const q = state.searchQuery;
  state.filteredMessages = q
    ? state.allMessages.filter(m => String(m.text || "").toLowerCase().includes(q)
        || String(m.from_display_name || m.from_username || "").toLowerCase().includes(q))
    : state.allMessages;
  els.messageCount.textContent = `${formatNum(state.filteredMessages.length)} messages`;
}

function renderMessageBatch(reset = false) {
  if (reset) {
    els.messageFeed.innerHTML = "";
    state.renderedCount = 0;
  }
  const from = state.renderedCount;
  const to   = Math.min(from + PAGE_SIZE, state.filteredMessages.length);
  if (from >= state.filteredMessages.length) return;

  const frag = document.createDocumentFragment();
  for (let i = from; i < to; i++) {
    frag.appendChild(buildMessageEl(state.filteredMessages[i]));
  }
  els.messageFeed.appendChild(frag);
  state.renderedCount = to;

  if (!state.filteredMessages.length && reset) {
    renderEmpty(els.messageFeed, "No messages match this filter.");
  }
}

function buildMessageEl(msg) {
  const template = document.getElementById("msgTemplate");
  const li = template.content.firstElementChild.cloneNode(true);

  if (msg._isOwn) li.classList.add("is-own");

  const author = msg.from_display_name || msg.from_username || `user_${msg.from_user_id ?? "?"}`;
  li.querySelector(".message-author").textContent = author;
  li.querySelector(".message-time").textContent = msg.date ? fmtTime(msg.date) : "—";
  li.querySelector(".message-time").dateTime = msg.date || "";

  // Media type badge
  if (msg.media_type) {
    const badge = li.querySelector(".message-type-badge");
    badge.hidden = false;
    badge.textContent = msg.media_type;
  }

  // Text with optional highlight
  const bodyEl = li.querySelector(".message-body");
  const raw = String(msg.text || "").trim() || (msg.media_type ? `[${msg.media_type}]` : "(empty)");
  bodyEl.innerHTML = highlightText(raw, state.searchQuery);

  // Footer
  const footer = li.querySelector(".message-footer");
  const replyEl = li.querySelector(".message-reply-hint");
  const toxEl   = li.querySelector(".message-toxicity");

  let hasFooter = false;

  if (msg.reply_to_msg_id) {
    replyEl.hidden = false;
    replyEl.textContent = `↩ reply to #${msg.reply_to_msg_id}`;
    hasFooter = true;
  }

  const tox = Number(msg.toxicity_score);
  if (isFinite(tox) && tox > 0.3) {
    toxEl.hidden = false;
    toxEl.textContent = `tox ${tox.toFixed(2)}`;
    hasFooter = true;
  }

  footer.hidden = !hasFooter;
  return li;
}

function setupSentinelObserver() {
  if (state.sentinelObserver) state.sentinelObserver.disconnect();
  state.sentinelObserver = new IntersectionObserver(
    (entries) => {
      if (entries[0].isIntersecting && state.renderedCount < state.filteredMessages.length) {
        renderMessageBatch(false);
      }
    },
    { root: els.messageFeed, threshold: 0.1 }
  );
  state.sentinelObserver.observe(els.feedSentinel);
}

// ═══════════════════════════════════════════════════════════════
//  UTILITIES
// ═══════════════════════════════════════════════════════════════

function highlightText(rawText, query) {
  const safe = esc(rawText);
  if (!query) return safe;
  const escapedQ = esc(query).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return safe.replace(new RegExp(escapedQ, "gi"), m => `<mark class="hl">${m}</mark>`);
}

function setStatus(text, mode) {
  els.statusText.textContent = text;
  els.statusBadge.classList.remove("is-ok", "is-busy", "is-warn");
  if (mode === "ok")   els.statusBadge.classList.add("is-ok");
  if (mode === "busy") els.statusBadge.classList.add("is-busy");
  if (mode === "warn") els.statusBadge.classList.add("is-warn");
}

function renderEmpty(container, text) {
  if (!container) return;
  container.innerHTML = "";
  const clone = document.getElementById("emptyTemplate").content.firstElementChild.cloneNode(true);
  clone.querySelector(".empty-state__text").textContent = text;
  container.appendChild(clone);
}

function clearSvg(el) { if (el) el.innerHTML = ""; }

async function fetchJSON(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) { const t = await res.text(); throw new Error(`HTTP ${res.status}: ${t}`); }
  return res.json();
}

function selectedSource() {
  const val = els.chatSelect.value;
  if (!val.includes("|")) return null;
  const [account, chatRaw] = val.split("|");
  const chat = parseInt(chatRaw, 10);
  if (!account || !isFinite(chat)) return null;
  return { account, chat };
}

function appendOption(sel, value, label) {
  const o = document.createElement("option");
  o.value = value; o.textContent = label;
  sel.appendChild(o);
}

function displayUser(u) {
  return nonEmpty(u.display_name, [u.username ? `@${u.username}` : "", u.user_id ? `user_${u.user_id}` : "unknown"]);
}

function numArr(arr, len) {
  if (!Array.isArray(arr)) return [];
  const out = arr.map(v => Number(v) || 0);
  while (out.length < len) out.push(0);
  return out.slice(0, len);
}

function argmax(arr) {
  let best = 0;
  arr.forEach((v, i) => { if (v > arr[best]) best = i; });
  return best;
}

function nonEmpty(primary, fallbacks) {
  const s = String(primary || "").trim();
  if (s) return s;
  for (const f of fallbacks) {
    const fs = String(f || "").trim();
    if (fs) return fs;
  }
  return "n/a";
}

function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

const _numFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
function formatNum(v) { const n = Number(v); return isFinite(n) ? _numFmt.format(n) : "0"; }
function formatFlt(v) { const n = Number(v); return isFinite(n) ? n.toFixed(2) : "0.00"; }
function formatPct(v) { const n = Number(v); return isFinite(n) ? n.toFixed(2) : "0.00"; }

function fmtDate(iso) {
  if (!iso) return "—";
  try { return new Date(iso).toLocaleDateString("en-GB", { day:"2-digit", month:"short", year:"numeric" }); }
  catch { return iso; }
}

function fmtTime(iso) {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toLocaleString("en-GB", { day:"2-digit", month:"short", hour:"2-digit", minute:"2-digit" });
  } catch { return iso; }
}

function esc(v) {
  return String(v)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}