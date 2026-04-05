"use strict";

// ═══════════════════════════════════════════════════════════════
//  CONSTANTS
// ═══════════════════════════════════════════════════════════════

const WEEKDAY_LABELS  = ["Вс","Пн","Вт","Ср","Чт","Пт","Сб"];
const MONTH_LABELS    = ["Янв","Фев","Мар","Апр","Май","Июн","Июл","Авг","Сен","Окт","Ноя","Дек"];
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
  anime_fandom:                "rose",
  travel_geography:            "sage",
  finance_crypto:              "gold",
  education_learning:          "sky",
  design_creativity:           "rose",
  devrel_content:              "sky",
  career_hiring:               "gold",
  legal_compliance:            "sage",
  automation_productivity:     "gold",
};

const ROLE_LABELS = {
  key_contributor: "Ключевой участник",
  expert_signal: "Экспертный голос",
  core_voice: "Ядро обсуждений",
  discussion_driver: "Драйвер дискуссий",
  silent_observer: "Тихий наблюдатель",
  participant: "Участник",
};

const TONE_LABELS = {
  aggressive: "Агрессивный",
  sharp: "Резкий",
  supportive_or_neutral: "Поддерживающий/нейтральный",
};

const TRAIT_LABELS = {
  dominant_presence: "Доминирующее присутствие",
  consistently_active: "Стабильно активный",
  high_activity: "Высокая активность",
  steady_activity: "Ровная активность",
  low_activity: "Низкая активность",
  high_conflict: "Высокая конфликтность",
  occasionally_conflict: "Эпизодическая конфликтность",
  calm: "Спокойный стиль",
};

const TRIGGER_LABELS = {
  conflict_topics: "Конфликтные темы",
  direct_replies: "Частые прямые ответы",
  insufficient_signal: "Недостаточно сигнала",
};

const INTEREST_LABELS = {
  software_engineering: "Разработка ПО",
  ai_ml_llm: "ИИ / ML / LLM",
  web_frontend: "Веб-фронтенд",
  security_privacy: "Безопасность и приватность",
  data_engineering_analytics: "Data engineering и аналитика",
  open_source_community: "Open Source и комьюнити",
  product_management: "Продуктовый менеджмент",
  project_delivery: "Доставка проекта",
  startup_business: "Стартапы и бизнес",
  communication_coordination: "Коммуникация и координация",
  humor_memes: "Юмор и мемы",
  philosophy_thinking: "Философия и мышление",
  ops_observability: "Ops и наблюдаемость",
  cloud_platforms: "Облачные платформы",
  health_wellness: "Здоровье и самочувствие",
  gaming: "Игры",
  media_entertainment: "Медиа и развлечения",
  anime_fandom: "Аниме и фандом",
  travel_geography: "Путешествия и география",
  finance_crypto: "Финансы и крипто",
  education_learning: "Обучение и развитие",
  design_creativity: "Дизайн и креатив",
  devrel_content: "Документация и контент",
  career_hiring: "Карьера и найм",
  legal_compliance: "Юридическое и комплаенс",
  automation_productivity: "Автоматизация и продуктивность",
};

const MEDIA_TYPE_LABELS = {
  photo: "Фото",
  video: "Видео",
  voice: "Голосовое",
  audio: "Аудио",
  sticker: "Стикер",
  gif: "GIF",
  document: "Документ",
  poll: "Опрос",
};

// ═══════════════════════════════════════════════════════════════
//  STATE
// ═══════════════════════════════════════════════════════════════

const state = {
  chats:        [],
  users:        [],
  userDirectory: new Map(),
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
      setStatus("чтение JSON", "busy");
      const text = await file.text();
      const parsed = JSON.parse(text);
      if (!parsed || typeof parsed !== "object") throw new Error("Некорректный snapshot.");
      state.snapshot = parsed;
      state.sourceLabel = `файл:${file.name}`;
      renderSnapshot();
      setStatus("снимок загружен вручную", "ok");
      els.sourceHint.textContent = "Включён ручной режим. Нажмите «Обновить», чтобы вернуться к API.";
    } catch (err) {
      console.error(err);
      setStatus("ошибка JSON", "warn");
      els.sourceHint.textContent = "Не удалось прочитать JSON. Загрузите корректный снимок пользователя.";
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
  setStatus("ожидание", "idle");
  const lists = [els.kpiGrid, els.behaviorGrid, els.hourChart, els.weekdayChart,
                 els.monthChart, els.topWords, els.smartWords,
                 els.incomingRelations, els.outgoingRelations, els.messageFeed];
  lists.forEach(el => renderEmpty(el, "Данные пока не загружены."));
  clearSvg(els.dailyTrend);
}

// ═══════════════════════════════════════════════════════════════
//  API
// ═══════════════════════════════════════════════════════════════

async function refreshSources() {
  setStatus("сканирую exports", "busy");
  state.apiAvailable = true;
  try {
    const chats = await fetchJSON("/api/chats");
    state.chats = Array.isArray(chats) ? chats : [];
    populateChatSelect();
    if (!state.chats.length) {
      state.users = [];
      state.userDirectory = new Map();
      populateUserSelect();
      els.chatUsersCount.textContent = "—";
      els.bestActivity.textContent = "—";
      els.sourceHint.textContent = "Снимки не найдены. Запустите user-snapshot и обновите источники.";
      setStatus("нет exports", "warn");
      return;
    }
    await loadUsersForSelectedChat();
    setStatus(`${state.chats.length} чатов`, "ok");
  } catch (err) {
    console.error(err);
    state.apiAvailable = false;
    state.chats = [];
    state.users = [];
    state.userDirectory = new Map();
    populateChatSelect();
    populateUserSelect();
    els.chatUsersCount.textContent = "—";
    els.bestActivity.textContent = "—";
    els.sourceHint.textContent = "API недоступен. Используйте ручную загрузку JSON.";
    setStatus("api недоступен", "warn");
  }
}

function populateChatSelect() {
  const prev = els.chatSelect.value;
  els.chatSelect.innerHTML = "";
  if (!state.chats.length) {
    appendOption(els.chatSelect, "", "Источники не найдены");
    els.chatSelect.disabled = true;
    return;
  }
  for (const chat of state.chats) {
    const val = `${chat.account_id}|${chat.chat_id}`;
    appendOption(els.chatSelect, val, `${chat.account_id} / ${chat.chat_id} (${chat.users_count} пользователей)`);
  }
  els.chatSelect.disabled = false;
  const hasPrev = state.chats.some(c => `${c.account_id}|${c.chat_id}` === prev);
  els.chatSelect.value = hasPrev ? prev : `${state.chats[0].account_id}|${state.chats[0].chat_id}`;
}

async function loadUsersForSelectedChat() {
  const src = selectedSource();
  if (!src) {
    state.users = [];
    state.userDirectory = new Map();
    populateUserSelect();
    return;
  }
  setStatus("загружаю пользователей", "busy");
  const params = new URLSearchParams({ account: src.account, chat: String(src.chat) });
  const users = await fetchJSON(`/api/users?${params}`);
  state.users = Array.isArray(users) ? users : [];
  rebuildUserDirectory();
  populateUserSelect();
  els.chatUsersCount.textContent = formatNum(state.users.length);
  const best = state.users.reduce((acc, u) => Math.max(acc, Number(u.activity_score) || 0), 0);
  els.bestActivity.textContent = best.toFixed(2);
  if (!state.users.length) {
    els.sourceHint.textContent = "В этом чате нет снимков пользователей. Сначала запустите user-snapshot.";
    setStatus("пользователей нет", "warn");
  } else {
    const top = state.users[0];
    els.sourceHint.textContent = `Топ: ${displayUser(top)} · ${formatNum(top.messages_total)} сообщений`;
    setStatus(`${state.users.length} пользователей`, "ok");
  }
}

function populateUserSelect() {
  const prev = els.userSelect.value;
  els.userSelect.innerHTML = "";
  if (!state.users.length) {
    appendOption(els.userSelect, "", "Сначала выберите чат");
    els.userSelect.disabled = true;
    els.loadBtn.disabled = true;
    return;
  }
  for (const u of state.users) {
    appendOption(els.userSelect, String(u.user_id), `${displayUser(u)} · ${formatNum(u.messages_total)} сообщений`);
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
  setStatus("загружаю снимок", "busy");
  const params = new URLSearchParams({ account: src.account, chat: String(src.chat), user_id: String(uid) });
  try {
    state.snapshot = await fetchJSON(`/api/user?${params}`);
    state.sourceLabel = `${src.account}/${src.chat}/user_${uid}`;
    renderSnapshot();
    setStatus("снимок готов", "ok");
  } catch (err) {
    console.error(err);
    setStatus("ошибка снимка", "warn");
    els.sourceHint.textContent = "Не удалось загрузить снимок. Проверьте директорию state.";
  }
}

// ═══════════════════════════════════════════════════════════════
//  RENDER SNAPSHOT (orchestrator)
// ═══════════════════════════════════════════════════════════════

function renderSnapshot() {
  if (!state.snapshot || typeof state.snapshot !== "object") return;
  const { profile = {}, stats = {}, persona = {} } = state.snapshot;
  rebuildUserDirectory(profile, state.snapshot.recent_messages || []);

  const name = nonEmpty(profile.display_name, [
    [profile.first_name, profile.last_name].filter(Boolean).join(" "),
    profile.username ? `@${profile.username}` : "",
    profile.user_id ? `user_${profile.user_id}` : "Неизвестный",
  ]);

  // Hero
  els.heroName.textContent = name;
  els.heroEyebrow.textContent = profile.username ? `@${profile.username}` : "Карточка участника";
  els.heroMeta.textContent =
    `Источник: ${state.sourceLabel || "ручной"} · ID: ${profile.user_id || "н/д"} · Сгенерировано: ${fmtDate(state.snapshot.generated_at)}`;
  els.heroSummary.textContent = buildPersonaSummary(persona, stats);
  renderHeroBadges(persona, stats);

  // Sidebar persona
  renderPersonaSidebar(persona, stats);

  // KPIs
  renderKPI(stats);
  renderBehaviorGrid(stats);

  // Charts
  renderBarChart(els.hourChart, numArr(stats.messages_by_hour, 24), Array.from({length:24}, (_,i)=>String(i).padStart(2,"0")));
  const peakHour = argmax(numArr(stats.messages_by_hour, 24));
  els.hourPeak.textContent = `Пик ${String(peakHour).padStart(2,"0")}:00 · ${formatNum(stats.messages_by_hour?.[peakHour] ?? 0)} сообщений`;

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
  if (persona.role)  items.push({ text: humanizeRole(persona.role), cls: "badge--gold" });
  if (persona.tone)  items.push({ text: humanizeTone(persona.tone), cls: "badge--sky" });
  if (Array.isArray(persona.traits)) {
    persona.traits.slice(0, 3).forEach(t => items.push({ text: humanizeTrait(t), cls: "badge--sage" }));
  }
  buildActivityBadges(stats).forEach(tag => items.push(tag));
  if (isFinite(Number(stats.activity_score)))
    items.push({ text: `Активность ${Number(stats.activity_score).toFixed(2)}`, cls: "badge--rose" });
  if (!items.length) items.push({ text: "Профиль загружен", cls: "" });

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

  els.personaRole.textContent = persona.role ? humanizeRole(persona.role) : "—";
  els.personaTone.textContent = persona.tone ? humanizeTone(persona.tone) : "—";
  els.personaConf.textContent = `уверенность: ${isFinite(Number(persona.confidence)) ? Number(persona.confidence).toFixed(2) : "—"}`;

  // Traits
  els.personaTraits.innerHTML = "";
  const combinedTraits = [
    ...(persona.traits || []).map(humanizeTrait),
    ...buildActivityBadges(stats).map(item => item.text),
  ];
  combinedTraits.forEach(t => {
    const b = document.createElement("span");
    b.className = "badge badge--gold";
    b.textContent = t;
    els.personaTraits.appendChild(b);
  });

  // Interests
  els.personaInterests.innerHTML = "";
  const interests = (persona.interests || []).slice(0, 12);
  interests.forEach(interest => {
    const cls = INTEREST_PALETTE[interest] || "sky";
    const b = document.createElement("span");
    b.className = `badge badge--${cls}`;
    b.textContent = humanizeInterest(interest);
    els.personaInterests.appendChild(b);
  });
  if (!interests.length) {
    const b = document.createElement("span");
    b.className = "badge badge--rose";
    b.textContent = "Не удалось распознать интересы по interest-сетам";
    els.personaInterests.appendChild(b);
  }

  // Triggers
  const triggers = persona.triggers || [];
  els.personaTriggersSection.hidden = !triggers.length;
  els.personaTriggers.innerHTML = "";
  triggers.forEach(t => {
    const b = document.createElement("span");
    b.className = "badge badge--rose";
    b.textContent = humanizeTrigger(t);
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
  const totalMessages = Math.max(1, Number(s.messages_total) || 0);
  const interactions = (Number(s.reply_out) || 0) + (Number(s.reply_in) || 0) + (Number(s.mention_out) || 0) + (Number(s.mention_in) || 0);
  const mediaSharePct = 100 * (Number(s.media_count) || 0) / totalMessages;
  const urlsPer100 = 100 * (Number(s.urls_shared) || 0) / totalMessages;

  const cards = [
    { label: "Сообщения",              value: formatNum(s.messages_total),          sub: `Активных дней: ${formatNum(s.active_days)}`,                          color: "gold" },
    { label: "Доля в чате",            value: `${formatPct(s.message_share_pct)}%`, sub: `Среднее в день: ${formatFlt(s.avg_messages_per_active_day)}`,          color: "" },
    { label: "Средняя длина",          value: formatFlt(s.avg_message_length),      sub: `Слов в сообщении: ${formatFlt(s.avg_words_per_message)}`,              color: "" },
    { label: "Содержательные слова",   value: formatNum(s.meaningful_words_total),  sub: `Доля: ${formatFlt(s.meaningful_word_rate)}`,                           color: "sage" },
    { label: "Медиа",                  value: formatNum(s.media_count),             sub: `Голосовых: ${formatNum(s.voice_count)} · Эмодзи: ${formatNum(s.emoji_count)}`, color: "" },
    { label: "Ответы исходящие",       value: formatNum(s.reply_out),               sub: `Ответы входящие: ${formatNum(s.reply_in)}`,                            color: "sky" },
    { label: "Упоминания исходящие",   value: formatNum(s.mention_out),             sub: `Упоминания входящие: ${formatNum(s.mention_in)}`,                      color: "" },
    { label: "Средняя токсичность",    value: formatFlt(s.avg_toxicity),            sub: `Токсичных сообщений: ${formatNum(s.toxic_messages)}`,                  color: "rose" },
    { label: "Поделился URL",          value: formatNum(s.urls_shared),             sub: `Текстовых сообщений: ${formatNum(s.text_messages)}`,                   color: "" },
    { label: "Вовлечённость",          value: formatFlt(s.engagement_rate),         sub: `Индекс активности: ${formatFlt(s.activity_score)}`,                    color: "gold" },
    { label: "Соц. взаимодействия",    value: formatNum(interactions),              sub: `На 100 сообщений: ${formatFlt((100*interactions)/totalMessages)}`,     color: "sky" },
    { label: "Медиа-насыщенность",     value: `${formatPct(mediaSharePct)}%`,       sub: `URL на 100 сообщений: ${formatFlt(urlsPer100)}`,                       color: "" },
  ];
  buildKpiGrid(els.kpiGrid, cards);
}

function renderBehaviorGrid(s) {
  const totalMessages = Math.max(1, Number(s.messages_total) || 0);
  const activeDays = Math.max(1, Number(s.active_days) || 0);
  const inbound = (Number(s.reply_in) || 0) + (Number(s.mention_in) || 0);
  const outbound = (Number(s.reply_out) || 0) + (Number(s.mention_out) || 0);
  const dialogDensity = 100 * (inbound + outbound) / totalMessages;
  const responseBalance = safeDiv((Number(s.reply_out) || 0) + 1, (Number(s.reply_in) || 0) + 1);
  const socialLoadPerDay = inbound / activeDays;
  const semanticIntensity = safeDiv((Number(s.meaningful_words_total) || 0), totalMessages);

  const cards = [
    { label: "Ночные сообщения",     value: formatNum(s.night_messages),          sub: `Доля: ${formatPct(s.night_share_pct)}%`,                       color: "sky" },
    { label: "Выходные сообщения",   value: formatNum(s.weekend_messages),        sub: `Доля: ${formatPct(s.weekend_share_pct)}%`,                     color: "" },
    { label: "Вопросы",              value: formatNum(s.question_messages),       sub: `Восклицания: ${formatNum(s.exclamation_messages)}`,            color: "rose" },
    { label: "Пустые сообщения",     value: formatNum(s.empty_messages),          sub: `Смысловые слова/сообщение: ${formatFlt(semanticIntensity)}`,    color: "" },
    { label: "Плотность диалога",    value: formatFlt(dialogDensity),             sub: `Взаимодействий на 100 сообщений`,                               color: "gold" },
    { label: "Баланс ответов",       value: formatFlt(responseBalance),           sub: `Исходящие/входящие ответы`,                                     color: "" },
    { label: "Соц. отклик/день",     value: formatFlt(socialLoadPerDay),          sub: `Входящие реакции в активный день`,                              color: "sky" },
    { label: "Темп участия",         value: formatFlt(s.avg_messages_per_active_day), sub: `Сообщений в активный день`,                                 color: "rose" },
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
  if (!values.length) { renderEmpty(container, "Нет данных для графика."); return; }
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

  if (!entries.length) { els.dateRangeLabel.textContent = "н/д"; return; }

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
  if (!Array.isArray(words) || !words.length) { renderEmpty(container, "Нет данных по словам."); return; }
  const max = words.reduce((m, w) => Math.max(m, Number(w[valueKey]) || 0), 1);
  words.forEach(w => {
    const val = Number(w[valueKey]) || 0;
    const pct = variant === "smart"
      ? Math.max(4, Math.min(100, val))
      : Math.max(4, (val / max) * 100);
    const displayValue = valueKey === "score"
      ? (variant === "smart" ? `${Number(val).toFixed(1)}/100` : Number(val).toFixed(2))
      : formatNum(val);
    const li = document.createElement("li");
    li.className = "word-item";
    li.innerHTML = `
      <span class="word-label">${esc(String(w.word ?? ""))}</span>
      <span class="word-track"><span class="word-fill${variant ? " word-fill--"+variant : ""}" style="width:${pct.toFixed(1)}%"></span></span>
      <span class="word-count">${displayValue}</span>
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
  if (!sorted.length) { renderEmpty(container, "Нет связей для отображения."); return; }

  sorted.forEach(edge => {
    const fromLabel = formatUserWithID(edge.from_user_id);
    const toLabel = formatUserWithID(edge.to_user_id);
    const li = document.createElement("li");
    li.className = "relation-item";
    li.innerHTML = `
      <div class="relation-top">
        <span class="relation-id">${esc(fromLabel)} → ${esc(toLabel)}</span>
        <span class="relation-weight">${formatFlt(edge.weight)}</span>
      </div>
      <div class="relation-meta">
        <span class="relation-chip">ответы <span>${formatNum(edge.replies)}</span></span>
        <span class="relation-chip">упоминания <span>${formatNum(edge.mentions)}</span></span>
        <span class="relation-chip">соседство <span>${formatNum(edge.temporal_adjacency)}</span></span>
        <span class="relation-chip">перекрытие <span>${formatNum(edge.context_overlap)}</span></span>
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
      <span class="topic-confidence">уверенность: ${isFinite(Number(t.confidence)) ? Number(t.confidence).toFixed(2) : "—"} · ${formatNum((t.message_ids || []).length)} сообщений</span>
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
    value: formatUserWithID(item.user_id),
    count: `×${item.count}`,
  }));
}

function renderContentList(container, items, mapper) {
  container.innerHTML = "";
  if (!items.length) { renderEmpty(container, "Нет данных."); return; }
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
  els.messageCount.textContent = `${formatNum(state.filteredMessages.length)} сообщений`;
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
    renderEmpty(els.messageFeed, "Нет сообщений по текущему фильтру.");
  }
}

function buildMessageEl(msg) {
  const template = document.getElementById("msgTemplate");
  const li = template.content.firstElementChild.cloneNode(true);

  if (msg._isOwn) li.classList.add("is-own");

  const author = formatMessageAuthor(msg);
  li.querySelector(".message-author").textContent = author;
  li.querySelector(".message-time").textContent = msg.date ? fmtTime(msg.date) : "—";
  li.querySelector(".message-time").dateTime = msg.date || "";

  // Media type badge
  if (msg.media_type) {
    const badge = li.querySelector(".message-type-badge");
    badge.hidden = false;
    badge.textContent = humanizeMediaType(msg.media_type);
  }

  // Text with optional highlight
  const bodyEl = li.querySelector(".message-body");
  const raw = String(msg.text || "").trim() || (msg.media_type ? `[${humanizeMediaType(msg.media_type)}]` : "(пусто)");
  bodyEl.innerHTML = highlightText(raw, state.searchQuery);

  // Footer
  const footer = li.querySelector(".message-footer");
  const replyEl = li.querySelector(".message-reply-hint");
  const toxEl   = li.querySelector(".message-toxicity");

  let hasFooter = false;

  if (msg.reply_to_msg_id) {
    replyEl.hidden = false;
    replyEl.textContent = `↩ ответ на #${msg.reply_to_msg_id}`;
    hasFooter = true;
  }

  const tox = Number(msg.toxicity_score);
  if (isFinite(tox) && tox > 0.3) {
    toxEl.hidden = false;
    toxEl.textContent = `токсичность ${tox.toFixed(2)}`;
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
//  DOMAIN HELPERS
// ═══════════════════════════════════════════════════════════════

function rebuildUserDirectory(profile = null, recentMessages = []) {
  const next = new Map();

  (state.users || []).forEach(u => {
    upsertDirectoryUser(next, {
      user_id: u.user_id,
      display_name: u.display_name,
      username: u.username,
      first_name: u.first_name,
      last_name: u.last_name,
    });
  });

  if (profile && typeof profile === "object") {
    upsertDirectoryUser(next, profile);
  }

  (recentMessages || []).forEach(m => {
    upsertDirectoryUser(next, {
      user_id: m.from_user_id,
      display_name: m.from_display_name,
      username: m.from_username,
    });
  });

  (state.snapshot?.content?.mentions || []).forEach(item => {
    upsertDirectoryUser(next, { user_id: item.user_id });
  });

  state.userDirectory = next;
}

function upsertDirectoryUser(directory, userLike) {
  const id = Number(userLike?.user_id);
  if (!isFinite(id) || id <= 0) return;

  const prev = directory.get(id) || {};
  const fullName = [userLike?.first_name, userLike?.last_name]
    .map(v => String(v || "").trim())
    .filter(Boolean)
    .join(" ");

  const displayRaw = String(userLike?.display_name || "").trim();
  const displayName = nonEmpty(displayRaw, [fullName, prev.display_name || ""]);
  const username = nonEmpty(normalizeUsername(userLike?.username), [normalizeUsername(prev.username)]);

  directory.set(id, {
    user_id: id,
    display_name: displayName === "н/д" ? "" : displayName,
    username: username === "н/д" ? "" : username,
  });
}

function normalizeUsername(username) {
  return String(username || "").trim().replace(/^@+/, "");
}

function formatUserWithID(userID) {
  const id = Number(userID);
  if (!isFinite(id) || id <= 0) return "Неизвестный пользователь";

  const known = state.userDirectory.get(id);
  const displayName = String(known?.display_name || "").trim();
  const username = normalizeUsername(known?.username);

  if (displayName && username) return `${displayName} (@${username}) · id:${id}`;
  if (displayName) return `${displayName} · id:${id}`;
  if (username) return `@${username} · id:${id}`;
  return `user_${id}`;
}

function formatMessageAuthor(msg) {
  const id = Number(msg?.from_user_id);
  const fromDisplay = String(msg?.from_display_name || "").trim();
  const fromUsername = normalizeUsername(msg?.from_username);
  const known = isFinite(id) && id > 0 ? state.userDirectory.get(id) : null;

  const displayName = nonEmpty(fromDisplay, [known?.display_name || ""]);
  const username = nonEmpty(fromUsername, [normalizeUsername(known?.username)]);

  const base = [];
  if (displayName !== "н/д") base.push(displayName);
  if (username !== "н/д") base.push(`@${username}`);
  if (!base.length) {
    if (isFinite(id) && id > 0) return `user_${id}`;
    return "Неизвестный";
  }
  return base.join(" · ");
}

function buildPersonaSummary(persona, stats) {
  const parts = [];
  if (persona?.role) parts.push(humanizeRole(persona.role));
  if (persona?.tone) parts.push(humanizeTone(persona.tone).toLowerCase());

  const interests = (persona?.interests || []).slice(0, 2).map(humanizeInterest);
  if (interests.length) {
    parts.push(`интересы: ${interests.join(", ")}`);
  } else {
    parts.push("интересы не распознаны по interest-сетам");
  }

  const total = Number(stats?.messages_total) || 0;
  const activeDays = Number(stats?.active_days) || 0;
  if (total > 0) {
    parts.push(`${formatNum(total)} сообщений за ${formatNum(activeDays)} дн.`);
  }

  return parts.length
    ? parts.join(" · ")
    : "Сигнала пока недостаточно для уверенного описания профиля.";
}

function buildActivityBadges(stats) {
  const score = Number(stats?.activity_score) || 0;
  const messages = Number(stats?.messages_total) || 0;
  const activeDays = Number(stats?.active_days) || 0;
  const engagement = Number(stats?.engagement_rate) || 0;
  const replyOut = Number(stats?.reply_out) || 0;
  const mentionOut = Number(stats?.mention_out) || 0;
  const nightShare = Number(stats?.night_share_pct) || 0;
  const weekendShare = Number(stats?.weekend_share_pct) || 0;

  const tags = [
    { text: `Звание: ${activityRankLabel(score)}`, cls: "badge--gold" },
    { text: `Дивизион: ${activityDivisionLabel(messages)}`, cls: "badge--sky" },
  ];

  if (activeDays >= 45) {
    tags.push({ text: "Марафонец активности", cls: "badge--sage" });
  } else if (activeDays >= 20) {
    tags.push({ text: "Стабильный ритм", cls: "badge--sage" });
  }

  if (engagement >= 0.65) {
    tags.push({ text: "Лидер вовлечения", cls: "badge--rose" });
  } else if (engagement >= 0.4) {
    tags.push({ text: "Катализатор диалога", cls: "badge--rose" });
  }

  if (replyOut + mentionOut >= 250) {
    tags.push({ text: "Навигатор обсуждений", cls: "badge--gold" });
  }
  if (nightShare >= 35) {
    tags.push({ text: "Ночная смена", cls: "badge--rose" });
  }
  if (weekendShare >= 40) {
    tags.push({ text: "Выходной активист", cls: "badge--sage" });
  }

  return tags.slice(0, 7);
}

function activityRankLabel(score) {
  if (score >= 0.9) return "Легенда чата";
  if (score >= 0.75) return "Грандмастер дискуссий";
  if (score >= 0.6) return "Командир обсуждений";
  if (score >= 0.45) return "Ветеран диалога";
  if (score >= 0.3) return "Активист";
  if (score >= 0.15) return "Разведчик";
  return "Новобранец";
}

function activityDivisionLabel(messages) {
  if (messages >= 5000) return "Алмаз I";
  if (messages >= 3000) return "Платина I";
  if (messages >= 1800) return "Золото I";
  if (messages >= 900) return "Серебро I";
  if (messages >= 300) return "Бронза I";
  return "Квалификация";
}

function humanizeRole(role) {
  return ROLE_LABELS[role] || fallbackLabel(role);
}

function humanizeTone(tone) {
  return TONE_LABELS[tone] || fallbackLabel(tone);
}

function humanizeTrait(trait) {
  return TRAIT_LABELS[trait] || fallbackLabel(trait);
}

function humanizeInterest(interest) {
  return INTEREST_LABELS[interest] || fallbackLabel(interest);
}

function humanizeTrigger(trigger) {
  return TRIGGER_LABELS[trigger] || fallbackLabel(trigger);
}

function humanizeMediaType(mediaType) {
  return MEDIA_TYPE_LABELS[mediaType] || fallbackLabel(mediaType);
}

function fallbackLabel(value) {
  const text = String(value || "").trim().replace(/[_-]+/g, " ");
  if (!text) return "—";
  return text.charAt(0).toUpperCase() + text.slice(1);
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
  return nonEmpty(u.display_name, [u.username ? `@${u.username}` : "", u.user_id ? `user_${u.user_id}` : "неизвестный"]);
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
  return "н/д";
}

function safeDiv(numerator, denominator) {
  const n = Number(numerator);
  const d = Number(denominator);
  if (!isFinite(n) || !isFinite(d) || d === 0) return 0;
  return n / d;
}

function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

const _numFmt = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 });
function formatNum(v) { const n = Number(v); return isFinite(n) ? _numFmt.format(n) : "0"; }
function formatFlt(v) { const n = Number(v); return isFinite(n) ? n.toFixed(2) : "0.00"; }
function formatPct(v) { const n = Number(v); return isFinite(n) ? n.toFixed(2) : "0.00"; }

function fmtDate(iso) {
  if (!iso) return "—";
  try { return new Date(iso).toLocaleDateString("ru-RU", { day:"2-digit", month:"short", year:"numeric" }); }
  catch { return iso; }
}

function fmtTime(iso) {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toLocaleString("ru-RU", { day:"2-digit", month:"short", hour:"2-digit", minute:"2-digit" });
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