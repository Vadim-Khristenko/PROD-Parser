package analysis

import (
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Vadim-Khristenko/PROD-Parser/internal/domain"
)

// interestRule определяет категорию интересов и набор ключевых слов/подстрок для её обнаружения.
// Матчинг регистронезависимый, подстроки: "баг" поймает "багфикс", "debugging" и т.д.
type interestRule struct {
	label    string
	keywords []string
}

var interestRules = []interestRule{

	// ═══════════════════════════════════════════════════
	//  ТЕХНОЛОГИИ
	// ═══════════════════════════════════════════════════

	{
		label: "software_engineering",
		keywords: []string{
			// EN — general
			"code", "coding", "codebase", "refactor", "rewrite", "clean code",
			"bug", "bugfix", "hotfix", "patch", "regression", "reproduce",
			"fix", "workaround", "hack", "edge case",
			"release", "deploy", "rollout", "rollback", "canary", "feature flag",
			"backend", "frontend", "fullstack", "monolith", "microservice",
			"pr", "pull request", "merge", "rebase", "commit", "diff", "review",
			"issue", "ticket", "todo", "fixme",
			"api", "rest", "graphql", "grpc", "websocket", "webhook",
			"sdk", "library", "dependency", "package", "module",
			"test", "unit test", "integration test", "e2e", "tdd", "coverage",
			"mock", "stub", "fixture",
			"architecture", "design pattern", "solid", "dry", "kiss",
			"singleton", "factory", "observer", "middleware",
			"async", "concurrency", "goroutine", "thread", "mutex", "race condition",
			"memory leak", "heap", "stack", "gc", "garbage collect",
			"compile", "build", "linker", "binary", "artifact",
			"version", "semver", "changelog",
			// EN — languages & runtimes
			"golang", "go ", "python", "typescript", "javascript", "java", "kotlin",
			"rust", "cpp", "c++", "c#", "scala", "elixir", "ruby", "swift",
			"php", "perl", "lua", "zig", "haskell", "ocaml",
			"node", "deno", "bun", "jvm", "wasm", "webassembly",
			// EN — data
			"sql", "nosql", "postgres", "postgresql", "mysql", "sqlite", "oracle",
			"redis", "memcached", "cassandra", "mongodb", "elasticsearch", "clickhouse",
			"kafka", "rabbitmq", "nats", "pubsub", "queue", "stream",
			"migration", "schema", "index", "query", "orm",
			// EN — infra
			"linux", "unix", "bash", "shell", "script",
			"docker", "container", "k8s", "kubernetes", "helm", "pod",
			"nginx", "caddy", "haproxy", "traefik",
			"server", "service", "daemon",
			// RU
			"код", "кодить", "кодовая база", "рефакторинг", "переписать",
			"баг", "багфикс", "хотфикс", "патч", "регрессия", "воспроизвести",
			"фикс", "воркараунд", "хак", "кейс",
			"релиз", "деплой", "выкатить", "выкатка", "откат",
			"бэкенд", "фронтенд", "фуллстек", "монолит", "микросервис",
			"ревью", "ревьюить", "мёрж", "ребейс", "коммит",
			"апи", "ручка", "эндпоинт",
			"зависимость", "либа", "библиотека", "модуль", "пакет",
			"тест", "покрытие", "мок",
			"архитектура", "паттерн", "абстракция",
			"конкурентность", "горутина", "мьютекс", "гонка данных",
			"утечка памяти", "куча", "сборщик мусора",
			"компилить", "билд", "бинарник", "артефакт",
			"версия", "чейнджлог",
			"скрипт", "шелл",
			"сервер", "сервис", "демон",
			"проект",
		},
	},

	{
		label: "ai_ml_llm",
		keywords: []string{
			// EN — LLMs & inference
			"llm", "gpt", "claude", "gemini", "mistral", "llama", "qwen", "deepseek",
			"openai", "anthropic", "groq", "together ai", "replicate", "hugging face",
			"prompt", "system prompt", "prompt engineering", "few-shot", "zero-shot", "chain of thought",
			"token", "context window", "context length", "kv cache",
			"inference", "generation", "completion", "streaming",
			"embedding", "vector", "similarity", "cosine",
			"rag", "retrieval", "rerank", "chunk",
			"agent", "tool use", "function calling", "mcp",
			"fine-tune", "lora", "qlora", "peft", "sft", "rlhf", "dpo",
			"quantiz", "gguf", "ggml", "awq", "exl2",
			// EN — ML / research
			"ai", "ml", "machine learning", "deep learning", "neural", "transformer",
			"attention", "self-attention", "ffn", "residual",
			"training", "epoch", "batch", "gradient", "loss", "backprop",
			"dataset", "data augmentation", "benchmark", "eval", "mmlu", "hellaswag",
			"stable diffusion", "diffusion", "gan", "vae", "image gen",
			"speech", "whisper", "tts", "stt",
			// EN — ops
			"vllm", "ollama", "llamacpp", "triton", "tensorrt", "onnx",
			"gpu", "cuda", "vram", "a100", "h100", "4090",
			// RU
			"нейросет", "нейронка", "нейро",
			"модель", "модельк",
			"промпт", "промптинг",
			"эмбед", "вектор", "похожест",
			"агент", "агентик",
			"дообуч", "файнтюн", "файн-тюн",
			"квантиз",
			"генерац", "инференс",
			"датасет", "обучени", "трениров",
			"токен",
		},
	},

	{
		label: "web_frontend",
		keywords: []string{
			// EN
			"react", "vue", "svelte", "angular", "solid", "qwik", "astro", "next", "nuxt", "remix",
			"jsx", "tsx", "component", "hook", "state", "props", "context", "redux", "zustand",
			"html", "css", "scss", "sass", "tailwind", "bootstrap", "shadcn",
			"dom", "browser", "chrome", "safari", "firefox",
			"bundle", "webpack", "vite", "esbuild", "rollup",
			"spa", "ssr", "ssg", "hydration", "island",
			"animation", "framer", "gsap", "transition",
			"responsive", "mobile", "accessibility", "a11y",
			"lighthouse", "web vitals", "cls", "lcp", "fid",
			// RU
			"компонент", "хук", "стейт", "пропс",
			"вёрстка", "вёрсток", "разметка",
			"стиль", "анимаци",
			"браузер",
		},
	},

	{
		label: "security_privacy",
		keywords: []string{
			// EN
			"security", "vulnerability", "vuln", "cve", "exploit", "payload",
			"injection", "xss", "sqli", "csrf", "ssrf", "rce", "lfi", "path traversal",
			"pentest", "ctf", "red team", "blue team", "soc",
			"auth", "oauth", "jwt", "session", "cookie", "token rotation",
			"password", "hash", "bcrypt", "argon", "salt",
			"encrypt", "decrypt", "tls", "ssl", "mtls", "certificate",
			"firewall", "waf", "ids", "ips",
			"censys", "shodan", "nuclei", "burp", "metasploit",
			"reverse shell", "c2", "malware", "ransomware",
			"zero day", "0day", "patch tuesday",
			"privacy", "gdpr", "pii", "data leak", "breach",
			// RU
			"безопасност", "уязвимост",
			"эксплойт", "пейлоад",
			"инъекци",
			"пентест",
			"авторизац", "аутентификац",
			"пароль", "хэш", "шифровани",
			"сертификат",
			"утечка данных", "слив",
		},
	},

	{
		label: "data_engineering_analytics",
		keywords: []string{
			// EN
			"etl", "elt", "pipeline", "airflow", "dagster", "prefect",
			"data warehouse", "data lake", "lakehouse",
			"dbt", "spark", "flink", "beam",
			"clickhouse", "bigquery", "snowflake", "redshift", "databricks",
			"analytics", "dashboard", "report", "chart", "kpi", "metric",
			"superset", "metabase", "grafana", "tableau", "looker",
			"pandas", "polars", "dask", "numpy",
			"parquet", "avro", "orc", "iceberg",
			"segment", "amplitude", "mixpanel", "ga4",
			// RU
			"аналитик", "дашборд", "отчёт", "график", "метрик",
			"данные", "датафрейм",
			"пайплайн", "витрина",
		},
	},

	{
		label: "open_source_community",
		keywords: []string{
			// EN
			"open source", "oss", "foss", "license", "mit", "apache", "gpl",
			"github", "gitlab", "gitea", "codeberg",
			"star", "fork", "contributor", "maintainer", "sponsor",
			"hacktoberfest", "gsoc",
			"community", "discord", "slack", "forum", "rfc", "spec",
			// RU
			"опенсорс", "опен-сорс",
			"контрибьют", "мейнтейнер",
			"лицензи", "сообщество",
		},
	},

	// ═══════════════════════════════════════════════════
	//  ПРОДУКТОВОЕ / БИЗНЕС
	// ═══════════════════════════════════════════════════

	{
		label: "product_management",
		keywords: []string{
			// EN
			"product", "roadmap", "vision", "strategy", "okr", "kpi",
			"feature", "epic", "user story", "acceptance criteria",
			"mvp", "prototype", "wireframe", "mockup", "design",
			"hypothesis", "experiment", "a/b test", "rollout",
			"customer", "user", "persona", "segment", "cohort",
			"churn", "ltv", "nps", "retention", "engagement",
			"feedback", "survey", "interview", "discovery",
			// RU
			"продукт", "роадмап", "стратеги", "видени",
			"фича", "фичатрек",
			"пользовател", "клиент", "сегмент",
			"гипотеза", "эксперимент",
			"онбординг", "онборд",
			"обратная связь", "интервью", "исследовани",
			"удержани", "отток",
		},
	},

	{
		label: "project_delivery",
		keywords: []string{
			// EN
			"sprint", "iteration", "standup", "scrum", "kanban", "agile",
			"deadline", "milestone", "eta", "due date", "overdue",
			"priority", "blocker", "dependency", "risk",
			"task", "subtask", "checklist", "done", "wip",
			"jira", "linear", "notion", "asana", "trello", "clickup",
			"qa", "testing", "staging", "uat", "sign-off",
			"scope", "scope creep", "estimate", "velocity",
			// RU
			"спринт", "стендап", "ретро",
			"дедлайн", "срок", "просрочк",
			"приоритет", "блокер",
			"задач", "подзадач",
			"тестировани", "стейджинг", "стейдж",
			"оценк", "скоуп",
			"план", "планировани",
		},
	},

	{
		label: "startup_business",
		keywords: []string{
			// EN
			"startup", "founder", "co-founder", "cto", "ceo", "coo",
			"funding", "seed", "series a", "vc", "investor", "pitch",
			"revenue", "mrr", "arr", "profit", "burn rate", "runway",
			"growth", "traction", "market fit", "pmf",
			"b2b", "b2c", "saas", "paas",
			"monetize", "subscription", "pricing", "upsell",
			// RU
			"стартап", "фаундер", "со-фаундер",
			"инвестор", "раунд", "питч", "привлечение",
			"выручка", "прибыль", "юнит-экономика",
			"рост", "трекшн", "монетизац",
			"подписка", "тариф",
		},
	},

	// ═══════════════════════════════════════════════════
	//  КОММУНИКАЦИЯ / СОЦИАЛЬНОЕ
	// ═══════════════════════════════════════════════════

	{
		label: "communication_coordination",
		keywords: []string{
			// EN
			"call", "meeting", "sync", "standup", "catchup", "1:1",
			"support", "help", "assist",
			"thanks", "thank you", "appreciate", "kudos",
			"chat", "reply", "mention", "dm", "ping",
			"announce", "fyi", "heads up",
			// RU
			"созвон", "встреч", "митинг", "колл",
			"поддержк", "помощ", "помоги",
			"спасибо", "благодар", "пасиб",
			"чат", "ответ", "пинг",
			"анонс", "объявлени",
		},
	},

	{
		label: "humor_memes",
		keywords: []string{
			// EN
			"lol", "lmao", "rofl", "haha", "kek", "lul",
			"meme", "shitpost", "based", "cringe", "bruh", "bro",
			"copypasta", "greentext", "troll", "bait",
			"sus", "poggers", "chad", "npc", "skill issue",
			"cope", "seethe", "ratio", "L + ratio",
			// RU
			"ахах", "хахах", "лол", "кек",
			"мем", "мемас", "приколюха", "прикол",
			"орнул", "ору", "умираю",
			"базд", "крингово", "крякнул",
			"скилл ишью", "ну и",
		},
	},

	{
		label: "philosophy_thinking",
		keywords: []string{
			// EN
			"philosophy", "ethics", "morality", "meaning", "consciousness",
			"free will", "determinism", "existential",
			"think", "thought", "opinion", "perspective", "belief",
			"truth", "knowledge", "epistemology",
			"stoic", "absurd", "nihilism",
			// RU
			"философи", "этик", "мораль", "смысл",
			"сознани", "свобода воли",
			"мысль", "размышлени", "мнени", "точка зрения",
			"истина", "знани",
			"стоицизм",
		},
	},

	// ═══════════════════════════════════════════════════
	//  OPS / НАБЛЮДАЕМОСТЬ
	// ═══════════════════════════════════════════════════

	{
		label: "ops_observability",
		keywords: []string{
			// EN
			"infra", "infrastructure", "devops", "sre", "platform eng",
			"ci", "cd", "github actions", "gitlab ci", "jenkins", "argocd", "flux",
			"terraform", "pulumi", "ansible", "chef", "puppet",
			"monitor", "alert", "pagerduty", "oncall", "incident", "postmortem",
			"metrics", "prometheus", "victoria", "datadog", "newrelic",
			"trace", "jaeger", "tempo", "opentelemetry", "otel",
			"log", "loki", "splunk", "elk", "fluentd",
			"latency", "p99", "sla", "slo", "sli",
			"timeout", "retry", "circuit breaker", "backoff",
			"uptime", "downtime", "outage",
			// RU
			"инфра", "инфраструктур",
			"монитор", "алёрт", "инцидент", "постмортем",
			"метрик", "трейс", "лог", "логи",
			"таймаут", "задержк", "латентност",
			"аптайм", "даунтайм", "падени",
		},
	},

	{
		label: "cloud_platforms",
		keywords: []string{
			// EN
			"aws", "amazon", "ec2", "s3", "lambda", "rds", "sqs", "sns",
			"gcp", "google cloud", "gke", "cloud run", "bigquery",
			"azure", "aks", "cosmos",
			"cloudflare", "vercel", "netlify", "fly.io", "render", "railway",
			"hetzner", "digitalocean", "linode", "vultr",
			"cdn", "edge", "region", "availability zone",
			// RU
			"облако", "облачн",
			"хостинг", "сервер",
			"регион",
		},
	},

	// ═══════════════════════════════════════════════════
	//  ОБРАЗ ЖИЗНИ / ЛИЧНОЕ
	// ═══════════════════════════════════════════════════

	{
		label: "health_wellness",
		keywords: []string{
			// EN
			"sleep", "tired", "exhausted", "burnout", "rest",
			"workout", "gym", "run", "sport", "exercise",
			"diet", "food", "eat", "nutrition", "fasting",
			"mental health", "anxiety", "stress", "meditation",
			"doctor", "sick", "ill", "health",
			// RU
			"сон", "устал", "выгорани", "отдых",
			"спортзал", "тренировк", "бег", "спорт",
			"диета", "еда", "питани", "голодани",
			"стресс", "тревог", "медитаци",
			"врач", "болезн", "здоровь",
		},
	},

	{
		label: "gaming",
		keywords: []string{
			// EN
			"game", "gaming", "steam", "play", "fps", "mmo", "rpg", "moba",
			"valorant", "cs2", "csgo", "dota", "lol", "minecraft", "roblox",
			"console", "ps5", "xbox", "switch", "nintendo",
			"speedrun", "grind", "rank", "elo", "meta",
			// RU
			"игр", "игра", "игры", "геймплей",
			"катк", "катаем", "катнем",
			"ранг", "рейтинг", "мета",
			"гринд", "фарм",
		},
	},

	{
		label: "media_entertainment",
		keywords: []string{
			// EN
			"movie", "film", "series", "anime", "manga", "book",
			"youtube", "twitch", "stream", "podcast",
			"music", "playlist", "track", "album", "concert",
			"netflix", "spotify", "crunchyroll",
			// RU
			"фильм", "сериал", "аниме", "манга", "книг",
			"ютуб", "стрим", "подкаст",
			"музык", "плейлист", "трек", "альбом", "концерт",
		},
	},

	{
		label: "travel_geography",
		keywords: []string{
			// EN
			"travel", "trip", "flight", "airport", "hotel", "visa",
			"country", "city", "europe", "asia",
			// RU
			"путешестви", "поездк", "перелёт", "аэропорт", "отель", "виза",
			"страна", "город", "европ", "азия", "москва", "питер", "пекин",
			"переехал", "переезд", "релокац",
		},
	},

	{
		label: "finance_crypto",
		keywords: []string{
			// EN
			"crypto", "bitcoin", "btc", "eth", "ethereum", "solana",
			"nft", "defi", "web3", "wallet", "blockchain",
			"invest", "stock", "market", "trading", "hedge fund",
			"salary", "budget", "money", "savings",
			// RU
			"крипта", "биткоин", "эфир",
			"инвестиц", "акци", "торговля",
			"зарплат", "бюджет", "деньги", "накопле",
		},
	},

	{
		label: "education_learning",
		keywords: []string{
			// EN
			"course", "tutorial", "learn", "study", "lecture", "university",
			"certificate", "degree", "bootcamp",
			"book", "paper", "research", "arxiv",
			// RU
			"курс", "туториал", "учёба", "обуч", "лекци", "универ", "вуз", "вышк", "hse",
			"сертификат", "диплом",
			"книг", "статья", "исследовани",
		},
	},

	{
		label: "design_creativity",
		keywords: []string{
			// EN
			"design", "ux", "ui", "figma", "wireframe", "prototype", "mockup",
			"typography", "illustration", "branding", "palette", "color", "motion design",
			"photoshop", "illustrator", "after effects", "dribbble", "behance",
			// RU
			"дизайн", "интерфейс", "фигма", "прототип", "макет",
			"типограф", "иллюстрац", "бренд", "палитр", "цвет",
			"моушн", "анимаци", "визуал",
		},
	},

	{
		label: "devrel_content",
		keywords: []string{
			// EN
			"docs", "documentation", "manual", "guide", "tutorial", "article", "blog", "post",
			"newsletter", "webinar", "workshop", "meetup", "conference", "talk", "speaker",
			"livestream", "recording", "knowledge base",
			// RU
			"документац", "дока", "мануал", "гайд", "туториал",
			"статья", "блог", "пост", "контент", "рассылк",
			"доклад", "спикер", "вебинар", "митап", "конференц", "стрим",
		},
	},

	{
		label: "career_hiring",
		keywords: []string{
			// EN
			"career", "hiring", "vacancy", "recruiter", "interview", "resume", "cv", "portfolio",
			"offer", "internship", "mentorship", "promotion", "compensation", "job", "headhunter",
			// RU
			"карьер", "ваканси", "найм", "рекрутер", "собеседовани", "резюме",
			"портфолио", "оффер", "стажировк", "ментор", "повышени", "зарплатн вилк",
		},
	},

	{
		label: "legal_compliance",
		keywords: []string{
			// EN
			"legal", "law", "contract", "agreement", "compliance", "regulation", "policy", "terms",
			"license", "audit", "soc2", "iso", "risk management", "consent", "privacy policy",
			// RU
			"юрид", "право", "договор", "соглашени", "комплаенс", "регуляц", "политик",
			"лицензи", "аудит", "персональн данн", "согласие", "норматив",
		},
	},

	{
		label: "automation_productivity",
		keywords: []string{
			// EN
			"automation", "automate", "workflow", "zapier", "n8n", "make.com", "ifttt",
			"no-code", "low-code", "macro", "template", "scheduler", "cron", "bot", "productivity",
			"shortcut", "integration",
			// RU
			"автоматизац", "воркфлоу", "запир", "n8n", "ноу-код", "лоу-код",
			"макрос", "шаблон", "крон", "бот", "продуктивност", "интеграц",
		},
	},
}

var interestStopWords = map[string]struct{}{
	"и": {}, "в": {}, "во": {}, "на": {}, "не": {}, "что": {}, "как": {}, "это": {}, "так": {}, "то": {}, "ну": {},
	"the": {}, "a": {}, "an": {}, "and": {}, "or": {}, "to": {}, "of": {}, "in": {}, "on": {}, "for": {},
	"is": {}, "are": {}, "am": {}, "be": {}, "this": {}, "that": {}, "it": {}, "you": {}, "we": {},
}

var interestStemSuffixes = []string{
	"ization", "isation", "ations", "ation", "ments", "ment", "ingly", "ingly", "ingly", "ingly", "ing", "ers", "er", "ies", "ied", "ed", "ly", "es", "s",
	"иями", "ями", "ами", "иях", "ях", "ах", "ого", "ему", "ому", "ыми", "ими", "ее", "ая", "яя", "ое", "ые", "ий", "ый", "ой", "ие", "ия", "ию", "ть", "ти", "ка", "ки", "ов", "ев",
}

// BuildPersonas creates heuristic personas ready to enrich by LLM later.
func BuildPersonas(accountID string, chatID int64, users []domain.UserStats, edges []domain.RelationEdge) []domain.Persona {
	edgesByUser := map[int64][]domain.RelationEdge{}
	for _, e := range edges {
		edgesByUser[e.FromUserID] = append(edgesByUser[e.FromUserID], e)
	}
	out := make([]domain.Persona, 0, len(users))
	for _, u := range users {
		role := detectRole(u)
		tone := detectTone(u)
		rel := make(map[int64]string)
		for _, e := range edgesByUser[u.UserID] {
			rel[e.ToUserID] = relationLabel(e)
		}
		out = append(out, domain.Persona{
			AccountID: accountID,
			ChatID:    chatID,
			UserID:    u.UserID,
			Role:      role,
			Tone:      tone,
			Traits: []string{
				activityTrait(u),
				toxicityTrait(u),
			},
			Interests:      wordsToInterests(interestSignals(u)),
			Triggers:       triggerHints(u),
			TypicalPhrases: typicalPhrases(u.TopWords),
			Relations:      rel,
			Summary:        summary(role, tone, u),
			Confidence:     personaConfidence(u),
			UpdatedAt:      time.Now().UTC(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	return out
}

func detectRole(u domain.UserStats) string {
	switch {
	case u.ActivityScore >= 0.6 && u.MessageSharePct >= 20:
		return "key_contributor"
	case u.MeaningfulWordRate >= 0.75 && u.MessagesTotal >= 100:
		return "expert_signal"
	case u.MessagesTotal > 2000:
		return "core_voice"
	case u.ReplyOut > u.MessagesTotal/2:
		return "discussion_driver"
	case u.MessagesTotal < 50:
		return "silent_observer"
	default:
		return "participant"
	}
}

func detectTone(u domain.UserStats) string {
	if u.AvgToxicity >= 0.55 {
		return "aggressive"
	}
	if u.AvgToxicity >= 0.3 {
		return "sharp"
	}
	return "supportive_or_neutral"
}

func activityTrait(u domain.UserStats) string {
	if u.MessageSharePct >= 30 {
		return "dominant_presence"
	}
	if u.ActiveDays >= 25 && u.AvgMessagesPerActiveDay >= 5 {
		return "consistently_active"
	}
	if u.MessagesTotal > 1500 {
		return "high_activity"
	}
	if u.MessagesTotal > 300 {
		return "steady_activity"
	}
	return "low_activity"
}

func toxicityTrait(u domain.UserStats) string {
	if u.AvgToxicity > 0.5 {
		return "high_conflict"
	}
	if u.AvgToxicity > 0.2 {
		return "occasionally_conflict"
	}
	return "calm"
}

func wordsToInterests(words []domain.WordScore) []string {
	if len(words) == 0 {
		return nil
	}
	candidates := keywordCandidates(words, 48)
	if len(candidates) == 0 {
		return nil
	}
	scores := make(map[string]float64, len(interestRules))
	for _, candidate := range candidates {
		term := normalizeInterestToken(candidate.Word)
		if term == "" {
			continue
		}
		weight := float64(candidate.Count)
		if weight < 1 {
			weight = 1
		}
		for _, rule := range interestRules {
			if strength := keywordMatchStrength(term, rule.keywords); strength > 0 {
				scores[rule.label] += weight * strength
			}
		}
	}
	type pair struct {
		label string
		score float64
	}
	all := make([]pair, 0, len(scores))
	for label, score := range scores {
		all = append(all, pair{label: label, score: score})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].score == all[j].score {
			return all[i].label < all[j].label
		}
		return all[i].score > all[j].score
	})
	if len(all) == 0 {
		return nil
	}

	threshold := 1.8
	maxScore := all[0].score
	if maxScore < threshold {
		threshold = math.Max(1.25, maxScore*0.78)
	}

	out := make([]string, 0, 5)
	for _, item := range all {
		if item.score < threshold {
			continue
		}
		out = append(out, item.label)
		if len(out) >= 5 {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func triggerHints(u domain.UserStats) []string {
	var out []string
	if u.AvgToxicity >= 0.5 {
		out = append(out, "conflict_topics")
	}
	if u.ReplyOut > u.MessagesTotal/2 {
		out = append(out, "direct_replies")
	}
	if len(out) == 0 {
		out = append(out, "insufficient_signal")
	}
	return out
}

func typicalPhrases(words []domain.WordScore) []string {
	if len(words) == 0 {
		return nil
	}
	filtered := keywordCandidates(words, 3)
	if len(filtered) > 0 {
		words = filtered
	}
	var b strings.Builder
	max := 3
	if len(words) < max {
		max = len(words)
	}
	for i := 0; i < max; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(words[i].Word)
	}
	return []string{b.String()}
}

func relationLabel(e domain.RelationEdge) string {
	switch {
	case e.Replies >= 15:
		return "frequent_dialogue"
	case e.TemporalAdjacency >= 20 && e.ContextOverlap >= 20:
		return "tight_context_loop"
	case e.Mentions >= 15:
		return "often_mentions"
	case e.CoTopicCount >= 10:
		return "shared_context"
	default:
		return "weak_link"
	}
}

func summary(role, tone string, u domain.UserStats) string {
	return role + ", " + tone + ", msgs=" + formatInt64(u.MessagesTotal)
}

func personaConfidence(u domain.UserStats) float64 {
	if u.MessagesTotal <= 0 {
		return 0
	}
	if u.MessagesTotal >= 200 {
		return 0.9
	}
	return 0.4 + 0.5*float64(u.MessagesTotal)/200
}

func keywordCandidates(words []domain.WordScore, limit int) []domain.WordScore {
	if len(words) == 0 || limit <= 0 {
		return nil
	}
	out := make([]domain.WordScore, 0, limit)
	for _, item := range words {
		term := strings.TrimSpace(strings.ToLower(item.Word))
		if term == "" {
			continue
		}
		if _, ok := interestStopWords[term]; ok {
			continue
		}
		if utf8.RuneCountInString(term) < 2 {
			continue
		}
		out = append(out, domain.WordScore{Word: term, Count: item.Count})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func interestSignals(u domain.UserStats) []domain.WordScore {
	if len(u.TopWords) == 0 && len(u.SmartWords) == 0 {
		return nil
	}

	merged := make(map[string]int64, len(u.TopWords)+len(u.SmartWords))
	order := make([]string, 0, len(u.TopWords)+len(u.SmartWords))

	add := func(word string, weight int64) {
		term := normalizeInterestToken(word)
		if term == "" {
			return
		}
		if _, stop := interestStopWords[term]; stop {
			return
		}
		if !hasMinRuneLen(term, 2) {
			return
		}
		if _, exists := merged[term]; !exists {
			order = append(order, term)
		}
		merged[term] += weight
	}

	for _, item := range u.TopWords {
		weight := item.Count
		if weight < 1 {
			weight = 1
		}
		add(item.Word, weight)
	}

	for _, item := range u.SmartWords {
		bonus := int64(math.Round(item.Score / 12.0))
		if bonus < 1 {
			bonus = 1
		}
		weight := item.Count + bonus
		add(item.Word, weight)
	}

	out := make([]domain.WordScore, 0, len(order))
	for _, term := range order {
		out = append(out, domain.WordScore{Word: term, Count: merged[term]})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Word < out[j].Word
		}
		return out[i].Count > out[j].Count
	})

	if len(out) > 48 {
		out = out[:48]
	}
	return out
}

func keywordMatchStrength(term string, keywords []string) float64 {
	if term == "" || len(keywords) == 0 {
		return 0
	}
	term = normalizeInterestToken(term)
	if term == "" {
		return 0
	}
	termStem := stemInterestToken(term)
	best := 0.0

	for _, keyword := range keywords {
		keyword = normalizeInterestToken(keyword)
		if keyword == "" {
			continue
		}
		keywordStem := stemInterestToken(keyword)

		if term == keyword {
			if best < 2.0 {
				best = 2.0
			}
			continue
		}
		if termStem != "" && keywordStem != "" && termStem == keywordStem {
			if best < 1.7 {
				best = 1.7
			}
			continue
		}

		if hasMinRuneLen(term, 4) && hasMinRuneLen(keyword, 4) {
			if strings.Contains(term, keyword) || strings.Contains(keyword, term) {
				if best < 1.35 {
					best = 1.35
				}
				continue
			}
		}

		if hasMinRuneLen(termStem, 4) && hasMinRuneLen(keywordStem, 4) {
			if strings.Contains(termStem, keywordStem) || strings.Contains(keywordStem, termStem) {
				if best < 1.3 {
					best = 1.3
				}
				continue
			}
			if sharedPrefixRunes(termStem, keywordStem) >= 6 && runeLenDelta(termStem, keywordStem) <= 1 {
				if best < 1.26 {
					best = 1.26
				}
				continue
			}
		}

		if sharedPrefixRunes(term, keyword) >= 7 && runeLenDelta(term, keyword) <= 1 {
			if best < 1.25 {
				best = 1.25
			}
		}
	}

	return best
}

func normalizeInterestToken(term string) string {
	term = strings.TrimSpace(strings.ToLower(term))
	term = strings.Trim(term, "#@")
	return term
}

func stemInterestToken(term string) string {
	term = normalizeInterestToken(term)
	if term == "" {
		return ""
	}
	for _, suffix := range interestStemSuffixes {
		if strings.HasSuffix(term, suffix) {
			base := strings.TrimSuffix(term, suffix)
			if hasMinRuneLen(base, 4) {
				return base
			}
		}
	}
	return term
}

func sharedPrefixRunes(a, b string) int {
	ar := []rune(a)
	br := []rune(b)
	n := len(ar)
	if len(br) < n {
		n = len(br)
	}
	prefix := 0
	for i := 0; i < n; i++ {
		if ar[i] != br[i] {
			break
		}
		prefix++
	}
	return prefix
}

func hasMinRuneLen(s string, min int) bool {
	if min <= 0 {
		return true
	}
	return utf8.RuneCountInString(s) >= min
}

func runeLenDelta(a, b string) int {
	da := utf8.RuneCountInString(a)
	db := utf8.RuneCountInString(b)
	if da > db {
		return da - db
	}
	return db - da
}
