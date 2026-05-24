package config

import (
	"os"
	"path/filepath"
	"strconv"
)

type Config struct {
	Port              string
	IngestDir         string
	ObsidianDir       string
	AudioDir          string
	WhisperURL        string
	WhisperClarityURL string // set to large-v3-turbo server (e.g. http://localhost:9001) for better clarity
	OllamaURL         string
	OllamaModel       string
	OllamaEmbedModel      string
	OllamaSummarizerModel string
	OllamaRouterModel     string // cheap routing model (e.g. qwen3:4b)
	OllamaSynthModel      string // expensive synthesis model (e.g. qwen3:14b)
	DBPath            string
	Workers           int
	OpenAIAPIKey      string // optional: when set, CLARITY_BACKEND=openai uses cloud Whisper API
	ClarityBackend    string // "local" (default) or "openai"

	// Clarity confidence thresholds — clips below these are re-sent to the clarity whisper.
	// avg_logprob is per-segment log probability (0 = perfect, more negative = worse).
	// no_speech_prob is the probability that the segment is silence/noise (0–1).
	ClarityLogprobThreshold float64 // env CLARITY_LOGPROB_THRESHOLD, default -0.7
	ClarityNoSpeechThreshold float64 // env CLARITY_NOSPEECH_THRESHOLD, default 0.6

	// OpenRouter — when set, the summarizer routes through OpenRouter's
	// OpenAI-compatible API instead of local Ollama. Quality is dramatically
	// better for fragmentary voice transcripts.
	OpenRouterAPIKey          string // env: OPENROUTER_API_KEY
	OpenRouterSummarizerModel string // env: OPENROUTER_SUMMARIZER_MODEL, default "anthropic/claude-haiku-4-5"

	// Gemini — when set, takes precedence over OpenRouter for the summarizer
	// and the daily-rollup narrative. Uses Gemini's OpenAI-compatible endpoint.
	GeminiAPIKey          string // env: GEMINI_API_KEY
	GeminiSummarizerModel string // env: GEMINI_SUMMARIZER_MODEL, default "gemini-2.5-flash"

	// Plex Smash Deck — voice "play <movie>" intents proxy through this
	// service so Nōgura never touches Plex credentials directly.
	PlexDashboardURL   string
	PlexDefaultClient  string // fallback target when the user doesn't name a client

	// PlexTVFilterThreshold is the minimum word-trigram match ratio between a
	// mic clip and the currently-playing TV captions before the clip is
	// dropped as TV chatter. 0.5 means 50% of the clip's trigrams must match.
	// Env: PLEX_TV_FILTER_THRESHOLD, default 0.5.
	PlexTVFilterThreshold float64

	// MediaSignalThreshold is the minimum media_confidence score a clip must
	// reach to be emitted without the [~media?] annotation. Clips below this
	// threshold are flagged as likely ambient media (TV, podcast, radio) in the
	// transcript and summarizer prompt, but are NEVER dropped.
	// Env: MEDIA_SIGNAL_THRESHOLD, default 0.6.
	MediaSignalThreshold float64

	// AudioDevice — if non-empty, the streamer auto-starts on this device at
	// boot. Empty = user must click "Start" in the UI. Use `pactl list sources
	// short` to see available device names.
	AudioDevice string

	// LGWebOSURL is the base URL of the lg-webos-smash-deck service.
	// Voice volume commands ("computer, turn volume to 50%") dispatch here.
	LGWebOSURL string

	// WyzeURL is the base URL of the wyze-smash-deck service.
	WyzeURL string

	// SceneRunnerURL is the base URL of the scene-runner / GoApe service.
	SceneRunnerURL string

	// ScaffoldEnabled gates the ScaffoldWorker. Must be explicitly set to true
	// via SCAFFOLD_ENABLED=true; defaults to false so no surprise branches are
	// created.
	ScaffoldEnabled bool

	// WhisperInitialPrompt biases the Whisper decoder toward domain-specific
	// vocabulary so proper nouns (Nōgura, Khoj, ECAPA, etc.) are not garbled.
	// Env: WHISPER_INITIAL_PROMPT. Pass an empty string to disable.
	WhisperInitialPrompt string

	// Mem0URL is the base URL of the mem0 FastAPI sidecar.
	// When empty (the default), all mem0 calls are no-ops.
	Mem0URL string

	// WorkspaceRoot is the directory that contains all sibling Go projects.
	// WorkspaceIndexer walks this directory to build the cross-project context index.
	WorkspaceRoot string

	// SmartTurnURL is the base URL of the smart-turn sidecar (port 9202).
	// When empty, smart-turn gating is disabled and the pipeline falls back to
	// VAD-only end-of-utterance detection. Env: SMART_TURN_URL.
	SmartTurnURL string

	// LiveQACompaction enables rolling context compaction for the live Q&A worker.
	// When true, the oldest half of the session buffer is summarised by the router
	// model whenever the buffer reaches liveQASessionBufferMax. Defaults to false —
	// opt-in only because the compaction LLM call adds latency to the buffer append
	// path. Env: LIVE_QA_COMPACTION=true.
	LiveQACompaction bool

	// InlineExtractorEnabled enables the InlineExtractorStage, which extracts
	// structured topic claims from each accepted clip in real-time (router model only).
	// Doubles LLM calls per clip. Opt-in via INLINE_EXTRACTOR_ENABLED=true.
	// Defaults to false.
	InlineExtractorEnabled bool
}

func Load() *Config {
	whisperURL := getEnv("WHISPER_URL", "http://localhost:9000")
	return &Config{
		Port:              getEnv("PORT", "8190"),
		IngestDir:         getEnv("INGEST_DIR", "/app/audio_ingest"),
		ObsidianDir:       getEnv("OBSIDIAN_DIR", "/app/obsidian_vault"),
		AudioDir:          getEnv("AUDIO_DIR", "data/audio"),
		WhisperURL:        whisperURL,
		WhisperClarityURL: getEnv("WHISPER_CLARITY_URL", whisperURL),
		OllamaURL:         getEnv("OLLAMA_URL", "http://localhost:11434"),
		OllamaModel:       getEnv("OLLAMA_MODEL", "qwen3:14b"),
		OllamaEmbedModel:  getEnv("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
		OllamaSummarizerModel: getEnv("OLLAMA_SUMMARIZER_MODEL", "qwen3:4b"),
		OllamaRouterModel:     getEnv("OLLAMA_ROUTER_MODEL", "qwen3:4b"),
		OllamaSynthModel:      getEnv("OLLAMA_SYNTH_MODEL", "qwen3:14b"),
		DBPath:         getEnv("DB_PATH", "data/nogura.db"),
		Workers:        1,
		OpenAIAPIKey:   getEnv("OPENAI_API_KEY", ""),
		ClarityBackend: getEnv("CLARITY_BACKEND", "local"),
		OpenRouterAPIKey:          getEnv("OPENROUTER_API_KEY", ""),
		OpenRouterSummarizerModel: getEnv("OPENROUTER_SUMMARIZER_MODEL", "anthropic/claude-haiku-4-5"),
		GeminiAPIKey:              getEnv("GEMINI_API_KEY", ""),
		GeminiSummarizerModel:     getEnv("GEMINI_SUMMARIZER_MODEL", "gemini-2.5-flash"),
		PlexDashboardURL:          getEnv("PLEX_DASHBOARD_URL", "http://localhost:8081"),
		PlexDefaultClient:         getEnv("PLEX_DEFAULT_CLIENT", ""),
		PlexTVFilterThreshold:     getEnvFloat("PLEX_TV_FILTER_THRESHOLD", 0.5),
		MediaSignalThreshold:      getEnvFloat("MEDIA_SIGNAL_THRESHOLD", 0.6),
		AudioDevice:       getEnv("AUDIO_DEVICE", "default"),
		LGWebOSURL:        getEnv("LG_WEBOS_URL", "http://localhost:8088"),
		WyzeURL:           getEnv("WYZE_URL", "http://localhost:8082"),
		SceneRunnerURL:    getEnv("SCENE_RUNNER_URL", "http://localhost:8100"),
		ClarityLogprobThreshold:  getEnvFloat("CLARITY_LOGPROB_THRESHOLD", -0.7),
		ClarityNoSpeechThreshold: getEnvFloat("CLARITY_NOSPEECH_THRESHOLD", 0.6),
		ScaffoldEnabled:          os.Getenv("SCAFFOLD_ENABLED") == "true",
		WhisperInitialPrompt: getEnv("WHISPER_INITIAL_PROMPT", "Nōgura, Khoj, ECAPA, mem0, OpenRouter, Ollama, qwen, Plex, Smash Deck, Patch Hatch, sqlite-vec, RNNoise."),
		Mem0URL:          getEnv("MEM0_URL", ""),
		WorkspaceRoot:    getEnv("WORKSPACE_ROOT", filepath.Join(os.Getenv("HOME"), "goprojects")),
		SmartTurnURL:           getEnv("SMART_TURN_URL", ""),
		LiveQACompaction:       os.Getenv("LIVE_QA_COMPACTION") == "true",
		InlineExtractorEnabled: os.Getenv("INLINE_EXTRACTOR_ENABLED") == "true",
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
