package pipeline

import (
	"regexp"
	"strings"
	"unicode"
)

// classifyText returns a score in [0.0, 1.0] where:
//
//	1.0 = strongly conversational (user's own speech)
//	0.0 = strongly broadcast/media (TV, podcast, news)
//
// It is a pure-Go, zero-LLM, zero-latency heuristic pass over transcribed
// text. It is called per-clip on the hot path — no logging, no allocations
// beyond the regex matches below.
func classifyText(text string) float64 {
	words := strings.Fields(text)
	if len(words) < 4 {
		return 1.0
	}

	lower := strings.ToLower(text)
	penalty := 0.0

	// ── Broadcast indicators ────────────────────────────────────────────────

	// 1. 3rd-person declarative statements
	//    "the [noun] announced/said/reported/confirmed/revealed"
	//    "investigators found", "officials said", "she/he told reporters"
	if reBroadcastDeclarative.MatchString(lower) {
		penalty += 0.30
	}

	// 2. News-style quantifiers: each match adds 0.15, capped at 0.25
	quantifierMatches := 0
	for _, kw := range newsQuantifierKeywords {
		if strings.Contains(lower, kw) {
			quantifierMatches++
		}
	}
	if quantifierMatches > 0 {
		q := float64(quantifierMatches) * 0.15
		if q > 0.25 {
			q = 0.25
		}
		penalty += q
	}

	// 2b. "According to" is a strong broadcast attribution marker on its own.
	if strings.Contains(lower, "according to") {
		penalty += 0.30
	}

	// 3. Passive + institution combo: "[institution] has [past participle]"
	if rePassiveInstitution.MatchString(lower) {
		penalty += 0.55
	}

	// 3b. Named institution + action (without "the"): "Postal Service inspectors identify"
	if reInstitutionAction.MatchString(text) {
		penalty += 0.55
	}

	// 3c. Policy/news vocabulary
	if rePolicyLanguage.MatchString(lower) {
		penalty += 0.25
	}

	// 3d. News-framing constructions: "remains a concern", "raises concerns",
	//     "poses a risk", "X in Washington/Brussels/Beijing" etc.
	if reNewsFraming.MatchString(lower) {
		penalty += 0.20
	}

	// 4. Quoted speech markers
	if strings.Contains(text, `: "`) || strings.Contains(lower, ` saying "`) || strings.Contains(lower, " told ") {
		penalty += 0.20
	}

	// 5. Named entity density: 3+ proper nouns in < 20 words
	if len(words) < 20 && countProperNouns(words) >= 3 {
		penalty += 0.20
	}

	// ── Conversational reducers ─────────────────────────────────────────────

	reduction := 0.0

	// 1. First-person pronouns: each reduces by 0.15, cap 0.30
	firstPersonCount := 0
	for _, kw := range firstPersonKeywords {
		if reWholeWord(kw).MatchString(lower) {
			firstPersonCount++
		}
	}
	if firstPersonCount > 0 {
		r := float64(firstPersonCount) * 0.15
		if r > 0.30 {
			r = 0.30
		}
		reduction += r
	}

	// 2. Filler words: reduce by 0.10, cap 0.20
	fillerCount := 0
	for _, kw := range fillerKeywords {
		if reWholeWord(kw).MatchString(lower) {
			fillerCount++
		}
	}
	if fillerCount > 0 {
		r := float64(fillerCount) * 0.10
		if r > 0.20 {
			r = 0.20
		}
		reduction += r
	}

	// 3. Direct address / question form: reduce by 0.15
	if reDirectAddress.MatchString(lower) {
		reduction += 0.15
	}

	// ── Final score ─────────────────────────────────────────────────────────

	net := penalty - reduction
	if net < 0 {
		net = 0
	}
	score := 1.0 - net
	if score < 0.0 {
		score = 0.0
	} else if score > 1.0 {
		score = 1.0
	}
	return score
}

// ── Compiled patterns ────────────────────────────────────────────────────────

var reBroadcastDeclarative = regexp.MustCompile(
	`\b(the\s+\w+\s+(announced|said|reported|confirmed|revealed)|` +
		`investigators?\s+found|officials?\s+said|officials?\s+(confirmed|reported|announced)|` +
		`inspectors?\s+(found|identified|said|confirmed)|` +
		`she\s+told\s+reporters|he\s+told\s+reporters|` +
		`(police|authorities|sources)\s+(said|confirmed|reported))\b`,
)

var rePassiveInstitution = regexp.MustCompile(
	`\b(the\s+fed|congress|the\s+senate|the\s+white\s+house|` +
		`the\s+department|the\s+administration|the\s+agency|` +
		`the\s+committee|the\s+government|the\s+court)\s+has\s+\w+(ed|n)\b`,
)

// reInstitutionAction matches "[Institution Name] [verb]" patterns without "the"
// e.g. "Postal Service inspectors identify", "Federal Reserve raises"
var reInstitutionAction = regexp.MustCompile(
	`\b[A-Z][a-z]+\s+[A-Z][a-z]+\s+(inspectors?|officials?|agents?|authorities)\b`,
)

var reDirectAddress = regexp.MustCompile(
	`\b(can\s+you|what\s+do\s+you\s+think|let\s+me|i\s+want\s+to)\b`,
)

// rePolicyLanguage matches language characteristic of news/policy writing
var rePolicyLanguage = regexp.MustCompile(
	`\b(policymakers?|lawmakers?|legislators?|fiscal|monetary|inflation|` +
		`sanctions?|tariffs?|geopolitical|constituents?|bipartisan)\b`,
)

// reNewsFraming matches structural framing phrases common in news writing
var reNewsFraming = regexp.MustCompile(
	`\b(remains?\s+a\s+(concern|risk|threat|challenge)|` +
		`poses?\s+a\s+(risk|threat|challenge)|` +
		`raises?\s+concerns?|` +
		`in\s+(Washington|Brussels|Beijing|Moscow|London|Geneva|Tokyo))\b`,
)

// reWholeWord builds a simple whole-word pattern for a keyword.
// Cached on first call via a small map; safe for read-concurrent use after
// init since Go's sync/atomic map isn't needed here (patterns are fixed).
var wholeWordCache = map[string]*regexp.Regexp{}

func reWholeWord(word string) *regexp.Regexp {
	if re, ok := wholeWordCache[word]; ok {
		return re
	}
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(word) + `\b`)
	wholeWordCache[word] = re
	return re
}

var newsQuantifierKeywords = []string{
	"percent", "billion", "million", "federal",
}

var firstPersonKeywords = []string{
	"i'm", "i've", "i'd", "i'll", "my", "we", "our", "i",
}

var fillerKeywords = []string{
	"uh", "um", "yeah", "so", "like", "you know",
}

// countProperNouns returns the number of Title-Case words that are not the
// first word of the text. Single-character words are excluded (initials etc.).
func countProperNouns(words []string) int {
	count := 0
	for i, w := range words {
		if i == 0 {
			continue
		}
		// Strip trailing punctuation
		clean := strings.TrimRight(w, ".,!?;:\"'")
		if len(clean) < 2 {
			continue
		}
		runes := []rune(clean)
		if unicode.IsUpper(runes[0]) && !allUpper(runes) {
			count++
		}
	}
	return count
}

func allUpper(runes []rune) bool {
	for _, r := range runes {
		if unicode.IsLower(r) {
			return false
		}
	}
	return true
}
