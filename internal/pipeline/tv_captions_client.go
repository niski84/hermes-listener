package pipeline

// PlexCaptionsClient fetches captions on demand from the plex-dashboard
// captions endpoint for a clip's CapturedAt timestamp. Results are cached in
// memory keyed by IMDb id with a 4h TTL so repeated clips from the same movie
// don't re-download or re-parse the captions text.
//
// Lookup pipeline:
//   1. GET /api/playback/at?time=<rfc3339> — cheap, returns imdbID + ratingKey.
//      204 means nothing was playing — short-circuit fail-open.
//   2. Cache lookup by imdbID. Hit → return cached grams.
//   3. GET /api/playback/captions?at=<rfc3339> — fetch + parse + trigram.
//   4. Store grams in cache for next time.
//
// Fail-open contract: any HTTP error, non-200, or empty caption body returns
// ok=false. NEVER returns an error.

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	tvCaptionsHTTPTimeout = 5 * time.Second
	tvCaptionsCacheTTL    = 4 * time.Hour
)

// captionsResponse mirrors the plex-dashboard /api/playback/captions payload.
// Extra fields are ignored.
type captionsResponse struct {
	Captions string `json:"captions"`
	Title    string `json:"title"`
	IMDBID   string `json:"imdbId"`
}

// playbackAtResponse mirrors plex-dashboard /api/playback/at. Only the fields
// we care about are listed.
type playbackAtResponse struct {
	IMDbID    string `json:"imdb_id"`
	RatingKey string `json:"rating_key"`
	Title     string `json:"title"`
}

// captionsCacheEntry is one IMDb id → trigram set + title, stamped with the
// time of the fetch so we can evict on TTL.
type captionsCacheEntry struct {
	grams     map[string]struct{}
	title     string
	imdbID    string
	fetchedAt time.Time
}

// PlexCaptionsClient performs on-demand caption lookups. Concurrent calls are
// safe; the cache is protected by a RWMutex.
type PlexCaptionsClient struct {
	BaseURL    string
	PlayerName string
	httpClient *http.Client

	mu    sync.RWMutex
	cache map[string]captionsCacheEntry // keyed by IMDb id
}

// NewPlexCaptionsClient builds a client. baseURL is the plex-dashboard root
// (e.g. http://localhost:8081). playerName is optional — empty means no
// `?player=` query param.
//
// The pollInterval argument is retained for backwards compatibility; the
// poller has been replaced by per-clip on-demand lookups, so it is unused.
func NewPlexCaptionsClient(baseURL, playerName string, _ time.Duration) *PlexCaptionsClient {
	return &PlexCaptionsClient{
		BaseURL:    baseURL,
		PlayerName: playerName,
		httpClient: &http.Client{Timeout: tvCaptionsHTTPTimeout},
		cache:      make(map[string]captionsCacheEntry),
	}
}

// Start is a no-op — kept so existing callers compile.
func (c *PlexCaptionsClient) Start(ctx context.Context) {
	_ = ctx
}

// CaptionsForTime returns the trigram set + movie title for whatever was
// playing at time t. Returns (nil, "", "", false) when no movie was playing
// or any fetch fails. NEVER returns an error.
func (c *PlexCaptionsClient) CaptionsForTime(ctx context.Context, t time.Time) (map[string]struct{}, string, string, bool) {
	if c == nil || c.BaseURL == "" {
		return nil, "", "", false
	}

	// Step 1: cheap probe to learn what was playing.
	imdbID, title, ok := c.fetchPlaybackAt(ctx, t)
	if !ok || imdbID == "" {
		return nil, "", "", false
	}

	// Step 2: cache lookup.
	if entry, ok := c.lookupCache(imdbID); ok {
		return entry.grams, entry.title, entry.imdbID, true
	}

	// Step 3: full captions fetch.
	grams, fetchedTitle, fetchedIMDb, ok := c.fetchCaptions(ctx, t)
	if !ok {
		return nil, "", "", false
	}
	useTitle := fetchedTitle
	if useTitle == "" {
		useTitle = title
	}
	useIMDb := fetchedIMDb
	if useIMDb == "" {
		useIMDb = imdbID
	}
	c.storeCache(useIMDb, captionsCacheEntry{
		grams:     grams,
		title:     useTitle,
		imdbID:    useIMDb,
		fetchedAt: time.Now(),
	})
	return grams, useTitle, useIMDb, true
}

func (c *PlexCaptionsClient) fetchPlaybackAt(ctx context.Context, t time.Time) (string, string, bool) {
	endpoint := c.BaseURL + "/api/playback/at"
	q := url.Values{}
	q.Set("time", t.UTC().Format(time.RFC3339))
	if c.PlayerName != "" {
		q.Set("player", c.PlayerName)
	}
	endpoint += "?" + q.Encode()

	reqCtx, cancel := context.WithTimeout(ctx, tvCaptionsHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		log.Printf("[tv_filter] playback/at build: %v", err)
		return "", "", false
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[tv_filter] playback/at fetch: %v", err)
		return "", "", false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return "", "", false
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("[tv_filter] playback/at http %d", resp.StatusCode)
		return "", "", false
	}
	var body playbackAtResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		log.Printf("[tv_filter] playback/at decode: %v", err)
		return "", "", false
	}
	return body.IMDbID, body.Title, true
}

func (c *PlexCaptionsClient) fetchCaptions(ctx context.Context, t time.Time) (map[string]struct{}, string, string, bool) {
	endpoint := c.BaseURL + "/api/playback/captions"
	q := url.Values{}
	q.Set("at", t.UTC().Format(time.RFC3339))
	if c.PlayerName != "" {
		q.Set("player", c.PlayerName)
	}
	endpoint += "?" + q.Encode()

	reqCtx, cancel := context.WithTimeout(ctx, tvCaptionsHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		log.Printf("[tv_filter] captions build: %v", err)
		return nil, "", "", false
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[tv_filter] captions fetch: %v", err)
		return nil, "", "", false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil, "", "", false
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("[tv_filter] captions http %d", resp.StatusCode)
		return nil, "", "", false
	}
	var body captionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		log.Printf("[tv_filter] captions decode: %v", err)
		return nil, "", "", false
	}
	words := normalizeForGrams(body.Captions)
	grams := wordTrigrams(words)
	if len(grams) == 0 {
		return nil, "", "", false
	}
	return grams, body.Title, body.IMDBID, true
}

// lookupCache returns a cache entry if present and still within TTL.
func (c *PlexCaptionsClient) lookupCache(imdbID string) (captionsCacheEntry, bool) {
	c.mu.RLock()
	entry, ok := c.cache[imdbID]
	c.mu.RUnlock()
	if !ok {
		return captionsCacheEntry{}, false
	}
	if time.Since(entry.fetchedAt) > tvCaptionsCacheTTL {
		c.mu.Lock()
		delete(c.cache, imdbID)
		c.mu.Unlock()
		return captionsCacheEntry{}, false
	}
	return entry, true
}

func (c *PlexCaptionsClient) storeCache(imdbID string, entry captionsCacheEntry) {
	c.mu.Lock()
	c.cache[imdbID] = entry
	c.mu.Unlock()
}

// Snapshot satisfies the CaptionsSnapshotter interface used by TVChatFilterStage
// and the legacy test suite. Returns trigrams + title for whatever is playing
// "now"; ok=false when no captions can be fetched. NEVER returns an error.
//
// This is a thin shim over CaptionsForTime — callers that need a specific
// timestamp should still use that method directly.
func (c *PlexCaptionsClient) Snapshot() (map[string]struct{}, string, bool) {
	if c == nil {
		return nil, "", false
	}
	grams, title, _, ok := c.CaptionsForTime(context.Background(), time.Now())
	return grams, title, ok
}
