package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// NewServer builds and returns the HTTP mux.
func NewServer() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "hermes-listener"})
	})

	return mux
}

func respondJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Printf("[hermes-listener] encode error: %v\n", err)
	}
}
