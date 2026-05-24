package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"hermes-listener/internal/startup"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9120"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Wire the capture stack (hub, daily-transcript writer, channel manager,
	// default mic). The mic starts on its own goroutine inside ChannelManager.
	audio, err := startup.SetupAudio(ctx)
	if err != nil {
		log.Fatalf("[hermes-listener] audio setup failed: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":   "ok",
			"service":  "hermes-listener",
			"channels": audio.ChannelManager.List(),
		})
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hermes-listener — passive voice listener\nGET /api/health for status\n")
	})

	server := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		log.Printf("[hermes-listener] HTTP ready at http://localhost:%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[hermes-listener] server failed: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[hermes-listener] shutdown signal received")
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	log.Println("[hermes-listener] stopped")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
