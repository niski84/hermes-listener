package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"hermes-listener/internal/settings"
	"hermes-listener/internal/startup"
	"hermes-listener/internal/web"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9120"
	}

	// .env lives at project root. Use working dir as the anchor when
	// invoked via systemd we set WorkingDirectory there too.
	envPath := filepath.Join(".", ".env")

	settingsStore, err := settings.New(envPath)
	if err != nil {
		log.Fatalf("[hermes-listener] settings: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	audio, err := startup.SetupAudio(ctx)
	if err != nil {
		log.Fatalf("[hermes-listener] audio setup failed: %v", err)
	}

	webSrv, err := web.NewServer(settingsStore, audio.ChannelManager, envPath)
	if err != nil {
		log.Fatalf("[hermes-listener] web setup failed: %v", err)
	}

	server := &http.Server{Addr: ":" + port, Handler: webSrv.Mux()}

	go func() {
		log.Printf("[hermes-listener] HTTP ready at http://localhost:%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[hermes-listener] server failed: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[hermes-listener] shutdown signal received")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	log.Println("[hermes-listener] stopped")
}
