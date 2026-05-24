package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"hermes-listener/internal/api"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9120"
	}
	mux := api.NewServer()
	fmt.Printf("[hermes-listener] ready at http://localhost:%s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
