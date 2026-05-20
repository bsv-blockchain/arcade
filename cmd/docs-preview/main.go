// Command docs-preview serves the rendered Arcade API docs landing page
// at http://localhost:8080/ so it can be inspected in a browser without
// standing up the full API server (Kafka, store, validator, ...).
//
// Usage:
//
//	go run ./cmd/docs-preview
//	PORT=9000 go run ./cmd/docs-preview
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bsv-blockchain/arcade/services/api_server"
)

func main() {
	port := 8080
	if p := os.Getenv("PORT"); p != "" {
		parsed, err := strconv.Atoi(p)
		if err != nil || parsed < 1 || parsed > 65535 {
			log.Fatal("invalid PORT: must be an integer between 1 and 65535")
		}
		port = parsed
	}
	addr := ":" + strconv.Itoa(port)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := api_server.RenderDocs(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	log.Printf("arcade docs preview listening on http://localhost%s/", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
