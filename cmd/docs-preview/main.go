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

	"github.com/bsv-blockchain/arcade/services/api_server"
)

func main() {
	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}

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

	log.Printf("arcade docs preview listening on http://localhost%s/", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
