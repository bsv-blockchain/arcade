// Package main contains an SSE client example.
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
)

//nolint:gocyclo // example client with multiple error handling paths
func main() {
	callbackToken := "example-token-123"
	url := fmt.Sprintf("http://localhost:3011/events?callbackToken=%s", callbackToken)

	// Create request with optional Last-Event-ID for catchup
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Uncomment to test catchup from specific timestamp (nanoseconds)
	// req.Header.Set("Last-Event-ID", "1699632000000000000")

	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to connect: %s", resp.Status)
	}

	log.Println("Connected to SSE stream...")

	scanner := bufio.NewScanner(resp.Body)
	var eventID, eventType, eventData string

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// Empty line signals end of event
			if eventData != "" {
				log.Printf("[ID: %s] [Type: %s] %s\n", eventID, eventType, eventData)
				eventID, eventType, eventData = "", "", ""
			}
			continue
		}

		if strings.HasPrefix(line, "id: ") {
			eventID = strings.TrimPrefix(line, "id: ")
		} else if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			eventData = strings.TrimPrefix(line, "data: ")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
