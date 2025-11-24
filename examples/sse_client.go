package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func main() {
	callbackToken := "example-token-123"
	url := fmt.Sprintf("http://localhost:8080/v1/events/%s", callbackToken)

	// Create request with optional Last-Event-ID for catchup
	req, err := http.NewRequest("GET", url, nil)
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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to connect: %s", resp.Status)
	}

	fmt.Println("Connected to SSE stream...")

	scanner := bufio.NewScanner(resp.Body)
	var eventID, eventType, eventData string

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// Empty line signals end of event
			if eventData != "" {
				fmt.Printf("[ID: %s] [Type: %s] %s\n", eventID, eventType, eventData)
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
