package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// Request structure for incoming data
type Request struct {
	Ev    string            `json:"ev"`
	Et    string            `json:"et"`
	ID    string            `json:"id"`
	UID   string            `json:"uid"`
	MID   string            `json:"mid"`
	T     string            `json:"t"`
	P     string            `json:"p"`
	L     string            `json:"l"`
	SC    string            `json:"sc"`
	ATRK  map[string]string `json:"atrk"`
	ATRV  map[string]string `json:"atrv"`
	ATRT  map[string]string `json:"atrt"`
	UATRK map[string]string `json:"uatrk"`
	UATRV map[string]string `json:"uatrv"`
	UATRT map[string]string `json:"uatrt"`
}

// Transformed structure for outgoing data
type Transformed struct {
	Event           string            `json:"event"`
	EventType       string            `json:"event_type"`
	AppID           string            `json:"app_id"`
	UserID          string            `json:"user_id"`
	MessageID       string            `json:"message_id"`
	PageTitle       string            `json:"page_title"`
	PageURL         string            `json:"page_url"`
	BrowserLanguage string            `json:"browser_language"`
	ScreenSize      string            `json:"screen_size"`
	Attributes      map[string]Attribute `json:"attributes"`
	Traits          map[string]Trait     `json:"traits"`
}

// Attribute structure for attributes in the transformed data
type Attribute struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

// Trait structure for traits in the transformed data
type Trait struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

func main() {
	// Create a channel to send requests to the worker
	requestChannel := make(chan Request, 10)

	// Use a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start the worker
	go worker(requestChannel, &wg)

	// Start the HTTP server
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		body := new(bytes.Buffer)
		_, err := body.ReadFrom(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}

		// Decode the incoming JSON request
		var req Request
		err = json.Unmarshal(body.Bytes(), &req)
		if err != nil {
			http.Error(w, "Error decoding JSON", http.StatusBadRequest)
			return
		}

		// Send the request to the channel for processing
		wg.Add(1)
		go func(req Request) {
			defer wg.Done()
			requestChannel <- req
		}(req)

		// Respond to the client
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Request received and processing started"))
	})

	// Start the HTTP server on port 8080
	fmt.Println("Server listening on :5000")
	http.ListenAndServe(":5000", nil)

	// Wait for all goroutines to finish before exiting
	wg.Wait()
}

func worker(ch <-chan Request, wg *sync.WaitGroup) {
	defer wg.Done()

	for req := range ch {
		// Process the request and transform the data
		transformedData := transformData(req)
		fmt.Println(":::::::::::::::::",transformedData)

		// Send the transformed data to the specified endpoint
		err := sendData(transformedData, "https://webhook.site/")
		if err != nil {
			fmt.Println("Error sending data:", err)
		}
	}
}

func transformData(req Request) Transformed {
	transformed := Transformed{
		Event:           req.Ev,
		EventType:       req.Et,
		AppID:           req.ID,
		UserID:          req.UID,
		MessageID:       req.MID,
		PageTitle:       req.T,
		PageURL:         req.P,
		BrowserLanguage: req.L,
		ScreenSize:      req.SC,
		Attributes:      make(map[string]Attribute),
		Traits:          make(map[string]Trait),
	}

	// Transform attributes
	for key, value := range req.ATRK {
		transformed.Attributes[key] = Attribute{
			Value: value,
			Type:  req.ATRT[key],
		}
	}

	// Transform user traits
	for key, value := range req.UATRK {
		transformed.Traits[key] = Trait{
			Value: value,
			Type:  req.UATRT[key],
		}
	}

	return transformed
}

func sendData(data Transformed, endpoint string) error {
	// Convert the transformed data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Send the JSON data to the specified endpoint
	_, err = http.Post(endpoint, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	return nil
}
