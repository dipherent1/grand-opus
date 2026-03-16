package crawler

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dipherent1/grand-opus/internal/domain"
)

func FetchURL(url string, wg *sync.WaitGroup, ch chan<- domain.Content, sem chan struct{}) {

	defer wg.Done()

	sem <- struct{}{}        // Acquire semaphore
	defer func() { <-sem }() // Release semaphore

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)

	if err != nil {
		log.Printf("Error fetching the resp, err: %s", err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Error fetching the URL: %v", err)
		return // MUST return here so you don't use a nil 'resp'
	}

	defer resp.Body.Close()

	// doc, err := goquery.NewDocumentFromReader(resp.Body)

	if err != nil {
		log.Printf("Error fetching the doc; err: %s", err)
		return
	}

	// _ := doc.Find("title").Text()

	ch <- domain.Content{URL: url}

}
