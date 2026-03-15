package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

type Result struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

func fetchURL(url string, wg *sync.WaitGroup, ch chan<- Result, sem chan struct{}) {

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

	doc, err := goquery.NewDocumentFromReader(resp.Body)

	if err != nil {
		log.Printf("Error fetching the doc; err: %s", err)
		return
	}

	title := doc.Find("title").Text()

	ch <- Result{URL: url, Title: title}

}

func main() {
	urls := []string{"https://example.com", "https://example.org"}

	var wg sync.WaitGroup

	ch := make(chan Result, len(urls))
	sem := make(chan struct{}, 5) // Limit to 5 concurrent fetches

	for _, url := range urls {
		wg.Add(1)
		go fetchURL(url, &wg, ch, sem)
	}

	wg.Wait()
	close(ch)

	for result := range ch {
		fmt.Printf("URL: %s\nTITLE: %s\n", result.URL, result.Title)
	}

}
