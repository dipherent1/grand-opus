package crawler

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"fmt"

	"github.com/PuerkitoBio/goquery"
	"github.com/dipherent1/grand-opus/internal/domain"
	"github.com/google/uuid"
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

	doc, err := goquery.NewDocumentFromReader(resp.Body)

	if err != nil {
		log.Printf("Error fetching the doc; err: %s", err)
		return
	}

	html, err := doc.Html()
	if err != nil {
		log.Printf("Error fetching the html content; err: %s", err)
	}

	bodyContent, err := doc.Find("body").Html()
	if err != nil {
		log.Printf("Error finding body html content; err: %s", err)
	}

	textContent := doc.Find("body").Text()

	ch <- domain.Content{
		Id:        uuid.New().String(),
		URL:       url,
		Title:     doc.Find("title").Text(),
		Desc:      doc.Find("meta[name='description']").AttrOr("content", ""),
		RawHtml:   html,
		Content:   bodyContent,
		Text:      textContent,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

}

func Crawl() {

	urls := []string{"https://example.com", "https://example.org", "https://github.com/tonywangcn/distributed-web-crawler/blob/master/go/src/crawler/crawler.go"}

	var wg sync.WaitGroup

	ch := make(chan domain.Content, len(urls))
	sem := make(chan struct{}, 5) // Limit to 5 concurrent fetches

	for _, url := range urls {
		wg.Add(1)
		go FetchURL(url, &wg, ch, sem)
	}

	wg.Wait()
	close(ch)

	for result := range ch {
		fmt.Printf("URL: %s\n", result.URL)
	}

}
