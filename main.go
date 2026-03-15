package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/PuerkitoBio/goquery"
)

type Result struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

func fetchURL(url string, wg *sync.WaitGroup, ch chan<- Result) {

	defer wg.Done()
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching the resp, err: %s", err)
	}

	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)

	if err != nil {
		log.Printf("Error fetching the doc; err: %s", err)
	}

	title := doc.Find("title").Text()

	ch <- Result{URL: url, Title: title}

}

func main() {
	urls := []string{"https://example.com", "https://example.org"}

	var wg sync.WaitGroup

	ch := make(chan Result, len(urls))

	for _, url := range urls {
		wg.Add(1)
		go fetchURL(url, &wg, ch)
	}

	wg.Wait()
	close(ch)

	for result := range ch {
		fmt.Printf("URL: %s\nTITLE: %s\n", result.URL, result.Title)
	}

}
