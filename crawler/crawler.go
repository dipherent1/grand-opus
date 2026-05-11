package crawler

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	neturl "net/url"

	"github.com/PuerkitoBio/goquery"
	"github.com/dipherent1/grand-opus/config"
	"github.com/dipherent1/grand-opus/internal/domain"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CrawlerConfig struct {
	MaxPages            int64
	MaxConcurrentFetches int

	// Dependencies
	DB *mongo.Database // Use a more descriptive name than 'client'

	// Internal State
	wg        *sync.WaitGroup
	pageCount *atomic.Int64
	visited   *sync.Map
	urlQueue  chan string
	resultsCh chan domain.Content
	sem       chan struct{}
}

func FetchURL(params CrawlerConfig, url string) {

	defer params.wg.Done()

	params.sem <- struct{}{}        // Acquire semaphore
	defer func() { <-params.sem }() // Release semaphore

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

	baseURL, err := neturl.Parse(url)
	if err != nil {
		log.Printf("Error parsing the URL: %v", err)
		return
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {

		if params.pageCount.Load() >= params.MaxPages {
			return
		}

		href, exists := s.Attr("href")
		if !exists {
			return
		}

		parsedHref, err := neturl.Parse(href)
		if err != nil {
			return
		}

		absURL := baseURL.ResolveReference(parsedHref)

		if _, loaded := params.visited.LoadOrStore(absURL.String(), true); !loaded {
			fmt.Printf("Found new link: %s \n", absURL.String())
			if params.pageCount.Add(1) <= params.MaxPages {
				// This block is the "safe zone"
				params.wg.Add(1)
				params.urlQueue <- absURL.String()
			} else {
				// We lost the race, so we undo our increment
				params.pageCount.Add(-1)
			}
		}
	})

	html, err := doc.Html()
	if err != nil {
		log.Printf("Error fetching the html content; err: %s", err)
	}

	bodyContent, err := doc.Find("body").Html()
	if err != nil {
		log.Printf("Error finding body html content; err: %s", err)
	}

	textContent := doc.Find("body").Text()

	params.resultsCh <- domain.Content{
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
	fmt.Printf("url: %s is stored in the channel \n", url)

}

func CreateIndexes(collection *mongo.Collection) error {
	IndexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "url", Value: 1}},
		Options: options.Index().SetUnique(true),
	}

	_, err := collection.Indexes().CreateOne(context.Background(), IndexModel)
	if err != nil {
		log.Printf("Error creating index: %v", err)
	}
	return err
}

func Crawl(client *mongo.Database) {

	cfg := config.LoadConfig()

	var pageCount atomic.Int64

	var wg sync.WaitGroup

	ch := make(chan domain.Content, 3)
	sem := make(chan struct{}, cfg.MaxConcurrentFetches)      // Limit to 5 concurrent fetches
	urlQueue := make(chan string, 100) // Buffer size for URL queue

	visited := sync.Map{}

	for _, url := range cfg.SeedUrls {
		if _, loaded := visited.LoadOrStore(url, true); !loaded {
			if pageCount.Add(1) <= int64(cfg.MaxPages) {
				wg.Add(1)
				urlQueue <- url
			}
		}
	}

	params := CrawlerConfig{
		MaxPages:             cfg.MaxPages,
		MaxConcurrentFetches: cfg.MaxConcurrentFetches,
		DB:                   client,
		wg:                   &wg,
		pageCount:            &pageCount,
		visited:              &visited,
		urlQueue:             urlQueue,
		resultsCh:            ch,
		sem:                  sem,
	}

	go func() {
		wg.Wait()
		close(urlQueue)
		close(ch)
	}()

	go func() {
		for url := range urlQueue {
			fmt.Printf("sent url: %s through goroutine \n", url)
			go FetchURL(params, url)
		}
	}()

	// urlCollection := client.Collection("urls")

	// err := CreateIndexes(urlCollection)
	// if err != nil {
	// 	log.Printf("Error creating indexes: %v", err)
	// }

	for result := range ch {
		fmt.Printf("Received content for URL: %s \n", result.URL)
	}
	// 	_, err := urlCollection.InsertOne(context.Background(), result)
	// 	if err != nil {
	// 		log.Printf("Error inserting result into MongoDB: %v", err)
	// 	} else {
	// 		fmt.Printf("url: %s is stored in the database \n", result.URL)
	// 	}
	// }

	fmt.Printf("finished executing page count: %d \n", pageCount.Load())
}
