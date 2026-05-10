package crawler

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	neturl "net/url"

	"github.com/PuerkitoBio/goquery"
	"github.com/dipherent1/grand-opus/internal/domain"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func FetchURL(url string, wg *sync.WaitGroup, ch chan<- domain.Content, urlQueue chan<- string, sem chan struct{}, visited *sync.Map) {

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

	baseURL, err := neturl.Parse(url)
	if err != nil {
		log.Printf("Error parsing the URL: %v", err)
		return
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists {
			return
		}
		parsedHref, err := neturl.Parse(href)
		if err != nil {
			return
		}

		absURL := baseURL.ResolveReference(parsedHref)
		
		if _, loaded := visited.LoadOrStore(absURL.String(), true); !loaded {
			fmt.Printf("Found new link: %s \n", absURL.String())
			wg.Add(1)
			urlQueue <- absURL.String()
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

	urls := []string{"https://example.com", "https://example.org", "https://github.com/tonywangcn/distributed-web-crawler/blob/master/go/src/crawler/crawler.go"}

	var wg sync.WaitGroup

	ch := make(chan domain.Content, len(urls))
	sem := make(chan struct{}, 5) // Limit to 5 concurrent fetches
	visited := sync.Map{}

	urlQueue := make(chan string, 100) // Buffer size for URL queue

	for _, url := range urls {
		wg.Add(1)
		urlQueue <- url
	}

	go func() {
		wg.Wait()
		close(urlQueue)
		close(ch)
	}()

	for url := range urlQueue {
		fmt.Printf("sent url: %s through goroutine \n", url)
		go FetchURL(url, &wg, ch, urlQueue, sem, &visited)
	}

	// urlCollection := client.Collection("urls")

	// err := CreateIndexes(urlCollection)
	// if err != nil {
	// 	log.Printf("Error creating indexes: %v", err)
	// }

	// for result := range ch {
	// 	_, err := urlCollection.InsertOne(context.Background(), result)
	// 	if err != nil {
	// 		log.Printf("Error inserting result into MongoDB: %v", err)
	// 	} else {
	// 		fmt.Printf("url: %s is stored in the database \n", result.URL)
	// 	}
	// }
}
