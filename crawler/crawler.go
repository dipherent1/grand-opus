package crawler

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	neturl "net/url"

	"github.com/PuerkitoBio/goquery"
	"github.com/dipherent1/grand-opus/config"
	"github.com/dipherent1/grand-opus/internal/domain"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	metricsPagesCrawled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "crawler_pages_crawled_total",
		Help: "The total number of successfully crawled pages",
	}, []string{"domain"})

	metricsCrawlErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "crawler_errors_total",
		Help: "The total number of errors encountered, partitioned by type",
	}, []string{"error_type"}) // e.g., "http_fetch", "parse_html", "db_insert"

	metricsActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "crawler_active_workers",
		Help: "The current number of concurrent fetch routines running",
	})

	metricsQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "crawler_url_queue_size",
		Help: "The current number of URLs waiting to be fetched",
	})

	metricsHttpRequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_http_request_duration_seconds",
		Help:    "The duration of HTTP requests to fetch URLs",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})

	metricsDbInsertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_db_insert_duration_seconds",
		Help:    "The duration of database insert operations",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	})
)

func getDomain(rawURL string) string {
	parsedURL, err := neturl.Parse(rawURL)
	if err != nil {
		return "unknown"
	}
	return parsedURL.Hostname()
}

type CrawlerConfig struct {
	MaxPages             int64
	MaxConcurrentFetches int
	SeedUrls             []string

	// Dependencies
	DB     *mongo.Database // Use a more descriptive name than 'client'
	Logger *slog.Logger

	// Internal State
	wg        *sync.WaitGroup
	pageCount *atomic.Int64
	visited   *sync.Map
	urlQueue  chan string
	resultsCh chan domain.Content
	sem       chan struct{}
}

func NewCrawler(db *mongo.Database, cfg *config.Config, logger *slog.Logger) *CrawlerConfig {
	return &CrawlerConfig{
		MaxPages:             cfg.MaxPages,
		MaxConcurrentFetches: cfg.MaxConcurrentFetches,
		SeedUrls:             cfg.SeedUrls,
		DB:                   db,
		Logger:               logger,
		wg:                   &sync.WaitGroup{},
		pageCount:            &atomic.Int64{},
		visited:              &sync.Map{},
		urlQueue:             make(chan string, 100),
		resultsCh:            make(chan domain.Content, 100),
		sem:                  make(chan struct{}, cfg.MaxConcurrentFetches),
	}
}

func (c *CrawlerConfig) FetchURL(url string) {

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		metricsHttpRequestDuration.Observe(duration)
	}()
	defer c.wg.Done()

	c.sem <- struct{}{}
	metricsActiveWorkers.Inc()
	// Acquire semaphore
	defer func() {
		<-c.sem
		metricsActiveWorkers.Dec()
	}() // Release semaphore

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)

	if err != nil {
		c.Logger.Error("Error fetching the resp", "error", err)

		metricsCrawlErrors.WithLabelValues("request_creation").Inc()

		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.Logger.Error("Error fetching the URL", "error", err)
		metricsCrawlErrors.WithLabelValues("http_fetch").Inc()
		return // MUST return here so you don't use a nil 'resp'
	}

	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)

	if err != nil {
		c.Logger.Error("Error fetching the doc", "error", err)
		metricsCrawlErrors.WithLabelValues("parse_html").Inc()
		return
	}

	baseURL, err := neturl.Parse(url)
	if err != nil {
		c.Logger.Error("Error parsing the URL", "error", err)
		return
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {

		if c.pageCount.Load() >= c.MaxPages {
			return
		}

		href, exists := s.Attr("href")
		if !exists {
			c.Logger.Error("Error fetching the href", "error", err)
			return
		}

		parsedHref, err := neturl.Parse(href)
		if err != nil {
			c.Logger.Error("Error parsing the href", "error", err)
			return
		}

		absURL := baseURL.ResolveReference(parsedHref)
		absURL.Fragment = ""
		cleanURL := absURL.String()

		if _, loaded := c.visited.LoadOrStore(absURL.String(), true); !loaded {
			c.Logger.Info("Found new link", "url", absURL.String())
			if c.pageCount.Add(1) <= c.MaxPages {
				c.Logger.Debug("Discovered new link", "parent_url", url, "new_url", cleanURL)
				c.wg.Add(1)
				c.urlQueue <- absURL.String()
				metricsQueueSize.Inc() // Metric: Queue grew

			} else {
				// We lost the race, so we undo our increment
				c.pageCount.Add(-1)
			}
		}
	})

	html, err := doc.Html()
	if err != nil {
		c.Logger.Error("Error fetching the html content", "error", err)
	}

	bodyContent, err := doc.Find("body").Html()
	if err != nil {
		c.Logger.Error("Error finding body html content", "error", err)
	}

	textContent := doc.Find("body").Text()

	c.resultsCh <- domain.Content{
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
	domain := getDomain(url)
	metricsPagesCrawled.WithLabelValues(domain).Inc()
	c.Logger.Info("Content fetched", "url", url)

}

func CreateIndexes(collection *mongo.Collection) error {
	IndexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "url", Value: 1}},
		Options: options.Index().SetUnique(true),
	}

	_, err := collection.Indexes().CreateOne(context.Background(), IndexModel)
	if err != nil {
		log.Fatal("Error creating index", "error", err)
	}
	return err
}

func (c *CrawlerConfig) Crawl() {


	for _, url := range c.SeedUrls {
		if _, loaded := c.visited.LoadOrStore(url, true); !loaded {
			if c.pageCount.Add(1) <= int64(c.MaxPages) {
				c.wg.Add(1)
				c.urlQueue <- url
				metricsQueueSize.Inc()

			}
		}
	}

	go func() {
		c.wg.Wait()
		close(c.urlQueue)
		close(c.resultsCh)
	}()

	go func() {
		for url := range c.urlQueue {
			c.Logger.Info("Sending URL for crawling", "url", url)
			metricsQueueSize.Dec() // Metric: Removed from queue
			go c.FetchURL(url)
		}
	}()

	// urlCollection := client.Collection("urls")

	// err := CreateIndexes(urlCollection)
	// if err != nil {
	// 	c.Logger.Error("Error creating indexes", "error", err)
	// }

	for result := range c.resultsCh {
		c.Logger.Info("Received content for URL", "url", result.URL)

		dbStart := time.Now()

		// Record the duration it took
		dbDuration := time.Since(dbStart).Seconds()
		metricsDbInsertDuration.Observe(dbDuration)

		// }
		// 	_, err := urlCollection.InsertOne(context.Background(), result)
		// 	if err != nil {
		// 		c.Logger.Error("Error inserting result into MongoDB", "error", err)
		// 	} else {
		// 		c.Logger.Info("Content inserted into MongoDB", "url", result.URL)
		// 	}
		// 	}
	}

	c.Logger.Info("Finished executing", "page_count", c.pageCount.Load())
}
