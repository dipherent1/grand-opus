package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/dipherent1/grand-opus/config"
	"github.com/dipherent1/grand-opus/crawler"
	"github.com/dipherent1/grand-opus/internal/domain"
	"github.com/dipherent1/grand-opus/pkg"
)

func main() {

	cfg := config.LoadConfig()

	client, err := pkg.ConnectToMongoDB(cfg.MongoURI)

	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			log.Fatal("Error disconnecting from MongoDB:", err)
		}
	}()

	urls := []string{"https://example.com", "https://example.org"}

	var wg sync.WaitGroup

	ch := make(chan domain.Content, len(urls))
	sem := make(chan struct{}, 5) // Limit to 5 concurrent fetches

	for _, url := range urls {
		wg.Add(1)
		go crawler.FetchURL(url, &wg, ch, sem)
	}

	wg.Wait()
	close(ch)

	for result := range ch {
		fmt.Printf("URL: %s\n", result.URL)
	}

}
