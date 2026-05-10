package main

import (
	"context"
	"log"

	"github.com/dipherent1/grand-opus/config"
	"github.com/dipherent1/grand-opus/crawler"
	"github.com/dipherent1/grand-opus/pkg"
)

func main() {

	cfg := config.LoadConfig()

	client, err := pkg.ConnectToMongoDB(cfg.MongoURI)
	db := client.Database(cfg.MongoDB)

	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			log.Fatal("Error disconnecting from MongoDB:", err)
		}
	}()

	crawler.Crawl(db)

}
