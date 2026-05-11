package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	MongoURI             string
	MongoDB              string
	SeedUrls             []string
	MaxPages             int64
	MaxConcurrentFetches int
	RequestTimeout       int
}

func LoadConfig() *Config {

	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	// Add paths to search for the config file
	viper.AddConfigPath(".")            // Look in the working directory
	viper.AddConfigPath("$HOME/.myapp") // Also look in home dir

	// Read the file
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	// Access values directly using dot notation for nesting
	seedURLs := viper.GetStringSlice("crawler.seed_urls")
	maxPages := viper.GetInt64("crawler.max_pages")
	concurrency := viper.GetInt("crawler.max_concurrent_fetches")
	requestTimeout := viper.GetInt("crawler.request_timeout_seconds")

	return &Config{
		MongoURI:             getEnv("MONGO_URI", ""),
		MongoDB:              getEnv("MONGO_DB", "test"),
		SeedUrls:             seedURLs,
		MaxPages:             maxPages,
		MaxConcurrentFetches: concurrency,
		RequestTimeout:       requestTimeout,
	}

}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
