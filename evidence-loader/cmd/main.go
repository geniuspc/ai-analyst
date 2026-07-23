package main

import (
	"ai-analyst/evidence-loader/internal/config"
	"ai-analyst/evidence-loader/internal/worker"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.LoadConfig()
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		MaxAttempts:  3,
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("redis is unavailable: %v\n", err)
		os.Exit(1)
	}
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		worker.MultiPartUploader(w, r, rdb, writer)
	})
	fmt.Println("Listening on port 8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		return
	}
}
