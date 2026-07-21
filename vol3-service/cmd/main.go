package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"ai-analyst/vol3-service/internal/config"
	"ai-analyst/vol3-service/internal/worker"
)

func main() {
	cfg := config.LoadConfig()
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBrokers},
		GroupID: cfg.KafkaGroupID,
		Topic:   cfg.KafkaTopic,
	})
	defer reader.Close()
	log.Printf("vol3-service initialized and listening on topic '%s'...", cfg.KafkaTopic)
	for {
		msgKafka, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error reading message:", err)
			continue
		}

		fmt.Printf("Received message: %s\n", string(msgKafka.Value))

		var msg worker.EvidenceMsg
		if err := json.Unmarshal(msgKafka.Value, &msg); err != nil {
			log.Println("Error unmarshalling message:", err)
			continue
		}

		if msg.ArtifactType == "logs" || !msg.Vol3Readiness {
			fmt.Printf("Skipping non-memory artifact for case: %s\n", msg.CaseID)
			continue
		}

		ctx := context.Background()
		worker.ProcessVolExec(ctx, msg, rdb)
		pipelineKey := fmt.Sprintf("pipeline:%s:vol3_done", msg.CaseID)
		if err := rdb.Set(ctx, pipelineKey, true, 24*time.Hour).Err(); err != nil {
			fmt.Println("Error setting pipeline status:", err)
		} else {
			fmt.Printf("Finished all plugins for case: %s\n", msg.CaseID)
		}
	}
}
