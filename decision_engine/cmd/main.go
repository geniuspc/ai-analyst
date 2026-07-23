package main

import (
	"ai-analyst/decision_engine/internal/config"
	"ai-analyst/decision_engine/internal/models"
	"ai-analyst/decision_engine/internal/ollama"
	"ai-analyst/decision_engine/internal/worker"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.LoadConfig()
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	defer rdb.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBrokers},
		GroupID: cfg.KafkaGroupID,
		Topic:   cfg.KafkaTopic,
	})
	defer reader.Close()

	dlWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers),
		Topic:    cfg.DLQTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer dlWriter.Close()
	fmt.Println("Starting decision engine. Consuming..")
	ctx := context.Background()
	ollamaURL := config.GetEnv("OLLAMA_URL", "http://localhost:11434")
	ollamaModel := config.GetEnv("OLLAMA_MODEL", "gemma3:4b")
	aiClient := ollama.NewClient(ollamaURL, ollamaModel)
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("Error message: ", err)
			continue
		}
		fmt.Printf("Received: %s\n", string(m.Value))
		var msg models.ML2ScoredMessage
		err = json.Unmarshal(m.Value, &msg)
		if err != nil {
			log.Println("JSON Error message: ", err)
			continue
		}
		redisKey := "case:" + msg.CaseID + ":status"
		exists, err := rdb.Exists(ctx, redisKey).Result()
		if err != nil {
			log.Println("Redis access error", err)
			continue
		}
		if exists == 0 {
			log.Printf("Case ID: %s not found in redis. Transferring to dead-letter topic.\n", msg.CaseID)
			err = dlWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(msg.CaseID),
				Value: m.Value,
			})
			if err != nil {
				log.Println("Failed to write to dead-letter queue: ", err)
			}
			continue
		}
		finalConfidence := msg.RevisedConfidence
		ts := time.Now().Format("2006-01-02 15:04:05")
		if msg.HallucinationRate > 0.30 {
			finalConfidence -= 0.15
		}
		if msg.HallucinationRate > 0.20 || len(msg.CounterArguments) > 0 {
			log.Printf("Case %s requires AI resolution due to risks/counter-arguments...", msg.CaseID)

			aiResolution, err := aiClient.MakeDecision(ctx, msg)
			if err != nil {
				log.Printf("Ollama fallback/error on case %s: %v", msg.CaseID, err)
			} else {
				log.Printf("[AI Resolution] Case: %s | Tool: %s | Reason: %s",
					msg.CaseID, aiResolution.ToolToCall, aiResolution.Reason)
			}
		}
		if finalConfidence < 0 {
			finalConfidence = 0.01
		}
		if finalConfidence > 0.75 {
			log.Printf("(HIGH risk) Case: %s , Conf: %.2f, Action: Immediate webhook with full IR report", msg.CaseID, finalConfidence)
			err := worker.SaveDataToStorage(msg)
			if err != nil {
				log.Println("Error saving data to storage: ", err)
			}
			worker.SaveStatusToRedis(ctx, rdb, msg.CaseID, "HIGH", ts)
		} else if finalConfidence >= 0.45 && finalConfidence <= 0.75 {
			log.Printf("(MEDIUM risk) Case: %s, Conf: %.2f, Action: IR Report, requires manual review", msg.CaseID, finalConfidence)
			worker.SaveStatusToRedis(ctx, rdb, msg.CaseID, "MEDIUM", ts)
		} else {
			log.Printf("(LOW risk) Case: %s, Conf: %.2f, No need in further action", msg.CaseID, finalConfidence)
			worker.SaveStatusToRedis(ctx, rdb, msg.CaseID, "LOW", ts)
		}

	}
}
