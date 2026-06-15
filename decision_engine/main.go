package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

var rdb *redis.Client

func initRedis() {
	addr := getEnv("REDIS_ADDR", "localhost:6379")
	rdb = redis.NewClient(&redis.Options{
		Addr: addr,
	})
}
func main() {
	initRedis()
	kafkaBroker := getEnv("KAFKA_BROKERS", "localhost:9092")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		GroupID: "be1-decision-engine",
		Topic:   "ml2-verified-findings",
	})
	defer reader.Close()

	dlWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "dead-letter-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer dlWriter.Close()
	fmt.Println("Starting decision engine. Consuming..")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error message: ", err)
			continue
		}
		fmt.Printf("Received: %s\n", string(m.Value))
		var msg ML2ScoredMessage
		err = json.Unmarshal(m.Value, &msg)
		if err != nil {
			log.Println("JSON Error message: ", err)
			continue
		}
		ctx := context.Background()
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
		if finalConfidence < 0 {
			finalConfidence = 0.01
		}
		if finalConfidence > 0.75 {
			log.Printf("(HIGH risk) Case: %s , Conf: %.2f, Action: Immediate webhook with full IR report", msg.CaseID, finalConfidence)
			err := saveDataToStorage(msg)
			if err != nil {
				log.Println("Error saving data to storage: ", err)
			}
			saveStatusToRedis(msg.CaseID, "HIGH", ts)
		} else if finalConfidence >= 0.45 && finalConfidence <= 0.75 {
			log.Printf("(MEDIUM risk) Case: %s, Conf: %.2f, Action: IR Report, requires manual review", msg.CaseID, finalConfidence)
			saveStatusToRedis(msg.CaseID, "MEDIUM", ts)
		} else {
			log.Printf("(LOW risk) Case: %s, Conf: %.2f, No need in further action", msg.CaseID, finalConfidence)
			saveStatusToRedis(msg.CaseID, "LOW", ts)
		}

	}
}

func saveDataToStorage(msg ML2ScoredMessage) error {
	dirPath := filepath.Join("output", msg.CaseID)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("error creating output directory %w", err)
	}
	mdPath := filepath.Join(dirPath, "ir_report.md")
	err := os.WriteFile(mdPath, []byte(msg.NarrativeMessageDescription), 0644)
	if err != nil {
		return fmt.Errorf("error writing md report: %w", err)
	}
	tracePath := filepath.Join(dirPath, "hypothesis_trace.json")
	traceData, err := json.MarshalIndent(msg.VerifiedFacts, "", " ")
	err = os.WriteFile(tracePath, traceData, 0644)
	if err != nil {
		return fmt.Errorf("error writing trace: %w", err)
	}
	log.Printf("Wrote verified facts to %s", dirPath)
	return nil
}

func saveStatusToRedis(caseID string, risk string, ts string) {
	ctx := context.Background()
	key := fmt.Sprintf("pipeline:%s:state", caseID)
	state := PipelineStatus{
		Step:      "completed",
		Status:    "done",
		Risk:      risk,
		Timestamp: ts,
	}

	data, err := json.Marshal(state)
	if err != nil {
		log.Println("Error marshalling state: ", err)
		return
	}
	err = rdb.Set(ctx, key, data, 86400*time.Second).Err()
	if err != nil {
		log.Println("Error occur during save to Redis: ", err)
	} else {
		log.Printf("Saved to Redis for: %s", caseID)
	}

}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
