package worker

import (
	"ai-analyst/decision_engine/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
)

func SaveDataToStorage(msg models.ML2ScoredMessage) error {
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

func SaveStatusToRedis(ctx context.Context, rdb *redis.Client, caseID string, risk string, ts string) {
	key := fmt.Sprintf("pipeline:%s:state", caseID)
	state := models.PipelineStatus{
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
