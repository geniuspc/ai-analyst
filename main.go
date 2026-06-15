package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"time"

	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/context"
)

var rdb *redis.Client

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

func main() {
	initRedis()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "vol3-service",
		Topic:   "evidence-ready",
	})
	defer reader.Close()
	linuxPlugins := []string{
		"linux.pslist",
		"linux.bash",
		"linux.netstat",
		"linux.lsmod",
		"linux.malfind",
		"linux.cmdline",
	}
	for {
		msgKafka, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error reading message:", err)
			continue
		}
		fmt.Printf("Received message: %s\n", string(msgKafka.Value))
		var msg EvidenceMsg
		if err := json.Unmarshal(msgKafka.Value, &msg); err != nil {
			log.Println("Error unmarshalling message:", err)
			continue
		}
		if msg.ArtifactType == "logs" || !msg.Vol3Readiness {
			fmt.Printf("Skipping non-memory artifact for case: %s\n", msg.CaseID)
			continue
		}
		ctx := context.Background()
		caseID := msg.CaseID
		executor := NewThreadPoolExecutor(6, len(linuxPlugins))
		for _, plugin := range linuxPlugins {
			executor.Submit(func() {
				resultKey := fmt.Sprintf("vol3:%s:%s", caseID, plugin)
				errorKey := fmt.Sprintf("vol3:%s:%s:error", caseID, plugin)
				if err := rdb.Get(ctx, resultKey).Err(); err == nil {
					fmt.Printf("Plugin %s found in cache, skipping\n", caseID, plugin)
					return
				}
				execCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()

				fmt.Printf("[Case %s] Running plugin: %s...\n", caseID, plugin)

				cmd := exec.CommandContext(execCtx, "python3", "vol.py", "-f", msg.MountPath, "-r", "json", plugin)
				output, err := cmd.CombinedOutput()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						fmt.Printf("[Case %s] Plugin %s timed out\n", caseID, plugin)
						rdb.Set(ctx, errorKey, "timeout", 2*time.Hour)
					} else {
						fmt.Printf("[Case %s] Plugin %s failed: %v\n", caseID, plugin, err)
						rdb.Set(ctx, errorKey, err.Error(), 2*time.Hour)
					}
				} else {
					err := rdb.Set(ctx, resultKey, output, 2*time.Hour).Err()
					if err != nil {
						fmt.Printf("[Case %s] Error saving %s to redis: %v\n", caseID, plugin, err)
					} else {
						fmt.Printf("[Case %s] Plugin %s completed successfully\n", caseID, plugin)
					}
				}
			})
		}
		executor.Shutdown()
		pipelineKey := fmt.Sprintf("pipeline:%s:vol3_done", caseID)
		if err := rdb.Set(ctx, pipelineKey, true, 86400*time.Second).Err(); err != nil {
			fmt.Println("Error setting pipeline status:", err)
		} else {
			fmt.Printf("Finished all plugins for case: %s\n", caseID)
		}

	}
}

func NewThreadPoolExecutor(numWorkers int, queueSize int) *ThreadPoolExecutor {
	executor := &ThreadPoolExecutor{
		taskQueue: make(chan Task, queueSize),
	}

	for i := 1; i <= numWorkers; i++ {
		executor.wg.Add(1)
		go executor.worker(i)
	}

	return executor
}

func (e *ThreadPoolExecutor) worker(id int) {
	defer e.wg.Done()
	for task := range e.taskQueue {
		task()
	}
}

func (e *ThreadPoolExecutor) Submit(task Task) {
	e.taskQueue <- task
}

func (e *ThreadPoolExecutor) Shutdown() {
	close(e.taskQueue)
	e.wg.Wait()
}
