package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type ArtifactFiles map[string]string

type EvidenceMsg struct {
	CaseID        string        `json:"case_id"`
	ArtifactType  string        `json:"artifact_type"`
	MountPath     string        `json:"mount_path"`
	OsHint        string        `json:"os_hint"`
	ByteSize      int64         `json:"size_bytes"`
	Vol3Readiness bool          `json:"vol3_ready"`
	CheckSum      string        `json:"checksum"`
	Files         ArtifactFiles `json:"files"`
	Timestamp     time.Time     `json:"ts"`
}

type Task func()

type ThreadPoolExecutor struct {
	taskQueue chan Task
	wg        sync.WaitGroup
}

func ProcessVolExec(ctx context.Context, msg EvidenceMsg, rdb *redis.Client) {
	linuxPlugins := []string{
		"linux.pslist.PsList",
		"linux.psaux.PsAux",
		"linux.lsof.Lsof",
		"linux.sockstat.Sockstat",
		"linux.ip.Addr",
		"linux.mountinfo.MountInfo",
		"linux.envars.Envars",
		"linux.malfind.Malfind",
	}

	caseID := msg.CaseID
	executor := NewThreadPoolExecutor(6, len(linuxPlugins))
	for _, plugin := range linuxPlugins {
		plugin := plugin
		executor.Submit(func() {
			resultKey := fmt.Sprintf("vol3:%s:%s", caseID, plugin)
			errorKey := fmt.Sprintf("vol3:%s:%s:error", caseID, plugin)

			if err := rdb.Get(ctx, resultKey).Err(); err == nil {
				fmt.Printf("[Case %s] Plugin %s found in cache, skipping\n", caseID, plugin)
				return
			}
			execCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
			defer cancel()
			fmt.Printf("[Case %s] Running plugin: %s...\n", caseID, plugin)

			cmd := exec.CommandContext(execCtx, "python3", "vol.py", "-f", msg.MountPath, "-r", "json", plugin)
			output, err := cmd.CombinedOutput()
			if err != nil {
				if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
					fmt.Printf("[Case %s] Plugin %s timed out\n", caseID, plugin)
					rdb.Set(ctx, errorKey, "timeout", 2*time.Hour)
				} else {
					fmt.Printf("[Case %s] Plugin %s failed: %v\n", caseID, plugin, err)
					rdb.Set(ctx, errorKey, err.Error(), 2*time.Hour)
				}
			} else {
				if err := rdb.Set(ctx, resultKey, output, 2*time.Hour).Err(); err != nil {
					fmt.Printf("[Case %s] Error saving %s to redis: %v\n", caseID, plugin, err)
				} else {
					fmt.Printf("[Case %s] Plugin %s completed successfully\n", caseID, plugin)
				}
			}
		})
	}

	executor.Shutdown()
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

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
