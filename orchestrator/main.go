package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ollama/ollama/api"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
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
		GroupID: "be1-orchestrator",
		Topic:   "ml1-scored",
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "agent-findings",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	fmt.Println("Start	 consuming...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error message: ", err)
			continue
		}
		fmt.Printf("Received: %s\n", string(m.Value))
		var msg ML1ScoredMessage
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Println("JSON Error message: ", err)
			continue
		}
		ctx := context.Background()
		lockKey := "lock:" + msg.CaseID
		acquired, lockErr := rdb.SetNX(ctx, lockKey, "1", 30*time.Second).Result()
		if lockErr != nil || !acquired {
			log.Printf("Case %s is locked by another person", msg.CaseID)
			continue
		}
		hypotheses := HypothesisTracker(msg)
		fmt.Printf("Case ID: %s confidence: %f\n", msg.CaseID, hypotheses.Rootkit.Confidence)
		saveStatusToRedis(msg.CaseID)
		fmt.Println("Case received", msg.CaseID)
		fmt.Println("Score: ", msg.Scores.Memory.Score)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(5)
		var ProcessTreeReadiness bool
		if hypotheses.Rootkit.Confidence > 0.7 {
			if len(msg.Scores.Memory.SuspiciousPIDs) > 0 {
				targetPID := msg.Scores.Memory.SuspiciousPIDs[0]
				dump := msg.Scores.Memory.DumpPath

				g.Go(func() error {
					found, err := GetProcessTree(ctx, dump, targetPID)
					ProcessTreeReadiness = found
					return err
				})
			}
		}

		var hasInjection bool
		if msg.Scores.Memory.Shap.MemoryInjection > 0.4 {
			if len(msg.Scores.Memory.SuspiciousPIDs) > 0 {
				targetPID := msg.Scores.Memory.SuspiciousPIDs[0]

				g.Go(func() error {
					found, err := CheckMemoryInjection(ctx, msg.Scores.Memory.DumpPath, targetPID)
					hasInjection = found
					return err
				})
			}
		}

		var hasExternalConnection bool
		if msg.Scores.Logs.Features.ExternalConnection == true {
			g.Go(func() error {
				found, err := GetNetworkConnections(ctx, msg.Scores.Memory.DumpPath)
				hasExternalConnection = found
				return err
			})
		}

		var hasSuspiciousBash bool
		if msg.Scores.Memory.Shap.SuspiciousBashCmd > 0.3 {
			g.Go(func() error {
				found, err := AnalyzeBashHistory(ctx, msg.Scores.Memory.DumpPath)
				hasSuspiciousBash = found
				return err
			})
		}

		var hasSuspiciousKernel bool
		if msg.Scores.Memory.Shap.SuspiciousKernelModule > 0.4 {
			g.Go(func() error {
				found, err := CheckKernelModules(ctx, msg.Scores.Memory.DumpPath)
				hasSuspiciousKernel = found
				return err
			})
		}

		var resultLog bool
		if msg.Scores.Logs.Features.LogTampering == true || msg.Scores.Logs.Features.BruteForce == true {
			g.Go(func() error {
				rawLogs, err := AnalyzeLogTimeline(ctx, msg.CaseID)
				if err != nil {
					return err
				}
				syslogAlert := []string{"segfault", "promiscuous", "root", "failed", "denied", "panic"}
				for _, line := range rawLogs {
					lower := strings.ToLower(line.Message)
					for _, filter := range syslogAlert {
						if strings.Contains(lower, filter) {
							resultLog = true
							return nil
						}
					}
				}
				return nil
			})
		}

		var persistenceProblems bool
		if msg.Scores.Logs.Features.NewSystemDService == true || msg.Scores.Logs.Features.NewCronJob == true {
			g.Go(func() error {
				found, err := CheckPersistence(ctx, msg.Scores.Memory.DumpPath)
				persistenceProblems = found
				return err
			})
		}

		var decodeResult bool
		if msg.Scores.Logs.Features.EncodedCmd == true {
			g.Go(func() error {
				found, err := DecodeCommands(ctx, msg.Scores.Memory.DumpPath)
				decodeResult = found
				return err
			})
		}
		if err := g.Wait(); err != nil {
			log.Println("Pipeline error: ", err)
		}
		if ProcessTreeReadiness {
			hypotheses.Rootkit.Confidence += 0.10
			hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, fmt.Sprintf("Anomaly chain detected in process tree"))
		}
		if hasInjection {
			hypotheses.Rootkit.Confidence += 0.15
			hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, fmt.Sprintf("Memory injection detected"))
		}
		if hasExternalConnection {
			hypotheses.Rootkit.Against = append(hypotheses.Rootkit.Against, fmt.Sprintf("External connection established"))
		}
		if hasSuspiciousKernel {
			hypotheses.Rootkit.Confidence += 0.15
			hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, fmt.Sprintf("Unknown kernel module detected"))
		}
		if hasSuspiciousBash {
			hypotheses.Rootkit.Confidence += 0.10
			hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, fmt.Sprintf("Suspicious commands in bash history"))
		}
		if resultLog {
			hypotheses.Rootkit.Against = append(hypotheses.Rootkit.Against, fmt.Sprintf("Dangerous syslog alerts have been found"))
		}
		if decodeResult {
			hypotheses.Rootkit.Against = append(hypotheses.Rootkit.Against, fmt.Sprintf("Decoded commands from: %v", msg.Scores.Memory.DumpPath))
		}
		if persistenceProblems {
			hypotheses.Rootkit.Confidence += 0.15
			hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, fmt.Sprintf("Persistence anomalies in: %v", msg.Scores.Memory.DumpPath))
		}
		var msgResult AgentFindingsMsg
		msgResult.CaseID = msg.CaseID
		msgResult.Scores.Memory = msg.Scores.Memory.Score
		msgResult.Scores.Logs = msg.Scores.Logs.Score
		msgResult.Hypothesis = hypotheses

		var auditSteps []AuditStep
		for i := 1; i <= 3; i++ {
			totalScore := []float64{
				hypotheses.Rootkit.Confidence,
				hypotheses.LateralMovement.Confidence,
				hypotheses.FalsePositive.Confidence,
			}
			sort.Float64s(totalScore)
			diff := totalScore[2] - totalScore[1]
			if diff >= 0.1 {
				break
			}

			log.Printf("(Self-Correction) Diff is %.2f. Consulting Gemma 3...", diff)
			toolToCall, err := callGemmaOrchestrator(ctx, hypotheses, diff)
			if err != nil {
				log.Println("AI orchestration error", err)
				break
			}
			newUUID := uuid.NewString()
			chars := newUUID[:4]
			timestamp := time.Now().Format("2006-01-02 15:04:05")
			switch toolToCall {
			case "analyze_log_timeline":
				if !resultLog {
					auditSteps = append(auditSteps, AuditStep{
						Step:      i,
						Action:    "analyze_log_timeline",
						Timestamp: timestamp,
						Result:    fmt.Sprintf("Uncertainty detected (Diff: %.2f)", diff),
					})
					rawLogs, err := AnalyzeLogTimeline(ctx, msg.CaseID)
					if err != nil {
						log.Println("Failed to extract logs for Gemma", err)
						break
					}
					log.Println("Passing raw logs to Gemma for Deep timeline analysis..")
					analysisData, err := deepAnalystGemma(ctx, msg.CaseID, rawLogs)
					if err == nil && analysisData != nil && analysisData.IsCoordinated {
						hypotheses.Rootkit.Confidence += 0.15
						reasoning := fmt.Sprintf("Gemma AI confirmed timeline anomaly: %s", analysisData.RootCause)
						hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, reasoning)
					} else if err != nil {
						log.Println("Gemma analysis failed", err)
					}
					logID := "log_time_line" + "_" + chars
					gemmaJson, _ := json.Marshal(analysisData)
					saveArtifactToRedis(logID, gemmaJson)
					msgResult.Artifacts = append(msgResult.Artifacts, Artifact{
						ID:     logID,
						Type:   "timeline",
						PID:    0,
						Detail: "Deep timeline analysis completed by Gemma",
					})
					resultLog = true
				}
			case "get_process_tree":
				if !ProcessTreeReadiness && len(msg.Scores.Memory.SuspiciousPIDs) > 0 {
					targetPID := msg.Scores.Memory.SuspiciousPIDs[0]
					auditSteps = append(auditSteps, AuditStep{
						Step: i, Action: "get_process_tree", Timestamp: timestamp, Result: "Executed by Gemma choice opinion",
					})
					found, err := GetProcessTree(ctx, msg.Scores.Memory.DumpPath, targetPID)
					if err == nil && found {
						hypotheses.Rootkit.Confidence += 0.15
						hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, "MCP Positive: Process tree anomaly.")
					}
					logID := "get_process_tree" + "_" + chars
					saveArtifactToRedis(logID, []byte(`{"status": "anomaly_found"}`))
					msgResult.Artifacts = append(msgResult.Artifacts, Artifact{
						ID:     logID,
						Type:   "process_tree",
						PID:    targetPID,
						Detail: "Process tree detected by MCP",
					})
					ProcessTreeReadiness = true
				}
			case "has_suspicious_bash":
				if !hasSuspiciousBash {
					auditSteps = append(auditSteps, AuditStep{
						Step:      i,
						Action:    "has_suspicios_bash",
						Timestamp: timestamp,
						Result:    fmt.Sprintf("Uncertainty detected, executed by Gemma choice opinion (Diff: %.2f)", diff),
					})
					found, err := DecodeCommands(ctx, msg.Scores.Memory.DumpPath)
					if err == nil && found {
						hypotheses.Rootkit.Confidence += 0.10
					}
					logID := "suspicious_bash" + "_" + chars
					saveArtifactToRedis(logID, []byte(`{"status": "suspicious_bash_found"}`))
					msgResult.Artifacts = append(msgResult.Artifacts, Artifact{
						ID:     logID,
						Type:   "bash_history",
						PID:    0,
						Detail: "Decoded suspicious bash commands by MCP",
					})
					hasSuspiciousBash = true
				}
			case "check_memory_injection":
				if !hasInjection && len(msg.Scores.Memory.SuspiciousPIDs) > 0 {
					targetPID := msg.Scores.Memory.SuspiciousPIDs[0]
					auditSteps = append(auditSteps, AuditStep{
						Step:      i,
						Action:    "check_memory_injection",
						Timestamp: timestamp,
						Result:    fmt.Sprintf("Executed by Gemma choice opinion, checked memory injections (Diff: %.2f)", diff),
					})
					found, _ := CheckMemoryInjection(ctx, msg.Scores.Memory.DumpPath, targetPID)
					if found {
						hypotheses.Rootkit.Confidence += 0.15
						hypotheses.Rootkit.For = append(hypotheses.Rootkit.For, "Memory injection detected")
					}
					logID := "memory_injection" + "_" + chars
					saveArtifactToRedis(logID, []byte(`{"status": "injection_confirmed"}`))
					msgResult.Artifacts = append(msgResult.Artifacts, Artifact{
						ID:     logID,
						Type:   "memory_injection",
						PID:    targetPID,
						Detail: "Memory injection detected by MCP",
					})
					hasInjection = true
				}
			default:
				log.Printf("Gemma suggested unknown or already executed tool %s", toolToCall)
			}
		}
		msgResult.Hypothesis = hypotheses
		msgResult.AuditSteps = auditSteps
		data, err := json.Marshal(msgResult)
		if err != nil {
			log.Println("JSON Marshal error", err)
			cancel()
			continue
		}
		err = writer.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(msg.CaseID),
				Value: data,
			})
		if err != nil {
			log.Println("Failed to write a message to the topic", err)
		} else {
			fmt.Printf("Successfully wrote message to topic %s\n", msg.CaseID)
		}
		cancel()
	}
}

func saveStatusToRedis(caseID string) {
	ctx := context.Background()
	key := "pipeline:" + caseID + ":be1_orch_start"

	err := rdb.Set(ctx, key, "true", 86400*time.Second).Err()
	if err != nil {
		log.Println("Error occur during save to Redis: ", err)
	}
}
func saveArtifactToRedis(artifactID string, payloadJSON []byte) {
	ctx := context.Background()
	key := "artifact:" + artifactID

	err := rdb.Set(ctx, key, payloadJSON, 172800*time.Second).Err()
	if err != nil {
		log.Println("Error occur during save to Redis: ", err)
	}
}

func callGemmaOrchestrator(ctx context.Context, hypotheses HypothesisCases, diff float64) (string, error) {
	filepath := "prompts/mcp_tool_orchestator"
	filebytes, err := os.ReadFile(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to read prompt file: %w", err)
	}
	client, err := api.ClientFromEnvironment()
	if err != nil {
		return "", fmt.Errorf("failed to create client: %w", err)
	}
	hypothesJson, _ := json.MarshalIndent(hypotheses, "", "  ")
	userPrompt := fmt.Sprintf(
		"%s\n\n Current Situation: \n Uncertainty detected, Difference between top hypotheses is %.2f.\n Current Hypotheses State:\n%s\n\n Decide which tool is most critical to resolve the ambiguilty right now. Return JSON with 'tool_to_call'.",
		string(filebytes), diff, string(hypothesJson),
	)
	req := &api.ChatRequest{
		Model: "gemma3",
		Messages: []api.Message{
			{
				Role:    "user",
				Content: userPrompt,
			},
		},
		Format: json.RawMessage(`"json"`),
		Stream: new(bool),
		Options: map[string]interface{}{
			"temperature": 0.1,
		},
	}
	var rawResponse string
	err = client.Chat(ctx, req, func(resp api.ChatResponse) error {
		rawResponse = resp.Message.Content
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("error during chat inference: %w", err)
	}
	var resolution AIResolution
	if err := json.Unmarshal([]byte(rawResponse), &resolution); err != nil {
		return "", fmt.Errorf("failed to parse to AIResolution: %w", err)
	}
	return resolution.ToolToCall, nil
}

func deepAnalystGemma(ctx context.Context, caseID string, logs []SyslogStats) (*TimelineAnalysisResult, error) {
	filepath := "prompts/timeline_analyst"
	systemPromptBytes, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read prompt file: %w", err)
	}
	client, err := api.ClientFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	timelineJson, _ := json.MarshalIndent(logs, "", "  ")
	userPrompt := fmt.Sprintf(
		"Please analyze the following chronological log events for Case ID: %s.\n\nLOG TIMELINE:\n%s",
		caseID, string(timelineJson),
	)
	req := &api.ChatRequest{
		Model: "gemma3",
		Messages: []api.Message{
			{
				Role:    "system",
				Content: string(systemPromptBytes),
			},
			{
				Role:    "user",
				Content: userPrompt,
			},
		},
		Format: json.RawMessage(`"json"`),
		Stream: new(bool),
	}
	var rawResponse string
	err = client.Chat(ctx, req, func(resp api.ChatResponse) error {
		rawResponse = resp.Message.Content
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error during chat inference: %w", err)
	}
	var analysis TimelineAnalysisResult
	if err := json.Unmarshal([]byte(rawResponse), &analysis); err != nil {
		return nil, fmt.Errorf("failed to parse timeline analysis: %w (raw: %s)", err, rawResponse)
	}
	return &analysis, nil
}

func GenerateFinalReportGemma(ctx context.Context, data ReportData) (string, error) {
	filepath := "prompts/report_writer"
	filebytes, err := os.ReadFile(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to read prompt file: %w", err)
	}
	client, err := api.ClientFromEnvironment()
	if err != nil {
		return "", fmt.Errorf("failed to create client: %w", err)
	}
	finalLogJson, _ := json.MarshalIndent(data, "", "  ")
	userPrompt := fmt.Sprintf(
		"Based on logs %s\n.Generate a formal Incident Response Report in Markdown format based on the following verified telemetry in file: %s\n", finalLogJson, string(filebytes))

}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
