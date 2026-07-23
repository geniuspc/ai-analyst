package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"ai-analyst/evidence-loader/internal/models"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func MultiPartUploader(w http.ResponseWriter, r *http.Request, rdb *redis.Client, writer *kafka.Writer) {
	r.Body = http.MaxBytesReader(w, r.Body, 20<<30)
	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "error occurred during multipart request", http.StatusBadRequest)
		return
	}
	caseID := uuid.NewString()
	baseDir := filepath.Join("/evidence", caseID, "raw")
	err = os.MkdirAll(baseDir, 0755)
	if err != nil {
		http.Error(w, fmt.Sprintf("error creating evidence directory: %v", err), http.StatusInternalServerError)
		return
	}

	var tempFilePath string
	var originalFileName string
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				os.RemoveAll(filepath.Join("/evidence", caseID))
				http.Error(w, "File too large (Max 20 GB)", http.StatusRequestEntityTooLarge)
				return
			}
			os.RemoveAll(baseDir)
			http.Error(w, "error reading multipart part", http.StatusInternalServerError)
			return
		}

		if part.FileName() != "" {
			originalFileName = filepath.Base(part.FileName())
			tempFilePath = filepath.Join(baseDir, originalFileName)
			fmt.Printf("Processing file %s\n", part.FileName())

			f, err := os.Create(tempFilePath)
			if err != nil {
				os.RemoveAll(filepath.Join("/evidence", caseID))
				http.Error(w, "error creating file", http.StatusInternalServerError)
				return
			}
			_, copyErr := io.Copy(f, part)
			closeErr := f.Close()
			part.Close()
			if copyErr != nil {
				os.RemoveAll(baseDir)
				http.Error(w, "copy error", http.StatusInternalServerError)
				return
			}
			if closeErr != nil {
				os.RemoveAll(baseDir)
				http.Error(w, "error closing file", http.StatusInternalServerError)
				return
			}
		} else {
			part.Close()
		}
	}
	if tempFilePath == "" {
		os.RemoveAll(baseDir)
		http.Error(w, "No file uploaded", http.StatusBadRequest)
		return
	}
	artifactType, _, err := EvaluatePipeline(r.Context(), caseID, tempFilePath, originalFileName, rdb, writer)
	if err != nil {
		os.RemoveAll(filepath.Join("/evidence", caseID))
		if strings.Contains(err.Error(), "broker failure") {
			http.Error(w, "Service unavailable: Redpanda error", http.StatusServiceUnavailable)
		} else if strings.Contains(err.Error(), "unknown artifact") {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		}
		return
	}
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"case_id":"%s", "type": "%s"}`, caseID, artifactType)

}

func CalcucateEntropy(filePath string) (float64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var frequencies [256]int64
	var totalBytes int64

	buffer := make([]byte, 32*1024)
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			totalBytes += int64(n)
			for i := 0; i < n; i++ {
				frequencies[buffer[i]]++
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	if totalBytes == 0 {
		return 0.0, nil
	}
	var entropy float64
	for _, count := range frequencies {
		if count == 0 {
			continue
		}
		p := float64(count) / float64(totalBytes)
		entropy -= p * math.Log2(p)
	}
	return entropy, nil
}

func EvaluatePipeline(ctx context.Context, caseID string, tempFilePath string, originalFileName string, rdb *redis.Client, writer *kafka.Writer) (string, string, error) {
	artifactType, err := DetectArtifact(tempFilePath)
	if err != nil {
		return "", "", fmt.Errorf("error detecting artifact type: %w", err)
	}
	finalPath, err := MountArtifact(caseID, tempFilePath, artifactType, originalFileName)
	if err != nil {
		return "", "", fmt.Errorf("error mounting artifact: %w", err)
	}
	isValid, err := FileValidation(ctx, artifactType, filepath.Base(finalPath), finalPath)
	if err != nil || !isValid {
		if err != nil {
			return "", "", fmt.Errorf("validation failed: %w", err)
		}
		return "", "", fmt.Errorf("file is invalid")
	}
	sumSHA, err := CalculateSHA(finalPath)
	if err != nil {
		return "", "", fmt.Errorf("error calculating SHA: %w", err)
	}
	checkSumKey := fmt.Sprintf("artifact:%s:checksum", caseID)
	sumValue := fmt.Sprintf("sha256:%s", sumSHA)
	err = rdb.Set(ctx, checkSumKey, sumValue, 48*time.Hour).Err()
	if err != nil {
		return "", "", fmt.Errorf("redis error setting checksum: %w", err)
	}
	pipelineKey := fmt.Sprintf("pipeline:%s:state", caseID)
	pipelineState := models.PipelineStatus{
		Step:      "be2_complete",
		Status:    "ready",
		Timestamp: time.Now(),
	}
	stateJson, err := json.Marshal(pipelineState)
	if err != nil {
		return "", "", fmt.Errorf("error marshaling pipelineState: %w", err)
	}
	err = rdb.Set(ctx, pipelineKey, stateJson, 24*time.Hour).Err()
	if err != nil {
		return "", "", fmt.Errorf("redis error pipelineState: %w", err)
	}
	jsonStr, err := ResultTopic(caseID, artifactType, finalPath, "ready", sumSHA)
	if err != nil {
		return "", "", fmt.Errorf("error generating JSON: %w", err)
	}
	message := kafka.Message{
		Key:   []byte(caseID),
		Value: []byte(jsonStr),
	}
	err = writer.WriteMessages(ctx, message)
	if err != nil {
		fmt.Printf("error occurred during sending message: %v\n", err)
	}
	log.Println("Successfully sent message to Redpanda")
	return artifactType, sumSHA, nil
}
func DetectArtifact(filePath string) (string, error) {
	name := strings.ToLower(filepath.Base(filePath))
	switch {
	case strings.HasSuffix(name, ".raw") || strings.HasSuffix(name, ".mem"):
		return "memory", nil
	case strings.HasSuffix(name, ".lime"):
		file, err := os.Open(filePath)
		if err != nil {
			return "", fmt.Errorf("error opening file: %w", err)
		}
		defer file.Close()
		buf := make([]byte, 4)
		_, err = file.Read(buf)
		if err != nil && err != io.EOF {
			return "", fmt.Errorf("error during reading file: %w", err)
		}
		if bytes.Equal(buf, []byte{0x4C, 0x69, 0x4D, 0x45}) {
			return "memory", nil
		}
		return "", fmt.Errorf("unknown magic bytes for LIME")
	case strings.HasSuffix(name, ".bin"):
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return "", err
		}
		filesize := fileInfo.Size()
		filesizeMB := float64(filesize) / (1024 * 1024)
		if filesizeMB <= 64 {
			return "", fmt.Errorf("file size is too small: %.2f MB", filesizeMB)
		}
		entropy, err := CalcucateEntropy(filePath)
		if err != nil {
			return "", fmt.Errorf("error calculating entropy for %s: %w", filePath, err)
		}
		if entropy < 6.5 {
			return "", fmt.Errorf("file entropy is too small: %.2f. It is likely not a memory dump", entropy)
		}
		return "memory", nil
	case strings.HasSuffix(name, ".log") || name == "auth.log" || name == "syslog" || name == "kern.log" || name == "audit.log":
		if name == "syslog" || name == "kern.log" {
			return "logs", nil
		}
		file, err := os.Open(filePath)
		if err != nil {
			return "", fmt.Errorf("error opening file: %w", err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "sshd") || strings.Contains(line, "sudo") {
				return "logs", nil
			}
			if strings.Contains(line, "type=SYSCALL") || strings.Contains(line, "type=USER_LOGIN") {
				return "logs", nil
			}
		}
	case strings.HasSuffix(name, ".json") || name == "journald":
		jsonFile, err := os.ReadFile(filePath)
		if err != nil {
			return "", err
		}
		if bytes.Contains(jsonFile, []byte("__REALTIME_TIMESTAMP")) {
			return "logs", nil
		}
	}
	return "", fmt.Errorf("unknown artifact type")
}

func MountArtifact(caseID string, tempFilePath string, artifactType string, name string) (string, error) {
	baseDir := filepath.Join("/evidence", caseID)
	var finalPath string
	if artifactType == "memory" {
		finalPath = filepath.Join(baseDir, "memory.raw")
	} else if artifactType == "logs" {
		logsDir := filepath.Join(baseDir, "logs")
		if err := os.MkdirAll(logsDir, 0755); err != nil {
			return "", fmt.Errorf("error creating logs directory: %w", err)
		}
		finalPath = filepath.Join(logsDir, name)
	} else {
		return "", fmt.Errorf("unknown artifact type: %s", artifactType)
	}
	if err := os.Rename(tempFilePath, finalPath); err != nil {
		return "", fmt.Errorf("error moving temporary file %s to %s: %w", tempFilePath, finalPath, err)
	}
	os.RemoveAll(filepath.Dir(tempFilePath))
	return finalPath, nil
}

func FileValidation(ctx context.Context, artifactType string, name string, filePath string) (bool, error) {
	if artifactType == "memory" {
		cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", filePath, "banners")
		out, err := cmd.Output()
		if err != nil {
			return false, fmt.Errorf("error executing command: %w", err)
		}
		if strings.Contains(string(out), "Linux version") {
			return true, nil
		}
		return false, fmt.Errorf("linux banner wasn't found")
	}
	if artifactType == "logs" {
		file, err := os.Open(filePath)
		if err != nil {
			return false, fmt.Errorf("error opening file: %w", err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		if name == "auth.log" || name == "syslog" || name == "kern.log" {
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				return false, fmt.Errorf("error checking file stats: %w", err)
			}
			if fileInfo.Size() == 0 {
				return false, fmt.Errorf("file %s is empty", filePath)
			}
			count := 0
			for scanner.Scan() {
				count++
			}
			if err := scanner.Err(); err != nil {
				fmt.Println("error scanning file", err)
			}
			if count < 10 {
				return false, fmt.Errorf("file %s has less than 10 lines", filePath)
			}
			return true, nil
		}

		if name == "audit.log" {
			for scanner.Scan() {
				line := scanner.Text()
				if strings.Contains(line, "type=SYSCALL") || strings.Contains(line, "type=USER_LOGIN") {
					return true, nil
				}
			}
			return false, fmt.Errorf("audit.log does not contain required SYSCALL or USER_LOGIN records")
		}
		if name == "journald" || name == "journald.json" || strings.HasSuffix(name, ".json") {
			jsonFile, err := os.ReadFile(filePath)
			if err != nil {
				return false, fmt.Errorf("error reading file: %w", err)
			}
			if !json.Valid(jsonFile) {
				return false, fmt.Errorf("invalid json file")
			}
			if bytes.Contains(jsonFile, []byte("__REALTIME_TIMESTAMP")) {
				return true, nil
			}
			return false, fmt.Errorf("missing realtime timestamp")
		}
	}
	return false, nil
}

func CalculateSHA(finalPath string) (string, error) {
	file, err := os.Open(finalPath)
	if err != nil {
		return "", fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("error hashing file: %w", err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func ResultTopic(caseID string, artifactType string, filePath string, status string, sumSHA string) (string, error) {
	osType := "linux"
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("error getting information about file: %w", err)
	}
	sizeBytes := fileInfo.Size()
	var evidenceFile models.EvidenceMeta
	switch artifactType {
	case "memory":
		evidenceFile = models.EvidenceMeta{
			CaseID:        caseID,
			ArtifactType:  artifactType,
			MountPath:     filePath,
			OsHint:        osType,
			ByteSize:      sizeBytes,
			Vol3Readiness: true,
			CheckSum:      sumSHA,
			Files: models.ArtifactFiles{
				Memory: filePath,
			},
			Timestamp: time.Now().UTC(),
		}
	case "logs":
		evidenceFile = models.EvidenceMeta{
			CaseID:        caseID,
			ArtifactType:  artifactType,
			MountPath:     filePath,
			OsHint:        osType,
			ByteSize:      sizeBytes,
			Vol3Readiness: false,
			CheckSum:      sumSHA,
			Files: models.ArtifactFiles{
				Logs: []string{filePath},
			},
			Timestamp: time.Now().UTC(),
		}
	}
	jsonBytes, err := json.Marshal(evidenceFile)
	if err != nil {
		return "", fmt.Errorf("error marshalling evidence file: %w", err)
	}
	return string(jsonBytes), nil
}
