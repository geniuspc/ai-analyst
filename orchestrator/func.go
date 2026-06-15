package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func HypothesisTracker(msg ML1ScoredMessage) HypothesisCases {
	var tracker HypothesisCases

	if msg.Scores.Memory.Score > 0.7 {
		tracker.Rootkit.Confidence = msg.Scores.Memory.Score * 0.8
		tracker.Rootkit.For = append(tracker.Rootkit.For, "Memory loss appeared")
	}

	if msg.Scores.Logs.Score > 0.7 {
		tracker.LateralMovement.Confidence = msg.Scores.Logs.Score * 0.8
		tracker.LateralMovement.For = append(tracker.LateralMovement.For, "High score suspicion")
	}

	if msg.Scores.Memory.Score < 0.4 && msg.Scores.Logs.Score < 0.4 {
		tracker.FalsePositive.Confidence = 0.7
		tracker.FalsePositive.For = append(tracker.FalsePositive.For, "Score isn't enough")
	}
	return tracker
}

func GetProcessTree(ctx context.Context, scorePath string, suspiciousPID int) (bool, error) {
	cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", scorePath, "-r", "json", "linux.pslist")
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("error occur during get process tree: %w ", err)
	}
	var flatData []Process
	errJson := json.Unmarshal(out, &flatData)
	if errJson != nil {
		return false, fmt.Errorf("json parse error: %w ", errJson)
	}
	procMap := make(map[int]Process)
	for _, proc := range flatData {
		procMap[proc.ID] = proc
	}
	currentID := suspiciousPID
	for currentID != 1 && currentID != 0 {
		proc, exists := procMap[currentID]
		if exists == true {
			log.Printf("Name: %s , PID: %d , PPID %d\n", proc.Name, proc.ID, proc.ParentID)
			return true, nil
		}
		if exists == false {
			break
		}
		currentID = proc.ParentID
	}
	fmt.Println(string(out))
	return false, nil
}

func CheckMemoryInjection(ctx context.Context, scorePath string, pid int) (bool, error) {
	pidStr := strconv.Itoa(pid)
	cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", scorePath, "-r", "json", "linux.malfind", "--pid", pidStr)
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("error occur during check memory injection: %w ", err)
	}
	var vadRegion []MalfindResponse
	errJson := json.Unmarshal(out, &vadRegion)
	if errJson != nil {
		return false, fmt.Errorf("json parse error: %w", errJson)
	}
	if len(vadRegion) != 0 {
		for _, region := range vadRegion {
			log.Printf("Address: %s, size: %d, access: %s, file exists: %t", region.Address, region.Size, region.AccessRights, region.FileExistence)
		}
		return true, nil
	}

	return false, nil
}

func GetNetworkConnections(ctx context.Context, scorePath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", scorePath, "-r", "json", "linux.netstat")
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("error occur during get network connections: %w ", err)
	}
	var networkConnections []NetworkConnection
	errJson := json.Unmarshal(out, &networkConnections)
	if errJson != nil {
		return false, fmt.Errorf("json parse error: %w", errJson)
	}
	if len(networkConnections) != 0 {
		for _, conn := range networkConnections {
			log.Printf("ParentID: %d, IP: %s", conn.ParentID, conn.IP)
		}
		return true, nil
	}
	return false, nil
}

func AnalyzeBashHistory(ctx context.Context, scorePath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", scorePath, "-r", "json", "linux.bash")
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("error occur during analyze bash history: %w", err)
	}
	var BashAnalyze []BashHistory
	errJson := json.Unmarshal(out, &BashAnalyze)
	if errJson != nil {
		return false, fmt.Errorf("json parse error: %w", errJson)
	}
	for _, line := range BashAnalyze {
		dangerousFilter := []string{"wget", "curl", "chmod +x", "reboot", "shutdown", "rm -rf /var/log"}
		for _, filter := range dangerousFilter {
			if strings.Contains(line.Command, filter) == true {
				log.Printf("Found dangerous line: %s , trigger:%s, time: %s (PID:%d)", line.Command, filter, line.Time, line.PID)
				return true, nil
			}
		}
	}
	return false, nil
}

func CheckKernelModules(ctx context.Context, scorePath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", scorePath, "-r", "json", "linux.lsmod")
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("error occur during check kernel modules: %w ", err)
	}
	var KernelAnalyzes []KernelModule
	errJson := json.Unmarshal(out, &KernelAnalyzes)
	if errJson != nil {
		return false, fmt.Errorf("json parse error: %w", errJson)
	}
	whitelist := map[string]bool{
		"ext4":       true,
		"intel_rapl": true,
		"vboxguest":  true,
		"scsi_mod":   true,
	}
	for _, mod := range KernelAnalyzes {
		current := whitelist[mod.Name]
		if current == true {
			continue
		}
		if current == false {
			log.Printf("Kernel module %s is not whitelisted", mod.Name)
			return true, nil
		}
	}
	return false, nil
}

func CheckPersistence(ctx context.Context, scorePath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", scorePath, "-r", "json", "linux.pslist")
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("Error occur during check persistence: %w ", err)
	}
	var flatData []Process
	errJson := json.Unmarshal(out, &flatData)
	if errJson != nil {
		return false, fmt.Errorf("json parse error: %w", errJson)
	}
	LinksMap := make(map[int][]Process)
	var cronPID int
	for _, proc := range flatData {
		LinksMap[proc.ParentID] = append(LinksMap[proc.ParentID], proc)
		if proc.Name == "cron" || proc.Name == "crond" {
			cronPID = proc.ID
		}
	}
	whitelistServices := map[string]bool{
		"systemd-journald": true,
		"systemd-udevd":    true,
		"sshd":             true,
		"crond":            true,
		"cron":             true,
		"rsyslogd":         true,
		"dbus-daemon":      true,
		"networkd":         true,
	}
	systemServices := LinksMap[1]
	for _, srv := range systemServices {
		isCorrect := whitelistServices[srv.Name]
		if isCorrect == false {
			log.Printf("Unknown service detected: %s (PID: %d)", srv.Name, srv.ID)
			return true, nil
		}
	}
	cronJobs := LinksMap[cronPID]
	for _, job := range cronJobs {
		if job.Name == "bash" || job.Name == "sh" || job.Name == "wget" || job.Name == "curl" {
			log.Printf("Attention: Cron launched dangerous process: %s (PID: %d)", job.Name, job.ID)
			return true, nil
		} else {
			log.Printf("Info: Cron is running child process: %s (PID: %d)", job.Name, job.ID)
		}
	}
	for _, proc := range flatData {
		if strings.Contains(proc.Name, "sshd") && proc.ParentID != 1 {
			log.Printf("Active SSH session detected: %s", proc.Name)
			return true, nil
		}
	}
	return false, nil
}

func AnalyzeLogTimeline(ctx context.Context, caseID string) ([]SyslogStats, error) {
	logPath := fmt.Sprintf("/evidence/%s/logs/syslog", caseID)
	file, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("error occur during analyze log timeline file at %s: %w", logPath, err)
	}
	defer file.Close()
	var logs []SyslogStats
	scanner := bufio.NewScanner(file)
	syslogAlert := []string{
		"segfault",
		"promiscuous",
		"root",
		"failed",
		"denied",
		"panic",
	}

	for scanner.Scan() {
		line := scanner.Text()
		lowerLine := strings.ToLower(line)
		isDangerous := false
		for _, filter := range syslogAlert {
			if strings.Contains(lowerLine, filter) {
				isDangerous = true
				break
			}
		}

		if isDangerous {
			parts := strings.SplitN(line, " ", 4)
			var timestamp, message string

			if len(parts) >= 4 {
				timestamp = fmt.Sprintf("%s %s %s", parts[0], parts[1], parts[2])
				message = parts[3]
			} else {
				timestamp = "unknown"
				message = line
			}

			logs = append(logs, SyslogStats{
				Timestamp: timestamp,
				Message:   message,
				Severity:  "high",
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading syslog: %w", err)
	}

	return logs, nil
}

func DecodeCommands(ctx context.Context, scorePath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "python3", "vol.py", "-f", scorePath, "-r", "json", "linux.cmdline")
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("error occur during decode commands: %w ", err)
	}
	var cmdOutput []KernelModule
	errJson := json.Unmarshal(out, &cmdOutput)
	if errJson != nil {
		return false, fmt.Errorf("json parse error: %w", errJson)
	}
	foundDecoded := false
	for _, line := range cmdOutput {
		encodedPayload := line.Name
		decodedLine, err := base64.StdEncoding.DecodeString(encodedPayload)
		if err != nil {
			continue
		}
		log.Printf("Successfully decoded payload: %s", string(decodedLine))
		foundDecoded = true
	}
	return foundDecoded, nil
}
