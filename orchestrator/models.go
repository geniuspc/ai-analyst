package main

import (
	"time"
)

type ML1ScoredMessage struct {
	CaseID string `json:"case_id"`
	Scores struct {
		Memory struct {
			Score          float64 `json:"score"`
			Memory         float64 `json:"memory"`
			DumpPath       string  `json:"dump_path"`
			SuspiciousPIDs []int   `json:"suspicious_pids"`
			Shap           struct {
				MemoryInjection        float64 `json:"memory_injection"`
				SuspiciousBashCmd      float64 `json:"suspicious_bash_cmd"`
				SuspiciousKernelModule float64 `json:"suspicious_kernel_module"`
			} `json:"shap"`
		} `json:"memory"`
		Logs struct {
			Score           float64  `json:"score"`
			AvailableStatus bool     `json:"available"`
			SuspiciousIPs   []string `json:"suspicious_ips"`
			Features        struct {
				FailedLogins       int  `json:"failed_logins"`
				BruteForce         bool `json:"brute_force"`
				RootSSHLogin       bool `json:"root_ssh_login"`
				NewSystemDService  bool `json:"new_systemd_service"`
				LogTampering       bool `json:"log_tampering"`
				ExternalConnection bool `json:"external_connection"`
				NewCronJob         bool `json:"new_cron_job"`
				EncodedCmd         bool `json:"encoded_cmd"`
			} `json:"features"`
		} `json:"logs"`
	} `json:"scores"`
	Discrepancy struct {
		Detected    bool   `json:"detected"`
		Type        string `json:"type"`
		Description string `json:"description"`
	} `json:"discrepancy"`
	Timestamp string `json:"ts"`
}

type Hypothesis struct {
	Confidence float64  `json:"confidence"`
	For        []string `json:"for"`
	Against    []string `json:"against"`
}

type HypothesisCases struct {
	Rootkit         Hypothesis `json:"rootkit"`
	LateralMovement Hypothesis `json:"lateral_movement"`
	FalsePositive   Hypothesis `json:"false_positive"`
}

type Process struct {
	ID       int    `json:"id"`
	ParentID int    `json:"parent_id"`
	Name     string `json:"name"`
}

type Node struct {
	Data     Process
	Children []*Node
}

type MalfindResponse struct {
	Address       string `json:"address"`
	Size          int    `json:"size"`
	AccessRights  string `json:"rwx"`
	FileExistence bool   `json:"file_existence"`
}

type NetworkConnection struct {
	ParentID int    `json:"parent_id"`
	IP       string `json:"ip"`
}

type BashHistory struct {
	PID     int    `json:"pid"`
	Command string `json:"command"`
	Time    string `json:"time"`
}

type KernelModule struct {
	Name string `json:"module"`
}

type PersistenceStatus struct {
	Service string `json:"service"`
	Cron    string `json:"cron"`
	SSHKey  string `json:"ssh_key"`
}

type SyslogStats struct {
	Timestamp  string    `json:"timestamp"`
	Message    string    `json:"message"`
	Severity   string    `json:"severity"`
	ParsedTime time.Time `json:"-"`
}

type Artifact struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	PID    int    `json:"pid"`
	Detail string `json:"detail"`
}
type AuditStep struct {
	Step      int    `json:"step"`
	Action    string `json:"action"`
	Timestamp string `json:"ts"`
	Result    string `json:"result"`
}
type AgentFindingsMsg struct {
	CaseID string `json:"case_id"`
	Scores struct {
		Memory float64 `json:"memory"`
		Logs   float64 `json:"logs"`
	} `json:"scores"`
	Hypothesis HypothesisCases `json:"hypothesis"`
	Artifacts  []Artifact      `json:"artifacts"`
	McpResults struct {
		ProcessTree struct {
			ParentID int      `json:"pid"`
			Chain    []string `json:"chain"`
		} `json:"get_process_tree"`
		KernelModules struct {
			Suspicious []string `json:"suspicious"`
		} `json:"check_kernel_modules"`
	} `json:"mcp_results"`
	AuditSteps []AuditStep `json:"audit_steps"`
}

type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Format string `json:"format"`
	Stream bool   `json:"stream"`
}

type AIResolution struct {
	ToolToCall string `json:"tool_to_call"`
	Reason     string `json:"reason"`
}
type OllamaResponse struct {
	Response string `json:"response"`
}

type TimelineAnalysisResult struct {
	RootCause          string   `json:"root_cause"`
	IsCoordinated      bool     `json:"is_coordinated"`
	ProgressionSummary []string `json:"progression_summary"`
}

type ReportData struct {
	CaseID          string  `json:"case_id"`
	Risk            string  `json:"risk"`
	FinalConfidence float64 `json:"final_confidence"`
	Msg             struct {
		VerifiedFacts               string `json:"verified_facts"`
		NarrativeMessageDescription string `json:"narrative_message_description"`
	}
}
