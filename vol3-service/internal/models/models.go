package models

import (
	"time"
)

type ArtifactFiles struct {
	Memory string   `json:"memory"`
	Logs   []string `json:"logs"`
}

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
