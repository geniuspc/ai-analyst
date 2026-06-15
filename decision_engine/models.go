package main

type Reference struct {
	Claim      string `json:"claim"`
	Type       string `json:"type"`
	ArtifactID string `json:"artifact_id"`
}

type CounterArgument struct {
	Argument   string `json:"argument"`
	ArtifactID string `json:"artifact_id"`
	Severity   string `json:"severity"`
}

type ML2ScoredMessage struct {
	CaseID                      string            `json:"case_id"`
	VerifiedFacts               []Reference       `json:"verified_facts"`
	Inferences                  []Reference       `json:"inferences"`
	CounterArguments            []CounterArgument `json:"counter_arguments"`
	RevisedConfidence           float64           `json:"revised_confidence"`
	HallucinationRate           float64           `json:"hallucination_rate"`
	NarrativeMessageDescription string            `json:"narrative_md"`
}

type PipelineStatus struct {
	Step      string `json:"step"`
	Status    string `json:"status"`
	Risk      string `json:"risk"`
	Timestamp string `json:"ts"`
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
