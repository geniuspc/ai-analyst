package ollama

import (
	"ai-analyst/decision_engine/internal/models"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	baseURL string
	model   string
	hc      *http.Client
}

func NewClient(baseURL string, model string) *Client {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	if model == "" {
		model = "gemma3:4b"
	}
	return &Client{
		baseURL: baseURL,
		model:   model,
		hc:      &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) MakeDecision(ctx context.Context, msg models.ML2ScoredMessage) (*models.AIResolution, error) {
	prompt := fmt.Sprintf(
		"You are a DFIR Decision Assistant. Analyze the following telemetry context and provide a resolution.\n"+
			"Context:\n"+
			"- Case ID: %s\n"+
			"- Revised Confidence: %.2f\n"+
			"- Hallucination Rate: %.2f\n"+
			"- Counter Arguments Count: %d\n\n"+
			"Determine if further action is required. Output strictly a JSON object with two fields:\n"+
			"1. \"tool_to_call\": name of action/tool (e.g., \"isolate_host\", \"manual_review\", \"none\")\n"+
			"2. \"reason\": short explanation.\n\n"+
			"JSON Output:",
		msg.CaseID, msg.RevisedConfidence, msg.HallucinationRate, msg.CounterArguments, len(msg.VerifiedFacts),
	)
	reqBody := models.OllamaRequest{
		Model:  c.model,
		Prompt: prompt,
		Format: "json",
		Stream: false,
	}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/generate", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama http call failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama http call failed with status code: %d", resp.StatusCode)
	}
	var ollamaResp models.OllamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&ollamaResp); err != nil {
		return nil, fmt.Errorf("failed to decode ollama response: %w", err)
	}
	var resolution models.AIResolution
	if err := json.Unmarshal([]byte(ollamaResp.Response), &resolution); err != nil {
		return nil, fmt.Errorf("failed to parse AIResolution json: %w", err)
	}
	return &resolution, nil
}
