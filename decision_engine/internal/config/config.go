package config

import "os"

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroupID string
	DLQTopic     string
	RedisAddr    string
	OllamaURL    string
	OllamaModel  string
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func LoadConfig() *Config {
	return &Config{
		KafkaBrokers: GetEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   GetEnv("KAFKA_TOPIC", "ml2-verified-findings"),
		KafkaGroupID: GetEnv("KAFKA_GROUP_ID", "be1-decision-engine"),
		DLQTopic:     GetEnv("DLQ_TOPIC", "dead-letter-topic"),
		RedisAddr:    GetEnv("REDIS_ADDR", "localhost:6379"),
		OllamaURL:    GetEnv("OLLAMA_URL", "http://localhost:11434"),
		OllamaModel:  GetEnv("OLLAMA_MODEL", "gemma3:4b"),
	}
}
