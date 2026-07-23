package config

import (
	"os"
	"strconv"
)

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroupID string
	RedisAddr    string
	WorkerPool   int
}

func LoadConfig() *Config {
	return &Config{
		KafkaBrokers: GetEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   GetEnv("KAFKA_TOPIC", "evidence-ready"),
		KafkaGroupID: GetEnv("KAFKA_GROUP_ID", "vol3-service"),
		RedisAddr:    GetEnv("REDIS_ADDR", "localhost:6379"),
		WorkerPool:   getEnvAsInt("WORKER_POOL_SIZE", 6),
	}
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
func getEnvAsInt(key string, fallback int) int {
	valueStr := GetEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return fallback
}
