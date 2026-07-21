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
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "evidence-ready"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "vol3-service"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		WorkerPool:   getEnvAsInt("WORKER_POOL_SIZE", 6),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return fallback
}
