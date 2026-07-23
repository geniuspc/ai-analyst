package config

import "os"

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroupID string
	RedisAddr    string
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
		KafkaTopic:   GetEnv("KAFKA_TOPIC", "evidence-ready"),
		KafkaGroupID: GetEnv("KAFKA_GROUP_ID", "be1-evidence-loader"),
		RedisAddr:    GetEnv("REDIS_ADDR", "localhost:6379"),
	}
}
