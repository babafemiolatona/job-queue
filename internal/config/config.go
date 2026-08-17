package config

import "os"

type Config struct {
	ServiceName string
	RedisAddr   string
	LogLevel    string
}

func Load(serviceName string) Config {
	return Config{
		ServiceName: serviceName,
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
