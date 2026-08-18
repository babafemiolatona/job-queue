package config

import "os"

type Config struct {
	ServiceName string
	RedisAddr   string
	HTTPAddr    string
	LogLevel    string
}

func Load(serviceName string) Config {
	return Config{
		ServiceName: serviceName,
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
