package config

import (
	"fmt"
	"os"
	"strconv"
)

type MinIOConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	BucketName      string
	Location        string
}

func LoadMinIOConfig() (MinIOConfig, error) {
	config := MinIOConfig{
		Endpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		AccessKeyID:     getEnv("MINIO_ACCESS_KEY", "minio_admin"),
		SecretAccessKey: getEnv("MINIO_SECRET_KEY", "minio_password"),
		UseSSL:          getEnvBool("MINIO_USE_SSL", false),
		BucketName:      getEnv("MINIO_BUCKET", "mybucket"),
		Location:        getEnv("MINIO_LOCATION", "us-east-1"),
	}

	if config.Endpoint == "" {
		return config, fmt.Errorf("MINIO_ENDPOINT is required")
	}
	if config.AccessKeyID == "" {
		return config, fmt.Errorf("MINIO_ACCESS_KEY is required")
	}
	if config.SecretAccessKey == "" {
		return config, fmt.Errorf("MINIO_SECRET_KEY is required")
	}
	if config.BucketName == "" {
		return config, fmt.Errorf("MINIO_BUCKET is required")
	}

	return config, nil
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}

	return boolValue
}
