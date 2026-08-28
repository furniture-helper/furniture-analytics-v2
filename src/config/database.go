package config

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

const (
	defaultDBMaxConns int32 = 10
	defaultDBMinConns int32 = 1

	defaultDBHost     = "localhost"
	defaultDBPort     = "5432"
	defaultDBUsername = "postgres"
	defaultDBName     = "postgres"
	defaultDBSSLMode  = "disable"
)

type DatabaseConfig struct {
	URL      string
	MaxConns int32
	MinConns int32
}

type DatabaseSecrets struct {
	Username string `json:"username"`
	Password string `json:"password"`
	DBName   string `json:"database_name"`
}

func GetDatabaseConfig(ctx context.Context) DatabaseConfig {
	if ctx == nil {
		ctx = context.Background()
	}

	return DatabaseConfig{
		URL:      buildDatabaseURL(ctx),
		MaxConns: getInt32Env("DB_MAX_CONNS", defaultDBMaxConns),
		MinConns: getInt32Env("DB_MIN_CONNS", defaultDBMinConns),
	}
}

func buildDatabaseURL(ctx context.Context) string {
	if os.Getenv("DATABASE_CREDENTIALS_TYPE") == "secrets_manager" {
		fmt.Println("Using secrets_manager database credentials")
		err := loadSecretsManagerToEnv(ctx)
		if err != nil {
			log.Fatalf("error fetching database credentials from AWS Secrets Manager: %v", err)
		}
	}

	return buildDatabaseURLFromEnv()
}

func loadSecretsManagerToEnv(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	secretName := os.Getenv("DATABASE_CREDENTIALS_SECRET_NAME")
	if secretName == "" {
		return fmt.Errorf("DATABASE_CREDENTIALS_SECRET_NAME must be set")
	}

	var secretsClient *secretsmanager.Client
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	secretsClient = secretsmanager.NewFromConfig(cfg)

	result, err := secretsClient.GetSecretValue(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to retrieve secret: %w", err)
	}

	var dbSecrets DatabaseSecrets
	err = json.Unmarshal([]byte(*result.SecretString), &dbSecrets)
	if err != nil {
		return fmt.Errorf("failed to unmarshal secret: %w", err)
	}

	fmt.Println("Database credentials retrieved successfully from Secrets Manager.")

	err = os.Setenv("PG_USER", dbSecrets.Username)
	if err != nil {
		return fmt.Errorf("failed to set PG_USER environment variable: %w", err)
	}

	err = os.Setenv("PG_PASSWORD", dbSecrets.Password)
	if err != nil {
		return fmt.Errorf("failed to set PG_PASSWORD environment variable: %w", err)
	}

	err = os.Setenv("PG_DATABASE", dbSecrets.DBName)
	if err != nil {
		return fmt.Errorf("failed to set PG_DATABASE environment variable: %w", err)
	}

	fmt.Println("Database credentials set as environment variables.")
	return nil

}

func buildDatabaseURLFromEnv() string {
	host := getStringEnv("PG_HOST", defaultDBHost)
	port := getStringEnv("PG_PORT", defaultDBPort)
	username := getStringEnv("PG_USER", defaultDBUsername)
	password := strings.TrimSpace(os.Getenv("PG_PASSWORD"))
	database := getStringEnv("PG_DATABASE", defaultDBName)
	sslmode := getStringEnv("PG_SSLMODE", defaultDBSSLMode)

	connURL := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   database,
	}

	connURL.User = url.UserPassword(username, password)

	query := url.Values{}
	query.Set("sslmode", sslmode)
	connURL.RawQuery = query.Encode()

	return connURL.String()
}

func getInt32Env(key string, fallback int32) int32 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed <= 0 {
		return fallback
	}

	return int32(parsed)
}

func getStringEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}
