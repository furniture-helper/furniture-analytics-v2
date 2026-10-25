package repositories

import (
	"context"
	"errors"
	"fmt"
	"furniture-analytics/config"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	rdsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPostgresPool(ctx context.Context, cfg config.DatabaseConfig) (*pgxpool.Pool, error) {
	if cfg.URL == "" {
		return nil, errors.New("DATABASE_URL is required")
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database config: %w", err)
	}

	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.MinConns = cfg.MinConns
	poolConfig.HealthCheckPeriod = 30 * time.Second

	if cfg.UseRDSIAMAuth {
		tokenProvider, err := newRDSIAMTokenProvider(ctx, cfg, poolConfig.ConnConfig)
		if err != nil {
			return nil, err
		}

		poolConfig.BeforeConnect = func(ctx context.Context, connConfig *pgx.ConnConfig) error {
			token, err := tokenProvider.Token(ctx)
			if err != nil {
				return fmt.Errorf("generate rds iam auth token: %w", err)
			}

			connConfig.Password = token
			return nil
		}

		maxLifetime := cfg.RDSIAMConnMaxLifetime
		if maxLifetime <= 0 || maxLifetime > 14*time.Minute {
			maxLifetime = 14 * time.Minute
		}
		poolConfig.MaxConnLifetime = maxLifetime
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := Ping(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("postgres pool is nil")
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	return nil
}

type rdsIAMTokenProvider struct {
	credentials   aws.CredentialsProvider
	endpoint      string
	region        string
	username      string
	refreshBefore time.Duration

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func newRDSIAMTokenProvider(ctx context.Context, cfg config.DatabaseConfig, connConfig *pgx.ConnConfig) (*rdsIAMTokenProvider, error) {
	if cfg.RDSIAMRegion == "" {
		return nil, errors.New("RDS_IAM_REGION or AWS_REGION must be set when DB_USE_RDS_IAM_AUTH=true")
	}

	if connConfig == nil {
		return nil, errors.New("postgres connection config is nil")
	}

	if connConfig.User == "" {
		return nil, errors.New("database username is required for RDS IAM auth")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.RDSIAMRegion))
	if err != nil {
		return nil, fmt.Errorf("load aws config for rds iam auth: %w", err)
	}

	endpoint := net.JoinHostPort(connConfig.Host, strconv.Itoa(int(connConfig.Port)))
	return &rdsIAMTokenProvider{
		credentials:   awsCfg.Credentials,
		endpoint:      endpoint,
		region:        cfg.RDSIAMRegion,
		username:      connConfig.User,
		refreshBefore: cfg.RDSIAMTokenRefreshBefore,
	}, nil
}

func (p *rdsIAMTokenProvider) Token(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.token != "" && time.Now().Before(p.expiresAt.Add(-p.refreshBefore)) {
		return p.token, nil
	}

	token, err := rdsauth.BuildAuthToken(ctx, p.endpoint, p.region, p.username, p.credentials)
	if err != nil {
		return "", err
	}

	p.token = token
	p.expiresAt = time.Now().Add(15 * time.Minute)
	return p.token, nil
}
