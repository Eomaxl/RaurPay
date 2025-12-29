package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config holds all configuration for the RaurPay platform
type Config struct {
	// server configuration
	Server ServerConfig `envconfig:"SERVER"`

	// Database Configuration
	Cassandra CassandraConfig `envconfig:"CASSANDRA"`
	Redis     RedisConfig     `envconfig:"REDIS"`

	// Kafka Configuration
	Kafka KafkaConfig `envconfig:"KAFKA"`

	// Application Configuration
	APP AppConfig `envconfig:"APP"`
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Port         int           `envconfig:"PORT" default:"8080"`
	GRPCPort     int           `envconfig:"GRPC_PORT" default:"9090"`
	ReadTimeout  time.Duration `envconfig:"READ_TIMEOUT" default:"30s"`
	WriteTimeout time.Duration `envconfig:"WRITE_TIMEOUT" default:"30s"`
	IdleTimeout  time.Duration `envconfig:"IDLE_TIMEOUT" default:"120s"`
}

// CassandraConfig hoilds Cassandra database configuration
type CassandraConfig struct {
	Hosts             []string      `envconfig:"HOSTS" default:"localhost:9042"`
	Keyspace          string        `envconfig:"KEYSPACE" default:"raurpay"`
	Consistency       string        `envconfig:"CONSISTENCY" default:"QUORUM`
	Timeout           time.Duration `envconfig:"TIMEOUT" default:"10s"`
	ConnectTimeout    time.Duration `envconfig:"CONNECT_TIMEOUT" default:"10s"`
	MaxRetries        int           `envconfig:"MAX_RETRIES" default:"3"`
	ReplicationFactor int           `envconfig:"REPLICATION_FACTOR" default:"3"`
}

// RedisConfig holds redis configuration
type RedisConfig struct {
	Addr         string        `envconfig:"ADDR" default:"localhost:6379"`
	Password     string        `envconfig:"PASSWORD" default:""`
	DB           int           `envconfig:"DB" default:"0"`
	PoolSize     int           `envconfig:"POOL_SIZE" default:"10"`
	MinIdleConns int           `envconfig:"MIN_IDLE_CONNS" default:"5"`
	DialTimeout  time.Duration `envconfig:"DIAL_TIMEOUT" default:"5s"`
	ReadTimeout  time.Duration `envconfig:"READ_TIMEOUT" default:"3s"`
	WriteTimeout time.Duration `envconfig:"WRITE_TIMEOUT" default:"3s"`
	IdleTimeout  time.Duration `envconfig:"IDLE_TIMEOUT" default:"5m"`
}

// KafkaConfig holds kafka configuration
type KafkaConfig struct {
	Brokers       []string `envconfig:"BROKERS" default:"localhost:9092"`
	ConsumerGroup string   `envconfig:"CONSUMER_GROUP" default:"raurpay"`
	RetryMax      int      `envconfig:"RETRY_MAX" default:"3"`
	BatchSize     int      `envconfig:"BATCH_SIZE" default:"100"`
}

// AppConfig holds application-specific configuration
type AppConfig struct {
	Environment       string        `envconfig:"ENVIRONMENT" default:"development"`
	LogLevel          string        `envconfig:"LOG_LEVEL" default:"info"`
	PaymentExpiration time.Duration `envconfig:"PAYMENT_EXPIRATION" default:"24h"`
	IdempotencyTTL    time.Duration `envconfig:"IDEMPOTENCY_TTL" default:"24h"`
	MaxDecimalPlaces  int32         `envconfig:"MAX_DECIMAL_PLACES" default:"2"`
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	var cfg Config
	err := envconfig.Process("RAURPAY", &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// LoadWithPrefix loads configuration with a custom prefix
func LoadWithPrefix(prefix string) (*Config, error) {
	var cfg Config
	err := envconfig.Process(prefix, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
