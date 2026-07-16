package config

import (
	"crypto/tls"
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type JWTConfig struct {
	Enabled        bool   `yaml:"enabled" env:"JWT_ENABLED" env-default:"false"`
	PrivateKeyPath string `yaml:"private_key_path" env:"JWT_PRIVATE_KEY_PATH"`
	PublicKeyPath  string `yaml:"public_key_path" env:"JWT_PUBLIC_KEY_PATH"`
	AccessTTL      int    `yaml:"access_ttl" env:"JWT_ACCESS_TTL" env-default:"15"`
}

type S3Config struct {
	Enabled   bool   `yaml:"enabled" env:"S3_ENABLED" env-default:"false"`
	Endpoint  string `yaml:"endpoint" env:"S3_ENDPOINT"`
	Region    string `yaml:"region" env:"S3_REGION" env-default:"ru-1"`
	AccessKey string `yaml:"access_key" env:"S3_ACCESS_KEY"`
	SecretKey string `yaml:"secret_key" env:"S3_SECRET_KEY"`
	Bucket    string `yaml:"bucket" env:"S3_BUCKET"`
}

type PostgresConfig struct {
	Host     string   `yaml:"db_host" env:"DB_HOST"`
	Port     string   `yaml:"db_port" env:"DB_PORT"`
	Name     string   `yaml:"db_name" env:"DB_NAME"`
	Names    []string `yaml:"db_names" env:"DB_NAMES"`
	User     string   `yaml:"user" env:"DB_USER" env-required:"true"`
	Password string   `yaml:"password" env:"DB_PASS" env-required:"true"`

	MaxConns int32 `yaml:"max_conns" env:"DB_MAX_CONNS" env-default:"10"`
	MinConns int32 `yaml:"min_conns" env:"DB_MIN_CONNS" env-default:"2"`
}

type RedisConfig struct {
	Mode         string   `yaml:"mode" env-default:"standalone"`
	MasterAddrs  []string `yaml:"master_addrs"`
	ReplicaAddrs []string `yaml:"replica_addrs"`
	Password     string   `yaml:"password" env:"REDIS_PASS"`
	PoolSize     int      `yaml:"pool_size" env-default:"100"`
}

type GRPCConfig struct {
	Port             string `yaml:"port" env:"GRPC_PORT" env-default:"50051"`
	MaxRecvMsgSize   int    `yaml:"max_recv_msg_size" env:"GRPC_MAX_RECV_MSG_SIZE" env-default:"4194304"`
	MaxSendMsgSize   int    `yaml:"max_send_msg_size" env:"GRPC_MAX_SEND_MSG_SIZE" env-default:"4194304"`
	KeepAliveTime    int    `yaml:"keepalive_time" env:"GRPC_KEEPALIVE_TIME" env-default:"7200"`
	KeepAliveTimeout int    `yaml:"keepalive_timeout" env:"GRPC_KEEPALIVE_TIMEOUT" env-default:"20"`

	TLS struct {
		Enabled  bool   `yaml:"enabled" env:"GRPC_TLS_ENABLED"`
		CertFile string `yaml:"cert_file" env:"GRPC_TLS_CERT"`
		KeyFile  string `yaml:"key_file" env:"GRPC_TLS_KEY"`
	} `yaml:"tls"`
}

type KafkaConfig struct {
	Enabled     bool     `yaml:"enabled" env:"KAFKA_ENABLED"`
	SASLEnabled bool     `yaml:"sasl_enabled" env:"KAFKA_SASL_MODE"`
	TLSMode     string   `yaml:"tls_mode" env:"KAFKA_TLS_MODE" env-default:"none"`
	GroupID     string   `yaml:"group_id" env:"KAFKA_GROUP_ID"`
	Brokers     []string `yaml:"brokers" env:"KAFKA_BROKERS"`
	Topics      []string `yaml:"topics" env:"KAFKA_TOPICS"`

	User     string `yaml:"user" env:"KAFKA_USER"`
	Password string `yaml:"password" env:"KAFKA_PASS"`

	StateReplicator StateReplicatorConfig `yaml:"state_replicator"`
	OutboxRelay     OutboxRelayConfig     `yaml:"outbox_relay"`
	TaskProcessor   TaskProcessorConfig   `yaml:"task_processor"`
}

type StateReplicatorConfig struct {
	Enabled      bool   `yaml:"enabled" env:"KAFKA_REPLICATOR_ENABLED"`
	UseMetaPodID bool   `yaml:"use_meta_pod_id"`
	GroupID      string `yaml:"group_id" env:"KAFKA_REPLICATOR_GROUP_ID"`
	Topic        string `yaml:"topic" env:"KAFKA_REPLICATOR_TOPIC"`

	BootstrapThreshold int  `yaml:"bootstrap_threshold" env:"BOOTSTRAP_THRESHOLD" env-default:"300"`
	BootstrapBlock     bool `yaml:"bootstrap_block" env:"BOOTSTRAP_BLOCK" env-default:"false"`
}

type TaskProcessorConfig struct {
	Enabled     bool   `yaml:"enabled" env:"KAFKA_TASK_ENABLED"`
	GroupID     string `yaml:"group_id" env:"KAFKA_TASK_GROUP_ID"`
	Topic       string `yaml:"topic" env:"KAFKA_TASK_TOPIC"`
	Concurrency int    `yaml:"concurrency" env:"KAFKA_TASK_CONCURRENCY" env-default:"1"`
}

type OutboxRelayConfig struct {
	Enabled      bool `yaml:"enabled" env:"KAFKA_OUTBOX_ENABLED"`
	Tick         int  `yaml:"tick" env:"KAFKA_OUTBOX_TICK" env-default:"100"`
	MaxAttempts  int  `yaml:"max_attempts" env:"KAFKA_OUTBOX_MAX_ATTEMPTS" env-default:"3"`
	WriteTimeout int  `yaml:"write_timeout" env:"KAFKA_OUTBOX_WRITE_TIMEOUT" env-default:"5"`
}

type CORSConfig struct {
	Enabled          bool     `yaml:"enabled"`
	AllowOrigins     []string `yaml:"allow_origins"`
	AllowMethods     []string `yaml:"allow_methods"`
	AllowHeaders     []string `yaml:"allow_headers"`
	AllowCredentials bool     `yaml:"allow_credentials"`
}

type CoreConfig struct {
	Postgres PostgresConfig `yaml:"postgresql"`
	Redis    RedisConfig    `yaml:"redis"`
	GRPC     GRPCConfig     `yaml:"grpc"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	CORS     CORSConfig     `yaml:"cors"`
	S3       S3Config       `yaml:"s3"`
	JWT      JWTConfig      `yaml:"jwt"`

	Prometheus struct {
		Enabled     bool   `yaml:"enabled" env:"PROMETHEUS_ENABLED"`
		Secure      bool   `yaml:"secure" env:"PROMETHEUS_SECURE"`
		User        string `yaml:"user" env:"METRICS_USER" env-required:"false"`
		Password    string `yaml:"password" env:"METRICS_PASS" env-required:"false"`
		ServiceName string `yaml:"service_name" env:"SERVICE_NAME" env-default:"unknown"`
	} `yaml:"prometheus"`

	Jaeger struct {
		Enabled     bool   `yaml:"enabled" env:"JAEGER_ENABLED"`
		ServiceName string `yaml:"service_name" env:"JAEGER_SERVICE_NAME"`
		URL         string `yaml:"url" env:"JAEGER_URL"`
	} `yaml:"jaeger"`

	Security struct {
		MasterKey string `yaml:"master_key" env:"MASTER_ENCRYPTION_KEY" env-required:"true"`
	} `yaml:"security"`
}

func (k *KafkaConfig) SASLMechanism() sasl.Mechanism {
	if k.SASLEnabled {
		return plain.Mechanism{
			Username: k.User,
			Password: k.Password,
		}
	}
	return nil
}

func (k *KafkaConfig) TLSConfig() *tls.Config {
	switch k.TLSMode {
	case "secure":
		return &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	case "insecure":
		return &tls.Config{
			InsecureSkipVerify: true,
		}
	case "none", "":
		return nil
	default:
		return nil
	}
}

func (p *PostgresConfig) DSN(dbName string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		p.User, p.Password, p.Host, p.Port, dbName,
	)
}

func LoadCoreConfig(path string) (*CoreConfig, error) {
	var cfg CoreConfig
	err := cleanenv.ReadConfig(path, &cfg)
	return &cfg, err
}
