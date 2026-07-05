package config

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server     ServerConfig     `mapstructure:"server"`
	MySQL      MySQLConfig      `mapstructure:"mysql"`
	Redis      RedisConfig      `mapstructure:"redis"`
	RabbitMQ   RabbitMQConfig   `mapstructure:"rabbitmq"`
	Aggregator AggregatorConfig `mapstructure:"aggregator"`
	Log        LogConfig        `mapstructure:"log"`
}

type ServerConfig struct {
	Addr         string `mapstructure:"addr"`
	ReadTimeout  int    `mapstructure:"read_timeout"`
	WriteTimeout int    `mapstructure:"write_timeout"`
}

type MySQLConfig struct {
	DSN          string `mapstructure:"dsn"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type RabbitMQConfig struct {
	URL             string `mapstructure:"url"`
	Exchange        string `mapstructure:"exchange"`
	Queue           string `mapstructure:"queue"`
	RoutingKey      string `mapstructure:"routing_key"`
	DeadLetterQueue string `mapstructure:"dead_letter_queue"`
}

type AggregatorConfig struct {
	WindowTTL            time.Duration `mapstructure:"window_ttl"`
	NumShards            int           `mapstructure:"num_shards"`
	ShutdownFlushTimeout time.Duration `mapstructure:"shutdown_flush_timeout"`
	MaxComboCount        int32         `mapstructure:"max_combo_count"`
	MaxWindowsPerShard   int           `mapstructure:"max_windows_per_shard"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("TIDAL")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		_, b, _, _ := runtime.Caller(0)
		root := filepath.Join(filepath.Dir(b), "../..")
		v.AddConfigPath(filepath.Join(root, "config"))
	}

	if err := v.ReadInConfig(); err != nil {
		// config file is optional when env vars are set
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	setDefaults(v)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.addr", ":8080")
	v.SetDefault("server.read_timeout", 10)
	v.SetDefault("server.write_timeout", 10)
	v.SetDefault("mysql.max_open_conns", 50)
	v.SetDefault("mysql.max_idle_conns", 20)
	v.SetDefault("redis.db", 0)
	v.SetDefault("aggregator.window_ttl", "3s")
	v.SetDefault("aggregator.shutdown_flush_timeout", "10s")
	v.SetDefault("aggregator.max_combo_count", 100)
	v.SetDefault("aggregator.max_windows_per_shard", 100000)
	v.SetDefault("log.level", "info")
}
