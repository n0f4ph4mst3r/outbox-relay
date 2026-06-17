package config

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

type Config struct {
	Env string `yaml:"env" env-default:"local"`

	DatabaseURL string `yaml:"database_url" env-required:"true" env:"DATABASE_URL"`

	Broker OutboxBroker `yaml:"broker_config"`
	Worker OutboxWorker `yaml:"outbox_worker"`
}

type OutboxBroker struct {
	BrokerURL string `yaml:"broker_url" env-required:"true" env:"BROKER_URL"`
}

type OutboxWorker struct {
	BatchLimit int64         `yaml:"batchLimit" env-default:"10"`
	EventTTL   time.Duration `yaml:"eventTTL" env-default:"24h"`
}

func MustLoad() *Config {
	if err := godotenv.Load(".env"); err != nil {
		log.Println("No .env file found, using system environment")
	}

	var configPath string

	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.Parse()

	if configPath == "" {
		configPath = os.Getenv("CONFIG_PATH")
		if configPath == "" {
			panic("config path is empty")
		} else {
			log.Println("Using config file:", configPath)
		}
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic("config file does not exist: " + configPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("cannot read config: " + err.Error())
	}

	return &Config{
		Env:         cfg.Env,
		Worker:      cfg.Worker,
		DatabaseURL: cfg.DatabaseURL,
		Broker:      cfg.Broker,
	}
}
