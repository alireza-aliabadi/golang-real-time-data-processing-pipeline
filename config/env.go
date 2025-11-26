package config

import (
	"log"
	"os"
	"github.com/joho/godotenv"
)

type Config struct {
	RabbitmqUrl string
}

func LoadConfig() *Config {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Note: Could not load .env file. Relying on system environment variables.")

	}

	rabbitmqUrl := os.Getenv("RABBITMQ_URL")
	if rabbitmqUrl == "" {
		log.Fatal("RABBITMQ_URL environment variable is required and not set.")
	}

	return &Config{
		RabbitmqUrl: rabbitmqUrl,
	}
}