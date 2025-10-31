package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
)

// WeatherData represents the structure of the weather.json file
type WeatherData struct {
	Timestamp   int64   `json:"timestamp"`
	Temperature float64 `json:"temperature"`
	Pressure    float64 `json:"pressure"`
	Humidity    float64 `json:"humidity"`
}

// Config holds application configuration from environment variables
type Config struct {
	JSONFilePath string
	DBUser       string
	DBPassword   string
	DBHost       string
	DBPort       string
	DBName       string
	CronSchedule string
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// loadConfig loads configuration from environment variables
func loadConfig() Config {
	return Config{
		JSONFilePath: getEnv("JSON_FILE_PATH", "/var/www/laravel-tene.life/public/files/weather.json"),
		DBUser:       os.Getenv("DB_USER"),
		DBPassword:   os.Getenv("DB_PASSWORD"),
		DBHost:       getEnv("DB_HOST", "localhost"),
		DBPort:       getEnv("DB_PORT", "3306"),
		DBName:       getEnv("DB_NAME", "tene_life"),
		CronSchedule: getEnv("CRON_SCHEDULE", "*/5 * * * *"), // Default: every 5 minutes (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)
	}
}

var config Config

func main() {
	log.Println("Weather data processor started")

	// Load .env file if it exists (for local development)
	// In production, environment variables are set by systemd
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables from system")
	} else {
		log.Println("Loaded configuration from .env file")
	}

	// Load configuration
	config = loadConfig()

	// Validate required configuration
	if config.DBUser == "" {
		log.Fatal("DB_USER environment variable is required")
	}
	if config.DBPassword == "" {
		log.Fatal("DB_PASSWORD environment variable is required")
	}

	log.Printf("Loaded configuration - DB: %s@%s:%s/%s, Schedule: %s",
		config.DBUser, config.DBHost, config.DBPort, config.DBName, config.CronSchedule)

	// Create cron scheduler
	c := cron.New()

	// Schedule job with configurable schedule
	_, err := c.AddFunc(config.CronSchedule, func() {
		log.Println("Starting scheduled weather data processing...")
		if err := processWeatherData(); err != nil {
			log.Printf("Error processing weather data: %v", err)
		} else {
			log.Println("Weather data processed successfully")
		}
	})

	if err != nil {
		log.Fatalf("Failed to schedule cron job: %v", err)
	}

	// Start the cron scheduler
	c.Start()

	log.Println("Cron scheduler started. Waiting for scheduled tasks...")
	log.Printf("Job will run with schedule: %s", config.CronSchedule)

	// Run once immediately on startup for testing
	log.Println("Running initial weather data processing...")
	if err := processWeatherData(); err != nil {
		log.Printf("Error in initial processing: %v", err)
	}

	// Keep the program running
	select {}
}

func processWeatherData() error {
	// Read the JSON file
	data, err := os.ReadFile(config.JSONFilePath)
	if err != nil {
		return fmt.Errorf("failed to read JSON file: %w", err)
	}

	// Parse JSON
	var weatherData WeatherData
	if err := json.Unmarshal(data, &weatherData); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	log.Printf("Parsed weather data: Temp=%.2f°C, Pressure=%.2fhPa, Humidity=%.2f%%",
		weatherData.Temperature, weatherData.Pressure, weatherData.Humidity)

	// Round values to 1 decimal place
	temperature := math.Round(weatherData.Temperature*10) / 10
	pressure := math.Round(weatherData.Pressure*10) / 10
	humidity := math.Round(weatherData.Humidity*10) / 10

	log.Printf("Rounded values: Temp=%.1f°C, Pressure=%.1fhPa, Humidity=%.1f%%",
		temperature, pressure, humidity)

	// Convert Unix timestamp to datetime
	measuredAt := time.Unix(weatherData.Timestamp, 0)

	// Connect to database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Insert data into database
	query := `INSERT INTO weather (measured_at, temperature, pressure, humidity)
              VALUES (?, ?, ?, ?)`

	result, err := db.Exec(query, measuredAt, temperature, pressure, humidity)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	lastID, _ := result.LastInsertId()
	log.Printf("Data inserted successfully with ID: %d", lastID)

	return nil
}
