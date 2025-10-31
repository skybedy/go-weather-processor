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

	// Schedule main data processing job (every 5 minutes)
	_, err := c.AddFunc(config.CronSchedule, func() {
		log.Println("Starting scheduled weather data processing...")
		if err := processWeatherData(); err != nil {
			log.Printf("Error processing weather data: %v", err)
		} else {
			log.Println("Weather data processed successfully")
		}
	})
	if err != nil {
		log.Fatalf("Failed to schedule main processing job: %v", err)
	}

	// Schedule daily statistics calculation (every day at 00:05)
	_, err = c.AddFunc("5 0 * * *", func() {
		log.Println("Starting daily statistics calculation...")
		// Connect to database
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("Error connecting to database for daily stats: %v", err)
			return
		}
		defer db.Close()

		if err := updateDailyStatistics(db); err != nil {
			log.Printf("Error calculating daily statistics: %v", err)
		} else {
			log.Println("Daily statistics calculated successfully")
		}
	})
	if err != nil {
		log.Fatalf("Failed to schedule daily statistics job: %v", err)
	}

	// Schedule weekly statistics calculation (every Monday at 00:10)
	_, err = c.AddFunc("10 0 * * 1", func() {
		log.Println("Starting weekly statistics calculation...")
		// Connect to database
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("Error connecting to database for weekly stats: %v", err)
			return
		}
		defer db.Close()

		if err := updateWeeklyStatistics(db); err != nil {
			log.Printf("Error calculating weekly statistics: %v", err)
		} else {
			log.Println("Weekly statistics calculated successfully")
		}
	})
	if err != nil {
		log.Fatalf("Failed to schedule weekly statistics job: %v", err)
	}

	// Schedule monthly statistics calculation (1st day of month at 00:15)
	_, err = c.AddFunc("15 0 1 * *", func() {
		log.Println("Starting monthly statistics calculation...")
		// Connect to database
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("Error connecting to database for monthly stats: %v", err)
			return
		}
		defer db.Close()

		if err := updateMonthlyStatistics(db); err != nil {
			log.Printf("Error calculating monthly statistics: %v", err)
		} else {
			log.Println("Monthly statistics calculated successfully")
		}
	})
	if err != nil {
		log.Fatalf("Failed to schedule monthly statistics job: %v", err)
	}

	// Start the cron scheduler
	c.Start()

	log.Println("Cron scheduler started. Waiting for scheduled tasks...")
	log.Printf("Main processing job will run with schedule: %s", config.CronSchedule)
	log.Println("Daily statistics: every day at 00:05")
	log.Println("Weekly statistics: every Monday at 00:10")
	log.Println("Monthly statistics: 1st day of month at 00:15")

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

	// Calculate hourly averages for current hour after every insert
	log.Println("Calculating hourly averages for current hour...")
	if err := updateHourlyAverages(db, measuredAt); err != nil {
		log.Printf("Warning: Failed to update hourly averages: %v", err)
		// Don't return error - raw data is already saved
	}

	return nil
}

// updateHourlyAverages calculates hourly averages for the current hour
func updateHourlyAverages(db *sql.DB, currentTime time.Time) error {
	// Calculate for the current hour
	date := currentTime.Format("2006-01-02")
	hour := currentTime.Hour()

	log.Printf("Calculating hourly averages for %s hour %d", date, hour)

	// Calculate averages for this hour
	var avgTemp, avgPressure, avgHumidity float64
	var samplesCount int

	query := `
		SELECT
			AVG(temperature) as avg_temp,
			AVG(pressure) as avg_pressure,
			AVG(humidity) as avg_humidity,
			COUNT(*) as samples
		FROM weather
		WHERE DATE(measured_at) = ? AND HOUR(measured_at) = ?
	`

	err := db.QueryRow(query, date, hour).Scan(&avgTemp, &avgPressure, &avgHumidity, &samplesCount)
	if err != nil {
		return fmt.Errorf("failed to calculate averages: %w", err)
	}

	if samplesCount == 0 {
		log.Printf("No samples found for %s hour %d, skipping", date, hour)
		return nil
	}

	// Round averages to 1 decimal place
	avgTemp = math.Round(avgTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10

	log.Printf("Hourly averages: Temp=%.1f°C, Pressure=%.1fhPa, Humidity=%.1f%% (samples: %d)",
		avgTemp, avgPressure, avgHumidity, samplesCount)

	// UPSERT into weather_hourly
	upsertQuery := `
		INSERT INTO weather_hourly (date, hour, avg_temperature, avg_pressure, avg_humidity, samples_count)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			avg_temperature = VALUES(avg_temperature),
			avg_pressure = VALUES(avg_pressure),
			avg_humidity = VALUES(avg_humidity),
			samples_count = VALUES(samples_count),
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = db.Exec(upsertQuery, date, hour, avgTemp, avgPressure, avgHumidity, samplesCount)
	if err != nil {
		return fmt.Errorf("failed to upsert hourly averages: %w", err)
	}

	log.Printf("Hourly averages updated successfully for %s hour %d", date, hour)
	return nil
}

// updateDailyStatistics calculates daily statistics for yesterday
func updateDailyStatistics(db *sql.DB) error {
	// Calculate for yesterday
	yesterday := time.Now().AddDate(0, 0, -1)
	date := yesterday.Format("2006-01-02")

	log.Printf("Calculating daily statistics for %s", date)

	// Calculate statistics for the day
	var avgTemp, minTemp, maxTemp float64
	var avgPressure, minPressure, maxPressure float64
	var avgHumidity, minHumidity, maxHumidity float64
	var samplesCount int

	query := `
		SELECT
			AVG(temperature) as avg_temp,
			MIN(temperature) as min_temp,
			MAX(temperature) as max_temp,
			AVG(pressure) as avg_pressure,
			MIN(pressure) as min_pressure,
			MAX(pressure) as max_pressure,
			AVG(humidity) as avg_humidity,
			MIN(humidity) as min_humidity,
			MAX(humidity) as max_humidity,
			COUNT(*) as samples
		FROM weather
		WHERE DATE(measured_at) = ?
	`

	err := db.QueryRow(query, date).Scan(
		&avgTemp, &minTemp, &maxTemp,
		&avgPressure, &minPressure, &maxPressure,
		&avgHumidity, &minHumidity, &maxHumidity,
		&samplesCount,
	)
	if err != nil {
		return fmt.Errorf("failed to calculate daily statistics: %w", err)
	}

	if samplesCount == 0 {
		log.Printf("No samples found for %s, skipping", date)
		return nil
	}

	// Round values to 1 decimal place
	avgTemp = math.Round(avgTemp*10) / 10
	minTemp = math.Round(minTemp*10) / 10
	maxTemp = math.Round(maxTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	minPressure = math.Round(minPressure*10) / 10
	maxPressure = math.Round(maxPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10
	minHumidity = math.Round(minHumidity*10) / 10
	maxHumidity = math.Round(maxHumidity*10) / 10

	log.Printf("Daily statistics for %s: Temp avg=%.1f min=%.1f max=%.1f, "+
		"Pressure avg=%.1f min=%.1f max=%.1f, Humidity avg=%.1f min=%.1f max=%.1f (samples: %d)",
		date, avgTemp, minTemp, maxTemp, avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity, samplesCount)

	// UPSERT into weather_daily
	upsertQuery := `
		INSERT INTO weather_daily (
			date,
			avg_temperature, min_temperature, max_temperature,
			avg_pressure, min_pressure, max_pressure,
			avg_humidity, min_humidity, max_humidity,
			samples_count
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			avg_temperature = VALUES(avg_temperature),
			min_temperature = VALUES(min_temperature),
			max_temperature = VALUES(max_temperature),
			avg_pressure = VALUES(avg_pressure),
			min_pressure = VALUES(min_pressure),
			max_pressure = VALUES(max_pressure),
			avg_humidity = VALUES(avg_humidity),
			min_humidity = VALUES(min_humidity),
			max_humidity = VALUES(max_humidity),
			samples_count = VALUES(samples_count),
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = db.Exec(upsertQuery, date,
		avgTemp, minTemp, maxTemp,
		avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity,
		samplesCount)
	if err != nil {
		return fmt.Errorf("failed to upsert daily statistics: %w", err)
	}

	log.Printf("Daily statistics updated successfully for %s", date)
	return nil
}

// updateWeeklyStatistics calculates weekly statistics for last week (Monday-Sunday)
func updateWeeklyStatistics(db *sql.DB) error {
	// Calculate for last week
	now := time.Now()
	// Get last Monday (start of last week)
	lastMonday := now.AddDate(0, 0, -int(now.Weekday())-6)
	if now.Weekday() == time.Sunday {
		lastMonday = now.AddDate(0, 0, -13)
	}
	// Normalize to start of day
	lastMonday = time.Date(lastMonday.Year(), lastMonday.Month(), lastMonday.Day(), 0, 0, 0, 0, lastMonday.Location())

	// Last Sunday (end of last week)
	lastSunday := lastMonday.AddDate(0, 0, 6)

	year, week := lastMonday.ISOWeek()
	weekStart := lastMonday.Format("2006-01-02")
	weekEnd := lastSunday.Format("2006-01-02")

	log.Printf("Calculating weekly statistics for year %d week %d (%s to %s)", year, week, weekStart, weekEnd)

	// Calculate statistics for the week
	var avgTemp, minTemp, maxTemp float64
	var avgPressure, minPressure, maxPressure float64
	var avgHumidity, minHumidity, maxHumidity float64
	var samplesCount int

	query := `
		SELECT
			AVG(temperature) as avg_temp,
			MIN(temperature) as min_temp,
			MAX(temperature) as max_temp,
			AVG(pressure) as avg_pressure,
			MIN(pressure) as min_pressure,
			MAX(pressure) as max_pressure,
			AVG(humidity) as avg_humidity,
			MIN(humidity) as min_humidity,
			MAX(humidity) as max_humidity,
			COUNT(*) as samples
		FROM weather
		WHERE DATE(measured_at) >= ? AND DATE(measured_at) <= ?
	`

	err := db.QueryRow(query, weekStart, weekEnd).Scan(
		&avgTemp, &minTemp, &maxTemp,
		&avgPressure, &minPressure, &maxPressure,
		&avgHumidity, &minHumidity, &maxHumidity,
		&samplesCount,
	)
	if err != nil {
		return fmt.Errorf("failed to calculate weekly statistics: %w", err)
	}

	if samplesCount == 0 {
		log.Printf("No samples found for year %d week %d, skipping", year, week)
		return nil
	}

	// Round values to 1 decimal place
	avgTemp = math.Round(avgTemp*10) / 10
	minTemp = math.Round(minTemp*10) / 10
	maxTemp = math.Round(maxTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	minPressure = math.Round(minPressure*10) / 10
	maxPressure = math.Round(maxPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10
	minHumidity = math.Round(minHumidity*10) / 10
	maxHumidity = math.Round(maxHumidity*10) / 10

	log.Printf("Weekly statistics for year %d week %d: Temp avg=%.1f min=%.1f max=%.1f, "+
		"Pressure avg=%.1f min=%.1f max=%.1f, Humidity avg=%.1f min=%.1f max=%.1f (samples: %d)",
		year, week, avgTemp, minTemp, maxTemp, avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity, samplesCount)

	// UPSERT into weather_weekly
	upsertQuery := `
		INSERT INTO weather_weekly (
			year, week, week_start, week_end,
			avg_temperature, min_temperature, max_temperature,
			avg_pressure, min_pressure, max_pressure,
			avg_humidity, min_humidity, max_humidity,
			samples_count
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			week_start = VALUES(week_start),
			week_end = VALUES(week_end),
			avg_temperature = VALUES(avg_temperature),
			min_temperature = VALUES(min_temperature),
			max_temperature = VALUES(max_temperature),
			avg_pressure = VALUES(avg_pressure),
			min_pressure = VALUES(min_pressure),
			max_pressure = VALUES(max_pressure),
			avg_humidity = VALUES(avg_humidity),
			min_humidity = VALUES(min_humidity),
			max_humidity = VALUES(max_humidity),
			samples_count = VALUES(samples_count),
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = db.Exec(upsertQuery, year, week, weekStart, weekEnd,
		avgTemp, minTemp, maxTemp,
		avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity,
		samplesCount)
	if err != nil {
		return fmt.Errorf("failed to upsert weekly statistics: %w", err)
	}

	log.Printf("Weekly statistics updated successfully for year %d week %d", year, week)
	return nil
}

// updateMonthlyStatistics calculates monthly statistics for last month
func updateMonthlyStatistics(db *sql.DB) error {
	// Calculate for last month
	now := time.Now()
	lastMonth := now.AddDate(0, -1, 0)
	year := lastMonth.Year()
	month := int(lastMonth.Month())

	log.Printf("Calculating monthly statistics for year %d month %d", year, month)

	// First and last day of last month
	firstDay := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, now.Location())
	lastDay := firstDay.AddDate(0, 1, -1)

	// Calculate statistics for the month
	var avgTemp, minTemp, maxTemp float64
	var avgPressure, minPressure, maxPressure float64
	var avgHumidity, minHumidity, maxHumidity float64
	var samplesCount int

	query := `
		SELECT
			AVG(temperature) as avg_temp,
			MIN(temperature) as min_temp,
			MAX(temperature) as max_temp,
			AVG(pressure) as avg_pressure,
			MIN(pressure) as min_pressure,
			MAX(pressure) as max_pressure,
			AVG(humidity) as avg_humidity,
			MIN(humidity) as min_humidity,
			MAX(humidity) as max_humidity,
			COUNT(*) as samples
		FROM weather
		WHERE DATE(measured_at) >= ? AND DATE(measured_at) <= ?
	`

	err := db.QueryRow(query, firstDay.Format("2006-01-02"), lastDay.Format("2006-01-02")).Scan(
		&avgTemp, &minTemp, &maxTemp,
		&avgPressure, &minPressure, &maxPressure,
		&avgHumidity, &minHumidity, &maxHumidity,
		&samplesCount,
	)
	if err != nil {
		return fmt.Errorf("failed to calculate monthly statistics: %w", err)
	}

	if samplesCount == 0 {
		log.Printf("No samples found for year %d month %d, skipping", year, month)
		return nil
	}

	// Round values to 1 decimal place
	avgTemp = math.Round(avgTemp*10) / 10
	minTemp = math.Round(minTemp*10) / 10
	maxTemp = math.Round(maxTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	minPressure = math.Round(minPressure*10) / 10
	maxPressure = math.Round(maxPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10
	minHumidity = math.Round(minHumidity*10) / 10
	maxHumidity = math.Round(maxHumidity*10) / 10

	log.Printf("Monthly statistics for year %d month %d: Temp avg=%.1f min=%.1f max=%.1f, "+
		"Pressure avg=%.1f min=%.1f max=%.1f, Humidity avg=%.1f min=%.1f max=%.1f (samples: %d)",
		year, month, avgTemp, minTemp, maxTemp, avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity, samplesCount)

	// UPSERT into weather_monthly
	upsertQuery := `
		INSERT INTO weather_monthly (
			year, month,
			avg_temperature, min_temperature, max_temperature,
			avg_pressure, min_pressure, max_pressure,
			avg_humidity, min_humidity, max_humidity,
			samples_count
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			avg_temperature = VALUES(avg_temperature),
			min_temperature = VALUES(min_temperature),
			max_temperature = VALUES(max_temperature),
			avg_pressure = VALUES(avg_pressure),
			min_pressure = VALUES(min_pressure),
			max_pressure = VALUES(max_pressure),
			avg_humidity = VALUES(avg_humidity),
			min_humidity = VALUES(min_humidity),
			max_humidity = VALUES(max_humidity),
			samples_count = VALUES(samples_count),
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = db.Exec(upsertQuery, year, month,
		avgTemp, minTemp, maxTemp,
		avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity,
		samplesCount)
	if err != nil {
		return fmt.Errorf("failed to upsert monthly statistics: %w", err)
	}

	log.Printf("Monthly statistics updated successfully for year %d month %d", year, month)
	return nil
}
