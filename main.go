package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/smtp"
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
	AlertEmail   string
}

// Global state for alerts to prevent spamming
var lastAlertSent time.Time
var isDown bool

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
		CronSchedule: getEnv("CRON_SCHEDULE", "*/5 * * * *"),
		AlertEmail:   getEnv("ALERT_EMAIL", "skybedy@gmail.com"),
	}
}

var config Config

func main() {
	log.Println("Weather data processor started with alert system")

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables from system")
	} else {
		log.Println("Loaded configuration from .env file")
	}

	config = loadConfig()

	if config.DBUser == "" {
		log.Fatal("DB_USER environment variable is required")
	}
	if config.DBPassword == "" {
		log.Fatal("DB_PASSWORD environment variable is required")
	}

	if len(os.Args) > 1 {
		if err := runCommand(os.Args[1]); err != nil {
			log.Fatalf("Command %q failed: %v", os.Args[1], err)
		}
		return
	}

	log.Printf("Loaded configuration - DB: %s@%s:%s/%s, Schedule: %s, Alert: %s",
		config.DBUser, config.DBHost, config.DBPort, config.DBName, config.CronSchedule, config.AlertEmail)

	c := cron.New()

	// Main 5-minute processing
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

	// Daily stats
	_, err = c.AddFunc("5 0 * * *", func() {
		log.Println("Starting daily statistics calculation...")
		db := openDB()
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

	// Weekly stats
	_, err = c.AddFunc("10 0 * * 1", func() {
		log.Println("Starting weekly statistics calculation...")
		db := openDB()
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

	// Monthly stats
	_, err = c.AddFunc("15 0 1 * *", func() {
		log.Println("Starting monthly statistics calculation...")
		db := openDB()
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

	c.Start()

	log.Println("Cron scheduler started.")

	// Run once immediately
	if err := processWeatherData(); err != nil {
		log.Printf("Error in initial processing: %v", err)
	}

	select {}
}

func openDB() *sql.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("DB connect error: %v", err)
	}
	return db
}

func runCommand(command string) error {
	switch command {
	case "backfill-missing-sea":
		db := openDB()
		defer db.Close()

		if err := backfillMissingSeaTemperatureAverages(db); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
		log.Println("Backfill of missing sea temperature averages finished successfully")
		return nil
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func sendEmail(subject, body string) error {
	from := "weather-processor@mail.timechip.cz"
	to := []string{config.AlertEmail}
	msg := []byte(fmt.Sprintf("To: %s\r\n"+
		"Subject: %s\r\n"+
		"\r\n"+
		"%s\r\n", config.AlertEmail, subject, body))

	// Connect to the remote SMTP server.
	c, err := smtp.Dial("127.0.0.1:25")
	if err != nil {
		return err
	}
	defer c.Close()

	// If the server supports STARTTLS, we should use it but skip verification
	if ok, _ := c.Extension("STARTTLS"); ok {
		config := &tls.Config{
			ServerName: "mail.timechip.cz",
			InsecureSkipVerify: true,
		}
		if err = c.StartTLS(config); err != nil {
			return fmt.Errorf("STARTTLS failed: %w", err)
		}
	}

	if err := c.Mail(from); err != nil {
		return err
	}
	for _, addr := range to {
		if err := c.Rcpt(addr); err != nil {
			return err
		}
	}

	w, err := c.Data()
	if err != nil {
		return err
	}
	_, err = w.Write(msg)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	log.Printf("Alert email sent to %s: %s", config.AlertEmail, subject)
	return nil
}

func processWeatherData() error {
	data, err := os.ReadFile(config.JSONFilePath)
	if err != nil {
		handleDownState("File Read Error", fmt.Sprintf("Could not read weather.json: %v", err))
		return fmt.Errorf("failed to read JSON file: %w", err)
	}

	var weatherData WeatherData
	if err := json.Unmarshal(data, &weatherData); err != nil {
		handleDownState("JSON Parse Error", fmt.Sprintf("Could not parse weather.json: %v", err))
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Check data age
	measuredAt := time.Unix(weatherData.Timestamp, 0).UTC()
	age := time.Since(measuredAt)
	if age > 15*time.Minute {
		handleDownState("Weather Data Stale", fmt.Sprintf("Last data received at %s (age: %v)", measuredAt.Format(time.RFC1123), age))
		return fmt.Errorf("weather data is too old: %s", age)
	}

	// If we were down and now data is fresh, notify recovery
	if isDown {
		isDown = false
		sendEmail("Weather System RECOVERED", fmt.Sprintf("Fresh data received at %s. System is back online.", time.Now().Format(time.RFC1123)))
	}

	temperature := math.Round(weatherData.Temperature*10) / 10
	pressure := math.Round(weatherData.Pressure*10) / 10
	humidity := math.Round(weatherData.Humidity*10) / 10

	db := openDB()
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	query := `INSERT INTO weather (measured_at, temperature, pressure, humidity)
              VALUES (?, ?, ?, ?)`

	result, err := db.Exec(query, measuredAt, temperature, pressure, humidity)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	lastID, _ := result.LastInsertId()
	log.Printf("Data inserted successfully with ID: %d", lastID)

	log.Println("Calculating hourly averages...")
	if err := updateHourlyAverages(db, measuredAt); err != nil {
		log.Printf("Warning: Failed to update hourly averages: %v", err)
	}

	return nil
}

func handleDownState(reason, details string) {
	if !isDown {
		isDown = true
		subject := fmt.Sprintf("ALERT: Weather System Down - %s", reason)
		body := fmt.Sprintf("The weather data processor has detected a problem:\n\nReason: %s\nDetails: %s\nTime: %s", 
			reason, details, time.Now().Format(time.RFC1123))
		
		if err := sendEmail(subject, body); err != nil {
			log.Printf("Failed to send alert email: %v", err)
		}
		lastAlertSent = time.Now()
	} else {
		log.Printf("System still down (%s), alert already sent.", reason)
	}
}

// ------------------------- HOURLY ------------------------------
func updateHourlyAverages(db *sql.DB, currentTime time.Time) error {
	date := currentTime.Format("2006-01-02")
	hour := currentTime.Hour()

	var avgTemp, avgPressure, avgHumidity float64
	var samplesCount int

	query := `
		SELECT
			AVG(temperature) AS avg_temp,
			AVG(pressure) AS avg_pressure,
			AVG(humidity) AS avg_humidity,
			COUNT(*) AS samples
		FROM weather
		WHERE DATE(measured_at) = ? AND HOUR(measured_at) = ?
		HAVING samples > 0
	`

	err := db.QueryRow(query, date, hour).Scan(&avgTemp, &avgPressure, &avgHumidity, &samplesCount)
	if err == sql.ErrNoRows {
		log.Printf("No samples found for %s hour %d, skipping", date, hour)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to calculate averages: %w", err)
	}

	avgTemp = math.Round(avgTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10

	upsert := `
		INSERT INTO weather_hourly (date, hour, avg_temperature, avg_pressure, avg_humidity, samples_count)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			avg_temperature = VALUES(avg_temperature),
			avg_pressure = VALUES(avg_pressure),
			avg_humidity = VALUES(avg_humidity),
			samples_count = VALUES(samples_count),
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = db.Exec(upsert, date, hour, avgTemp, avgPressure, avgHumidity, samplesCount)
	if err != nil {
		return fmt.Errorf("failed to upsert hourly averages: %w", err)
	}

	return nil
}

// ------------------------- DAILY ------------------------------
func updateDailyStatistics(db *sql.DB) error {

	yesterday := time.Now().AddDate(0, 0, -1)
	date := yesterday.Format("2006-01-02")

	var avgTemp, minTemp, maxTemp float64
	var avgPressure, minPressure, maxPressure float64
	var avgHumidity, minHumidity, maxHumidity float64
	var samplesCount int

	query := `
		SELECT
			AVG(temperature), MIN(temperature), MAX(temperature),
			AVG(pressure), MIN(pressure), MAX(pressure),
			AVG(humidity), MIN(humidity), MAX(humidity),
			COUNT(*) AS samples
		FROM weather
		WHERE DATE(measured_at) = ?
		HAVING samples > 0
	`

	err := db.QueryRow(query, date).Scan(
		&avgTemp, &minTemp, &maxTemp,
		&avgPressure, &minPressure, &maxPressure,
		&avgHumidity, &minHumidity, &maxHumidity,
		&samplesCount)
	if err == sql.ErrNoRows {
		log.Printf("No samples found for %s, skipping", date)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to calculate daily statistics: %w", err)
	}

	avgTemp = math.Round(avgTemp*10) / 10
	minTemp = math.Round(minTemp*10) / 10
	maxTemp = math.Round(maxTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	minPressure = math.Round(minPressure*10) / 10
	maxPressure = math.Round(maxPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10
	minHumidity = math.Round(minHumidity*10) / 10
	maxHumidity = math.Round(maxHumidity*10) / 10

	upsert := `
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
		-- sea_temperature is NOT updated here, only manually via API
	`

	_, err = db.Exec(upsert, date,
		avgTemp, minTemp, maxTemp,
		avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity,
		samplesCount)

	return err
}

// ------------------------- WEEKLY ------------------------------
func updateWeeklyStatistics(db *sql.DB) error {

	now := time.Now()
	lastMonday := now.AddDate(0, 0, -int(now.Weekday())-6)
	if now.Weekday() == time.Sunday {
		lastMonday = now.AddDate(0, 0, -13)
	}
	lastMonday = time.Date(lastMonday.Year(), lastMonday.Month(), lastMonday.Day(), 0, 0, 0, 0, lastMonday.Location())

	lastSunday := lastMonday.AddDate(0, 0, 6)

	year, week := lastMonday.ISOWeek()
	weekStart := lastMonday.Format("2006-01-02")
	weekEnd := lastSunday.Format("2006-01-02")

	var avgTemp, minTemp, maxTemp float64
	var avgPressure, minPressure, maxPressure float64
	var avgHumidity, minHumidity, maxHumidity float64
	var samplesCount int
	var avgSeaTemperature sql.NullFloat64

	query := `
		SELECT
			AVG(temperature), MIN(temperature), MAX(temperature),
			AVG(pressure), MIN(pressure), MAX(pressure),
			AVG(humidity), MIN(humidity), MAX(humidity),
			COUNT(*) AS samples
		FROM weather
		WHERE DATE(measured_at) >= ? AND DATE(measured_at) <= ?
		HAVING samples > 0
	`

	err := db.QueryRow(query, weekStart, weekEnd).Scan(
		&avgTemp, &minTemp, &maxTemp,
		&avgPressure, &minPressure, &maxPressure,
		&avgHumidity, &minHumidity, &maxHumidity,
		&samplesCount)
	if err == sql.ErrNoRows {
		log.Printf("No samples found for week %d/%d", week, year)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to calculate weekly statistics: %w", err)
	}

	avgTemp = math.Round(avgTemp*10) / 10
	minTemp = math.Round(minTemp*10) / 10
	maxTemp = math.Round(maxTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	minPressure = math.Round(minPressure*10) / 10
	maxPressure = math.Round(maxPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10
	minHumidity = math.Round(minHumidity*10) / 10
	maxHumidity = math.Round(maxHumidity*10) / 10
	if err := db.QueryRow(`
		SELECT ROUND(AVG(day_avg), 1)
		FROM (
			SELECT DATE(measured_at) AS measured_day, AVG(temperature) AS day_avg
			FROM water_temperatures
			WHERE DATE(measured_at) >= ? AND DATE(measured_at) <= ?
			GROUP BY DATE(measured_at)
		) d
	`, weekStart, weekEnd).Scan(&avgSeaTemperature); err != nil {
		return fmt.Errorf("failed to calculate weekly sea temperature average: %w", err)
	}

	upsert := `
		INSERT INTO weather_weekly (
			year, week, week_start, week_end,
			avg_sea_temperature,
			avg_temperature, min_temperature, max_temperature,
			avg_pressure, min_pressure, max_pressure,
			avg_humidity, min_humidity, max_humidity,
			samples_count
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			week_start = VALUES(week_start),
			week_end = VALUES(week_end),
			avg_sea_temperature = VALUES(avg_sea_temperature),
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

	_, err = db.Exec(upsert, year, week, weekStart, weekEnd, avgSeaTemperature,
		avgTemp, minTemp, maxTemp,
		avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity,
		samplesCount)

	return err
}

// ------------------------- MONTHLY ------------------------------
func updateMonthlyStatistics(db *sql.DB) error {

	now := time.Now()
	lastMonth := now.AddDate(0, -1, 0)

	year := lastMonth.Year()
	month := int(lastMonth.Month())

	firstDay := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, now.Location())
	lastDay := firstDay.AddDate(0, 1, -1)

	var avgTemp, minTemp, maxTemp float64
	var avgPressure, minPressure, maxPressure float64
	var avgHumidity, minHumidity, maxHumidity float64
	var samplesCount int
	var avgSeaTemperature sql.NullFloat64

	query := `
		SELECT
			AVG(temperature), MIN(temperature), MAX(temperature),
			AVG(pressure), MIN(pressure), MAX(pressure),
			AVG(humidity), MIN(humidity), MAX(humidity),
			COUNT(*) AS samples
		FROM weather
		WHERE DATE(measured_at) >= ? AND DATE(measured_at) <= ?
		HAVING samples > 0
	`

	err := db.QueryRow(query,
		firstDay.Format("2006-01-02"),
		lastDay.Format("2006-01-02")).Scan(
		&avgTemp, &minTemp, &maxTemp,
		&avgPressure, &minPressure, &maxPressure,
		&avgHumidity, &minHumidity, &maxHumidity,
		&samplesCount)

	if err == sql.ErrNoRows {
		log.Printf("No samples found for %d-%02d", year, month)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to calculate monthly statistics: %w", err)
	}

	avgTemp = math.Round(avgTemp*10) / 10
	minTemp = math.Round(minTemp*10) / 10
	maxTemp = math.Round(maxTemp*10) / 10
	avgPressure = math.Round(avgPressure*10) / 10
	minPressure = math.Round(minPressure*10) / 10
	maxPressure = math.Round(maxPressure*10) / 10
	avgHumidity = math.Round(avgHumidity*10) / 10
	minHumidity = math.Round(minHumidity*10) / 10
	maxHumidity = math.Round(maxHumidity*10) / 10
	if err := db.QueryRow(`
		SELECT ROUND(AVG(day_avg), 1)
		FROM (
			SELECT DATE(measured_at) AS measured_day, AVG(temperature) AS day_avg
			FROM water_temperatures
			WHERE DATE(measured_at) >= ? AND DATE(measured_at) <= ?
			GROUP BY DATE(measured_at)
		) d
	`, firstDay.Format("2006-01-02"), lastDay.Format("2006-01-02")).Scan(&avgSeaTemperature); err != nil {
		return fmt.Errorf("failed to calculate monthly sea temperature average: %w", err)
	}

	upsert := `
		INSERT INTO weather_monthly (
			year, month,
			avg_sea_temperature,
			avg_temperature, min_temperature, max_temperature,
			avg_pressure, min_pressure, max_pressure,
			avg_humidity, min_humidity, max_humidity,
			samples_count
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			avg_sea_temperature = VALUES(avg_sea_temperature),
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

	_, err = db.Exec(upsert, year, month, avgSeaTemperature,
		avgTemp, minTemp, maxTemp,
		avgPressure, minPressure, maxPressure,
		avgHumidity, minHumidity, maxHumidity,
		samplesCount)

	return err
}

func backfillMissingSeaTemperatureAverages(db *sql.DB) error {
	weeklyBackfill := `
		UPDATE weather_weekly ww
		INNER JOIN (
			SELECT
				CAST(SUBSTRING(yw, 1, 4) AS UNSIGNED) AS year,
				CAST(SUBSTRING(yw, 5, 2) AS UNSIGNED) AS week,
				ROUND(AVG(day_avg), 1) AS avg_sea_temperature
			FROM (
				SELECT
					DATE(measured_at) AS measured_day,
					AVG(temperature) AS day_avg,
					LPAD(YEARWEEK(DATE(measured_at), 3), 6, '0') AS yw
				FROM water_temperatures
				GROUP BY DATE(measured_at)
			) daily
			GROUP BY yw
		) src
			ON src.year = ww.year
			AND src.week = ww.week
		SET
			ww.avg_sea_temperature = src.avg_sea_temperature,
			ww.updated_at = CURRENT_TIMESTAMP
		WHERE ww.avg_sea_temperature IS NULL
	`
	if _, err := db.Exec(weeklyBackfill); err != nil {
		return fmt.Errorf("failed weekly sea backfill: %w", err)
	}

	monthlyBackfill := `
		UPDATE weather_monthly wm
		INNER JOIN (
			SELECT
				YEAR(measured_day) AS year,
				MONTH(measured_day) AS month,
				ROUND(AVG(day_avg), 1) AS avg_sea_temperature
			FROM (
				SELECT
					DATE(measured_at) AS measured_day,
					AVG(temperature) AS day_avg
				FROM water_temperatures
				GROUP BY DATE(measured_at)
			) daily
			GROUP BY YEAR(measured_day), MONTH(measured_day)
		) src
			ON src.year = wm.year
			AND src.month = wm.month
		SET
			wm.avg_sea_temperature = src.avg_sea_temperature,
			wm.updated_at = CURRENT_TIMESTAMP
		WHERE wm.avg_sea_temperature IS NULL
	`
	if _, err := db.Exec(monthlyBackfill); err != nil {
		return fmt.Errorf("failed monthly sea backfill: %w", err)
	}

	return nil
}
