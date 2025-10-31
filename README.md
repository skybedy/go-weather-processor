# Weather Data Processor

Go aplikace pro automatické zpracování a ukládání počasových dat do MySQL databáze.

## Funkce

- Automatické čtení dat z JSON souboru
- Parsování teplotních dat (teplota, tlak, vlhkost)
- Ukládání dat do MySQL databáze
- Konfigurovatelný cron schedule
- Podpora environment variables pro různá prostředí

## Požadavky

- Go 1.22.2 nebo novější
- MySQL databáze s tabulkou `weather`
- Přístup k JSON souboru s počasovými daty

## Instalace a Deploy na produkci

### 1. Klonování repozitáře

```bash
cd /var/www/go-projects
git clone <your-github-repo-url> go-weather-processor
cd go-weather-processor
```

### 2. Instalace závislostí

```bash
go mod download
```

### 3. Build aplikace

```bash
go build -o go-weather-processor main.go
```

### 4. Konfigurace systemd service

```bash
# Zkopíruj service soubor
sudo cp weather-processor.service /etc/systemd/system/

# Přenačti systemd
sudo systemctl daemon-reload

# Povolit automatický start při startu systému
sudo systemctl enable weather-processor

# Spustit service
sudo systemctl start weather-processor

# Zkontrolovat status
sudo systemctl status weather-processor
```

### 5. Sledování logů

```bash
# Zobrazit aktuální logy
sudo journalctl -u weather-processor -f

# Zobrazit posledních 100 řádků
sudo journalctl -u weather-processor -n 100
```

### 6. Správa service

```bash
# Restart service
sudo systemctl restart weather-processor

# Zastavit service
sudo systemctl stop weather-processor

# Zobrazit status
sudo systemctl status weather-processor
```

## Konfigurace

Aplikace používá environment variables s různými nastaveními pro lokální vývoj a produkci.

### Lokální vývoj (.env soubor)

Pro lokální vývoj vytvoř `.env` soubor podle `.env.example`:

```bash
cp .env.example .env
# Uprav hodnoty v .env pro tvé lokální prostředí
```

**DŮLEŽITÉ:** `.env` soubor obsahuje citlivé údaje a **NESMÍ** být commitnut do gitu (je v `.gitignore`).

### Produkce (systemd environment variables)

V produkci se environment variables nastavují v souboru `weather-processor.service` a **NESMÍ** obsahovat `.env` soubor.

### Environment Variables

| Variable | Popis | Povinné | Výchozí hodnota |
|----------|-------|---------|-----------------|
| `DB_USER` | Uživatelské jméno databáze | **ANO** | - |
| `DB_PASSWORD` | Heslo databáze | **ANO** | - |
| `JSON_FILE_PATH` | Cesta k JSON souboru | Ne | `/var/www/laravel-tene.life/public/files/weather.json` |
| `DB_HOST` | Host databáze | Ne | `localhost` |
| `DB_PORT` | Port databáze | Ne | `3306` |
| `DB_NAME` | Jméno databáze | Ne | `tene_life` |
| `CRON_SCHEDULE` | Cron výraz pro scheduling | Ne | `1-56/5 * * * *` (každých 5 minut na XX:01, XX:06, XX:11...) |

### Cron Schedule příklady

- `1-56/5 * * * *` - Každých 5 minut na minutách 1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56 (výchozí)
- `0 * * * *` - Každou hodinu v 0 minut
- `*/2 * * * *` - Každé 2 minuty (pro testování)
- `0 */6 * * *` - Každých 6 hodin
- `0 0 * * *` - Každý den o půlnoci

## Lokální vývoj

### Nastavení lokálního prostředí

1. Vytvoř `.env` soubor:
```bash
cp .env.example .env
```

2. Uprav `.env` soubor s tvými lokálními databázovými údaji:
```env
DB_USER=root
DB_PASSWORD=root
CRON_SCHEDULE=*/2 * * * *  # Pro testování každé 2 minuty
```

3. Spusť aplikaci:
```bash
go run main.go
```

Aplikace automaticky načte `.env` soubor při startu.

## Struktura databáze

Tabulka `weather` musí mít následující strukturu:

```sql
CREATE TABLE weather (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    measured_at DATETIME NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    pressure DECIMAL(7,2) NOT NULL,
    humidity DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_measured_at (measured_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

## Troubleshooting

### Service se nespouští

```bash
# Zkontroluj logy
sudo journalctl -u weather-processor -n 50

# Zkontroluj syntax service souboru
sudo systemd-analyze verify /etc/systemd/system/weather-processor.service
```

### Chyby připojení k databázi

- Zkontroluj správnost přihlašovacích údajů v `/etc/systemd/system/weather-processor.service`
- Ověř, že MySQL běží: `sudo systemctl status mysql`
- Ověř, že uživatel má oprávnění k databázi

### JSON soubor nenalezen

- Zkontroluj cestu k souboru v konfiguraci
- Ověř oprávnění k souboru: `ls -la /var/www/laravel-tene.life/public/files/weather.json`

## Licence

Proprietární
