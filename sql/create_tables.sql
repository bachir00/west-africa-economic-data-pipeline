\c west_africa_economic
-- Crée la table principale
CREATE TABLE IF NOT EXISTS west_africa_economic_data (
    id                      SERIAL PRIMARY KEY,
    country_code            VARCHAR(3) NOT NULL,
    country_name            VARCHAR(100) NOT NULL,
    year                    INTEGER NOT NULL,
    gdp_current_usd         NUMERIC(20, 2),
    population_total        BIGINT,
    inflation_rate          NUMERIC(10, 4),
    unemployment_rate       NUMERIC(10, 4),
    internet_users_percent  NUMERIC(10, 4),
    gdp_per_capita          NUMERIC(15, 2),
    digital_adoption_score  NUMERIC(8, 4),
    economic_health_score   NUMERIC(8, 2),
    data_quality_flag       VARCHAR(10),
    ingestion_timestamp     TIMESTAMP DEFAULT NOW(),
    UNIQUE(country_code, year)
);
-- Crée l'index
CREATE INDEX IF NOT EXISTS idx_country_year 
    ON west_africa_economic_data(country_code, year);
-- Vérifie que la table existe
\dt