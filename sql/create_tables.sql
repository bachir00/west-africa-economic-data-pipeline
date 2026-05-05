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

CREATE INDEX IF NOT EXISTS idx_country_year 
    ON west_africa_economic_data(country_code, year);

CREATE INDEX IF NOT EXISTS idx_ingestion_ts 
    ON west_africa_economic_data(ingestion_timestamp);

-- Analytical view
CREATE OR REPLACE VIEW v_country_latest_stats AS
SELECT 
    country_name,
    year,
    ROUND(gdp_current_usd / 1e9, 2)    AS gdp_billions_usd,
    population_total,
    ROUND(gdp_per_capita, 0)            AS gdp_per_capita,
    ROUND(inflation_rate, 2)            AS inflation_pct,
    ROUND(unemployment_rate, 2)         AS unemployment_pct,
    ROUND(internet_users_percent, 1)    AS internet_users_pct,
    ROUND(economic_health_score, 1)     AS health_score
FROM west_africa_economic_data
WHERE year = (
    SELECT MAX(year) FROM west_africa_economic_data
)
ORDER BY gdp_current_usd DESC NULLS LAST;