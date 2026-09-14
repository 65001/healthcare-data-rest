//! Environment-based configuration, matching the table in Architecture.md.
//! Loaded via `dotenvy` (see `main.rs`) plus `std::env` fallback defaults.

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub server_host: String,
    pub server_port: u16,

    pub geocoding_census_enabled: bool,
    pub geocoding_nominatim_enabled: bool,
    pub geocoding_google_maps_enabled: bool,
    pub geocoding_google_maps_api_key: Option<String>,

    pub probe_concurrency: usize,
    pub probe_rate_limit_per_sec: f64,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(default)
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    /// Reads configuration from the process environment. Call
    /// `dotenvy::dotenv().ok()` before this (in `main.rs`) so a `.env`
    /// file is picked up too; this function itself only reads
    /// `std::env`.
    pub fn from_env() -> Self {
        Self {
            database_url: env_or("DATABASE_URL", "sqlite:data.db"),
            server_host: env_or("SERVER_HOST", "0.0.0.0"),
            server_port: env_num("SERVER_PORT", 3000),

            geocoding_census_enabled: env_bool("GEOCODING_CENSUS_ENABLED", true),
            geocoding_nominatim_enabled: env_bool("GEOCODING_NOMINATIM_ENABLED", true),
            geocoding_google_maps_enabled: env_bool("GEOCODING_GOOGLE_MAPS_ENABLED", false),
            geocoding_google_maps_api_key: std::env::var("GEOCODING_GOOGLE_MAPS_API_KEY")
                .ok()
                .filter(|s| !s.is_empty()),

            probe_concurrency: env_num("PROBE_CONCURRENCY", 10),
            probe_rate_limit_per_sec: env_num("PROBE_RATE_LIMIT_PER_SEC", 5.0),
        }
    }
}
