use clap::Args;

#[derive(Debug, Args, Clone)]
pub struct PostgresSqlArguments {
    /// The host of the PostgreSQL database.
    #[arg(long, env = "PGHOST", default_value = "localhost")]
    pub host: String,

    /// The port of the PostgreSQL database.
    #[arg(long, env = "PGPORT", default_value_t = 5432)]
    pub port: u16,

    /// The username for the PostgreSQL database.
    #[arg(long, env = "PGUSER", default_value = "postgres")]
    pub username: String,

    /// The password for the PostgreSQL database.
    #[arg(long, env = "PGPASSWORD")]
    pub password: Option<String>,

    /// The name of the PostgreSQL database.
    #[arg(long, env = "PGDATABASE", default_value = "postgres")]
    pub database: String,

    /// The maximum number of connections to the PostgreSQL database.
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value_t = 10)]
    pub max_connections: u32,
}

impl PostgresSqlArguments {
    pub fn get_connection_string(&self) -> String {
        let password_part = match &self.password {
            Some(p) => format!(":{}", p),
            None => "".to_string(),
        };
        format!(
            "postgres://{}{}@{}:{}/{}",
            self.username, password_part, self.host, self.port, self.database
        )
        .replace(":@", "@") // Handle empty password cleanly if needed by driver, though typically distinct
    }
}
