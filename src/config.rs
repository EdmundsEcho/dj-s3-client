use color_eyre::Result;
use dotenv::dotenv;
use eyre::WrapErr;

use aws_sdk_s3::Config as SdkConfig;
use serde::Deserialize;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub etl_obj_filename: String,
    pub test_project: String,
    pub app_name: String,
    pub sdk_config: SdkConfig,
}

fn init_tracer() {
    #[cfg(debug_assertions)]
    let tracer = tracing_subscriber::fmt();
    #[cfg(not(debug_assertions))]
    let tracer = tracing_subscriber::fmt().json();

    tracer.with_env_filter(EnvFilter::from_default_env()).init();
}

impl Config {
    pub fn from_env() -> Result<Config> {
        dotenv().ok();

        init_tracer();

        info!("Loading configuration");

        let mut c = config::Config::new();

        let sdk_config = aws_config::load_from_env().await;

        c.merge(config::Environment::default())?;

        let config = c
            .try_into()
            .context("loading configuration from environment");

        debug!("Config: {:?}", config);
        config
    }
}
