use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const DEFAULT_ID: &str = "Undefined";
pub const DEFAULT_PORT:u32 = 15000;
pub const DEFAULT_DB:&str = "127.0.0.1:9042";

pub const ARGUMENTS_FILE_PATH:&str =  concat!("/etc/", env!("CARGO_PKG_NAME"), "/args.toml");

pub const CONFIG_FILE_PATH: &str = concat!("/var/lib/", env!("CARGO_PKG_NAME"), "/config.json");


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Arguments {
    pub log_level: String,
    pub instance_id: String,
    pub database:String,
    pub pool_size: usize,
    pub receivers: usize,
    pub port:u32,
    pub debug:bool,
    pub config_path:PathBuf,
    pub output_path:Option<PathBuf>,
    pub start:bool,
    pub concurrent:bool,
    pub buffer_size: usize,
    pub receive_hwm: i32,
    pub disable_handshake:bool,
    pub join_channels:bool,
    pub blocking_config:bool,
    pub pause:bool,
}

impl Default for Arguments {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            instance_id: DEFAULT_ID.to_string(),
            database: DEFAULT_DB.to_string(),
            pool_size: 100,
            receivers: 1,
            port:DEFAULT_PORT,
            debug: false,
            config_path: PathBuf::from(CONFIG_FILE_PATH),
            output_path: None,
            start: false,
            concurrent: false,
            buffer_size: 100,
            receive_hwm: 1000,
            disable_handshake: false,
            join_channels: false,
            blocking_config: true,
            pause: false,
        }
    }

}

impl Arguments {
    pub fn read(file:&PathBuf) -> Result<Arguments, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(file)?;
        let config = toml::from_str(&text)?;
        Ok(config)
    }


    pub fn parse() -> Self {
        let cli = Cli::parse();

        let args_file = match &cli.args_file{
            None => {&PathBuf::from(ARGUMENTS_FILE_PATH)}
            Some(file) => {file}
        };

        let mut arguments = match Arguments::read(args_file) {
            Ok(arguments) => {
                log::info!("Loaded arguments from {}", ARGUMENTS_FILE_PATH);
                arguments
            }
            Err(e) => {
               log::info!("Error loading arguments from {}: {}", ARGUMENTS_FILE_PATH, e);
                Arguments::default()
            }
        };

        cli.apply(&mut arguments);
        arguments
    }
}

#[derive(Parser, Debug)]
#[command(name = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Cli {
    #[arg(short = 'l', long ,  help = "Log level")]
    pub log_level: Option<String>,

    #[arg(short = 'i', long, help = "Application instance ID")]
    pub instance_id: Option<String>,

    #[arg(short = 'e', long, help = "Scylla database URL")]
    pub database: Option<String>,

    #[arg(short = 'p', long,  help = "Port of the API")]
    pub port: Option<u32>,

    #[arg(short = 'd', long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", help = "Debug flag")]
    pub debug: Option<bool>,

    #[arg(short = 's', long,  help = "Maximum endpoints per context")]
    pub pool_size: Option<usize>,

    #[arg(short = 'r', long,  help = "Number of receivers per context")]
    pub receivers: Option<usize>,

    #[arg(short = 'c', long , help = "Channel-list configuration/persistence file name")]
    pub config_path: Option<PathBuf>,

    #[arg(short = 'o', long, help = "Data output path")]
    pub output_path: Option<PathBuf>,

    #[arg(short = 'a', long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", help = "Auto-start connections")]
    pub start: Option<bool>,

    #[arg(short = 't', long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", help = "Does not order sequentially messages from each endpoint")]
    pub concurrent: Option<bool>,

    #[arg(short = 'b', long, help = "Endpoint buffer size, if not concurrent")]
    pub buffer_size: Option<usize>,

    #[arg(short = 'w', long, help = "Receive High Water Mark")]
    pub receive_hwm: Option<i32>,

    #[arg(short = 'y', long,  action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", help = "Disable handshake check")]
    pub disable_handshake: Option<bool>,

    #[arg(short = 'j', long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", help = "Synchronize ingestion of all channels in a message before processing next")]
    pub join_channels: Option<bool>,

    #[arg(short = 'k', long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", help = "Configuration commands are blocking")]
    pub blocking_config: Option<bool>,

    #[arg(long , action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", help = "Start the application in paused state - no database access")]
    pub pause: Option<bool>,

    // Only en command-line, not present on the file
    #[arg(long, help = "Path to the application arguments TOML file")]
    pub args_file: Option<PathBuf>,
}

impl Cli {
    pub fn apply(self, arguments: &mut Arguments) {
        if let Some(value) = self.log_level {
            arguments.log_level = value;
        }

        if let Some(value) = self.instance_id {
            arguments.instance_id = value;
        }

        if let Some(value) = self.database {
            arguments.database = value;
        }

        if let Some(value) = self.port {
            arguments.port = value;
        }

        if let Some(value) = self.debug {
            arguments.debug = value;
        }

        if let Some(value) = self.pool_size {
            arguments.pool_size = value;
        }

        if let Some(value) = self.receivers {
            arguments.receivers = value;
        }

        if let Some(value) = self.config_path {
            arguments.config_path = value;
        }

        if let Some(value) = self.output_path {
            arguments.output_path = Some(value);
        }

        if let Some(value) = self.start {
            arguments.start = value;
        }

        if let Some(value) = self.concurrent {
            arguments.concurrent = value;
        }

        if let Some(value) = self.buffer_size {
            arguments.buffer_size = value;
        }

        if let Some(value) = self.receive_hwm {
            arguments.receive_hwm = value;
        }

        if let Some(value) = self.disable_handshake {
            arguments.disable_handshake = value;
        }

        if let Some(value) = self.join_channels {
            arguments.join_channels = value;
        }

        if let Some(value) = self.blocking_config {
            arguments.blocking_config = value;
        }
        if let Some(value) = self.pause {
            arguments.pause = value;
        }
    }
}
