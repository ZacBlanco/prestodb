use std::{
    collections::{HashSet},
    fmt::Display,
    hash::Hash,
    str::FromStr,
    sync::{Mutex, OnceLock},
};

use config::{Config, ConfigError, FileFormat};
use lazy_static::lazy_static;

pub const CONFIG_ENV_PREFIX: &str = "PRESTOX";

pub fn config_registry() -> &'static Mutex<HashSet<String>> {
    static INSTANCE: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    INSTANCE.get_or_init(|| Mutex::new(HashSet::new()))
}

// downside here is that we can't iterate over the entire property list as everything is generated
// lazily. Need some kind of macro to generate an initializer function
lazy_static! {
    pub static ref PROPERTY_REGISTRY: Mutex<HashSet<PropertyInfo>> = Mutex::new(HashSet::new());
    pub static ref DISCOVERY_URI: ConfigProperty<String> = ConfigBuilder::new("discovery.uri")
        .description("URL to register as a worker in the presto cluster")
        .build();
    pub static ref PRESTO_VERSION: ConfigProperty<String> = ConfigBuilder::new("presto.version")
        .description("presto version")
        .build();
    pub static ref HTTP_SERVER_PORT: ConfigProperty<u16> =
        ConfigBuilder::new("http.server.http.port")
            .default(9090)
            .build();
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PropertyInfo {
    name: String,
    description: String,
    env_var: String,
}

impl<T: FromStr + Clone> From<&ConfigProperty<T>> for PropertyInfo {
    fn from(value: &ConfigProperty<T>) -> Self {
        PropertyInfo {
            name: value.name.to_string(),
            description: value.description.to_string(),
            env_var: value.env_var.to_string(),
        }
    }
}

#[derive(Default)]
pub struct ConfigBuilder<T>
where
    T: Default + FromStr + Clone,
{
    name: Option<String>,
    description: Option<String>,
    env_var: Option<String>,
    default: Option<T>,
}

pub struct ConfigProperty<T>
where
    T: FromStr + Clone,
{
    name: String,
    description: String,
    env_var: String,
    default: Option<T>,
}

pub fn generate_config() -> Config {
    config::Config::builder()
        .add_source(config::File::new("etc/config.properties", FileFormat::Toml).required(false))
        .add_source(
            config::Environment::default()
                .separator("_")
                .prefix(CONFIG_ENV_PREFIX),
        )
        .build()
        .expect("config should have built")
}

impl<T: FromStr + Default + Clone> ConfigBuilder<T> {
    pub fn new(name: &str) -> Self {
        assert!(name.is_ascii(), "config property name must only be ascii");
        if !name.chars().all(|c| c.is_alphanumeric() || c == '.') {}
        ConfigBuilder {
            name: Some(name.to_string()),
            ..Default::default()
        }
    }

    pub fn description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    pub fn default(mut self, value: T) -> Self {
        self.default = Some(value);
        self
    }

    pub fn env(mut self, env: &str) -> Self {
        self.env_var = Some(env.to_string());
        self
    }

    pub fn build<'a: 'static>(self) -> ConfigProperty<T> {
        let name = self.name.expect("config property must have a name");
        let property = ConfigProperty {
            name: name.clone(),
            description: self.description.unwrap_or("".to_string()),
            env_var: self
                .env_var
                .unwrap_or_else(|| name.to_uppercase().replace(".", "_")),
            default: self.default,
        };
        {
            let mut prop_map = config_registry().lock().unwrap();
            if prop_map.contains(property.name.as_str()) {
                panic!("duplicate property entries found");
            }
            prop_map.insert(name.clone());
        }
        {
            let mut reg = PROPERTY_REGISTRY.lock().unwrap();
            reg.insert(PropertyInfo::from(&property));
        }
        property
    }
}

impl<T: FromStr + Clone> ConfigProperty<T> {
    pub fn lookup(&self, config: &Config) -> Option<T> {
        match config.get_string(&self.name) {
            Ok(val) => match T::from_str(&val) {
                Ok(item) => Some(item),
                Err(_) => panic!("Failed to parse {}", self),
            },
            Err(ConfigError::NotFound(_)) => self.default.clone(),
            _ => None,
        }
    }
}

impl<T: FromStr + Clone> Display for ConfigProperty<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{}", &self.name))
    }
}
