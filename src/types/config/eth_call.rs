use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct EthCallConfig {
    pub function: String,
    pub output_type: EvmType,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvmType {
    Int256,
    Uint256
}