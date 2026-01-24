use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct EthCallConfig {
    pub function: String,
    pub output_type: EvmType,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvmType {
    Int256,
    Uint256
}