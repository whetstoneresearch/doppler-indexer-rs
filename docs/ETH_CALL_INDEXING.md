# ETH Call Indexing

The indexer supports tracking the results of `eth_call` at each block, allowing you to capture historical state like token balances, oracle prices, and other view function results.

## Configuration

Add `calls` to any contract in your config:

```json
{
    "ChainlinkEthOracle": {
        "address": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70",
        "calls": [
            {
                "function": "latestAnswer()",
                "output_type": "int256"
            }
        ]
    }
}
```

### Parameterized Calls

For functions that take parameters, specify the `params` array:

```json
{
    "WETH": {
        "address": "0x4200000000000000000000000000000000000006",
        "calls": [
            {
                "function": "balanceOf(address)",
                "output_type": "uint256",
                "params": [
                    {
                        "type": "address",
                        "values": [
                            "0x1234567890abcdef1234567890abcdef12345678",
                            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                        ]
                    }
                ]
            }
        ]
    }
}
```

This will call `balanceOf` for each address at every indexed block.

### Multi-Parameter Functions

For functions with multiple parameters, the indexer creates a cartesian product of all values:

```json
{
    "Token": {
        "address": "0x...",
        "calls": [
            {
                "function": "allowance(address,address)",
                "output_type": "uint256",
                "params": [
                    {
                        "type": "address",
                        "values": ["0xOwner1...", "0xOwner2..."]
                    },
                    {
                        "type": "address",
                        "values": ["0xSpender1...", "0xSpender2..."]
                    }
                ]
            }
        ]
    }
}
```

This produces 4 calls per block: `allowance(Owner1, Spender1)`, `allowance(Owner1, Spender2)`, `allowance(Owner2, Spender1)`, `allowance(Owner2, Spender2)`.

## Supported Types

### Output Types
- `int256`, `uint256`

### Parameter Types
- **Integers**: `uint8`, `uint16`, `uint32`, `uint64`, `uint80`, `uint128`, `uint256`, `int8`, `int32`, `int64`, `int128`, `int256`
- **Other**: `address`, `bool`, `bytes32`, `bytes`, `string`

## Parameter Values

Values can be specified as:
- **Strings**: `"0x1234..."` for addresses, hex values, or large numbers
- **Numbers**: `12345` for small integers
- **Booleans**: `true` or `false`

Examples:
```json
{ "type": "address", "values": ["0x1234567890abcdef1234567890abcdef12345678"] }
{ "type": "uint256", "values": ["1000000000000000000", "0xde0b6b3a7640000"] }
{ "type": "bool", "values": [true, false] }
{ "type": "bytes32", "values": ["0x0000000000000000000000000000000000000000000000000000000000000001"] }
```

## Output

Results are written to parquet files at `data/raw/{chain}/eth_calls/`:
- File naming: `eth_calls_{contract}_{function}_{start}-{end}.parquet`
- Columns: `block_number`, `block_timestamp`, `contract_address`, `value`, `param_0`, `param_1`, ...

The `value` column contains the ABI-encoded return value. Parameter columns (`param_0`, etc.) contain the ABI-encoded parameter values used for that call.
