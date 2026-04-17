use std::borrow::Cow;

use alloy_primitives::{address, Address};

pub fn is_precompile_address(addr: Address) -> bool {
    let precompile_addresses: Vec<Address> = vec![
        address!("0x0000000000000000000000000000000000000001"), // ecrecover
        address!("0x0000000000000000000000000000000000000002"), // SHA2-256
        address!("0x0000000000000000000000000000000000000003"), // RIPEMD-160
        address!("0x0000000000000000000000000000000000000004"), // identity
        address!("0x0000000000000000000000000000000000000005"), // modexp
        address!("0x0000000000000000000000000000000000000006"), // bn256Add
        address!("0x0000000000000000000000000000000000000007"), // bn256ScalarMul
        address!("0x0000000000000000000000000000000000000008"), // bn256Pairing
        address!("0x0000000000000000000000000000000000000009"), // blake2f
        address!("0x000000000000000000000000000000000000000a"), // bls12381g1add
        address!("0x000000000000000000000000000000000000000b"), // bls12381g1mul
        address!("0x000000000000000000000000000000000000000c"), // bls12381g1multiexp
        address!("0x000000000000000000000000000000000000000d"), // bls12381g2add
        address!("0x000000000000000000000000000000000000000e"), // bls12381g2mul
        address!("0x000000000000000000000000000000000000000f"), // bls12381g2multiexp
        address!("0x0000000000000000000000000000000000000010"), // bls12381pairing
        address!("0x0000000000000000000000000000000000000011"), // bls12381msm
    ];

    let is_precompile: bool = precompile_addresses.contains(&addr);

    is_precompile
}

/// PostgreSQL text columns reject embedded NUL bytes, but some on-chain string
/// values arrive padded or corrupted with `\0`. Strip them while borrowing when
/// possible to avoid unnecessary allocations on the hot path.
pub fn strip_nul_bytes(value: &str) -> Cow<'_, str> {
    if value.as_bytes().contains(&0) {
        Cow::Owned(value.replace('\0', ""))
    } else {
        Cow::Borrowed(value)
    }
}

#[cfg(test)]
mod tests {
    use super::strip_nul_bytes;

    #[test]
    fn strip_nul_bytes_borrows_when_clean() {
        let input = "hello";
        let output = strip_nul_bytes(input);
        assert_eq!(output, "hello");
        assert!(matches!(output, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn strip_nul_bytes_removes_embedded_nuls() {
        let output = strip_nul_bytes("he\0ll\0o");
        assert_eq!(output, "hello");
    }
}
