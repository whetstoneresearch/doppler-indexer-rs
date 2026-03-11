//! Generic types for config loading patterns.
//!
//! This module provides reusable generic types that eliminate duplication
//! in config type definitions.

use serde::de::{self, Visitor};
use serde::Deserialize;
use std::fmt;
use std::marker::PhantomData;

/// Generic enum for config values that can be either inline or loaded from a path.
///
/// Replaces ContractsOrPath, TokensOrPath, and FactoryCollectionsOrPath with a single
/// generic type that can be used with any config type.
///
/// # Example
/// ```ignore
/// type ContractsOrPath = InlineOrPath<Contracts>;
/// type TokensOrPath = InlineOrPath<Tokens>;
/// ```
#[derive(Debug)]
pub enum InlineOrPath<T> {
    /// Config data provided inline in the JSON
    Inline(T),
    /// Path to a file or directory containing the config
    Path(String),
}

#[allow(dead_code)]
impl<T> InlineOrPath<T> {
    /// Returns true if this is an inline value
    pub fn is_inline(&self) -> bool {
        matches!(self, InlineOrPath::Inline(_))
    }

    /// Returns true if this is a path reference
    pub fn is_path(&self) -> bool {
        matches!(self, InlineOrPath::Path(_))
    }

    /// Extract the inline value, if present
    pub fn into_inline(self) -> Option<T> {
        match self {
            InlineOrPath::Inline(v) => Some(v),
            InlineOrPath::Path(_) => None,
        }
    }

    /// Extract the path, if present
    pub fn as_path(&self) -> Option<&str> {
        match self {
            InlineOrPath::Inline(_) => None,
            InlineOrPath::Path(p) => Some(p),
        }
    }
}

impl<'de, T> Deserialize<'de> for InlineOrPath<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Use untagged deserialization: try parsing as T first, then as String
        struct InlineOrPathVisitor<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for InlineOrPathVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = InlineOrPath<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an inline value or a path string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(InlineOrPath::Path(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(InlineOrPath::Path(v))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let value = T::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(InlineOrPath::Inline(value))
            }
        }

        deserializer.deserialize_any(InlineOrPathVisitor(PhantomData))
    }
}

/// Generic enum for config values that can be either a single item or multiple items.
///
/// Replaces EventTriggerConfigs and FactoryEventConfigOrArray with a single generic
/// type that can be used with any config type.
///
/// # Example
/// ```ignore
/// type EventTriggerConfigs = SingleOrMultiple<EventTriggerConfig>;
/// type FactoryEventConfigOrArray = SingleOrMultiple<FactoryEventConfig>;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum SingleOrMultiple<T> {
    /// A single config item
    Single(T),
    /// Multiple config items
    Multiple(Vec<T>),
}

impl<T> SingleOrMultiple<T> {
    /// Iterate over all items
    pub fn iter(&self) -> SingleOrMultipleIter<'_, T> {
        SingleOrMultipleIter {
            inner: self,
            index: 0,
        }
    }

    /// Get the number of items
    pub fn len(&self) -> usize {
        match self {
            SingleOrMultiple::Single(_) => 1,
            SingleOrMultiple::Multiple(items) => items.len(),
        }
    }

    /// Check if empty (only possible for Multiple with empty vec)
    pub fn is_empty(&self) -> bool {
        match self {
            SingleOrMultiple::Single(_) => false,
            SingleOrMultiple::Multiple(items) => items.is_empty(),
        }
    }

    /// Convert to a Vec
    pub fn into_vec(self) -> Vec<T> {
        match self {
            SingleOrMultiple::Single(item) => vec![item],
            SingleOrMultiple::Multiple(items) => items,
        }
    }

    /// Get all items as references
    pub fn as_slice(&self) -> Vec<&T> {
        match self {
            SingleOrMultiple::Single(item) => vec![item],
            SingleOrMultiple::Multiple(items) => items.iter().collect(),
        }
    }
}

impl<T> Default for SingleOrMultiple<T> {
    fn default() -> Self {
        SingleOrMultiple::Multiple(Vec::new())
    }
}

/// Iterator for SingleOrMultiple
pub struct SingleOrMultipleIter<'a, T> {
    inner: &'a SingleOrMultiple<T>,
    index: usize,
}

impl<'a, T> Iterator for SingleOrMultipleIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            SingleOrMultiple::Single(item) => {
                if self.index == 0 {
                    self.index += 1;
                    Some(item)
                } else {
                    None
                }
            }
            SingleOrMultiple::Multiple(items) => {
                if self.index < items.len() {
                    let item = &items[self.index];
                    self.index += 1;
                    Some(item)
                } else {
                    None
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self.inner {
            SingleOrMultiple::Single(_) => {
                if self.index == 0 {
                    1
                } else {
                    0
                }
            }
            SingleOrMultiple::Multiple(items) => items.len().saturating_sub(self.index),
        };
        (remaining, Some(remaining))
    }
}

impl<'a, T> ExactSizeIterator for SingleOrMultipleIter<'a, T> {}

impl<'a, T> IntoIterator for &'a SingleOrMultiple<T> {
    type Item = &'a T;
    type IntoIter = SingleOrMultipleIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'de, T> Deserialize<'de> for SingleOrMultiple<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SingleOrMultipleVisitor<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for SingleOrMultipleVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = SingleOrMultiple<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a single item or an array of items")
            }

            fn visit_map<M>(self, map: M) -> Result<SingleOrMultiple<T>, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                let item = T::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SingleOrMultiple::Single(item))
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<SingleOrMultiple<T>, S::Error>
            where
                S: de::SeqAccess<'de>,
            {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<T>()? {
                    items.push(item);
                }
                Ok(SingleOrMultiple::Multiple(items))
            }
        }

        deserializer.deserialize_any(SingleOrMultipleVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashMap;

    #[derive(Debug, Deserialize, PartialEq, Clone)]
    struct TestConfig {
        name: String,
        value: u32,
    }

    // Tests for InlineOrPath
    mod inline_or_path_tests {
        use super::*;

        #[test]
        fn test_deserialize_path() {
            let json = r#""./path/to/file.json""#;
            let result: InlineOrPath<HashMap<String, String>> = serde_json::from_str(json).unwrap();
            assert!(result.is_path());
            assert_eq!(result.as_path(), Some("./path/to/file.json"));
        }

        #[test]
        fn test_deserialize_inline_map() {
            let json = r#"{"key1": "value1", "key2": "value2"}"#;
            let result: InlineOrPath<HashMap<String, String>> = serde_json::from_str(json).unwrap();
            assert!(result.is_inline());
            if let InlineOrPath::Inline(map) = result {
                assert_eq!(map.get("key1"), Some(&"value1".to_string()));
                assert_eq!(map.get("key2"), Some(&"value2".to_string()));
            }
        }

        #[test]
        fn test_into_inline() {
            let inline: InlineOrPath<HashMap<String, String>> =
                InlineOrPath::Inline(HashMap::new());
            assert!(inline.into_inline().is_some());

            let path: InlineOrPath<HashMap<String, String>> =
                InlineOrPath::Path("./test".to_string());
            assert!(path.into_inline().is_none());
        }
    }

    // Tests for SingleOrMultiple
    mod single_or_multiple_tests {
        use super::*;

        #[test]
        fn test_deserialize_single() {
            let json = r#"{"name": "test", "value": 42}"#;
            let result: SingleOrMultiple<TestConfig> = serde_json::from_str(json).unwrap();
            assert_eq!(result.len(), 1);
            assert!(!result.is_empty());
            if let SingleOrMultiple::Single(config) = result {
                assert_eq!(config.name, "test");
                assert_eq!(config.value, 42);
            }
        }

        #[test]
        fn test_deserialize_multiple() {
            let json = r#"[{"name": "test1", "value": 1}, {"name": "test2", "value": 2}]"#;
            let result: SingleOrMultiple<TestConfig> = serde_json::from_str(json).unwrap();
            assert_eq!(result.len(), 2);
            if let SingleOrMultiple::Multiple(configs) = &result {
                assert_eq!(configs[0].name, "test1");
                assert_eq!(configs[1].name, "test2");
            }
        }

        #[test]
        fn test_deserialize_empty_array() {
            let json = r#"[]"#;
            let result: SingleOrMultiple<TestConfig> = serde_json::from_str(json).unwrap();
            assert_eq!(result.len(), 0);
            assert!(result.is_empty());
        }

        #[test]
        fn test_iter() {
            let multi: SingleOrMultiple<TestConfig> = SingleOrMultiple::Multiple(vec![
                TestConfig {
                    name: "a".to_string(),
                    value: 1,
                },
                TestConfig {
                    name: "b".to_string(),
                    value: 2,
                },
            ]);

            let names: Vec<_> = multi.iter().map(|c| c.name.as_str()).collect();
            assert_eq!(names, vec!["a", "b"]);
        }

        #[test]
        fn test_into_vec() {
            let single: SingleOrMultiple<TestConfig> = SingleOrMultiple::Single(TestConfig {
                name: "test".to_string(),
                value: 1,
            });
            let vec = single.into_vec();
            assert_eq!(vec.len(), 1);
            assert_eq!(vec[0].name, "test");
        }

        #[test]
        fn test_as_slice() {
            let multi: SingleOrMultiple<TestConfig> = SingleOrMultiple::Multiple(vec![
                TestConfig {
                    name: "a".to_string(),
                    value: 1,
                },
                TestConfig {
                    name: "b".to_string(),
                    value: 2,
                },
            ]);

            let slice = multi.as_slice();
            assert_eq!(slice.len(), 2);
            assert_eq!(slice[0].name, "a");
        }

        #[test]
        fn test_iterator_size_hint() {
            let single: SingleOrMultiple<u32> = SingleOrMultiple::Single(1);
            let mut iter = single.iter();
            assert_eq!(iter.size_hint(), (1, Some(1)));
            iter.next();
            assert_eq!(iter.size_hint(), (0, Some(0)));

            let multi: SingleOrMultiple<u32> = SingleOrMultiple::Multiple(vec![1, 2, 3]);
            let mut iter = multi.iter();
            assert_eq!(iter.size_hint(), (3, Some(3)));
            iter.next();
            assert_eq!(iter.size_hint(), (2, Some(2)));
        }
    }
}
