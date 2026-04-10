use std::collections::HashSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairScope {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub sources: Option<HashSet<String>>,
    pub functions: Option<HashSet<String>>,
}

impl RepairScope {
    pub fn is_unscoped(&self) -> bool {
        self.from_block.is_none()
            && self.to_block.is_none()
            && self.sources.is_none()
            && self.functions.is_none()
    }

    pub fn matches_range(&self, range_start: u64, range_end_exclusive: u64) -> bool {
        if range_end_exclusive <= range_start {
            return false;
        }

        let range_end_inclusive = range_end_exclusive - 1;

        if let Some(from_block) = self.from_block {
            if range_end_inclusive < from_block {
                return false;
            }
        }

        if let Some(to_block) = self.to_block {
            if range_start > to_block {
                return false;
            }
        }

        true
    }

    pub fn matches_source_function(&self, source: &str, function: &str) -> bool {
        if let Some(sources) = &self.sources {
            if !sources.contains(source) {
                return false;
            }
        }

        if let Some(functions) = &self.functions {
            if !functions.contains(function) {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::RepairScope;
    use std::collections::HashSet;

    #[test]
    fn matches_inclusive_block_bounds() {
        let scope = RepairScope {
            from_block: Some(200),
            to_block: Some(299),
            sources: None,
            functions: None,
        };

        assert!(scope.matches_range(100, 201));
        assert!(scope.matches_range(299, 300));
        assert!(scope.matches_range(250, 260));
        assert!(!scope.matches_range(100, 200));
        assert!(!scope.matches_range(300, 400));
    }

    #[test]
    fn matches_source_and_function_filters() {
        let scope = RepairScope {
            from_block: None,
            to_block: None,
            sources: Some(HashSet::from([String::from("A")])),
            functions: Some(HashSet::from([String::from("f")])),
        };

        assert!(scope.matches_source_function("A", "f"));
        assert!(!scope.matches_source_function("B", "f"));
        assert!(!scope.matches_source_function("A", "g"));
    }
}
