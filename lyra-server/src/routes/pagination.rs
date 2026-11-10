// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbAny;

use super::{
    AppError,
    responses::PageResponse,
};
use crate::services::catalog::{
    self,
    pipeline::{
        Catalog,
        KeyKind,
        SortKey,
    },
};

/// The largest seed: the largest integer a JSON number carries exactly through a JavaScript
/// client.
pub(crate) const MAX_SEED: u64 = (1 << 53) - 1;

/// The page of a list a request asks for.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PageRequest {
    offset: u64,
    limit: u64,
    seed: Option<u64>,
}

impl super::PageQuery {
    pub(crate) fn resolve(self) -> PageRequest {
        PageRequest {
            offset: self.offset.unwrap_or(0),
            limit: self
                .limit
                .unwrap_or(super::DEFAULT_PAGE_LIMIT)
                .clamp(1, super::PAGE_HARD_LIMIT),
            seed: None,
        }
    }
}

impl PageRequest {
    pub(crate) fn first(limit: u64) -> Self {
        Self {
            offset: 0,
            limit: limit.clamp(1, super::PAGE_HARD_LIMIT),
            seed: None,
        }
    }

    /// The page with the seed a random sort shuffles by.
    pub(crate) fn seeded(mut self, seed: super::SeedQuery) -> Result<Self, AppError> {
        if seed.seed.is_some_and(|seed| seed > MAX_SEED) {
            return Err(AppError::bad_request(format!(
                "seed must be at most {MAX_SEED}"
            )));
        }
        self.seed = seed.seed;
        Ok(self)
    }

    /// The page's items of `items`, the whole list in order.
    pub(crate) fn slice<T>(&self, items: Vec<T>) -> PageResponse<T> {
        let total = items.len() as u64;
        let offset = self.offset.min(total);
        PageResponse {
            items: items
                .into_iter()
                .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                .take(usize::try_from(self.limit).unwrap_or(usize::MAX))
                .collect(),
            total,
            offset,
            seed: None,
        }
    }

    /// The page's entities of the catalog list `query` describes, shuffled by the page's seed,
    /// or one picked here, when the query sorts at random.
    pub(crate) fn catalog<C: Catalog>(
        &self,
        db: &DbAny,
        viewer: &catalog::Viewer,
        mut query: catalog::Query<C>,
    ) -> Result<PageResponse<C::Item>, AppError> {
        let seed = query
            .sort
            .iter()
            .any(|(key, _)| key.kind() == KeyKind::Random)
            .then(|| {
                self.seed
                    .unwrap_or_else(|| rand::random::<u64>() & MAX_SEED)
            });
        query.seed = seed.unwrap_or_default();
        let page = catalog::page(db, viewer, &query, self.offset, Some(self.limit))?;
        Ok(PageResponse {
            items: page.items,
            total: page.total,
            offset: page.offset,
            seed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(offset: u64, limit: u64) -> PageRequest {
        PageRequest {
            offset,
            limit,
            seed: None,
        }
    }

    #[test]
    fn slice_pages_through_a_list_once_and_in_order() {
        let items = (0..7).collect::<Vec<_>>();
        let mut seen = Vec::new();
        for offset in [0, 3, 6] {
            let page = request(offset, 3).slice(items.clone());
            assert_eq!(page.total, 7);
            assert_eq!(page.offset, offset);
            seen.extend(page.items);
        }
        assert_eq!(seen, items);
    }

    #[test]
    fn slice_past_the_end_is_empty_with_the_total() {
        for offset in [3, 4, u64::MAX] {
            let page = request(offset, 2).slice(vec!["a", "b", "c"]);
            assert!(page.items.is_empty());
            assert_eq!(page.total, 3);
            assert_eq!(page.offset, 3);
        }
    }

    #[test]
    fn seeded_rejects_a_seed_javascript_would_round() {
        let seed = |seed| request(0, 1).seeded(crate::routes::SeedQuery { seed: Some(seed) });
        assert_eq!(
            seed(MAX_SEED).ok().and_then(|page| page.seed),
            Some(MAX_SEED)
        );
        assert!(seed(MAX_SEED + 1).is_err());
    }
}
