// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::{
    STATE,
    db,
    services::pagination::{
        PaginationError,
        SnapshotKey,
        SnapshotPage,
    },
};

use super::AppError;

#[derive(Clone)]
pub(crate) struct SnapshotPageRequest {
    limit: usize,
    cursor: Option<String>,
    generation: std::sync::Arc<crate::GenerationState>,
}

impl super::PageQuery {
    pub(crate) fn resolve_snapshot(self) -> SnapshotPageRequest {
        SnapshotPageRequest {
            limit: self
                .limit
                .unwrap_or(super::DEFAULT_PAGE_LIMIT)
                .clamp(1, super::PAGE_HARD_LIMIT) as usize,
            cursor: self.cursor,
            generation: STATE.generation(),
        }
    }
}

impl SnapshotPageRequest {
    #[cfg(test)]
    pub(crate) fn first_page(limit: usize) -> Self {
        Self {
            limit: limit.clamp(1, super::PAGE_HARD_LIMIT as usize),
            cursor: None,
            generation: STATE.generation(),
        }
    }

    pub(crate) fn resume(&self, key: &SnapshotKey) -> Result<Option<SnapshotPage>, AppError> {
        self.cursor
            .as_deref()
            .map(|cursor| {
                self.generation
                    .pagination
                    .resume(key, cursor, self.limit)
                    .map_err(AppError::from)
            })
            .transpose()
    }

    pub(crate) fn start(
        &self,
        key: &SnapshotKey,
        item_ids: Vec<String>,
    ) -> Result<SnapshotPage, AppError> {
        self.generation
            .pagination
            .start(key, item_ids, self.limit)
            .map_err(AppError::from)
    }
}

pub(crate) fn load_snapshot_items<T>(
    db: &agdb::DbAny,
    item_ids: &[String],
    mut load: impl FnMut(&agdb::DbAny, agdb::DbId) -> anyhow::Result<Option<T>>,
    mut is_visible: impl FnMut(&agdb::DbAny, agdb::DbId) -> anyhow::Result<bool>,
) -> anyhow::Result<Vec<T>> {
    let public_ids = item_ids.iter().map(String::as_str).collect::<Vec<_>>();
    let db_ids = db::lookup::find_node_ids_by_ids(db, &public_ids)?;
    let mut items = Vec::with_capacity(item_ids.len());
    for public_id in item_ids {
        let Some(db_id) = db_ids.get(public_id).copied() else {
            continue;
        };
        if !is_visible(db, db_id)? {
            continue;
        }
        if let Some(item) = load(db, db_id)? {
            items.push(item);
        }
    }
    Ok(items)
}

impl From<PaginationError> for AppError {
    fn from(err: PaginationError) -> Self {
        match err {
            PaginationError::MalformedCursor => Self::bad_request(err.to_string()),
            PaginationError::ExpiredCursor | PaginationError::ContextMismatch => {
                Self::conflict(err.to_string())
            }
            PaginationError::SnapshotTooLarge => Self::service_unavailable(err.to_string()),
        }
    }
}
