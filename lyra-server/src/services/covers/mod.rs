// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod display;
mod image;
pub(crate) mod lookup;
pub(crate) mod providers;
mod resolve;
pub(crate) mod selection;
pub(crate) mod shown;
pub(crate) mod stale;
mod sync;

use std::path::Path;

/// A cover and the node it belongs to: the item itself, a track's release, or the release a
/// playlist or genre displays.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedCover {
    pub(crate) owner_db_id: agdb::DbId,
    pub(crate) cover: lyra_db::Cover,
}

#[derive(Clone, Copy)]
pub(crate) struct CoverPaths<'a> {
    pub(crate) library_root: Option<&'a Path>,
    pub(crate) covers_root: &'a Path,
}

/// Which covers a refresh or sync fetches from providers.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CoverUpdate {
    /// Fetches none.
    Skip,
    /// Fetches covers only for what has none.
    #[default]
    Missing,
    /// Replaces the provider covers already there.
    Replace,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CoverSyncOptions {
    update: CoverUpdate,
    /// Looks covers up again instead of using what earlier lookups cached.
    bypass_cache: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("bypass_cover_cache needs covers other than \"skip\"")]
pub(crate) struct CoverSyncOptionsError;

impl CoverSyncOptions {
    pub(crate) const SKIP: Self = Self {
        update: CoverUpdate::Skip,
        bypass_cache: false,
    };

    pub(crate) fn new(
        update: CoverUpdate,
        bypass_cache: bool,
    ) -> Result<Self, CoverSyncOptionsError> {
        if update == CoverUpdate::Skip && bypass_cache {
            return Err(CoverSyncOptionsError);
        }
        Ok(Self {
            update,
            bypass_cache,
        })
    }

    pub(crate) fn skips(self) -> bool {
        self.update == CoverUpdate::Skip
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CoverTransformOptions {
    pub(crate) format: Option<::image::ImageFormat>,
    pub(crate) quality: Option<u8>,
    pub(crate) max_width: Option<u32>,
    pub(crate) max_height: Option<u32>,
}

impl CoverTransformOptions {
    pub(crate) fn is_empty(&self) -> bool {
        self.format.is_none()
            && self.quality.is_none()
            && self.max_width.is_none()
            && self.max_height.is_none()
    }
}

pub(crate) struct TransformedCoverImage {
    pub(crate) bytes: Vec<u8>,
    pub(crate) mime_type: &'static str,
}

#[derive(Clone, Debug)]
pub(crate) struct CoverImageCandidate {
    pub(crate) url: String,
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderCoverSearchResult {
    pub(crate) provider_id: String,
    pub(crate) candidates: Vec<CoverImageCandidate>,
    pub(crate) selected_index: Option<u32>,
}

impl ProviderCoverSearchResult {
    pub(crate) fn selected_candidate(&self) -> Option<&CoverImageCandidate> {
        let selected = self
            .selected_index
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| usize::try_from(index).ok())
            .filter(|index| *index < self.candidates.len());
        selected.and_then(|index| self.candidates.get(index))
    }
}

// Re-exports for convenient access from the rest of the crate.
pub(crate) use image::{
    cover_mime_from_path,
    parse_cover_image_format,
    transform_cover_image,
};
pub(crate) use providers::{
    clear_cover_search_cache,
    search_artist_cover_candidates,
    search_release_cover_candidates,
};
pub(crate) use resolve::configured_covers_root;
pub(crate) use sync::{
    MAX_COVER_BYTES,
    artist_cover_wanted,
    discard_cover_file,
    discard_unreferenced_cover_files,
    forget_gone_covers,
    record_local_release_cover,
    refresh_gone_covers_for_library,
    sync_and_persist_release_covers_for_library,
    sync_artist_cover,
    sync_release_cover_for_tracks,
};
