// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod image;
pub(crate) mod providers;
mod resolve;
mod sync;

use std::path::Path;

#[derive(Clone, Copy, Default)]
pub(crate) struct CoverPaths<'a> {
    pub(crate) library_root: Option<&'a Path>,
    pub(crate) covers_root: Option<&'a Path>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct CoverSyncOptions {
    pub(crate) replace_existing: bool,
    pub(crate) force_refresh: bool,
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
pub(crate) use resolve::{
    configured_covers_root,
    resolve_cover_for_artist_id,
    resolve_cover_for_release_id,
};
pub(crate) use sync::{
    eager_sync_cover_metadata,
    resolve_release_covers,
    sync_artist_cover,
    sync_release_cover_for_tracks,
    sync_release_cover_metadata_from_resolved,
    sync_release_covers_for_library,
    upsert_artist_cover_metadata,
    upsert_release_cover_metadata,
};

#[cfg(test)]
mod tests {
    use super::resolve::*;
    use std::path::{
        Path,
        PathBuf,
    };

    #[test]
    fn resolve_cover_storage_root_returns_relative_path_as_global_root() {
        let root = resolve_cover_storage_root(Some(Path::new(".lyra/covers")));

        assert_eq!(root, Some(PathBuf::from(".lyra/covers")));
    }

    #[test]
    fn resolve_cover_storage_root_keeps_absolute_config_path() {
        let root = resolve_cover_storage_root(Some(Path::new("/var/lib/lyra/covers")));

        assert_eq!(root, Some(PathBuf::from("/var/lib/lyra/covers")));
    }
}
