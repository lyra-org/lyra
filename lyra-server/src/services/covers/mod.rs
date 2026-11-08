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

/// Why a cover request's options are refused.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CoverTransformOptionsError {
    #[error("quality must be between 0 and 100")]
    Quality,
    #[error("{0} must be greater than 0")]
    ZeroBound(&'static str),
}

impl CoverTransformOptions {
    /// The options a cover request names: a quality of at most 100 and bounds greater than 0.
    pub(crate) fn new(
        format: Option<::image::ImageFormat>,
        quality: Option<u32>,
        max_width: Option<u32>,
        max_height: Option<u32>,
    ) -> Result<Self, CoverTransformOptionsError> {
        let quality = quality
            .map(|quality| {
                u8::try_from(quality)
                    .ok()
                    .filter(|quality| *quality <= 100)
                    .ok_or(CoverTransformOptionsError::Quality)
            })
            .transpose()?;
        for (name, bound) in [("max_width", max_width), ("max_height", max_height)] {
            if bound == Some(0) {
                return Err(CoverTransformOptionsError::ZeroBound(name));
            }
        }
        Ok(Self {
            format,
            quality,
            max_width,
            max_height,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.format.is_none()
            && self.quality.is_none()
            && self.max_width.is_none()
            && self.max_height.is_none()
    }
}

pub(crate) enum CoverRendition {
    /// The file is served as it is: the options would change nothing, or the image would not
    /// transform.
    Original { mime_type: &'static str },
    /// The transformed file, kept in the cover cache, opened while it is there.
    Cached {
        file: std::fs::File,
        len: u64,
        mime_type: &'static str,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CoverTransformError {
    #[error("cover transforms are busy")]
    Busy,
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
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
    COVER_EXTENSIONS,
    MAX_COVER_BYTES,
    TransformCache,
    cover_mime_from_path,
    forget_cover_transforms,
    parse_cover_image_format,
    render_cover,
};
pub(crate) use providers::{
    clear_cover_search_cache,
    search_artist_cover_candidates,
    search_release_cover_candidates,
};
pub(crate) use resolve::{
    configured_covers_root,
    is_cover_file,
};
pub(crate) use sync::{
    artist_cover_wanted,
    discard_cover,
    discard_unreferenced_cover_files,
    forget_gone_covers,
    record_local_release_cover,
    refresh_gone_covers_for_library,
    sync_and_persist_release_covers_for_library,
    sync_artist_cover,
    sync_release_cover_for_tracks,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_options_keep_what_they_are_given() {
        let options = CoverTransformOptions::new(
            Some(::image::ImageFormat::WebP),
            Some(85),
            Some(640),
            Some(480),
        )
        .expect("valid options");
        assert_eq!(options.format, Some(::image::ImageFormat::WebP));
        assert_eq!(options.quality, Some(85));
        assert_eq!(
            (options.max_width, options.max_height),
            (Some(640), Some(480))
        );
        assert_eq!(
            CoverTransformOptions::new(None, Some(100), None, None)
                .expect("valid options")
                .quality,
            Some(100)
        );
        assert!(
            CoverTransformOptions::new(None, None, None, None)
                .expect("no options")
                .is_empty()
        );
    }

    #[test]
    fn transform_options_refuse_what_no_cover_can_be_served_as() {
        let refused = |quality, max_width, max_height| {
            CoverTransformOptions::new(None, quality, max_width, max_height)
                .expect_err("refused")
                .to_string()
        };
        for quality in [101, 256, u32::MAX] {
            assert_eq!(
                refused(Some(quality), None, None),
                "quality must be between 0 and 100"
            );
        }
        assert_eq!(
            refused(None, Some(0), Some(320)),
            "max_width must be greater than 0"
        );
        assert_eq!(
            refused(None, Some(320), Some(0)),
            "max_height must be greater than 0"
        );
    }
}
