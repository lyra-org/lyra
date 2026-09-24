// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use super::EntityProjectionInfo;
use crate::db::{
    self,
    NodeId,
    external_ids::ExternalId,
};
use crate::services::EntityType;
use crate::services::providers::{
    IdLink,
    IdLinkGenerators,
    IdLinkLocale,
    IdLinkRequest,
    IdLinkTarget,
};

/// ID rows of each projected entity whose `links` include was requested,
/// gathered while projecting so they are loaded once.
#[derive(Debug, Default)]
pub(crate) struct IdLinkRows(HashMap<DbId, (EntityType, Vec<ExternalId>)>);

impl IdLinkRows {
    pub(super) fn insert(&mut self, db_id: DbId, entity: EntityType, rows: Vec<ExternalId>) {
        self.0.insert(db_id, (entity, rows));
    }
}

/// Link targets with their library locale: the `library_id` library when
/// given, otherwise the entity's library with the lowest db id, as provider
/// refresh does. Provider names come from the stored provider configs.
pub(crate) fn id_link_request(
    db: &DbAny,
    rows: IdLinkRows,
    library_id: Option<DbId>,
    generators: &IdLinkGenerators,
) -> anyhow::Result<IdLinkRequest> {
    let request_locale = library_id
        .map(|library_id| {
            db::libraries::get_by_id(db, library_id)
                .map(|library| library.as_ref().map(IdLinkLocale::from).unwrap_or_default())
        })
        .transpose()?;
    let mut targets = Vec::with_capacity(rows.0.len());
    for (entity_db_id, (entity, rows)) in rows.0 {
        let library = if !generators.needs_context(entity, &rows) {
            IdLinkLocale::default()
        } else if let Some(locale) = &request_locale {
            locale.clone()
        } else {
            db::libraries::get_for_entity(db, entity_db_id)?
                .iter()
                .filter(|library| library.db_id.is_some())
                .min_by_key(|library| library.db_id.map(|id| id.0))
                .map(IdLinkLocale::from)
                .unwrap_or_default()
        };
        targets.push(IdLinkTarget {
            entity_db_id,
            entity,
            library,
            rows,
        });
    }

    let provider_names = if targets.iter().any(|target| !target.rows.is_empty()) {
        db::providers::get(db)?
            .into_iter()
            .map(|provider| (provider.provider_id, provider.display_name))
            .collect()
    } else {
        HashMap::new()
    };
    Ok(IdLinkRequest {
        targets,
        provider_names,
    })
}

/// Fills the `links` include of every projected entity, including release tracks.
pub(crate) fn apply_id_links(
    projections: &mut [EntityProjectionInfo],
    links: &HashMap<DbId, Vec<IdLink>>,
) {
    let resolve = |db_id: &Option<NodeId>| {
        Some(
            db_id
                .as_ref()
                .and_then(|db_id| links.get(&DbId::from(db_id.clone())))
                .cloned()
                .unwrap_or_default(),
        )
    };
    for projection in projections {
        match projection {
            EntityProjectionInfo::Release(release) => {
                release.includes.links = resolve(&release.entity.db_id);
                for track in release.includes.tracks.iter_mut().flatten() {
                    track.links = resolve(&track.db_id);
                }
            }
            EntityProjectionInfo::Track(track) => {
                track.includes.links = resolve(&track.entity.db_id);
            }
            EntityProjectionInfo::Artist(artist) => {
                artist.includes.links = resolve(&artist.entity.db_id);
            }
        }
    }
}
