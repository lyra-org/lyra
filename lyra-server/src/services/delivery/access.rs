// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::DeliveryError;
use crate::services::{
    STATE,
    auth::{
        CredentialHolder,
        Principal,
        ResolvedAuth,
        caller::{
            Caller,
            ClientAddress,
        },
        media_tokens::{
            MediaTokenAccess,
            MediaTokenPurpose,
        },
        require_permission,
    },
    hls::state::HlsSessionHolder,
};

/// Checks that `principal` may be served a track for `purpose`.
pub(crate) fn authorize_track_purpose(
    principal: &Principal,
    purpose: MediaTokenPurpose,
) -> Result<(), DeliveryError> {
    match purpose {
        MediaTokenPurpose::Stream | MediaTokenPurpose::HlsPlaylist => Ok(()),
        MediaTokenPurpose::Download => {
            require_permission(principal, lyra_db::Permission::Download).map_err(Into::into)
        }
    }
}

/// How a request reached a track. A credential's holder is read as they stand under the guard
/// that resolves the track's source, where their library access and what the request's purpose
/// requires are checked. A media token was bound to the track's public id when it was issued; its
/// holder is checked like a credential's under that guard, and it only serves the source file it
/// was issued against. A public plugin route serves any track, as its protocol does; it carries
/// whom its transcodes are accounted to, and nothing else.
#[derive(Debug)]
pub(crate) enum TrackAccess {
    Credential {
        holder: CredentialHolder,
        purpose: MediaTokenPurpose,
    },
    MediaToken(MediaTokenAccess),
    PublicPluginRoute(Caller),
}

impl TrackAccess {
    /// Who an HLS session created through this access serves.
    pub(crate) fn session_holder(&self) -> HlsSessionHolder {
        match self {
            Self::Credential { holder, .. } => HlsSessionHolder::Credential(holder.clone()),
            Self::MediaToken(token) => HlsSessionHolder::Credential(token.holder.clone()),
            Self::PublicPluginRoute(_) => HlsSessionHolder::Public,
        }
    }

    /// Whom a transcode for this request is charged to: the credential's or media token's holder,
    /// or for a public plugin route the caller its handler resolved or else the client.
    pub(crate) fn caller(&self) -> Caller {
        match self {
            Self::Credential { holder, .. } => holder.caller(),
            Self::MediaToken(token) => token.holder.caller(),
            Self::PublicPluginRoute(caller) => caller.clone(),
        }
    }
}

/// The holder's principal as it stands now, read under the caller's guard. Refused once the user
/// is gone or the credential no longer authenticates them.
pub(super) fn require_live_principal(
    db: &impl lyra_db::DbAccess,
    holder: &CredentialHolder,
) -> Result<Principal, DeliveryError> {
    holder
        .principal(db)?
        .ok_or_else(|| DeliveryError::unauthorized("the credential is no longer valid"))
}

/// Checks, for each HLS segment, that the session's holder may still be served the track: their
/// credential still authenticates them, and they can still see the track.
pub(crate) async fn require_session_access(
    holder: &HlsSessionHolder,
    track_public_id: &str,
) -> Result<(), DeliveryError> {
    let HlsSessionHolder::Credential(holder) = holder else {
        return Ok(());
    };
    let db = &*STATE.db.read().await;
    let not_found = || DeliveryError::not_found("HLS session not found");
    let principal = require_live_principal(db, holder)?;
    let track_db_id =
        lyra_db::lookup::find_node_id_by_id(db, track_public_id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        db,
        &principal,
        track_db_id,
        not_found,
    )?;
    Ok(())
}

/// Plugin responses skip the native route guards, so track serving must
/// re-apply the same permission and library-access enforcement here.
pub(crate) async fn require_plugin_track_access(
    auth: Option<&ResolvedAuth>,
    track_id: &str,
    purpose: MediaTokenPurpose,
) -> Result<(String, TrackAccess), DeliveryError> {
    let Some(auth) = auth else {
        return Err(DeliveryError::unauthorized("authentication required"));
    };
    let principal = &auth.principal;
    let db = STATE.db.read().await;
    principal.require(&*db)?;
    authorize_track_purpose(principal, purpose)?;
    let not_found = || DeliveryError::not_found(format!("Track not found: {track_id}"));
    let track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, track_id)?.ok_or_else(not_found)?;
    crate::services::auth::access::require_entity_accessible(
        &*db,
        principal,
        track_db_id,
        not_found,
    )?;
    Ok((
        track_id.to_string(),
        TrackAccess::Credential {
            holder: auth.holder(),
            purpose,
        },
    ))
}

/// A plugin route its protocol makes public streams any track, without the caller's access check.
/// Its transcodes still count against someone: the caller, when the handler resolved one, or
/// else the client's address.
async fn public_plugin_track_access(
    auth: Option<&ResolvedAuth>,
    client: ClientAddress,
    track_id: &str,
) -> Result<(String, TrackAccess), DeliveryError> {
    let db = STATE.db.read().await;
    if lyra_db::lookup::find_node_id_by_id(&*db, track_id)?.is_none() {
        return Err(DeliveryError::not_found(format!(
            "Track not found: {track_id}"
        )));
    }
    let caller = match auth {
        Some(auth) => Caller::User(auth.principal.user_public_id.clone()),
        None => Caller::Client(client),
    };
    Ok((track_id.to_string(), TrackAccess::PublicPluginRoute(caller)))
}

/// The track a `stream_track` response for `track_id` serves and the access it is served
/// under: the caller's, or none when the response is `public`, which still counts its
/// transcodes against the caller or the client's address.
pub(crate) async fn stream_track_access(
    auth: Option<&ResolvedAuth>,
    client: ClientAddress,
    track_id: &str,
    public: bool,
) -> Result<(String, TrackAccess), DeliveryError> {
    if public {
        public_plugin_track_access(auth, client, track_id).await
    } else {
        require_plugin_track_access(auth, track_id, MediaTokenPurpose::Stream).await
    }
}
