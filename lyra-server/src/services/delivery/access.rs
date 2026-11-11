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
        access::TrackPurpose,
        caller::{
            Caller,
            ClientAddress,
        },
        media_tokens::MediaTokenAccess,
        require_permission,
    },
    hls::state::HlsSessionHolder,
};

/// Checks that `principal` may be served a track for `purpose`.
pub(crate) fn authorize_track_purpose(
    principal: &Principal,
    purpose: TrackPurpose,
) -> Result<(), DeliveryError> {
    match purpose {
        TrackPurpose::Stream | TrackPurpose::HlsPlaylist => Ok(()),
        TrackPurpose::Download => {
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
        purpose: TrackPurpose,
    },
    MediaToken(MediaTokenAccess),
    PublicPluginRoute(Caller),
}

impl TrackAccess {
    /// Whose credential the access is checked against and for what; none for a public plugin
    /// route.
    pub(super) fn held(&self) -> Option<(&CredentialHolder, TrackPurpose)> {
        match self {
            Self::Credential { holder, purpose } => Some((holder, *purpose)),
            Self::MediaToken(token) => Some((&token.holder, token.purpose)),
            Self::PublicPluginRoute(_) => None,
        }
    }

    /// Who an HLS session created through this access serves.
    pub(crate) fn session_holder(&self) -> HlsSessionHolder {
        self.held().map_or(HlsSessionHolder::Public, |(holder, _)| {
            HlsSessionHolder::Credential(holder.clone())
        })
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

/// The access a plugin response serves a track under: its caller's credential, checked where the
/// track's source is resolved, since plugin responses skip the native route guards.
pub(crate) fn require_plugin_track_access(
    auth: Option<&ResolvedAuth>,
    purpose: TrackPurpose,
) -> Result<TrackAccess, DeliveryError> {
    let auth = auth.ok_or_else(|| DeliveryError::unauthorized("authentication required"))?;
    Ok(TrackAccess::Credential {
        holder: auth.holder(),
        purpose,
    })
}

/// The access a `stream_track` response serves under: the caller's, or none when the response is
/// `public`, which serves any track as its protocol does and still counts its transcodes against
/// the caller or else the client's address.
pub(crate) fn stream_track_access(
    auth: Option<&ResolvedAuth>,
    client: ClientAddress,
    public: bool,
) -> Result<TrackAccess, DeliveryError> {
    if !public {
        return require_plugin_track_access(auth, TrackPurpose::Stream);
    }
    let caller = match auth {
        Some(auth) => auth.holder().caller(),
        None => Caller::Client(client),
    };
    Ok(TrackAccess::PublicPluginRoute(caller))
}
