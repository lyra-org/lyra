// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        LazyLock,
        Mutex,
    },
    time::{
        Duration,
        Instant,
    },
};

use super::{
    EditPlan,
    MetadataEditingError,
};

const PREVIEW_LIFETIME: Duration = Duration::from_secs(5 * 60);
const MAX_PREVIEWS: usize = 256;
const MAX_PREVIEWS_PER_USER: usize = 16;
const MAX_PREVIEW_BYTES: usize = 2 * 1024 * 1024;
const MAX_PREVIEW_BYTES_PER_USER: usize = 8 * 1024 * 1024;
const MAX_PREVIEW_BYTES_GLOBAL: usize = 64 * 1024 * 1024;

struct StoredPreview {
    user_id: String,
    entity_id: String,
    plan: EditPlan,
    estimated_bytes: usize,
    expires_at: Instant,
}

static PREVIEWS: LazyLock<Mutex<HashMap<String, StoredPreview>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn invalid_preview() -> MetadataEditingError {
    MetadataEditingError::BadRequest(
        "preview_id is invalid, expired, already applied, or belongs to another request"
            .to_string(),
    )
}

fn estimated_bytes(plan: &EditPlan) -> usize {
    plan.fields.values().fold(0, |total, field| {
        total
            .saturating_add(field.before.to_string().len())
            .saturating_add(field.after.to_string().len())
            .saturating_add(128)
    })
}

pub(super) fn issue(
    user_id: &str,
    entity_id: &str,
    plan: EditPlan,
) -> Result<String, MetadataEditingError> {
    let estimated_bytes = estimated_bytes(&plan);
    if estimated_bytes > MAX_PREVIEW_BYTES {
        return Err(MetadataEditingError::BadRequest(
            "metadata preview is too large".to_string(),
        ));
    }
    let now = Instant::now();
    let mut previews = PREVIEWS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    previews.retain(|_, preview| preview.expires_at > now);
    loop {
        let user_count = previews
            .values()
            .filter(|preview| preview.user_id == user_id)
            .count();
        let user_bytes = previews
            .values()
            .filter(|preview| preview.user_id == user_id)
            .map(|preview| preview.estimated_bytes)
            .sum::<usize>();
        let global_bytes = previews
            .values()
            .map(|preview| preview.estimated_bytes)
            .sum::<usize>();
        let over_user_limit = user_count >= MAX_PREVIEWS_PER_USER
            || user_bytes.saturating_add(estimated_bytes) > MAX_PREVIEW_BYTES_PER_USER;
        let over_global_limit = previews.len() >= MAX_PREVIEWS
            || global_bytes.saturating_add(estimated_bytes) > MAX_PREVIEW_BYTES_GLOBAL;
        if !over_user_limit && !over_global_limit {
            break;
        }
        let Some(oldest_id) = previews
            .iter()
            .filter(|(_, preview)| !over_user_limit || preview.user_id == user_id)
            .min_by_key(|(_, preview)| preview.expires_at)
            .map(|(id, _)| id.clone())
        else {
            break;
        };
        previews.remove(&oldest_id);
    }

    let preview_id = loop {
        let candidate = nanoid::nanoid!();
        if !previews.contains_key(&candidate) {
            break candidate;
        }
    };
    previews.insert(
        preview_id.clone(),
        StoredPreview {
            user_id: user_id.to_string(),
            entity_id: entity_id.to_string(),
            plan,
            estimated_bytes,
            expires_at: now + PREVIEW_LIFETIME,
        },
    );
    Ok(preview_id)
}

pub(super) fn take(
    preview_id: &str,
    user_id: &str,
    entity_id: &str,
) -> Result<EditPlan, MetadataEditingError> {
    if preview_id.is_empty() {
        return Err(invalid_preview());
    }
    let now = Instant::now();
    let mut previews = PREVIEWS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    previews.retain(|_, preview| preview.expires_at > now);
    let Some(preview) = previews.get(preview_id) else {
        return Err(invalid_preview());
    };
    if preview.user_id != user_id || preview.entity_id != entity_id {
        return Err(invalid_preview());
    }
    Ok(previews
        .remove(preview_id)
        .expect("validated preview remains in the registry")
        .plan)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::Value;

    use super::*;
    use crate::services::metadata::editing::{
        PlannedField,
        model::{
            MetadataEntityType,
            MetadataField,
            MetadataValueSource,
        },
    };

    fn plan(before: Value) -> EditPlan {
        EditPlan {
            entity_type: MetadataEntityType::Track,
            fields: BTreeMap::from([(
                MetadataField::Title,
                PlannedField {
                    before,
                    source_before: MetadataValueSource::Resolved,
                    after: Value::String("after".to_string()),
                    source_after: MetadataValueSource::Manual,
                },
            )]),
        }
    }

    #[test]
    fn rejects_an_oversized_preview() {
        let error = issue(
            "oversized-preview-user",
            "oversized-preview-entity",
            plan(Value::String("x".repeat(MAX_PREVIEW_BYTES))),
        )
        .expect_err("oversized preview must be rejected");
        assert!(matches!(error, MetadataEditingError::BadRequest(_)));
    }

    #[test]
    fn evicts_the_oldest_preview_at_the_per_user_limit() {
        let user = "per-user-preview-limit-user";
        let entity = "per-user-preview-limit-entity";
        let mut tokens = Vec::new();
        for index in 0..=MAX_PREVIEWS_PER_USER {
            tokens.push(issue(user, entity, plan(Value::from(index))).expect("preview issued"));
        }
        assert!(take(&tokens[0], user, entity).is_err());
        for token in &tokens[1..] {
            take(token, user, entity).expect("newer preview remains available");
        }
    }
}
