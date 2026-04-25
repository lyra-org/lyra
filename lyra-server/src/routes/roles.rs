// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::ops::{
    Deref,
    DerefMut,
};

use aide::axum::{
    ApiRouter,
    routing::{
        get_with,
        patch_with,
    },
};
use aide::transform::TransformOperation;
use axum::Json;
use axum::extract::Path;
use axum::http::{
    HeaderMap,
    StatusCode,
};
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        Permission,
        roles::Role,
    },
    routes::AppError,
    services::auth::{
        Principal,
        require_manage_roles,
    },
};

#[derive(Debug, Serialize, JsonSchema)]
struct RoleResponse {
    id: String,
    name: String,
    permissions: Vec<Permission>,
    builtin: bool,
}

#[derive(Deserialize, JsonSchema)]
struct RoleRequest {
    #[schemars(description = "Role name. ASCII, no spaces.")]
    name: String,
    #[schemars(description = "Permissions granted to the role.")]
    permissions: Vec<Permission>,
}

#[derive(Deserialize, JsonSchema)]
struct UpdateRoleDefinitionRequest {
    #[schemars(description = "Updated role name. ASCII, no spaces.")]
    name: Option<String>,
    #[schemars(description = "Updated permission set for the role.")]
    permissions: Option<Vec<Permission>>,
}

fn validate_role_name(name: &str) -> Result<String, AppError> {
    let normalized = name.trim().to_lowercase();
    if normalized.is_empty() {
        return Err(AppError::bad_request("role name cannot be empty"));
    }
    if !normalized.is_ascii() {
        return Err(AppError::bad_request("role name must be ASCII"));
    }
    if normalized.contains(' ') {
        return Err(AppError::bad_request("role name cannot contain any spaces"));
    }
    if db::roles::is_builtin_role_name(&normalized) {
        return Err(AppError::bad_request(format!(
            "role name '{}' is reserved",
            normalized
        )));
    }
    Ok(normalized)
}

fn normalize_permissions(permissions: Vec<Permission>) -> Vec<Permission> {
    let mut normalized = Vec::new();
    for permission in permissions {
        if !normalized.contains(&permission) {
            normalized.push(permission);
        }
    }
    normalized
}

fn principal_has_admin(principal: &Principal) -> bool {
    db::roles::has_permission(&principal.permissions, Permission::Admin)
}

fn role_to_response(role: Role) -> RoleResponse {
    RoleResponse {
        id: role.id,
        name: role.name.clone(),
        permissions: role.permissions,
        builtin: db::roles::is_builtin_role_name(&role.name),
    }
}

fn resolve_role_by_public_id(
    db: &agdb::DbAny,
    role_id: &str,
) -> Result<(agdb::DbId, Role), AppError> {
    let role_db_id = db::lookup::find_node_id_by_id(db, role_id)?
        .ok_or_else(|| AppError::not_found(format!("role not found: {role_id}")))?;
    let role = db::roles::get_by_id(db, role_db_id)?
        .ok_or_else(|| AppError::not_found(format!("role not found: {role_id}")))?;
    Ok((role_db_id, role))
}

fn forbid_builtin_role_mutation(role: &Role, action: &str) -> Result<(), AppError> {
    if db::roles::is_builtin_role_name(&role.name) {
        return Err(AppError::forbidden(format!(
            "builtin role '{}' cannot be {}",
            role.name, action
        )));
    }
    Ok(())
}

fn require_admin_for_admin_role(
    principal: &Principal,
    permissions: &[Permission],
    action: &str,
) -> Result<(), AppError> {
    if permissions.contains(&Permission::Admin) && !principal_has_admin(principal) {
        return Err(AppError::forbidden(format!(
            "Admin permission required to {} an admin role",
            action
        )));
    }
    Ok(())
}

fn ensure_admin_permission_removal_allowed(
    db: &agdb::DbAny,
    principal: &Principal,
    role_db_id: agdb::DbId,
    role: &Role,
    next_permissions: &[Permission],
) -> Result<(), AppError> {
    let current_has_admin = role.permissions.contains(&Permission::Admin);
    let next_has_admin = next_permissions.contains(&Permission::Admin);
    if !current_has_admin || next_has_admin {
        return Ok(());
    }

    if !principal_has_admin(principal) {
        return Err(AppError::forbidden(
            "Admin permission required to remove admin permission from a role",
        ));
    }

    let assigned_users = db::roles::get_users_with_role(db, role_db_id)?;
    let default_username = STATE.config.get().auth.default_username.to_lowercase();
    if assigned_users
        .iter()
        .any(|user| user.username == default_username)
    {
        return Err(AppError::bad_request(
            "cannot remove admin permission from a role assigned to the default user",
        ));
    }
    if assigned_users
        .iter()
        .any(|user| user.db_id == Some(principal.user_db_id))
    {
        return Err(AppError::bad_request(
            "cannot remove admin permission from your own role",
        ));
    }

    let remaining_admins = db::roles::count_admins(db)?.saturating_sub(assigned_users.len());
    if remaining_admins == 0 {
        return Err(AppError::bad_request(
            "cannot remove admin permission from the last admin role",
        ));
    }

    Ok(())
}

async fn list_roles(headers: HeaderMap) -> Result<Json<Vec<RoleResponse>>, AppError> {
    let _principal = require_manage_roles(&headers).await?;

    let db = STATE.db.read().await;
    let roles = db::roles::get(&db)?;
    Ok(Json(roles.into_iter().map(role_to_response).collect()))
}

async fn create_role(
    headers: HeaderMap,
    Json(body): Json<RoleRequest>,
) -> Result<Json<RoleResponse>, AppError> {
    let principal = require_manage_roles(&headers).await?;
    let name = validate_role_name(&body.name)?;
    let permissions = normalize_permissions(body.permissions);
    require_admin_for_admin_role(&principal, &permissions, "create")?;

    let mut db = STATE.db.write().await;
    if db::roles::get_by_name(&db, &name)?.is_some() {
        return Err(AppError::bad_request(format!(
            "role '{}' already exists",
            name
        )));
    }

    let role = Role {
        db_id: None,
        id: nanoid!(),
        name,
        permissions,
    };
    let role_db_id = db::roles::create(db.deref_mut(), &role)?;
    let created = db::roles::get_by_id(db.deref(), role_db_id)?
        .ok_or_else(|| AppError::not_found("created role not found"))?;

    Ok(Json(role_to_response(created)))
}

async fn update_role_definition(
    headers: HeaderMap,
    Path(role_id): Path<String>,
    Json(body): Json<UpdateRoleDefinitionRequest>,
) -> Result<Json<RoleResponse>, AppError> {
    let principal = require_manage_roles(&headers).await?;
    if body.name.is_none() && body.permissions.is_none() {
        return Err(AppError::bad_request("no role fields provided"));
    }

    let mut db = STATE.db.write().await;
    let (role_db_id, current_role) = resolve_role_by_public_id(db.deref(), &role_id)?;
    forbid_builtin_role_mutation(&current_role, "updated")?;

    let next_name = match body.name {
        Some(name) => validate_role_name(&name)?,
        None => current_role.name.clone(),
    };
    let next_permissions = body
        .permissions
        .map(normalize_permissions)
        .unwrap_or_else(|| current_role.permissions.clone());

    require_admin_for_admin_role(&principal, &next_permissions, "update")?;
    ensure_admin_permission_removal_allowed(
        db.deref(),
        &principal,
        role_db_id,
        &current_role,
        &next_permissions,
    )?;

    if let Some(existing) = db::roles::get_by_name(db.deref(), &next_name)?
        && existing.db_id != Some(role_db_id)
    {
        return Err(AppError::bad_request(format!(
            "role '{}' already exists",
            next_name
        )));
    }

    let updated_role = Role {
        db_id: Some(role_db_id),
        id: current_role.id,
        name: next_name,
        permissions: next_permissions,
    };
    db::roles::update(db.deref_mut(), &updated_role)?;

    Ok(Json(role_to_response(updated_role)))
}

async fn delete_role_definition(
    headers: HeaderMap,
    Path(role_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_roles(&headers).await?;

    let mut db = STATE.db.write().await;
    let (role_db_id, role) = resolve_role_by_public_id(db.deref(), &role_id)?;
    forbid_builtin_role_mutation(&role, "deleted")?;
    require_admin_for_admin_role(&principal, &role.permissions, "delete")?;

    let assigned_users = db::roles::count_users_with_role(db.deref(), role_db_id)?;
    if assigned_users > 0 {
        return Err(AppError::conflict(format!(
            "cannot delete role '{}' while it is assigned to users",
            role.name
        )));
    }

    db::roles::delete(db.deref_mut(), role_db_id)?;
    Ok(StatusCode::NO_CONTENT)
}

fn list_roles_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List roles")
        .description("Returns all roles. Requires ManageRoles permission.")
}

fn create_role_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create role").description(
        "Creates a custom role. Built-in role names are reserved. Creating admin-capable roles requires Admin permission.",
    )
}

fn update_role_definition_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update role").description(
        "Updates a custom role's name and/or permissions. Built-in roles are immutable. Editing admin-capable roles requires Admin permission.",
    )
}

fn delete_role_definition_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete role").description(
        "Deletes a custom role when it is not assigned to any users. Built-in roles are immutable. Deleting admin-capable roles requires Admin permission.",
    ).response::<204, ()>()
}

pub fn role_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/",
            get_with(list_roles, list_roles_docs).post_with(create_role, create_role_docs),
        )
        .api_route(
            "/{role_id}",
            patch_with(update_role_definition, update_role_definition_docs)
                .delete_with(delete_role_definition, delete_role_definition_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::User,
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use axum::{
        Json,
        extract::Path,
        http::{
            HeaderMap,
            StatusCode,
            header::AUTHORIZATION,
        },
        response::IntoResponse,
    };
    use std::{
        path::PathBuf,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    async fn initialize_test_runtime() -> anyhow::Result<PathBuf> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-role-routes-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;
        Ok(test_dir)
    }

    async fn create_headers_with_role(
        username: &str,
        role_name: &str,
        role_permissions: Vec<Permission>,
    ) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user = User {
                db_id: None,
                id: nanoid!(),
                username: username.to_string(),
                password: "unused".to_string(),
            };
            let user_db_id = db::users::create(&mut db, &user)?;

            let assigned_role_name = if db::roles::is_builtin_role_name(role_name) {
                role_name.to_string()
            } else {
                let role = Role {
                    db_id: None,
                    id: nanoid!(),
                    name: role_name.to_string(),
                    permissions: role_permissions,
                };
                db::roles::create(&mut db, &role)?;
                role_name.to_string()
            };
            db::roles::ensure_user_has_role(&mut db, user_db_id, &assigned_role_name)?;
            user_db_id
        };

        let session = sessions::create_session_for_user(user_db_id).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    #[tokio::test]
    async fn admin_can_create_update_and_delete_custom_role() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_role("admin-user", "admin", vec![]).await?;

        let Json(created) = create_role(
            headers.clone(),
            Json(RoleRequest {
                name: "editor".to_string(),
                permissions: vec![Permission::ManageMetadata, Permission::ManageRoles],
            }),
        )
        .await
        .expect("admin should be able to create custom role");
        assert_eq!(created.name, "editor");
        assert_eq!(
            created.permissions,
            vec![Permission::ManageMetadata, Permission::ManageRoles]
        );
        assert!(!created.builtin);

        let Json(updated) = update_role_definition(
            headers.clone(),
            Path(created.id.clone()),
            Json(UpdateRoleDefinitionRequest {
                name: Some("editor_plus".to_string()),
                permissions: Some(vec![Permission::ManageMetadata]),
            }),
        )
        .await
        .expect("admin should be able to update custom role");
        assert_eq!(updated.name, "editor_plus");
        assert_eq!(updated.permissions, vec![Permission::ManageMetadata]);

        delete_role_definition(headers, Path(created.id))
            .await
            .expect("admin should be able to delete custom role");

        let db = STATE.db.read().await;
        assert!(db::roles::get_by_name(&db, "editor_plus")?.is_none());

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn manage_roles_without_admin_cannot_create_admin_role() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_role(
            "role-manager",
            "role-manager",
            vec![Permission::ManageRoles],
        )
        .await?;

        let status = create_role(
            headers,
            Json(RoleRequest {
                name: "ops_admin".to_string(),
                permissions: vec![Permission::Admin],
            }),
        )
        .await
        .expect_err("non-admin role manager should not create admin role")
        .into_response()
        .status();
        assert_eq!(status, StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn builtin_roles_are_immutable() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let headers = create_headers_with_role("admin-user", "admin", vec![]).await?;

        let admin_role_id = {
            let db = STATE.db.read().await;
            db::roles::get_by_name(&db, db::roles::BUILTIN_ADMIN_ROLE)?
                .expect("builtin admin role")
                .id
        };

        let update_status = update_role_definition(
            headers.clone(),
            Path(admin_role_id.clone()),
            Json(UpdateRoleDefinitionRequest {
                name: Some("renamed_admin".to_string()),
                permissions: None,
            }),
        )
        .await
        .expect_err("builtin admin role should not be mutable")
        .into_response()
        .status();
        assert_eq!(update_status, StatusCode::FORBIDDEN);

        let delete_status = delete_role_definition(headers, Path(admin_role_id))
            .await
            .expect_err("builtin admin role should not be deletable")
            .into_response()
            .status();
        assert_eq!(delete_status, StatusCode::FORBIDDEN);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }
}
