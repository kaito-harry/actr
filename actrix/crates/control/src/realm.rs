use actrix_proto::{RealmInfo, admin::v1::SecretRotationState};
use platform::realm::Realm;

/// Convert a Realm record into proto RealmInfo
pub fn realm_to_proto(realm: &Realm) -> RealmInfo {
    let secret_rotation_state = if !realm.secret_current.is_empty() {
        let (previous_hash, previous_valid_until) = match &realm.secret_previous {
            Some((hash, valid_until)) => (Some(hash.clone()), Some(*valid_until as i64)),
            None => (None, None),
        };
        Some(SecretRotationState {
            current_hash_preview: realm.secret_current.clone(),
            previous_hash_preview: previous_hash,
            previous_valid_until,
        })
    } else {
        None
    };

    RealmInfo {
        realm_id: realm.id,
        name: realm.name.clone(),
        enabled: realm.enabled,
        created_at: realm.created_at as i64,
        updated_at: realm.updated_at.map(|v| v as i64),
        expires_at: realm.expires_at.unwrap_or(0),
        status: realm.status.to_string(),
        secret_rotation_state,
    }
}
