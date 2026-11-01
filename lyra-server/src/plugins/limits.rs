// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The limits a module states, exported as `MAX_<WHAT>` fields so plugins check against the
//! value the host enforces instead of copying it.

use harmony_luau as luau;

pub(crate) struct Limit {
    pub(crate) name: &'static str,
    pub(crate) value: usize,
    #[cfg_attr(not(feature = "docgen"), allow(dead_code))]
    pub(crate) description: &'static str,
}

pub(crate) fn install(root: &luau::Table, limits: &[Limit]) -> luau::runtime::Result<()> {
    for limit in limits {
        root.set(limit.name, luau::Value::Number(limit.value as f64))?;
    }
    Ok(())
}

#[cfg(feature = "docgen")]
pub(crate) fn describe(descriptor: &mut harmony_luau::ModuleDescriptor, limits: &[Limit]) {
    use harmony_luau::LuauTypeInfo;

    for limit in limits {
        descriptor.fields.push(harmony_luau::ModuleFieldDescriptor {
            path: vec![limit.name],
            description: Some(limit.description),
            ty: u64::luau_type(),
        });
    }
}

/// `install_limits` and `describe_limits` for a module's `extend` and `describe`, exporting
/// `limits`.
macro_rules! module_limits {
    ($limits:expr) => {
        fn install_limits(spec: harmony_core::ModuleSpec) -> harmony_core::ModuleSpec {
            spec.initializer(|_vm, _origin, root| crate::plugins::limits::install(root, $limits))
        }

        #[cfg(feature = "docgen")]
        fn describe_limits(descriptor: &mut harmony_luau::ModuleDescriptor) {
            crate::plugins::limits::describe(descriptor, $limits)
        }
    };
}

pub(crate) use module_limits;
