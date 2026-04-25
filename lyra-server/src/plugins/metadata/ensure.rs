// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau::DescribeInterface;

#[harmony_macros::interface]
pub(super) struct EnsureArtistRequest {
    id_type: String,
    id_value: String,
    artist_name: Option<String>,
    sort_name: Option<String>,
    artist_type: Option<String>,
    description: Option<String>,
}

pub(super) fn interface_descriptors() -> Vec<harmony_luau::InterfaceDescriptor> {
    vec![EnsureArtistRequest::interface_descriptor()]
}
