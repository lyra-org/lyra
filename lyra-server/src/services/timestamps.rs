// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! RFC 3339 rendering of stored Unix timestamps.

use time::{
    OffsetDateTime,
    format_description::well_known::Rfc3339,
};

pub(crate) fn unix_secs_to_rfc3339_u64(seconds: u64) -> String {
    let seconds = i64::try_from(seconds).expect("Unix timestamp seconds should fit in i64");
    unix_secs_to_rfc3339_i64(seconds)
}

pub(crate) fn unix_secs_to_rfc3339_i64(seconds: i64) -> String {
    OffsetDateTime::from_unix_timestamp(seconds)
        .expect("Unix timestamp seconds should be RFC3339-representable")
        .format(&Rfc3339)
        .expect("RFC3339 formatting should succeed")
}

pub(crate) fn unix_ms_to_rfc3339_u64(milliseconds: u64) -> String {
    unix_ms_to_rfc3339_i128(i128::from(milliseconds))
}

pub(crate) fn unix_ms_to_rfc3339_i64(milliseconds: i64) -> String {
    unix_ms_to_rfc3339_i128(i128::from(milliseconds))
}

fn unix_ms_to_rfc3339_i128(milliseconds: i128) -> String {
    let nanoseconds = milliseconds
        .checked_mul(1_000_000)
        .expect("Unix timestamp milliseconds should fit in nanoseconds");
    OffsetDateTime::from_unix_timestamp_nanos(nanoseconds)
        .expect("Unix timestamp milliseconds should be RFC3339-representable")
        .format(&Rfc3339)
        .expect("RFC3339 formatting should succeed")
}
