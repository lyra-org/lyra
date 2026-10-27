// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Whom a request's use is charged to: the user when the request authenticates, otherwise its
//! [`ClientAddress`]. Where a limiter guards a pool the whole server shares, anonymous callers are
//! also charged together to one [`Account::Anonymous`], so authenticated users keep their share
//! however many addresses anonymous clients rotate through.

mod address;

pub(crate) use address::ClientAddress;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Caller {
    /// An authenticated user, by public id.
    User(String),
    Client(ClientAddress),
}

/// What a limiter counts a caller's use against.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Account {
    Caller(Caller),
    /// Every anonymous caller together.
    Anonymous,
}

impl Caller {
    pub(crate) fn is_anonymous(&self) -> bool {
        matches!(self, Self::Client(_))
    }

    /// The accounts one use by this caller is charged to: its own, and for an anonymous caller
    /// also the one all anonymous callers share.
    pub(crate) fn accounts(&self) -> impl Iterator<Item = Account> {
        let shared = self.is_anonymous().then_some(Account::Anonymous);
        std::iter::once(Account::Caller(self.clone())).chain(shared)
    }
}
