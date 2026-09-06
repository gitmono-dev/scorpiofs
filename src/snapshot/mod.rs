//! Immutable source identities and reads. This module does not follow live refs
//! or claim a server-published namespace until the backend advertises it.

pub mod backend;
pub mod http;
pub mod identity;
pub mod namespace;
