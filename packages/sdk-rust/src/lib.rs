// The crate's documentation is its README: every code block in it is a
// doctest, so the examples cannot rot.
#![doc = include_str!("../README.md")]

mod admin;
mod claim_check;
mod claim_ref;
mod client;
mod message;
mod routes;
mod signature;
mod transport;
mod webhook;

// The wire types: a `Transport` implementation and a `parse_headers` caller
// build `http`/`bytes` values, and they must be the versions this crate uses.
pub use bytes;
pub use http;

pub use crate::admin::{
    AdminClient, AdminError, AdminHealth, DlqEntry, DlqPage, ListDeadLettersParams,
    ListQuarantinedParams, QuarantineEntry, QuarantinePage, Replay, ReplayList, ReplayPatch,
    ReplaySpec,
};
pub use crate::claim_check::{ClaimCheckClient, ClaimCheckError, ClaimCheckHealth};
pub use crate::claim_ref::{InvalidClaimRefError, ParsedClaimRef, parse_claim_ref};
pub use crate::client::{ClientBuilder, ConfigError, UnavailableReason};
pub use crate::message::{InvalidMessageError, Message, decode_message};
pub use crate::routes::{
    Access, DryRunIpRule, DryRunReason, DryRunRequest, DryRunResult, IpRule, IpRuleScope, IpRules,
    ListRoutesParams, Route, RouteInput, RoutePage, RoutePatch, RoutesClient, RoutesError,
    RoutesHealth, RoutesRejection,
};
pub use crate::signature::{
    DEFAULT_TOLERANCE_SECONDS, InvalidSignatureError, VerifiedSignature, VerifyOptions,
    verify_signature,
};
pub use crate::transport::{ReqwestTransport, Transport, TransportError};
pub use crate::webhook::{HookHeaders, MissingHookIdError, parse_headers};

#[cfg(test)]
mod tests {
    use super::{
        AdminClient, ClaimCheckClient, ClientBuilder, ListDeadLettersParams, ListRoutesParams,
        RoutesClient,
    };
    use crate::client::test_support::Recording;

    /// Every client's futures must be `Send`, or a worker cannot `tokio::spawn`
    /// them.
    #[test]
    fn client_futures_are_send() {
        fn assert_send<F: Send>(future: F) {
            // Compile-time property: the future is dropped, never polled.
            drop(future);
        }

        let claim: ClaimCheckClient<Recording> =
            ClientBuilder::with_transport("http://gateway.invalid", Recording::new(200, "{}"))
                .claim_check()
                .expect("valid client");
        assert_send(claim.redeem(
            "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002",
            &"0".repeat(64),
        ));

        let routes: RoutesClient<Recording> =
            ClientBuilder::with_transport("http://gateway.invalid", Recording::new(200, "{}"))
                .routes()
                .expect("valid client");
        assert_send(routes.list_routes(&ListRoutesParams::default()));

        let admin: AdminClient<Recording> =
            ClientBuilder::with_transport("http://gateway.invalid", Recording::new(200, "{}"))
                .admin()
                .expect("valid client");
        assert_send(admin.list_dead_letters(&ListDeadLettersParams::default()));
    }
}
