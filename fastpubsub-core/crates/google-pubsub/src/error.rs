//! The conversion of the Google client errors to adapter errors.

use std::fmt::Display;

use fastpubsub_messaging::{Error, ErrorKind};
use google_cloud_gax::error::rpc::Code;
use google_cloud_pubsub::error::PublishError;

/// Convert a Pub/Sub RPC error to an adapter error.
///
/// The gRPC status code selects the kind: `NOT_FOUND` gives
/// [`ErrorKind::NotFound`], `ALREADY_EXISTS` gives
/// [`ErrorKind::AlreadyExists`], and each other code gives
/// [`ErrorKind::Other`].
pub(crate) fn from_rpc(error: &google_cloud_pubsub::Error) -> Error {
    let kind = match error.status().map(|status| status.code) {
        Some(Code::NotFound) => ErrorKind::NotFound,
        Some(Code::AlreadyExists) => ErrorKind::AlreadyExists,
        _ => ErrorKind::Other,
    };
    Error::new(kind, error.to_string())
}

/// Convert a publish error to an adapter error.
///
/// An RPC error goes through [`from_rpc`]. Each other publish error, for
/// example a paused ordering key, gives [`ErrorKind::Other`].
pub(crate) fn from_publish(error: &PublishError) -> Error {
    match error {
        PublishError::Rpc(inner) => from_rpc(inner),
        other => Error::other(other.to_string()),
    }
}

/// Convert an error from a client builder to an adapter error.
///
/// A builder fails when it cannot get the credentials or cannot make the
/// transport, so the kind is always [`ErrorKind::Other`].
pub(crate) fn from_build(error: &impl Display) -> Error {
    Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use fastpubsub_messaging::ErrorKind;
    use google_cloud_gax::error::rpc::{Code, Status};

    use super::from_rpc;

    fn service_error(code: Code) -> google_cloud_pubsub::Error {
        google_cloud_pubsub::Error::service(Status::default().set_code(code).set_message("text"))
    }

    #[test]
    fn maps_the_status_codes_to_the_error_kinds() {
        assert_eq!(
            from_rpc(&service_error(Code::NotFound)).kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            from_rpc(&service_error(Code::AlreadyExists)).kind(),
            ErrorKind::AlreadyExists
        );
        assert_eq!(
            from_rpc(&service_error(Code::PermissionDenied)).kind(),
            ErrorKind::Other
        );
    }
}
